"""Start and describe a bulk put items operation."""
from __future__ import annotations

import uuid

from .pricing import BULK_ACCELERATED_PER_GB, BULK_STANDARD_PER_GB, GIB

TERMINAL = {"COMPLETED", "COMPLETED_WITH_ERRORS", "CANCELLED", "FAILED"}


def start(
    ddb,
    *,
    table: str,
    input_bucket: str,
    input_prefix: str,
    output_bucket: str,
    output_prefix: str,
    tier: str = "STANDARD",
    max_write_request_units: int | None = None,
    client_token: str | None = None,
) -> str:
    """Start the operation and return its ARN.

    The ClientToken makes a retried start idempotent (DynamoDB remembers it for 8 hours): a
    retry with the SAME token returns the existing operation instead of launching a second one.
    """
    kwargs = dict(
        TableName=table,
        InputS3Configuration={
            "S3Bucket": input_bucket,
            "S3KeyPrefix": input_prefix,
            "InputFormat": "DYNAMODB_JSON",
            "InputCompressionType": "GZIP",
        },
        OutputS3Configuration={
            "S3Bucket": output_bucket,
            "S3KeyPrefix": output_prefix,
            # Without CONDITION_CHECK_FAILURE DynamoDB keeps no record or count of skipped items.
            "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"],
        },
        PerformanceTier=tier,
        ClientToken=client_token or str(uuid.uuid4()),
    )
    if max_write_request_units:
        kwargs["MaxWriteRequestUnits"] = max_write_request_units
    resp = ddb.start_bulk_put_items(**kwargs)
    return resp["BulkOperationDescription"]["BulkOperationArn"]


def describe(ddb, arn: str) -> dict:
    return ddb.describe_bulk_operation(BulkOperationArn=arn)["BulkOperationDescription"]


def billed_bytes(desc: dict) -> int:
    """BulkOperationUsage maps to the bill. Vector bytes are metered separately; ignored here."""
    return sum(u.get("BulkPutBytes", 0) for u in desc.get("BulkOperationUsage", []))


def summary(desc: dict) -> dict:
    """JSON-safe summary of a DescribeBulkOperation response."""
    tier = desc.get("PerformanceTier", "STANDARD")
    rate = BULK_ACCELERATED_PER_GB if tier == "ACCELERATED" else BULK_STANDARD_PER_GB
    metered = billed_bytes(desc)
    elapsed = None
    if desc.get("StartTime") and desc.get("EndTime"):
        elapsed = round((desc["EndTime"] - desc["StartTime"]).total_seconds())
    return {
        "arn": desc["BulkOperationArn"],
        "status": desc["BulkOperationStatus"],
        "failure_code": desc.get("FailureCode"),
        "failure_message": desc.get("FailureMessage"),
        "tier": tier,
        "records_read": desc.get("ProcessedItemCount", 0),
        "failed_items": desc.get("FailedItemCount", 0),  # condition-check failures are NOT counted here
        "metered_bytes": metered,
        "approx_cost_usd": round(metered / GIB * rate, 4),
        "elapsed_seconds": elapsed,
    }
