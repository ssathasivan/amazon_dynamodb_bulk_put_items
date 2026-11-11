"""Lambda handlers for the three load workflows.

  start   Bulk put items, Lambda 1: starts the operation. Its role is the bulk put items CALLER,
          so DynamoDB reads the input and writes the reports with this role.
  check   Bulk put items, Lambda 2: called every 30 seconds. Returns the status, and when the
          operation has finished, reads the reports and writes a summary to the output bucket.
  batch   BatchWriteItem comparison: lists the input files, writes one file per invocation,
          and summarizes the run.
"""
from __future__ import annotations

import gzip
import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from . import reconcile, runner
from .pricing import ON_DEMAND_PER_MILLION_WRU

OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "")
OUTPUT_PREFIX = "bulk-put-output/"

METRICS_NAMESPACE = "BulkPutDemo"

s3 = boto3.client("s3")
ddb = boto3.client("dynamodb")
cloudwatch = boto3.client("cloudwatch")
ddb_batch = boto3.client(
    "dynamodb", config=Config(retries={"mode": "adaptive", "max_attempts": 10}, max_pool_connections=32)
)


def _put_summary(name: str, summary: dict) -> str:
    key = f"{OUTPUT_PREFIX}summaries/{name}.json"
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=key, Body=json.dumps(summary, indent=2, default=str).encode())
    return f"s3://{OUTPUT_BUCKET}/{key}"


# ------------------------------------------------------------- bulk put items: start
def start(event, context):
    arn = runner.start(
        ddb,
        table=event["table"],
        input_bucket=event["bucket"],
        input_prefix=event["prefix"],  # batches/<id>/data/
        output_bucket=OUTPUT_BUCKET,
        output_prefix=OUTPUT_PREFIX,
        tier=event["tier"],
        client_token=event["token"],  # <workflow>-<execution name>: a retried start can't start twice
    )
    return {"arn": arn}


# ------------------------------------------------------------- bulk put items: check
def check(event, context):
    desc = runner.describe(ddb, event["arn"])
    status = desc["BulkOperationStatus"]
    out = {
        "status": status,
        "done": status in runner.TERMINAL,
        "records_read": desc.get("ProcessedItemCount", 0),
        "processed_bytes": desc.get("ProcessedInputBytes", 0),
        "total_bytes": desc.get("TotalInputBytes"),  # absent until input validation finishes
    }
    # Bulk writes don't appear in the table's CloudWatch metrics, so publish progress ourselves
    # for the dashboard. ProcessedItemCount is a running total that refreshes in steps.
    cloudwatch.put_metric_data(
        Namespace=METRICS_NAMESPACE,
        MetricData=[
            {
                "MetricName": "ItemsProcessed",
                "Dimensions": [{"Name": "Table", "Value": event["table"]}],
                "Value": out["records_read"],
                "Unit": "Count",
            }
        ],
    )
    if out["done"]:
        op = runner.summary(desc)
        rec = reconcile.reconcile(s3, OUTPUT_BUCKET, OUTPUT_PREFIX, op["arn"])
        ok = (
            op["status"] in ("COMPLETED", "COMPLETED_WITH_ERRORS")
            and rec["manifest_found"]
            and not rec["not_fully_processed"]
        )
        uri = _put_summary(op["arn"].rsplit("/", 1)[-1], {"ok": ok, "table": event["table"], "operation": op, "reconciliation": rec})
        out["result"] = {
            "ok": ok,
            "method": f"Bulk put items, {op['tier'].title()}",
            "table": event["table"],
            "status": op["status"],
            "failure_code": op["failure_code"],
            "items": op["records_read"],
            "failed_items": op["failed_items"],
            "condition_failures": rec["condition_failure_records"],
            "files_fully_processed": f"{rec['fully_processed']} of {rec['objects']}",
            "load_seconds": op["elapsed_seconds"],
            "metered_bytes": op["metered_bytes"],
            "cost_usd": op["approx_cost_usd"],
            "summary_uri": uri,
        }
    return out


# ------------------------------------------------------------- BatchWriteItem baseline
def batch(event, context):
    action = event["action"]
    if action == "list":
        return _list_files(event)
    if action == "write":
        return _write_file(event)
    if action == "summarize":
        return _summarize(event)
    raise ValueError(f"unknown action {action!r}")


def _list_files(event) -> dict:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=event["bucket"], Prefix=event["prefix"]):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json.gz")]
    return {"files": [{"bucket": event["bucket"], "key": k, "table": event["table"]} for k in sorted(keys)]}


def _write_batch(table: str, items: list[dict]) -> float:
    """Write up to 25 items. Retry UnprocessedItems and throttling with backoff and jitter."""
    request = {table: [{"PutRequest": {"Item": i}} for i in items]}
    consumed = 0.0
    attempt = 0
    while request:
        try:
            resp = ddb_batch.batch_write_item(RequestItems=request, ReturnConsumedCapacity="TOTAL")
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("ProvisionedThroughputExceededException", "ThrottlingException"):
                raise
            resp = {"UnprocessedItems": request}
        consumed += sum(c["CapacityUnits"] for c in resp.get("ConsumedCapacity", []))
        request = resp.get("UnprocessedItems") or {}
        if request:
            attempt += 1
            time.sleep(min(10.0, 0.05 * 2**attempt) * random.random())
        else:
            attempt = 0
    return consumed


def _write_file(event) -> dict:
    body = s3.get_object(Bucket=event["bucket"], Key=event["key"])["Body"]
    items = []
    with gzip.GzipFile(fileobj=body) as gz:
        for line in gz:
            if line.strip():
                items.append(json.loads(line)["Item"])  # BatchWriteItem can't apply the record's condition
    t0 = time.time()
    batches = [items[i : i + 25] for i in range(0, len(items), 25)]
    with ThreadPoolExecutor(max_workers=16) as pool:
        wru = sum(pool.map(lambda b: _write_batch(event["table"], b), batches))
    return {"items": len(items), "wru": wru, "seconds": round(time.time() - t0, 1)}


def _summarize(event) -> dict:
    results = event["results"]
    items = sum(r["items"] for r in results)
    wru = sum(r["wru"] for r in results)
    started = datetime.fromisoformat(event["started"].replace("Z", "+00:00"))
    load_seconds = round((datetime.now(timezone.utc) - started).total_seconds())
    cost = round(wru / 1_000_000 * ON_DEMAND_PER_MILLION_WRU, 4)
    uri = _put_summary(
        f"batchwrite-{event['execution']}",
        {"table": event["table"], "files": len(results), "items": items, "wru": wru, "load_seconds": load_seconds, "cost_usd": cost},
    )
    return {
        "ok": len(results) > 0,
        "method": "BatchWriteItem",
        "table": event["table"],
        "items": items,
        "files": len(results),
        "write_request_units": wru,
        "load_seconds": load_seconds,
        "cost_usd": cost,
        "summary_uri": uri,
    }
