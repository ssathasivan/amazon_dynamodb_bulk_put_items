"""Reconcile an operation: API counts + manifest + S3 report files (the documented workflow)."""
from __future__ import annotations

import gzip
import json
from collections import Counter


def _op_id(arn: str) -> str:
    return arn.rsplit("/", 1)[-1]


def _account(arn: str) -> str:
    return arn.split(":")[4]


def report_prefix(output_prefix: str, arn: str) -> str:
    base = f"{output_prefix.strip('/')}/" if output_prefix.strip("/") else ""
    return f"{base}AWSDynamoDB/BulkOperation/{_account(arn)}:{_op_id(arn)}"


def _list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def _shorten(value, limit: int = 40):
    """Trim long strings (the demo payload) so samples stay readable."""
    if isinstance(value, dict):
        return {k: _shorten(v, limit) for k, v in value.items()}
    if isinstance(value, list):
        return [_shorten(v, limit) for v in value]
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "..."
    return value


def reconcile(s3, bucket: str, output_prefix: str, arn: str, samples: int = 2) -> dict:
    """Read the manifest and stream the gzip reports.

    The manifest sits under <operation-id>/, but reports sit in one folder per internal job
    (<operation-id>-0/, <operation-id>-1/, ...). Listing by the prefix WITHOUT a trailing slash
    finds both. Report files are streamed and counted, not loaded, because condition-check
    records carry old and new images and can be larger than the input.
    """
    prefix = report_prefix(output_prefix, arn)
    manifest: list[dict] = []
    counts = {"errors": 0, "condition_failures": 0}
    sample: dict[str, list] = {"errors": [], "condition_failures": []}

    for key in _list_keys(s3, bucket, prefix):
        if key.endswith("manifest-files.json"):
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
            manifest = [json.loads(line) for line in body.splitlines() if line.strip()]
            continue
        if not key.endswith(".json.gz"):
            continue
        kind = "errors" if "/errors/" in key else "condition_failures" if "/condition-check-failures/" in key else None
        if kind is None:
            continue
        with gzip.GzipFile(fileobj=s3.get_object(Bucket=bucket, Key=key)["Body"]) as gz:
            for line in gz:
                if not line.strip():
                    continue
                counts[kind] += 1
                if len(sample[kind]) < samples:
                    sample[kind].append(_shorten(json.loads(line)))

    statuses = Counter(m["fileProcessingStatus"] for m in manifest)
    return {
        "prefix": prefix,
        "manifest_found": bool(manifest),
        "objects": len(manifest),
        "fully_processed": statuses.get("FULLY_PROCESSED", 0),
        "not_fully_processed": [
            {"object": m["inputSourceArn"], "status": m["fileProcessingStatus"], "ranges": m["processedRanges"]}
            for m in manifest
            if m["fileProcessingStatus"] != "FULLY_PROCESSED"
        ],
        "error_records": counts["errors"],
        "condition_failure_records": counts["condition_failures"],
        "samples": sample,
    }
