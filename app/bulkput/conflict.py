"""Test setup 2: conflict handling and reconciling with the report.

Runs on BulkPutStandardTable and BulkPutAcceleratedTable AFTER test setup 1, when both tables
hold sku-000000000 .. sku-000999999 at Version 1. `reset` puts the touched keys back to that
state, so the test can run again.

The input has one group of records per case. Each group has GROUP_SIZE records:

  group           keys                     record                                       expected
  plain           0 .. 19,999              Version 2, no condition                      written, Version 2
  insert_exists   20,000 .. 39,999         attribute_not_exists(sku), key exists        skipped, stays Version 1
  insert_new      1,000,000 .. 1,019,999   attribute_not_exists(sku), new key           written, Version 1
  newer           40,000 .. 59,999         Version 2, #v < :v with :v = 2               written, Version 2
  stale           60,000 .. 79,999         Version 1, #v < :v with :v = 1               skipped, stays Version 1
  broken          BROKEN records           no sku (the key attribute)                   in the ERROR report
"""
from __future__ import annotations

import gzip
import json
import random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

GROUP_SIZE = 20_000
BROKEN = 10
NEW_KEY_START = 1_000_000

# name: (first key, Version in the record, condition kind, expected Version in the table after the run)
GROUPS = {
    "plain": (0, 2, None, 2),
    "insert_exists": (20_000, 2, "insert", 1),
    "insert_new": (NEW_KEY_START, 1, "insert", 1),
    "newer": (40_000, 2, "version", 2),
    "stale": (60_000, 1, "version", 1),
}


def sku(i: int) -> str:
    return f"sku-{i:09d}"


def _item(i: int, version: int, payload: str) -> dict:
    return {
        "sku": {"S": sku(i)},
        "category": {"S": f"cat-{i % 50:02d}"},
        "Version": {"N": str(version)},
        "title": {"S": f"Product {i}"},
        "payload": {"S": payload},
    }


def _record(item: dict, kind: str | None) -> dict:
    rec: dict = {"Item": item}
    if kind == "insert":
        rec["ConditionExpression"] = "attribute_not_exists(sku)"
    elif kind == "version":
        rec["ConditionExpression"] = "#v < :v"
        rec["ExpressionAttributeNames"] = {"#v": "Version"}
        rec["ExpressionAttributeValues"] = {":v": item["Version"]}
    return rec


def expected() -> dict:
    written = GROUP_SIZE * 3  # plain, insert_new, newer
    skipped = GROUP_SIZE * 2  # insert_exists, stale
    return {
        "records": GROUP_SIZE * len(GROUPS) + BROKEN,
        "written": written,
        "condition_failures": skipped,
        "errors": BROKEN,
    }


def write_files(out_dir: Path, shards: int = 4, payload_bytes: int = 900, seed: int = 42) -> list[Path]:
    """Write the records, shuffled, to `shards` gzip JSON Lines files."""
    payload = "x" * payload_bytes
    records = []
    for first, version, kind, _ in GROUPS.values():
        records += [_record(_item(i, version, payload), kind) for i in range(first, first + GROUP_SIZE)]
    for b in range(BROKEN):
        broken = _item(NEW_KEY_START + GROUP_SIZE + b, 1, payload)
        del broken["sku"]
        records.append(_record(broken, None))
    random.Random(seed).shuffle(records)

    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for s in range(shards):
        path = out_dir / f"part-{s:05d}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8", newline="\n") as fh:
            for rec in records[s::shards]:
                fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        paths.append(path)
    return paths


def _batch_write(ddb, table: str, requests: list[dict]) -> None:
    for i in range(0, len(requests), 25):
        pending = {table: requests[i : i + 25]}
        while pending:
            pending = ddb.batch_write_item(RequestItems=pending).get("UnprocessedItems") or {}


def reset(ddb, table: str, payload_bytes: int = 900) -> None:
    """Put the touched keys back to the state after test setup 1: existing keys at Version 1, no new keys."""
    payload = "x" * payload_bytes
    puts, deletes = [], []
    for first, _, _, _ in GROUPS.values():
        for i in range(first, first + GROUP_SIZE):
            if i >= NEW_KEY_START:
                deletes.append({"DeleteRequest": {"Key": {"sku": {"S": sku(i)}}}})
            else:
                puts.append({"PutRequest": {"Item": _item(i, 1, payload)}})
    requests = puts + deletes
    chunks = [requests[i : i + 2_500] for i in range(0, len(requests), 2_500)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda c: _batch_write(ddb, table, c), chunks))


def _get_versions(ddb, table: str, keys: list[str]) -> dict[str, int]:
    found: dict[str, int] = {}
    for i in range(0, len(keys), 100):
        request = {table: {"Keys": [{"sku": {"S": k}} for k in keys[i : i + 100]], "ProjectionExpression": "sku, Version"}}
        while request:
            resp = ddb.batch_get_item(RequestItems=request)
            for item in resp["Responses"].get(table, []):
                found[item["sku"]["S"]] = int(item["Version"]["N"])
            request = resp.get("UnprocessedKeys") or {}
    return found


def check_table(ddb, table: str) -> dict:
    """Read every touched key and compare its Version with the expected one, per group."""
    result = {}
    for name, (first, _, _, want) in GROUPS.items():
        keys = [sku(i) for i in range(first, first + GROUP_SIZE)]
        chunks = [keys[i : i + 1_000] for i in range(0, len(keys), 1_000)]
        found: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            for part in pool.map(lambda c: _get_versions(ddb, table, c), chunks):
                found.update(part)
        ok = sum(1 for k in keys if found.get(k) == want)
        result[name] = {"expected_version": want, "keys": len(keys), "match": ok, "ok": ok == len(keys)}
    return result
