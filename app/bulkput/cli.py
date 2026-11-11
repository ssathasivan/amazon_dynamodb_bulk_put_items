"""Local helper commands.

    python -m bulkput generate --count 1000000 --batch catalog-1m           # create files, upload them
    python -m bulkput start --batch catalog-1m                              # start all 3 workflows
    python -m bulkput compare                                               # compare the latest runs

    Test setup 2 (conflict handling), on the tables from test setup 1:
    python -m bulkput conflict-reset                                        # touched keys back to Version 1
    python -m bulkput conflict-generate                                     # create the conflict records, upload them
    python -m bulkput start --batch conflict-test --bulk-only               # start the 2 bulk put workflows
    python -m bulkput conflict-check                                        # check the reports and the tables
    python -m bulkput cost --items 100000000 --item-bytes 1024              # offline, no AWS needed
"""
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import conflict, data, pricing

STACK = "BulkPutDemoStack"
WORKFLOWS = [
    ("BulkPutStandardArn", "Bulk put items, Standard", "BulkPutStandardTable"),
    ("BulkPutAcceleratedArn", "Bulk put items, Accelerated", "BulkPutAcceleratedTable"),
    ("BatchWriteArn", "BatchWriteItem", "BatchWriteTable"),
]
BULK_WORKFLOWS = WORKFLOWS[:2]


def _session(args):
    import boto3

    return boto3.Session(profile_name=args.profile, region_name=args.region)


def _outputs(session) -> dict:
    stack = session.client("cloudformation").describe_stacks(StackName=STACK)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}


def cmd_generate(args) -> None:
    session = _session(args)
    bucket = _outputs(session)["InputBucketName"]
    base = f"batches/{args.batch}/data/"
    s3 = session.client("s3")
    with tempfile.TemporaryDirectory() as tmp:
        paths = data.write_catalog(args.count, args.shards, Path(tmp), args.payload_bytes)
        size = sum(p.stat().st_size for p in paths)
        for p in paths:
            s3.upload_file(str(p), bucket, base + p.name)
    print(f"Uploaded {args.count:,} products in {len(paths)} files ({size / 1e6:,.1f} MB gzip) to s3://{bucket}/{base}")
    print(f"Start all three loads with: python -m bulkput start --batch {args.batch}")


def cmd_start(args) -> None:
    """Start all three workflows with the same input and execution name."""
    session = _session(args)
    outputs = _outputs(session)
    run_input = json.dumps({"bucket": outputs["InputBucketName"], "prefix": f"batches/{args.batch}/data/"})
    name = f"{args.batch}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    sfn = session.client("stepfunctions")
    workflows = BULK_WORKFLOWS if args.bulk_only else WORKFLOWS
    for output_key, label, _ in workflows:
        sfn.start_execution(stateMachineArn=outputs[output_key], name=name, input=run_input)
        print(f"Started {label}: {name}")
    print(f"Input: {run_input}")


def _first_progress_seconds(sfn, execution_arn: str, started) -> float | None:
    """Seconds from execution start to the first check that saw records being read."""
    for page in sfn.get_paginator("get_execution_history").paginate(executionArn=execution_arn):
        for e in page["events"]:
            if e["type"] != "TaskStateExited" or not e["stateExitedEventDetails"]["name"].endswith("CheckStatus"):
                continue
            check = json.loads(e["stateExitedEventDetails"]["output"]).get("check", {})
            if check.get("records_read", 0) > 0 or check.get("done"):
                return (e["timestamp"] - started).total_seconds()
    return None


def _metric_sum(cw, table: str, metric: str, start, end) -> float:
    resp = cw.get_metric_statistics(
        Namespace="AWS/DynamoDB",
        MetricName=metric,
        Dimensions=[{"Name": "TableName", "Value": table}],
        StartTime=start - timedelta(minutes=5),
        EndTime=end + timedelta(minutes=5),
        Period=60,
        Statistics=["Sum"],
    )
    return sum(p["Sum"] for p in resp["Datapoints"])


def cmd_compare(args) -> None:
    session = _session(args)
    outputs = _outputs(session)
    sfn = session.client("stepfunctions")
    cw = session.client("cloudwatch")

    rows = []
    for output_key, label, table in WORKFLOWS:
        execs = sfn.list_executions(stateMachineArn=outputs[output_key], maxResults=1)["executions"]
        if not execs:
            rows.append({"Method": label, "Status": "not run"})
            continue
        ex = sfn.describe_execution(executionArn=execs[0]["executionArn"])
        end = ex.get("stopDate")
        row = {"Method": label, "Table": table, "Workflow": ex["status"]}
        if not end:
            rows.append(row)
            continue
        total = (end - ex["startDate"]).total_seconds()
        out = json.loads(ex.get("output") or "{}")
        result = out.get("check", {}).get("result") or out.get("summary") or {}
        items = result.get("items", 0)
        load = result.get("load_seconds") or total
        row.update(
            {
                "Items": f"{items:,}",
                "Total time": f"{total:,.0f} s",
                "Load time": f"{load:,.0f} s",
                "Items/s": f"{items / load:,.0f}" if load else "-",
                "Cost": f"${result.get('cost_usd', 0):,.2f}",
                "Waiting to start": (
                    f"{_first_progress_seconds(sfn, ex['executionArn'], ex['startDate']):,.0f} s"
                    if "check" in out
                    else "-"
                ),
                "Table WCU used": f"{_metric_sum(cw, table, 'ConsumedWriteCapacityUnits', ex['startDate'], end):,.0f}",
                "Write throttles": f"{_metric_sum(cw, table, 'WriteThrottleEvents', ex['startDate'], end):,.0f}",
            }
        )
        rows.append(row)

    columns = ["Method", "Workflow", "Items", "Total time", "Load time", "Waiting to start", "Items/s", "Cost", "Table WCU used", "Write throttles"]
    width = {c: max(len(c), *(len(str(r.get(c, "-"))) for r in rows)) for c in columns}
    print("  ".join(c.ljust(width[c]) for c in columns))
    for r in rows:
        print("  ".join(str(r.get(c, "-")).ljust(width[c]) for c in columns))
    print("\nCloudWatch metrics can lag by a few minutes. Run compare again if the WCU column looks low.")


def cmd_conflict_generate(args) -> None:
    session = _session(args)
    bucket = _outputs(session)["InputBucketName"]
    base = f"batches/{args.batch}/data/"
    s3 = session.client("s3")
    with tempfile.TemporaryDirectory() as tmp:
        paths = conflict.write_files(Path(tmp), args.shards)
        for p in paths:
            s3.upload_file(str(p), bucket, base + p.name)
    exp = conflict.expected()
    print(f"Uploaded {exp['records']:,} records in {len(paths)} files to s3://{bucket}/{base}")
    print(f"Expected: {exp['written']:,} written, {exp['condition_failures']:,} condition failures, {exp['errors']:,} errors")


def cmd_conflict_reset(args) -> None:
    ddb = _session(args).client("dynamodb")
    for _, label, table in BULK_WORKFLOWS:
        conflict.reset(ddb, table)
        print(f"Reset {table} ({label}): touched keys back to Version 1, new keys deleted")


def cmd_conflict_check(args) -> None:
    session = _session(args)
    outputs = _outputs(session)
    sfn = session.client("stepfunctions")
    ddb = session.client("dynamodb")
    exp = conflict.expected()
    all_ok = True
    for output_key, label, table in BULK_WORKFLOWS:
        execs = sfn.list_executions(stateMachineArn=outputs[output_key], maxResults=1)["executions"]
        if not execs:
            print(f"{label}: not run")
            all_ok = False
            continue
        ex = sfn.describe_execution(executionArn=execs[0]["executionArn"])
        result = json.loads(ex.get("output") or "{}").get("check", {}).get("result", {})
        print(f"\n{label}  ({ex['name']}, {ex['status']})")
        print(f"  Operation status    : {result.get('status')}")
        report = [
            ("Records read", result.get("items"), exp["records"]),
            ("Errors", result.get("failed_items"), exp["errors"]),
            ("Condition failures", result.get("condition_failures"), exp["condition_failures"]),
            ("Written", (result.get("items") or 0) - (result.get("failed_items") or 0) - (result.get("condition_failures") or 0), exp["written"]),
        ]
        for name, got, want in report:
            ok = got == want
            all_ok &= ok
            print(f"  {name:<20}: {got:>7,} (expected {want:,})  {'OK' if ok else 'MISMATCH'}" if got is not None else f"  {name:<20}: -")
        print(f"  Files fully processed: {result.get('files_fully_processed')}")
        print("  Table check:")
        for group, r in conflict.check_table(ddb, table).items():
            all_ok &= r["ok"]
            print(f"    {group:<14} Version {r['expected_version']}: {r['match']:,} of {r['keys']:,}  {'OK' if r['ok'] else 'MISMATCH'}")
    print("\nResult: " + ("matches the plan" if all_ok else "DOES NOT match the plan"))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bulkput", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile")
    p.add_argument("--region")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="create the product files and upload them")
    g.add_argument("--count", type=int, default=1_000_000)
    g.add_argument("--batch", default="catalog-1m")
    g.add_argument("--shards", type=int, default=20)
    g.add_argument("--payload-bytes", type=int, default=900)
    g.set_defaults(fn=cmd_generate)

    s = sub.add_parser("start", help="start all three workflows with the same input")
    s.add_argument("--batch", default="catalog-1m")
    s.add_argument("--bulk-only", action="store_true", help="start only BulkPutStandard and BulkPutAccelerated")
    s.set_defaults(fn=cmd_start)

    cg = sub.add_parser("conflict-generate", help="test setup 2: create the conflict records and upload them")
    cg.add_argument("--batch", default="conflict-test")
    cg.add_argument("--shards", type=int, default=4)
    cg.set_defaults(fn=cmd_conflict_generate)
    sub.add_parser("conflict-reset", help="test setup 2: put the touched keys back to Version 1").set_defaults(fn=cmd_conflict_reset)
    sub.add_parser("conflict-check", help="test setup 2: check the reports and the tables").set_defaults(fn=cmd_conflict_check)

    sub.add_parser("compare", help="compare the latest run of each workflow").set_defaults(fn=cmd_compare)

    c = sub.add_parser("cost", help="offline cost comparison, no AWS access needed")
    c.add_argument("--items", type=int, required=True)
    c.add_argument("--item-bytes", type=int, default=1024)
    c.add_argument("--gsis", type=int, default=0)
    c.add_argument("--window-hours", type=float, default=1.0)
    c.set_defaults(fn=lambda a: pricing.print_comparison(a.items, a.item_bytes, a.gsis, a.window_hours))
    return p


def main() -> None:
    args = build_parser().parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
