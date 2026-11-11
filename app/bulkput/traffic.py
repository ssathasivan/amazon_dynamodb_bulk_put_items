"""Test setup 3 with app traffic: steady application traffic against a table while a load runs.

Each invocation sends `reads` GetItem and `writes` PutItem requests per second to one table,
for up to `minutes` minutes, and publishes one data point per minute to CloudWatch
(namespace BulkPutDemo, dimension Table):

  AppReadLatency, AppWriteLatency      milliseconds, as Values/Counts so CloudWatch can compute p50/p99
  AppReads, AppWrites                  successful requests
  AppReadThrottles, AppWriteThrottles  requests rejected with a throttling error
  AppReadErrors, AppWriteErrors        any other error

The client makes ONE attempt per request (no SDK retries), so every throttle the table returns
is counted instead of being hidden by a retry. Reads go to random catalog keys; writes go to a
separate `app-` key range so they never change the items the loads write.
"""
from __future__ import annotations

import random
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

THROTTLE_CODES = {"ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded"}
NAMESPACE = "BulkPutDemo"
CATALOG_KEYS = 10_000_000
APP_KEYS = 100_000


class _Window:
    """Thread-safe counters and latencies for one minute."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        self.latency = {"read": Counter(), "write": Counter()}
        self.counts = Counter()

    def ok(self, kind: str, ms: float) -> None:
        with self.lock:
            self.latency[kind][round(ms, 1)] += 1
            self.counts[f"{kind}_ok"] += 1

    def fail(self, kind: str, throttled: bool) -> None:
        with self.lock:
            self.counts[f"{kind}_{'throttle' if throttled else 'error'}"] += 1

    def take(self) -> tuple[dict, Counter]:
        with self.lock:
            latency, counts = self.latency, self.counts
            self.reset()
        return latency, counts


def _values_counts(hist: Counter) -> tuple[list[float], list[float]]:
    """PutMetricData accepts up to 150 distinct values per datum. Coarsen until it fits."""
    step = 0.1
    while len(hist) > 150:
        step *= 2
        merged: Counter = Counter()
        for v, c in hist.items():
            merged[round(round(v / step) * step, 1)] += c
        hist = merged
    items = sorted(hist.items())
    return [v for v, _ in items], [float(c) for _, c in items]


def _publish(cloudwatch, table: str, latency: dict, counts: Counter, when: datetime) -> None:
    dims = [{"Name": "Table", "Value": table}]
    data = []
    for kind, name in (("read", "AppReadLatency"), ("write", "AppWriteLatency")):
        if latency[kind]:
            values, weights = _values_counts(latency[kind])
            data.append({"MetricName": name, "Dimensions": dims, "Timestamp": when, "Values": values, "Counts": weights, "Unit": "Milliseconds"})
    for key, name in (
        ("read_ok", "AppReads"), ("write_ok", "AppWrites"),
        ("read_throttle", "AppReadThrottles"), ("write_throttle", "AppWriteThrottles"),
        ("read_error", "AppReadErrors"), ("write_error", "AppWriteErrors"),
    ):
        data.append({"MetricName": name, "Dimensions": dims, "Timestamp": when, "Value": float(counts.get(key, 0)), "Unit": "Count"})
    cloudwatch.put_metric_data(Namespace=NAMESPACE, MetricData=data)


def run(ddb, cloudwatch, table: str, reads: int, writes: int, seconds: float, payload_bytes: int = 900) -> dict:
    """Send steady traffic for `seconds`, publishing a data point at the end of every minute."""
    from botocore.exceptions import ClientError

    window = _Window()
    payload = "a" * payload_bytes
    rng = random.Random()

    def get() -> None:
        key = f"sku-{rng.randrange(CATALOG_KEYS):09d}"
        t0 = time.perf_counter()
        try:
            ddb.get_item(TableName=table, Key={"sku": {"S": key}})
            window.ok("read", (time.perf_counter() - t0) * 1000)
        except ClientError as e:
            window.fail("read", e.response["Error"]["Code"] in THROTTLE_CODES)
        except Exception:  # noqa: BLE001 - count connection errors too, never stop the generator
            window.fail("read", False)

    def put() -> None:
        item = {
            "sku": {"S": f"app-{rng.randrange(APP_KEYS):06d}"},
            "Version": {"N": "1"},
            "payload": {"S": payload},
        }
        t0 = time.perf_counter()
        try:
            ddb.put_item(TableName=table, Item=item)
            window.ok("write", (time.perf_counter() - t0) * 1000)
        except ClientError as e:
            window.fail("write", e.response["Error"]["Code"] in THROTTLE_CODES)
        except Exception:  # noqa: BLE001
            window.fail("write", False)

    # Spread the requests evenly over each second: `reads` gets and `writes` puts, interleaved.
    rate = reads + writes
    schedule = [put if (i * writes) // rate != ((i + 1) * writes) // rate else get for i in range(rate)]
    totals: Counter = Counter()
    start = time.monotonic()
    next_publish = start + 60
    minutes = 0
    with ThreadPoolExecutor(max_workers=48) as pool:
        i = 0
        while True:
            now = time.monotonic()
            if now >= next_publish or now - start >= seconds:
                latency, counts = window.take()
                totals.update(counts)
                _publish(cloudwatch, table, latency, counts, datetime.now(timezone.utc))
                minutes += 1
                next_publish += 60
                if now - start >= seconds:
                    break
            target = start + i / rate
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            pool.submit(schedule[i % rate])
            i += 1
    return {"table": table, "minutes": minutes, **dict(totals)}
