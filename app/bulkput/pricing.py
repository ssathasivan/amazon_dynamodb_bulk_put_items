"""Back-of-the-envelope cost model, US East (N. Virginia) list prices, November 2026.

Always re-check https://aws.amazon.com/dynamodb/pricing/ for your Region and table class.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

GIB = 1024**3
KB = 1024

BULK_STANDARD_PER_GB = 0.10
BULK_ACCELERATED_PER_GB = 0.20
ON_DEMAND_PER_MILLION_WRU = 0.625
PROVISIONED_WCU_HOUR = 0.00065
S3_IMPORT_PER_GB = 0.15  # new tables only, billed on uncompressed source size


@dataclass
class CostRow:
    name: str
    dollars: float
    note: str = ""


def bulk_metered_bytes(items: int, item_bytes: int, gsis: int = 0) -> int:
    """Bulk put items bills actual bytes with a 1 KB minimum per item, per table and per index."""
    return items * max(item_bytes, KB) * (1 + gsis)


def write_request_units(items: int, item_bytes: int, gsis: int = 0) -> int:
    """PutItem / BatchWriteItem round each item UP to the next 1 KB, per table and per index."""
    return items * math.ceil(item_bytes / KB) * (1 + gsis)


def compare(items: int, item_bytes: int, gsis: int = 0, window_hours: float = 1.0) -> list[CostRow]:
    metered_gib = bulk_metered_bytes(items, item_bytes, gsis) / GIB
    wru = write_request_units(items, item_bytes, gsis)
    wcu_needed = math.ceil(wru / (window_hours * 3600))
    return [
        CostRow("Bulk put items - Standard", metered_gib * BULK_STANDARD_PER_GB, f"{metered_gib:,.2f} GiB metered"),
        CostRow("Bulk put items - Accelerated", metered_gib * BULK_ACCELERATED_PER_GB, f"{metered_gib:,.2f} GiB metered"),
        CostRow(
            "BatchWriteItem, on-demand table",
            wru / 1_000_000 * ON_DEMAND_PER_MILLION_WRU,
            f"{wru:,} WRU",
        ),
        CostRow(
            f"BatchWriteItem, provisioned for {window_hours:g}h",
            wcu_needed * window_hours * PROVISIONED_WCU_HOUR,
            f"{wcu_needed:,} WCU (default per-table quota is 40,000)",
        ),
    ]


def print_comparison(items: int, item_bytes: int, gsis: int, window_hours: float) -> None:
    rows = compare(items, item_bytes, gsis, window_hours)
    print(f"{items:,} items x {item_bytes:,} bytes, {gsis} GSI(s)\n")
    width = max(len(r.name) for r in rows)
    for r in rows:
        print(f"  {r.name:<{width}}  ${r.dollars:>12,.2f}   {r.note}")
    print("\n  Excludes S3 requests/storage for input and reports, Streams, and data transfer.")
