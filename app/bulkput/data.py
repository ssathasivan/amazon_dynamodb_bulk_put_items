"""Test setup 1: generate the product catalog input files.

Each record is a plain write with no condition, so bulk put items does exactly what
BatchWriteItem does. That keeps the three-way comparison fair.
"""
from __future__ import annotations

import gzip
import random
from pathlib import Path


def write_catalog(count: int, shards: int, out_dir: Path, payload_bytes: int = 900, seed: int = 42) -> list[Path]:
    """Stream a large product catalog to `shards` gzip JSON Lines files without holding it in memory.

    Keys are shuffled and split round-robin, so every file spans the whole key space and the
    files are within one record of each other in size.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    order = list(range(count))
    random.Random(seed).shuffle(order)
    template = (
        '{"Item":{"sku":{"S":"sku-%09d"},"category":{"S":"cat-%02d"},"Version":{"N":"1"},'
        '"title":{"S":"Product %d"},"payload":{"S":"' + "x" * payload_bytes + '"}}}\n'
    )
    paths: list[Path] = []
    for s in range(shards):
        path = out_dir / f"part-{s:05d}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8", newline="\n", compresslevel=1) as fh:
            fh.writelines(template % (i, i % 50, i) for i in order[s::shards])
        paths.append(path)
    return paths
