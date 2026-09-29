#!/usr/bin/env python3
"""Compare a CSV of item prirefs against an Elasticsearch index and list what is missing.

Reads a CSV whose first column contains prirefs (a header row is fine), checks
each one against the index (documents are keyed by priref as _id) and writes the
missing prirefs to an output CSV that can be fed straight back into the indexer:

    python3 elasticsearch_check_missing_items.py --csv all_items.csv
    python3 elasticsearch_index_items.py --prirefs-csv all_items_missing_from_es.csv

Uses only the Python standard library. Safe to re-run; read-only against ES.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request

DEFAULT_ES = os.environ.get("ES_SEARCH_PATH", "http://localhost:9200")
DEFAULT_INDEX = "dpi_items"


def read_prirefs(path: str):
    """Yield unique numeric prirefs from the first column of a CSV (header tolerated)."""
    seen = set()
    with open(path, newline="", encoding="utf-8-sig") as fh:
        for row in csv.reader(fh):
            if not row:
                continue
            value = row[0].strip()
            if value.isdigit() and value not in seen:
                seen.add(value)
                yield value


def find_present(es: str, index: str, ids: list[str], timeout: int = 120) -> set[str]:
    """Return the subset of ids that exist in the index (terms search on _id)."""
    payload = json.dumps({
        "size": len(ids),
        "track_total_hits": False,
        "_source": False,
        "query": {"terms": {"_id": ids}},
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{es.rstrip('/')}/{index}/_search",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.load(resp)
            return {hit["_id"] for hit in body["hits"]["hits"]}
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, KeyError) as exc:
            last_error = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"search failed after retries: {last_error}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="CSV with prirefs in the first column")
    parser.add_argument("--index", default=DEFAULT_INDEX, help=f"ES index (default: {DEFAULT_INDEX})")
    parser.add_argument("--es", default=DEFAULT_ES, help=f"Elasticsearch URL (default: {DEFAULT_ES})")
    parser.add_argument("--batch", type=int, default=10000, help="Ids per ES request (default: 10000)")
    parser.add_argument("--out", default=None,
                        help="Output CSV of missing prirefs (default: <csv>_missing_from_es.csv)")
    args = parser.parse_args()

    out_path = args.out or f"{os.path.splitext(args.csv)[0]}_missing_from_es.csv"

    total = present = 0
    missing: list[str] = []
    batch: list[str] = []
    batches = 0

    def flush() -> None:
        nonlocal present, batches
        if not batch:
            return
        found = find_present(args.es, args.index, batch)
        present += len(found)
        missing.extend(p for p in batch if p not in found)
        batches += 1
        batch.clear()
        print(f"\r  checked {total:>9,}  present {present:>9,}  missing {len(missing):>9,}", end="", flush=True)

    start = time.time()
    print(f"Reading prirefs from {args.csv} -> checking {args.index} at {args.es}")
    for priref in read_prirefs(args.csv):
        total += 1
        batch.append(priref)
        if len(batch) >= args.batch:
            flush()
    flush()

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["priref"])
        for priref in missing:
            writer.writerow([priref])

    elapsed = time.time() - start
    print()
    print(f"prirefs in CSV:   {total:,}")
    print(f"present in {args.index}: {present:,}")
    print(f"missing:          {len(missing):,}  ({100 * len(missing) / total if total else 0:.2f}%)")
    print(f"missing list written to: {out_path}")
    print(f"elapsed: {elapsed:.1f}s ({batches} ES requests)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
