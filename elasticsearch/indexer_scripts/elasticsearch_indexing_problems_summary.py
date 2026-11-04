#!/usr/bin/env python3
"""Write a human-friendly CSV summary of indexing problems for one index.

Combines the dead-letter JSONL and the slow-records CSV into one spreadsheet-
friendly file with a ``type`` column (``error`` or ``slow``). This is a
read-only report: the source of truth stays in the two raw files.

Columns: priref, type, last_seen, stage, detail, attempts, status.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any

KINDS: dict[str, dict[str, str]] = {
    "items": {
        "prefix": "item",
        "dead_letter": "item_dead_letter.jsonl",
        "slow": "item_slow_records.csv",
    },
    "objects": {
        "prefix": "screencraft_object",
        "dead_letter": "screencraft_object_dead_letter.jsonl",
        "slow": "screencraft_object_slow_records.csv",
    },
    "works": {
        "prefix": "screencraft_work",
        "dead_letter": "screencraft_work_dead_letter.jsonl",
        "slow": "screencraft_work_slow_records.csv",
    },
    "cleanup": {
        "prefix": "screencraft_work_cleanup",
        "dead_letter": "screencraft_work_cleanup_dead_letter.jsonl",
        "slow": "screencraft_work_cleanup_slow_records.csv",
    },
}

FIELDS = ["priref", "type", "last_seen", "stage", "detail", "attempts", "status"]


def one_line(value: Any, limit: int = 240) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return records


def latest_failures(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        priref = str(record.get("priref") or "").strip()
        if priref.isdigit():
            latest[priref] = record
    return latest


def read_slow(path: Path) -> dict[str, str]:
    slow: dict[str, str] = {}
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.reader(fh):
                if not row or not row[0].strip().isdigit():
                    continue
                priref = row[0].strip()
                seconds = row[1].strip() if len(row) > 1 else ""
                if seconds:
                    slow[priref] = seconds
                else:
                    slow.setdefault(priref, "")
    except FileNotFoundError:
        pass
    return slow


def load_state(path: Path) -> dict[str, dict[str, Any]]:
    try:
        with open(path, encoding="utf-8") as fh:
            state = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return state if isinstance(state, dict) else {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write a CSV summary of dead-letter and slow-record problems.",
    )
    parser.add_argument(
        "--kind",
        default="items",
        choices=sorted(KINDS),
        help="Which index to report on (default: items)",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: ${LOG_PATH}/<prefix>_indexing_problems.csv)",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=5,
        help="Attempts after which a record is reported as parked (default: 5)",
    )
    parser.add_argument(
        "--include-resolved",
        action="store_true",
        help="Include dead-letter entries that have since been re-indexed successfully",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_path = os.environ.get("LOG_PATH")
    if not log_path:
        raise SystemExit("LOG_PATH is not set")

    kind = KINDS[args.kind]
    base = Path(log_path)
    failures = latest_failures(read_jsonl(base / kind["dead_letter"]))
    slow = read_slow(base / kind["slow"])
    state = load_state(base / f"{kind['prefix']}_dead_letter_attempts.json")

    rows: list[dict[str, str]] = []
    for priref, record in failures.items():
        if priref in slow:
            continue
        entry = state.get(priref, {})
        attempts = int(entry.get("attempts", 0))
        resolved_at = entry.get("resolved_at")
        resolved = bool(resolved_at) and str(record.get("timestamp") or "") <= str(
            resolved_at
        )
        if resolved and not args.include_resolved:
            continue
        if resolved:
            status = "resolved"
        elif attempts >= args.max_attempts:
            status = "parked"
        else:
            status = "retrying"
        rows.append(
            {
                "priref": priref,
                "type": "error",
                "last_seen": str(record.get("timestamp") or ""),
                "stage": str(record.get("stage") or ""),
                "detail": one_line(record.get("error")),
                "attempts": str(attempts),
                "status": status,
            }
        )
    for priref, seconds in slow.items():
        rows.append(
            {
                "priref": priref,
                "type": "slow",
                "last_seen": "",
                "stage": "",
                "detail": f"{seconds}s" if seconds else "",
                "attempts": "",
                "status": "slow",
            }
        )

    rows.sort(key=lambda r: (r["type"] != "error", r["type"], r["priref"]))

    if args.out:
        out = Path(args.out)
    else:
        out = base / f"{kind['prefix']}_indexing_problems.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    errors = sum(1 for r in rows if r["type"] == "error")
    print(
        f"{args.kind}: wrote {len(rows)} rows to {out} "
        f"(errors={errors} slow={len(rows) - errors})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
