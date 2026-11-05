#!/usr/bin/env python3
"""Retry dead-letter failures for one Elasticsearch index.

Reads the append-only dead-letter JSONL, keeps the latest failure per priref,
skips prirefs already known to be slow, caps the batch size and parks records
that keep failing. Runs the matching indexer with ``--prirefs-csv``.

Designed to be safe to run daily from cron: it takes a lock, never retries
more than ``--max-batch`` records, and resets a priref's attempt count as soon
as a retry succeeds.

The attempt state lives in ``${LOG_PATH}/<prefix>_dead_letter_attempts.json``.
Delete a priref's entry to unpark it. The summary is appended to
``${LOG_PATH}/<prefix>_dead_letter_retry.log``.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

KINDS: dict[str, dict[str, str]] = {
    "items": {
        "prefix": "item",
        "dead_letter": "item_dead_letter.jsonl",
        "slow": "item_slow_records.csv",
        "indexer": "elasticsearch_index_items.py",
        "indexer_log": "item_indexing.log",
    },
    "objects": {
        "prefix": "screencraft_object",
        "dead_letter": "screencraft_object_dead_letter.jsonl",
        "slow": "screencraft_object_slow_records.csv",
        "indexer": "elasticsearch_index_screencraft_objects.py",
        "indexer_log": "screencraft_object_indexing.log",
    },
    "works": {
        "prefix": "screencraft_work",
        "dead_letter": "screencraft_work_dead_letter.jsonl",
        "slow": "screencraft_work_slow_records.csv",
        "indexer": "elasticsearch_index_screencraft_works.py",
        "indexer_log": "screencraft_work_indexing.log",
    },
    "cleanup": {
        "prefix": "screencraft_work_cleanup",
        "dead_letter": "screencraft_work_cleanup_dead_letter.jsonl",
        "slow": "screencraft_work_cleanup_slow_records.csv",
        "indexer": "elasticsearch_cleanup_screencraft_works.py",
        "indexer_log": "screencraft_work_cleanup.log",
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    """Return the latest dead-letter entry per priref (file order is newest last)."""
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        priref = str(record.get("priref") or "").strip()
        if not priref.isdigit():
            continue
        latest[priref] = record
    return latest


def read_slow_prirefs(path: Path) -> set[str]:
    prirefs: set[str] = set()
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.reader(fh):
                if row and row[0].strip().isdigit():
                    prirefs.add(row[0].strip())
    except FileNotFoundError:
        pass
    return prirefs


def load_state(path: Path) -> dict[str, dict[str, Any]]:
    try:
        with open(path, encoding="utf-8") as fh:
            state = json.load(fh)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        broken = path.with_suffix(path.suffix + ".bad")
        path.replace(broken)
        print(f"WARNING: corrupt state file moved to {broken}", file=sys.stderr)
        return {}
    if not isinstance(state, dict):
        return {}
    return state


def save_state(path: Path, state: dict[str, dict[str, Any]]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=1, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, path)


def _is_resolved(state_entry: dict[str, Any], failure_timestamp: Any) -> bool:
    """True when the last retry succeeded after this dead-letter entry."""
    resolved = state_entry.get("resolved_at")
    if not resolved:
        return False
    return str(failure_timestamp or "") <= str(resolved)


def log_path() -> Path:
    value = os.environ.get("LOG_PATH")
    if not value:
        raise SystemExit("LOG_PATH is not set")
    return Path(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry dead-letter failures for one Elasticsearch index.",
    )
    parser.add_argument(
        "--kind",
        default="items",
        choices=sorted(KINDS),
        help="Which index's dead-letter file to retry (default: items)",
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        default=500,
        help="Maximum prirefs to retry in one run (default: 500)",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=5,
        help="Park a priref after this many failed retries (default: 5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be retried without running the indexer",
    )
    return parser.parse_args()


def indexer_summary(log_file: Path, start_offset: int) -> dict[str, str]:
    """Parse the last SUMMARY line the indexer appended to its own log."""
    try:
        with open(log_file, encoding="utf-8") as fh:
            fh.seek(start_offset)
            lines = fh.read().splitlines()
    except OSError:
        return {}
    for line in reversed(lines):
        if " SUMMARY " in line:
            fields: dict[str, str] = {}
            for token in line.split():
                if "=" in token:
                    key, value = token.split("=", 1)
                    fields[key] = value
            return fields
    return {}


def main() -> int:
    args = parse_args()
    if args.max_batch < 1:
        raise SystemExit("--max-batch must be at least 1")
    if args.max_attempts < 1:
        raise SystemExit("--max-attempts must be at least 1")

    kind = KINDS[args.kind]
    base = log_path()
    dead_letter = base / kind["dead_letter"]
    slow_csv = base / kind["slow"]
    state_path = base / f"{kind['prefix']}_dead_letter_attempts.json"
    retry_log = base / f"{kind['prefix']}_dead_letter_retry.log"
    indexer_log = base / kind["indexer_log"]
    indexer = Path(__file__).with_name(kind["indexer"])
    lock_path = base / f"{kind['prefix']}_dead_letter_retry.lock"

    lock_fh = open(lock_path, "w")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("another dead-letter retry is already running - exiting")
        return 0

    failures = latest_failures(read_jsonl(dead_letter))
    slow = read_slow_prirefs(slow_csv)
    state = load_state(state_path)

    parked = {
        p
        for p, s in state.items()
        if int(s.get("attempts", 0)) >= args.max_attempts and not s.get("resolved_at")
    }
    candidates = [
        (priref, record)
        for priref, record in failures.items()
        if priref not in slow
        and priref not in parked
        and not _is_resolved(state.get(priref, {}), record.get("timestamp"))
    ]
    candidates.sort(key=lambda item: str(item[1].get("timestamp") or ""))
    batch = candidates[: args.max_batch]

    resolved_count = sum(
        1
        for priref, record in failures.items()
        if _is_resolved(state.get(priref, {}), record.get("timestamp"))
    )
    slow_overlap = sum(1 for priref in failures if priref in slow)
    print(
        f"{args.kind}: dead_letter_prirefs={len(failures)} "
        f"resolved={resolved_count} slow={slow_overlap} "
        f"parked={len(parked)} eligible={len(candidates)} batch={len(batch)}"
    )

    if args.dry_run or not batch:
        for priref, record in batch[:10]:
            print(
                f"  would retry {priref} stage={record.get('stage')} "
                f"attempts={state.get(priref, {}).get('attempts', 0)}"
            )
        return 0

    retry_csv = base / f"{kind['prefix']}_dead_letter_retry.csv"
    with open(retry_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        for priref, _ in batch:
            writer.writerow([priref])

    dead_letter_before = len(read_jsonl(dead_letter))
    try:
        log_offset = indexer_log.stat().st_size
    except OSError:
        log_offset = 0

    started = utc_now()
    print(f"retrying {len(batch)} priref(s) with {indexer.name} ...")
    result = subprocess.run(
        [sys.executable, str(indexer), "--prirefs-csv", str(retry_csv)],
        check=False,
    )

    # Anything appended to the dead-letter during the run is still failing.
    new_records = read_jsonl(dead_letter)[dead_letter_before:]
    failed_now = {
        str(r.get("priref") or "").strip()
        for r in new_records
        if str(r.get("priref") or "").strip().isdigit()
    }
    failed_details = {
        str(r.get("priref") or "").strip(): r for r in new_records
    }

    indexed = 0
    for priref, _ in batch:
        if priref in failed_now:
            entry = state.setdefault(priref, {"attempts": 0})
            entry["attempts"] = int(entry.get("attempts", 0)) + 1
            entry.pop("resolved_at", None)
            entry["last_attempt"] = started
            entry["stage"] = failed_details.get(priref, {}).get("stage")
            error = str(failed_details.get(priref, {}).get("error") or "")
            entry["error"] = error[:300]
        else:
            state[priref] = {"resolved_at": started, "attempts": 0}
            indexed += 1

    save_state(state_path, state)
    summary = indexer_summary(indexer_log, log_offset)
    still_failing = len(batch) - indexed
    parked_total = sum(
        1
        for p, s in state.items()
        if int(s.get("attempts", 0)) >= args.max_attempts and not s.get("resolved_at")
    )

    line = (
        f"{utc_now()} SUMMARY kind={args.kind} attempted={len(batch)} "
        f"indexed={indexed} still_failing={still_failing} "
        f"parked_total={parked_total} skipped_slow={len(slow)} "
        f"es_index_ok={summary.get('es_index_ok', '?')} "
        f"es_index_fail={summary.get('es_index_fail', '?')} "
        f"exit={result.returncode}"
    )
    with open(retry_log, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
