#!/usr/bin/env python3
"""
Manually operated script for deletion of unwanted
proxy files in DPI browser/MediaTheque, where an
elasticsearch index exists and needs removing
"""


from __future__ import annotations
import os
import argparse
import csv
import json
import logging
import sys
import time
from typing import Iterator

from elastic_transport import ApiError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


# =========================
# Configuration
# =========================

ES_URL = os.environ.get("ES_SEARCH_PATH")
ES_INDEX = "dpi_items"

LOG_PATH = os.path.join(os.environ.get("LOG_PATH"), "item_bulk_delete.log")
DEAD_LETTER_PATH = os.path.join(os.environ.get("LOG_PATH"), "item_bulk_delete_dead_letter.jsonl")

# Elasticsearch settings
ES_REQUEST_TIMEOUT = 60
ES_MAX_RETRIES = 5
ES_RETRY_ON_TIMEOUT = True

# Bulk settings
BULK_CHUNK_SIZE = 50
BULK_MAX_CHUNK_BYTES = 5 * 1024 * 1024
BULK_INITIAL_BACKOFF = 2
BULK_MAX_BACKOFF = 60
BULK_MAX_RETRIES = 3

# Progress logging
PROGRESS_EVERY = 10

# Dead-letter truncation sizes
MAX_ERROR_SNIPPET = 5000


# =========================
# Logging
# =========================

logger = logging.getLogger("item_bulk_delete")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

file_handler = logging.FileHandler(LOG_PATH, mode="a")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)


# =========================
# Stats
# =========================

class Stats:
    def __init__(self) -> None:
        self.prirefs_total = 0
        self.deleted_ok = 0
        self.deleted_fail = 0
        self.dead_letter_written = 0
        self.not_found = 0
        self.start_time = time.time()

    def log_summary(self) -> None:
        elapsed = time.time() - self.start_time
        logger.info(
            "SUMMARY elapsed=%.2fs total=%d deleted_ok=%d not_found=%d "
            "deleted_fail=%d dead_letter_written=%d",
            elapsed,
            self.prirefs_total,
            self.deleted_ok,
            self.not_found,
            self.deleted_fail,
            self.dead_letter_written,
        )


# =========================
# Helpers
# =========================

def utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def truncate_text(value: str | None, max_len: int) -> str | None:
    if value is None:
        return None
    if len(value) <= max_len:
        return value
    return value[:max_len] + f"... [truncated {len(value) - max_len} chars]"


def safe_json_dumps(value, max_len: int) -> str | None:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = repr(value)
    return truncate_text(text, max_len)


# =========================
# ES helpers
# =========================

def build_es_client() -> Elasticsearch:
    return Elasticsearch(
        ES_URL,
        request_timeout=ES_REQUEST_TIMEOUT,
        max_retries=ES_MAX_RETRIES,
        retry_on_timeout=ES_RETRY_ON_TIMEOUT,
    )


def ping_es(es: Elasticsearch) -> None:
    try:
        info = es.info()
        logger.info(
            "Connected to Elasticsearch at %s; cluster_name=%s version=%s",
            ES_URL,
            info.get("cluster_name"),
            info.get("version", {}).get("number"),
        )
    except Exception as e:
        raise RuntimeError(f"Could not connect to Elasticsearch at {ES_URL}: {e}") from e


# =========================
# Dead letter
# =========================

def write_dead_letter(
    stats: Stats,
    stage: str,
    priref: str,
    error: str,
    *,
    http_status: int | None = None,
    es_error: object | None = None,
) -> None:
    record = {
        "timestamp": utc_now_iso(),
        "stage": stage,
        "priref": priref,
        "error": truncate_text(error, MAX_ERROR_SNIPPET),
        "http_status": http_status,
        "es_error": safe_json_dumps(es_error, MAX_ERROR_SNIPPET) if es_error is not None else None,
    }

    with open(DEAD_LETTER_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    stats.dead_letter_written += 1


# =========================
# CSV reading
# =========================

def read_prirefs_from_csv(csv_path: str) -> list[str]:
    """Read a single-column CSV of prirefs.

    The CSV may or may not have a header row.  If the first cell is
    non-numeric it is treated as a column header and skipped.
    """
    prirefs: list[str] = []

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        first_row = True
        for row in reader:
            if not row or not row[0].strip():
                continue
            value = row[0].strip()
            if first_row and not value.isdigit():
                # Skip header row
                first_row = False
                continue
            first_row = False
            prirefs.append(value)

    return prirefs


# =========================
# Bulk delete actions
# =========================

def make_delete_action(priref: str) -> dict:
    return {
        "_op_type": "delete",
        "_index": ES_INDEX,
        "_id": priref,
    }


def delete_action_generator(prirefs: list[str], stats: Stats) -> Iterator[dict]:
    for count, priref in enumerate(prirefs, 1):
        if count % PROGRESS_EVERY == 0:
            logger.info("Progress: prepared %d/%d prirefs for deletion", count, len(prirefs))
        yield make_delete_action(priref)


def bulk_delete(es: Elasticsearch, actions: Iterator[dict], stats: Stats) -> None:
    for ok, item in streaming_bulk(
        client=es,
        actions=actions,
        chunk_size=BULK_CHUNK_SIZE,
        max_chunk_bytes=BULK_MAX_CHUNK_BYTES,
        raise_on_error=False,
        raise_on_exception=False,
        max_retries=BULK_MAX_RETRIES,
        initial_backoff=BULK_INITIAL_BACKOFF,
        max_backoff=BULK_MAX_BACKOFF,
    ):
        op_type, result = next(iter(item.items()))
        priref = result.get("_id")

        if ok:
            # delete operation returns "deleted" or "not_found"
            delete_result = result.get("result", "")
            if delete_result == "not_found":
                stats.not_found += 1
                logger.warning("%s - document not found (already deleted or never existed)", priref)
            else:
                stats.deleted_ok += 1
                if stats.deleted_ok % PROGRESS_EVERY == 0:
                    logger.info(
                        "Elasticsearch deleted %d docs so far; latest priref=%s result=%s",
                        stats.deleted_ok,
                        priref,
                        delete_result,
                    )
        else:
            stats.deleted_fail += 1
            es_error = result.get("error")
            status_code = result.get("status")
            logger.error(
                "%s - Elasticsearch bulk delete failed status=%s error=%s",
                priref,
                status_code,
                es_error,
            )
            write_dead_letter(
                stats,
                stage="es_delete",
                priref=priref,
                error=f"Elasticsearch bulk delete failed",
                http_status=status_code,
                es_error=es_error,
            )


# =========================
# Argument parsing
# =========================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk-delete Elasticsearch documents by priref from a CSV file.",
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file with a single column of prirefs (may include a header row)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and validate the CSV, count documents to delete, but do not actually delete anything",
    )

    return parser.parse_args()


# =========================
# Main
# =========================

def main() -> int:
    args = parse_args()

    stats = Stats()
    es = build_es_client()

    # --- Read CSV ---
    try:
        prirefs = read_prirefs_from_csv(args.csv)
    except FileNotFoundError:
        logger.error("CSV file not found: %s", args.csv)
        return 2
    except Exception as e:
        logger.exception("Error reading CSV %s: %s", args.csv, e)
        return 2

    if not prirefs:
        logger.warning("No prirefs found in CSV %s — nothing to do.", args.csv)
        return 0

    logger.info(
        "Loaded %d priref(s) from CSV %s",
        len(prirefs),
        args.csv,
    )

    # --- Dry-run ---
    if args.dry_run:
        logger.info("DRY RUN — %d documents would be deleted from index '%s'", len(prirefs), ES_INDEX)
        for i, p in enumerate(prirefs, 1):
            print(f"  would delete priref={p}")
            if i >= 20 and len(prirefs) > 20:
                print(f"  ... and {len(prirefs) - 20} more")
                break
        logger.info("DRY RUN complete — no documents were deleted.")
        return 0

    # --- Execute ---
    try:
        ping_es(es)

        logger.info(
            "Starting bulk delete of %d documents from index '%s'; dead-letter file=%s",
            len(prirefs),
            ES_INDEX,
            DEAD_LETTER_PATH,
        )

        stats.prirefs_total = len(prirefs)
        actions = delete_action_generator(prirefs, stats)
        bulk_delete(es, actions, stats)
        stats.log_summary()
        return 0

    except ApiError as e:
        logger.exception("Elasticsearch API error: %s", e)
        return 1
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
