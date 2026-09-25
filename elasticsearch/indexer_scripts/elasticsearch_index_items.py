#!/usr/bin/env python3

from __future__ import annotations

import os
import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone, timedelta, date
from typing import Iterator, Optional
from urllib.parse import quote

import requests
import defusedxml.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from xmljson import parker

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elastic_transport import ApiError


# =========================
# Configuration
# =========================

CID_BASE_URL = os.environ.get("CID_API1")
DEFAULT_QUERY_MODE = "both"
DIRECT_QUERY_MAX_DAYS = 2  # if range is more than this, split into per-day CID calls
MAX_DIRECT_PRIREFS = 1000
CID_REQUEST_DELAY = 0.25
CID_ITEM_URL_TEMPLATE = ("{base_url}?database=elasticsearchitems&search=priref={priref}")

ES_URL = os.environ.get("ES_SEARCH_PATH")
ES_INDEX = "dpi_items"

LOGS = os.environ.get("LOG_PATH")
OUTPUT_FILE_PATH = os.path.join(LOGS, "item_prirefs.txt")
LOG_PATH = os.path.join(LOGS, "item_indexing.log")
DEAD_LETTER_PATH = os.path.join(LOGS, "item_dead_letter.jsonl")

# HTTP / CID settings
HTTP_TIMEOUT = (10, 600)
HTTP_RETRIES = 5
HTTP_BACKOFF = 1.0

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
MAX_XML_SNIPPET = 2000
MAX_DOC_SNIPPET = 5000
MAX_ERROR_SNIPPET = 5000


# =========================
# Logging
# =========================

logger = logging.getLogger("item_indexer")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

file_handler = logging.FileHandler(LOG_PATH, mode="a")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)


# =========================
# Helpers
# =========================


class Stats:
    def __init__(self) -> None:
        self.prirefs_total = 0
        self.prirefs_unique = 0
        self.cid_fetch_ok = 0
        self.cid_fetch_fail = 0
        self.xml_parse_ok = 0
        self.xml_parse_fail = 0
        self.docs_prepared = 0
        self.es_index_ok = 0
        self.es_index_fail = 0
        self.dead_letter_written = 0
        self.start_time = time.time()

    def log_summary(self) -> None:
        elapsed = time.time() - self.start_time
        logger.info(
            "SUMMARY elapsed=%.2fs total_prirefs=%d unique_prirefs=%d "
            "cid_fetch_ok=%d cid_fetch_fail=%d xml_parse_ok=%d xml_parse_fail=%d "
            "docs_prepared=%d es_index_ok=%d es_index_fail=%d dead_letter_written=%d",
            elapsed,
            self.prirefs_total,
            self.prirefs_unique,
            self.cid_fetch_ok,
            self.cid_fetch_fail,
            self.xml_parse_ok,
            self.xml_parse_fail,
            self.docs_prepared,
            self.es_index_ok,
            self.es_index_fail,
            self.dead_letter_written,
        )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def truncate_text(value: Optional[str], max_len: int) -> Optional[str]:
    if value is None:
        return None
    if len(value) <= max_len:
        return value
    return value[:max_len] + f"... [truncated {len(value) - max_len} chars]"


def safe_json_dumps(value, max_len: int) -> Optional[str]:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = repr(value)
    return truncate_text(text, max_len)


def validate_date(date_str: str) -> str:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{date_str}'. Expected format: YYYY-MM-DD"
        ) from e
    return date_str


def validate_prirefs(priref_string: str) -> list[str]:
    prirefs = [p.strip() for p in priref_string.split(",")]
    if not prirefs or any(not p for p in prirefs):
        raise argparse.ArgumentTypeError(
            f"Invalid prirefs '{priref_string}'. Each priref must be non-empty numeric value, comma-separated"
        )
    if len(prirefs) > MAX_DIRECT_PRIREFS:
        raise argparse.ArgumentTypeError(
            f"Too many prirefs ({len(prirefs)}). Maximum allowed is {MAX_DIRECT_PRIREFS}"
        )
    for p in prirefs:
        if not p.isdigit():
            raise argparse.ArgumentTypeError(
                f"Invalid priref '{p}'. Prirefs must be numeric only"
            )
    return prirefs


def read_prirefs_from_csv(csv_path: str) -> list[str]:
    prirefs: list[str] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        first_row = True
        for row in reader:
            if not row or not row[0].strip():
                continue
            value = row[0].strip()
            if first_row and not value.isdigit():
                first_row = False
                continue
            first_row = False
            prirefs.append(value)
    if not prirefs:
        raise argparse.ArgumentTypeError(
            f"No valid numeric prirefs found in CSV '{csv_path}'"
        )
    for p in prirefs:
        if not p.isdigit():
            raise argparse.ArgumentTypeError(
                f"Invalid priref '{p}' in CSV '{csv_path}'. Prirefs must be numeric only"
            )
    return prirefs


def parse_yyyy_mm_dd(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def format_yyyy_mm_dd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def date_span_days_inclusive(date_from: str, date_to: str) -> int:
    start = parse_yyyy_mm_dd(date_from)
    end = parse_yyyy_mm_dd(date_to)
    return (end - start).days + 1


def default_target_date() -> str:
    return (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")


def resolve_date_range(
    date_from: Optional[str], date_to: Optional[str]
) -> tuple[str, str]:
    fallback = default_target_date()
    resolved_from = date_from or fallback
    resolved_to = date_to or fallback
    return resolved_from, resolved_to


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch item prirefs from CID, retrieve XML, and bulk index into Elasticsearch."
    )

    parser.add_argument(
        "--date-from",
        default=None,
        type=validate_date,
        help="Lower bound date inclusive, format YYYY-MM-DD (default: today-2)",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        type=validate_date,
        help="Upper bound date inclusive, format YYYY-MM-DD (default: today-2)",
    )

    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--prirefs",
        default=None,
        type=validate_prirefs,
        help="Comma-separated list of prirefs to index directly (max 1000, numeric only). Mutually exclusive with date arguments",
    )
    date_group.add_argument(
        "--prirefs-csv",
        default=None,
        type=read_prirefs_from_csv,
        help="Path to CSV file with a single column of prirefs (no limit). Mutually exclusive with date arguments",
    )

    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY_MODE,
        choices=("items", "works", "both"),
        help="Which CID query set to run: items, works, or both (default: both). Ignored when --prirefs is used",
    )
    return parser.parse_args()


def build_priref_url(date_from: str, date_to: str, query_label: str) -> str:
    safe_chars = "()=*' "

    if query_label == "items":
        search = (
            f"Df=item and (modification>='{date_from}' and modification<='{date_to} 23:59:59')"
        )
    elif query_label == "works":
        search = (
            "Df=item and "
            f"(part_of_reference->part_of_reference->(edit.date>='{date_from}' "
            f"and edit.date<='{date_to}'))"
        )
    else:
        raise ValueError(f"Unknown query_label: {query_label}")

    url = (
        f"{CID_BASE_URL}?database=prirefcollectraw"
        f"&search={quote(search, safe=safe_chars)}"
        f"&limit=0"
    )
    return url.replace(" ", "%20")


def get_query_labels(query_mode: str) -> list[str]:
    if query_mode == "items":
        return ["items"]
    if query_mode == "works":
        return ["works"]
    if query_mode == "both":
        return ["items", "works"]
    raise ValueError(f"Unsupported query mode: {query_mode}")


def build_requests_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        status=HTTP_RETRIES,
        backoff_factor=HTTP_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def build_es_client() -> Elasticsearch:
    return Elasticsearch(
        ES_URL,
        request_timeout=ES_REQUEST_TIMEOUT,
        max_retries=ES_MAX_RETRIES,
        retry_on_timeout=ES_RETRY_ON_TIMEOUT,
    )


def write_dead_letter(
    stats: Stats,
    stage: str,
    priref: str,
    error: str,
    *,
    cid_url: Optional[str] = None,
    http_status: Optional[int] = None,
    xml_text: Optional[str] = None,
    document: Optional[dict] = None,
    es_error: Optional[object] = None,
) -> None:
    record = {
        "timestamp": utc_now_iso(),
        "stage": stage,
        "priref": priref,
        "error": truncate_text(error, MAX_ERROR_SNIPPET),
        "cid_url": cid_url,
        "http_status": http_status,
        "xml_snippet": truncate_text(xml_text, MAX_XML_SNIPPET) if xml_text else None,
        "document_snippet": safe_json_dumps(document, MAX_DOC_SNIPPET)
        if document is not None
        else None,
        "es_error": safe_json_dumps(es_error, MAX_ERROR_SNIPPET)
        if es_error is not None
        else None,
    }

    with open(DEAD_LETTER_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    stats.dead_letter_written += 1


def fetch_text(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.text


def append_priref_block(
    output_path: str, label: str, day_from: str, day_to: str, text: str
) -> None:
    with open(output_path, "a", encoding="utf-8") as txtfile:
        txtfile.write(f"# --- {label} query | {day_from} to {day_to} ---\n")
        txtfile.write(text.rstrip() + "\n\n")


def fetch_prirefs(
    session: requests.Session,
    stats: Stats,
    date_from: str,
    date_to: str,
    query_mode: str,
) -> list[str]:
    logger.info(
        "Fetching prirefs from CID for date range %s to %s using query mode '%s'",
        date_from,
        date_to,
        query_mode,
    )

    span_days = date_span_days_inclusive(date_from, date_to)
    query_labels = get_query_labels(query_mode)

    logger.info(
        "Resolved CID priref date span: %d day(s); direct-query threshold=%d day(s)",
        span_days,
        DIRECT_QUERY_MAX_DAYS,
    )

    raw_prirefs: list[str] = []

    # overwrite output file for this run
    with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as txtfile:
        txtfile.write(
            f"# CID priref output for run\n"
            f"# date_from={date_from}\n"
            f"# date_to={date_to}\n"
            f"# query_mode={query_mode}\n"
            f"# span_days={span_days}\n\n"
        )

    if span_days <= DIRECT_QUERY_MAX_DAYS:
        logger.info(
            "Using direct CID query mode for %d day(s) (<= %d)",
            span_days,
            DIRECT_QUERY_MAX_DAYS,
        )
        for label in query_labels:
            url = build_priref_url(date_from, date_to, label)
            logger.info("CID %s URL: %s", label, url)
            text = fetch_text(session, url)
            append_priref_block(OUTPUT_FILE_PATH, label, date_from, date_to, text)
            raw_prirefs.extend(
                line.strip() for line in text.splitlines() if line.strip()
            )
    else:
        logger.info(
            "Using per-day CID query mode because span is %d day(s) (> %d)",
            span_days,
            DIRECT_QUERY_MAX_DAYS,
        )

        start = parse_yyyy_mm_dd(date_from)
        end = parse_yyyy_mm_dd(date_to)

        for label in query_labels:
            logger.info("Starting per-day CID iteration for query '%s'", label)
            for day in daterange(start, end):
                day_str = format_yyyy_mm_dd(day)
                url = build_priref_url(day_str, day_str, label)
                logger.info("CID %s URL for %s: %s", label, day_str, url)
                text = fetch_text(session, url)
                append_priref_block(OUTPUT_FILE_PATH, label, day_str, day_str, text)
                raw_prirefs.extend(
                    line.strip() for line in text.splitlines() if line.strip()
                )

    logger.info("Prirefs written to %s", OUTPUT_FILE_PATH)

    stats.prirefs_total = len(raw_prirefs)

    seen = set()
    prirefs = []
    for p in raw_prirefs:
        if p not in seen:
            seen.add(p)
            prirefs.append(p)

    stats.prirefs_unique = len(prirefs)
    logger.info(
        "Fetched prirefs total=%d unique=%d duplicates_removed=%d",
        stats.prirefs_total,
        stats.prirefs_unique,
        stats.prirefs_total - stats.prirefs_unique,
    )
    return prirefs


def fetch_item_xml(session: requests.Session, priref: str) -> str:
    url = CID_ITEM_URL_TEMPLATE.format(base_url=CID_BASE_URL, priref=priref)
    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.text


def xml_to_document(xml_text: str, priref: str) -> dict:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise ValueError(f"{priref} - XML parse error: {e}") from e

    if root.tag != "item" and root.find(".//item") is None:
        snippet = xml_text[:500].replace("\n", " ")
        raise ValueError(
            f"{priref} - invalid XML (no <item> element). Response starts: {snippet}"
        )

    doc = parker.data(root)

    if not isinstance(doc, dict):
        raise ValueError(
            f"{priref} - converted document is not a dict, got {type(doc).__name__}"
        )

    return doc


def make_bulk_action(priref: str, doc: dict) -> dict:
    return {
        "_op_type": "index",
        "_index": ES_INDEX,
        "_id": priref,
        "_source": doc,
    }


def action_generator(
    session: requests.Session,
    prirefs: list[str],
    stats: Stats,
) -> Iterator[dict]:
    for count, priref in enumerate(prirefs, 1):
        if count % PROGRESS_EVERY == 0:
            logger.info("Progress: prepared %d/%d prirefs", count, len(prirefs))

        cid_url = CID_ITEM_URL_TEMPLATE.format(base_url=CID_BASE_URL, priref=priref)
        xml_text = None

        try:
            xml_text = fetch_item_xml(session, priref)
            stats.cid_fetch_ok += 1
            time.sleep(CID_REQUEST_DELAY)
        except requests.HTTPError as e:
            stats.cid_fetch_fail += 1
            status_code = e.response.status_code if e.response is not None else None
            logger.error(
                "%s - CID HTTP error status=%s url=%s error=%s",
                priref,
                status_code,
                cid_url,
                e,
            )
            write_dead_letter(
                stats,
                stage="cid_fetch",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                http_status=status_code,
            )
            continue
        except requests.RequestException as e:
            stats.cid_fetch_fail += 1
            logger.error(
                "%s - could not fetch xml from CID API url=%s error=%s",
                priref,
                cid_url,
                e,
            )
            write_dead_letter(
                stats,
                stage="cid_fetch",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
            )
            continue

        try:
            doc = xml_to_document(xml_text, priref)
            stats.xml_parse_ok += 1
        except ValueError as e:
            stats.xml_parse_fail += 1
            logger.error(str(e))
            write_dead_letter(
                stats,
                stage="xml_parse",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                xml_text=xml_text,
            )
            continue
        except Exception as e:
            stats.xml_parse_fail += 1
            logger.exception("%s - unexpected XML conversion error", priref)
            write_dead_letter(
                stats,
                stage="xml_parse",
                priref=priref,
                error=f"unexpected XML conversion error: {e}",
                cid_url=cid_url,
                xml_text=xml_text,
            )
            continue

        stats.docs_prepared += 1
        yield make_bulk_action(priref, doc)


def bulk_index(es: Elasticsearch, actions: Iterator[dict], stats: Stats) -> None:
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
            stats.es_index_ok += 1
            if stats.es_index_ok % PROGRESS_EVERY == 0:
                logger.info(
                    "Elasticsearch indexed %d docs so far; latest priref=%s result=%s version=%s",
                    stats.es_index_ok,
                    priref,
                    result.get("result"),
                    result.get("_version"),
                )
        else:
            stats.es_index_fail += 1
            es_error = result.get("error")
            logger.error(
                "%s - Elasticsearch bulk %s failed status=%s error=%s",
                priref,
                op_type,
                result.get("status"),
                es_error,
            )
            write_dead_letter(
                stats,
                stage="es_index",
                priref=priref,
                error=f"Elasticsearch bulk {op_type} failed",
                http_status=result.get("status"),
                es_error=es_error,
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
        raise RuntimeError(
            f"Could not connect to Elasticsearch at {ES_URL}: {e}"
        ) from e


def main() -> int:
    args = parse_args()

    stats = Stats()
    session = build_requests_session()
    es = build_es_client()

    if args.prirefs:
        logger.info(
            "Direct priref mode; priref_count=%d query=ignored dead-letter file=%s",
            len(args.prirefs),
            DEAD_LETTER_PATH,
        )
        prirefs = args.prirefs
    elif args.prirefs_csv:
        prirefs = args.prirefs_csv
        logger.info(
            "CSV priref mode; priref_count=%d csv=%s dead-letter file=%s",
            len(prirefs),
            args.prirefs_csv,
            DEAD_LETTER_PATH,
        )
    else:
        resolved_date_from, resolved_date_to = resolve_date_range(
            args.date_from, args.date_to
        )

        if resolved_date_from > resolved_date_to:
            logger.error("Invalid date range: --date-from must be <= --date-to")
            return 2

        logger.info(
            "Starting run; date_from=%s date_to=%s query=%s dead-letter file=%s",
            resolved_date_from,
            resolved_date_to,
            args.query,
            DEAD_LETTER_PATH,
        )

        prirefs = fetch_prirefs(
            session,
            stats,
            resolved_date_from,
            resolved_date_to,
            args.query,
        )

    try:
        ping_es(es)
        actions = action_generator(session, prirefs, stats)
        bulk_index(es, actions, stats)
        stats.log_summary()
        return 0

    except ApiError as e:
        logger.exception("Elasticsearch API error: %s", e)
        return 1
    except requests.RequestException as e:
        logger.exception("HTTP error during run: %s", e)
        return 1
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
