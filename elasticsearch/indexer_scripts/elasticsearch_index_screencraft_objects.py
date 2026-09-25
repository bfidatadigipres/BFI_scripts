#!/usr/bin/env python3
"""
Fetch screencraft Object prirefs from CID, retrieve XML, and bulk index into
Elasticsearch (dpi_screencraft).
"""

from __future__ import annotations

import os
import argparse
from urllib.parse import quote

import requests

from elasticsearch_index_shared import (
    Stats,
    action_generator,
    build_es_client,
    build_requests_session,
    bulk_index,
    fetch_prirefs,
    fetch_prirefs_single,
    ping_es,
    resolve_date_range,
    setup_logger,
    validate_date,
    validate_prirefs,
    validate_prirefs_csv,
    xml_to_document as generic_xml_to_document,
)
from elastic_transport import ApiError

# =========================
# Configuration
# =========================

CID_BASE_URL = os.environ.get("CID_API1")
ES_URL = os.environ.get("ES_SEARCH_PATH")
ES_INDEX = "dpi_screencraft"
DB_NAME = "elasticsearchscreencraft_objects"
ROOT_XML_TAG = "screencraft"

DEFAULT_DATE_QUERY = (
    "Df='archival item','digital derivative','internal object'"
    " and (modification>='{date_from}' and modification<='{date_to} 23:59:59')"
)

CID_ITEM_URL_TEMPLATE = (
    "{base_url}?database={db_name}&search=priref={priref}"
)

LOGS = os.environ.get("LOG_PATH")
OUTPUT_FILE_PATH = os.path.join(LOGS, "screencraft_object_prirefs.txt")
LOG_PATH = os.path.join(LOGS, "screencraft_object_indexing.log")
DEAD_LETTER_PATH = os.path.join(LOGS, "screencraft_object_dead_letter.jsonl")

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
# CID query helpers
# =========================


def build_priref_url(date_from: str, date_to: str) -> str:
    safe_chars = "()=*' "
    search = DEFAULT_DATE_QUERY.format(date_from=date_from, date_to=date_to)
    url = (
        f"{CID_BASE_URL}?database=prirefcollectraw"
        f"&search={quote(search, safe=safe_chars)}"
        f"&limit=0"
    )
    return url.replace(" ", "%20")


def build_custom_url(search: str) -> str:
    safe_chars = "()=*' "
    url = (
        f"{CID_BASE_URL}?database=prirefcollectraw"
        f"&search={quote(search, safe=safe_chars)}"
        f"&limit=0"
    )
    return url.replace(" ", "%20")


# =========================
# XML / document helpers
# =========================


def fetch_item_xml(session: requests.Session, priref: str) -> str:
    url = CID_ITEM_URL_TEMPLATE.format(
        base_url=CID_BASE_URL,
        db_name=DB_NAME,
        priref=priref
    )
    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.text


def xml_to_document(xml_text: str, priref: str) -> dict:
    doc = generic_xml_to_document(xml_text, priref, root_tag=ROOT_XML_TAG)
    pd = doc.get("production_date")
    if isinstance(pd, str) and len(pd) >= 4:
        try:
            doc["production_date"] = int(pd[:4])
        except ValueError:
            pass
    return doc


# =========================
# CLI
# =========================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch screencraft Object prirefs from CID, retrieve XML, "
        "and bulk index into Elasticsearch."
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

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--prirefs",
        default=None,
        type=validate_prirefs,
        help="Comma-separated list of prirefs to index directly (max 1000). "
        "Mutually exclusive with --search, --prirefs-csv, and date arguments",
    )
    group.add_argument(
        "--prirefs-csv",
        default=None,
        type=validate_prirefs_csv,
        help="Path to CSV file with a single column of prirefs (no limit). "
        "Mutually exclusive with --prirefs, --search, and date arguments",
    )
    group.add_argument(
        "--search",
        default=None,
        type=str,
        help="Custom CID search query string. Overrides the default date-range "
        "query. Mutually exclusive with --prirefs",
    )

    return parser.parse_args()


# =========================
# Main
# =========================


def main() -> int:
    args = parse_args()
    logger = setup_logger("screencraft_object_indexer", LOG_PATH)

    stats = Stats()
    session = build_requests_session(
        http_timeout=HTTP_TIMEOUT,
        http_retries=HTTP_RETRIES,
        http_backoff=HTTP_BACKOFF,
    )
    es = build_es_client(
        ES_URL,
        request_timeout=ES_REQUEST_TIMEOUT,
        max_retries=ES_MAX_RETRIES,
        retry_on_timeout=ES_RETRY_ON_TIMEOUT,
    )

    if args.prirefs:
        logger.info(
            "Direct priref mode; priref_count=%s dead-letter file=%s",
            len(args.prirefs),
            DEAD_LETTER_PATH,
        )
        prirefs = args.prirefs
    elif args.prirefs_csv:
        prirefs = args.prirefs_csv
        logger.info(
            "CSV priref mode; priref_count=%s csv=%s dead-letter file=%s",
            len(prirefs),
            args.prirefs_csv,
            DEAD_LETTER_PATH,
        )
    elif args.search:
        logger.info(
            "Custom search mode; search=%s dead-letter file=%s",
            args.search,
            DEAD_LETTER_PATH,
        )
        prirefs = fetch_prirefs_single(
            session,
            stats,
            args.search,
            build_custom_url,
            output_file_path=OUTPUT_FILE_PATH,
            http_timeout=HTTP_TIMEOUT,
            logger=logger,
        )
    else:
        resolved_from, resolved_to = resolve_date_range(args.date_from, args.date_to)

        if resolved_from > resolved_to:
            logger.error("Invalid date range: --date-from must be <= --date-to")
            return 2

        logger.info(
            "Starting run; date_from=%s date_to=%s dead-letter file=%s",
            resolved_from,
            resolved_to,
            DEAD_LETTER_PATH,
        )

        prirefs = fetch_prirefs(
            session,
            stats,
            resolved_from,
            resolved_to,
            build_priref_url,
            output_file_path=OUTPUT_FILE_PATH,
            query_label="objects",
            http_timeout=HTTP_TIMEOUT,
            logger=logger,
        )

    try:
        ping_es(es, ES_URL, logger)

        item_url = CID_ITEM_URL_TEMPLATE.format(
            base_url=CID_BASE_URL,
            db_name=DB_NAME,
            priref="{priref}"
        )

        actions = action_generator(
            session,
            prirefs,
            stats,
            es_index=ES_INDEX,
            fetch_xml=fetch_item_xml,
            xml_to_doc=xml_to_document,
            dead_letter_path=DEAD_LETTER_PATH,
            cid_item_url_template=item_url,
            progress_every=PROGRESS_EVERY,
            max_xml_snippet=MAX_XML_SNIPPET,
            max_doc_snippet=MAX_DOC_SNIPPET,
            max_error_snippet=MAX_ERROR_SNIPPET,
            logger=logger,
        )

        bulk_index(
            es,
            actions,
            stats,
            dead_letter_path=DEAD_LETTER_PATH,
            chunk_size=BULK_CHUNK_SIZE,
            max_chunk_bytes=BULK_MAX_CHUNK_BYTES,
            max_retries=BULK_MAX_RETRIES,
            initial_backoff=BULK_INITIAL_BACKOFF,
            max_backoff=BULK_MAX_BACKOFF,
            progress_every=PROGRESS_EVERY,
            max_error_snippet=MAX_ERROR_SNIPPET,
            logger=logger,
        )

        stats.log_summary(logger)
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
