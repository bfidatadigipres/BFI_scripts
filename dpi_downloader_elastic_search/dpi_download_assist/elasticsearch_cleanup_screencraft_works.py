#!/usr/bin/env python3
"""
Detect and clean stale Work documents in dpi_screencraft_works.

For screencraft object records changed within a date window (or supplied
directly by priref), the script:

1. reads each record's live CID Work link (moving_image_work.priref)
2. searches the works index for docs whose media_objects contain the
   record's object number
3. classifies each hit as correct (priref matches the live CID work link)
   or stale
4. for each stale Work doc, checks CID for records still linking to it,
   counting only in-scope links (linked objects that carry media):
   - if some exist, the Work doc is refreshed from CID (update)
   - if none exist, the Work doc is deleted
"""

from __future__ import annotations

import os
import argparse
import time
from typing import Optional
from urllib.parse import quote

import requests
import defusedxml.ElementTree as ET

from elasticsearch_index_shared import (
    build_es_client,
    build_requests_session,
    fetch_prirefs,
    fetch_text,
    ping_es,
    resolve_date_range,
    setup_logger,
    validate_date,
    validate_prirefs,
    validate_prirefs_csv,
    validate_xml_root,
    write_dead_letter,
    xml_to_document as generic_xml_to_document,
)
from elastic_transport import ApiError
from elasticsearch.helpers import streaming_bulk

# =========================
# Configuration
# =========================

CID_BASE_URL = os.environ.get("CID_API1")
ES_URL = os.environ.get("ES_SEARCH_PATH")
ES_INDEX = "dpi_screencraft_works"
OBJECTS_DB = "elasticsearchscreencraft_objects"
WORKS_DB = "elasticsearchscreencraft_works"
ROOT_XML_TAG = "screencraft"
LOG = os.environ.get("LOG_PATH")
OBJECTS_DF = "'archival item','digital derivative','internal object'"
DEFAULT_DATE_QUERY = (
    "Df={df} and (modification>='{date_from}' and modification<='{date_to}')"
)
REVERSE_QUERY_TEMPLATE = "related_object.reference->(priref={priref})"
CID_ITEM_URL_TEMPLATE = (
    "{base_url}?database={db_name}&search=priref={priref}"
)
OUTPUT_FILE_PATH = os.path.join(LOG, "screencraft_work_cleanup_prirefs.txt")
LOG_PATH = os.path.join(LOG, "screencraft_work_cleanup.log")
DEAD_LETTER_PATH = os.path.join(LOG, "screencraft_work_cleanup_dead_letter.jsonl")

# HTTP / CID settings
HTTP_TIMEOUT = (10, 60)
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
# Stats tracking
# =========================


class CleanupStats:
    def __init__(self) -> None:
        self.prirefs_total = 0
        self.prirefs_unique = 0
        self.cid_fetch_ok = 0
        self.cid_fetch_fail = 0
        self.xml_parse_ok = 0
        self.xml_parse_fail = 0
        self.no_work_link = 0
        self.es_search_fail = 0
        self.es_no_hits = 0
        self.es_correct_only = 0
        self.anomalies = 0
        self.stale_docs = 0
        self.reverse_links = 0
        self.reverse_links_in_scope = 0
        self.reverse_links_out_of_scope = 0
        self.stale_updated = 0
        self.stale_deleted = 0
        self.es_ok = 0
        self.es_fail = 0
        self.dead_letter_written = 0
        self.start_time = time.time()

    def log_summary(self, logger) -> None:
        elapsed = time.time() - self.start_time
        logger.info(
            "SUMMARY elapsed=%.2fs total_prirefs=%d unique_prirefs=%d "
            "cid_fetch_ok=%d cid_fetch_fail=%d xml_parse_ok=%d xml_parse_fail=%d "
            "no_work_link=%d es_search_fail=%d es_no_hits=%d es_correct_only=%d "
            "anomalies=%d stale_docs=%d reverse_links=%d "
            "reverse_links_in_scope=%d reverse_links_out_of_scope=%d "
            "stale_updated=%d "
            "stale_deleted=%d es_ok=%d es_fail=%d dead_letter_written=%d",
            elapsed,
            self.prirefs_total,
            self.prirefs_unique,
            self.cid_fetch_ok,
            self.cid_fetch_fail,
            self.xml_parse_ok,
            self.xml_parse_fail,
            self.no_work_link,
            self.es_search_fail,
            self.es_no_hits,
            self.es_correct_only,
            self.anomalies,
            self.stale_docs,
            self.reverse_links,
            self.reverse_links_in_scope,
            self.reverse_links_out_of_scope,
            self.stale_updated,
            self.stale_deleted,
            self.es_ok,
            self.es_fail,
            self.dead_letter_written,
        )


# =========================
# CID query helpers
# =========================


def quote_search(search: str) -> str:
    return quote(search, safe="()=*'").replace(" ", "%20")


def build_priref_url(date_from: str, date_to: str) -> str:
    search = DEFAULT_DATE_QUERY.format(
        df=OBJECTS_DF, date_from=date_from, date_to=date_to
    )
    url = (
        f"{CID_BASE_URL}?database=prirefcollectraw"
        f"&search={quote_search(search)}"
        f"&limit=0"
    )
    return url


def build_reverse_url(priref: str) -> str:
    search = REVERSE_QUERY_TEMPLATE.format(priref=priref)
    url = (
        f"{CID_BASE_URL}?database=prirefcollectraw"
        f"&search={quote_search(search)}"
        f"&limit=0"
    )
    return url


# =========================
# CID / XML helpers
# =========================


def fetch_item_xml(session: requests.Session, priref: str) -> str:
    url = CID_ITEM_URL_TEMPLATE.format(
        base_url=CID_BASE_URL,
        db_name=OBJECTS_DB,
        priref=priref
    )
    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.text


def fetch_work_xml(session: requests.Session, priref: str) -> str:
    url = CID_ITEM_URL_TEMPLATE.format(
        base_url=CID_BASE_URL,
        db_name=WORKS_DB,
        priref=priref
    )
    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.text


def extract_object_fields(
    xml_text: str, priref: str
) -> tuple[Optional[str], Optional[str]]:
    """Return (object_number, work_link_priref) from a screencraft object XML."""
    validate_xml_root(xml_text, priref, ROOT_XML_TAG)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise ValueError(f"{priref} - XML parse error: {e}") from e

    object_number = None
    number_el = root.find("screencraft_object_number")
    if number_el is not None and number_el.text:
        object_number = number_el.text.strip()

    work_link = None
    mw_el = root.find("moving_image_work")
    if mw_el is not None:
        priref_el = mw_el.find("priref")
        if priref_el is not None and priref_el.text:
            work_link = priref_el.text.strip()

    return object_number, work_link


def fetch_reverse_links(session: requests.Session, priref: str) -> list[str]:
    """Return prirefs of records whose related_object references the work."""
    url = build_reverse_url(priref)
    text = fetch_text(session, url, HTTP_TIMEOUT)
    return [line.strip() for line in text.splitlines() if line.strip()]


def object_has_media(xml_text: str) -> bool:
    """True if a screencraft object XML carries an indexed media record."""
    root = ET.fromstring(xml_text)
    for block in root.findall("media_objects"):
        for tag in ("original_filename", "identifier_preservation_file"):
            element = block.find(tag)
            if element is not None and element.text:
                return True
    return False


def count_in_scope_reverse_links(
    session: requests.Session,
    stats: CleanupStats,
    priref: str,
    logger,
) -> tuple[int, int, int]:
    """Count reverse screencraft links for a work, split by index scope.

    Returns (total, in_scope, out_of_scope). A link is in scope when the
    linked object carries media (media_objects with identifiers). Objects
    whose XML cannot be fetched or parsed are treated as in scope so that
    nothing unverifiable is ever deleted.
    """
    try:
        linked_prirefs = fetch_reverse_links(session, priref)
    except requests.RequestException as e:
        stats.cid_fetch_fail += 1
        logger.error("%s - reverse link check failed error=%s", priref, e)
        write_dead_letter(
            stats,
            DEAD_LETTER_PATH,
            stage="cid_reverse",
            priref=priref,
            error=f"reverse link check failed: {e}",
            cid_url=build_reverse_url(priref),
            max_error_snippet=MAX_ERROR_SNIPPET,
        )
        return 0, 0, 0

    in_scope = 0
    out_of_scope = 0
    for linked_priref in linked_prirefs:
        cid_url = CID_ITEM_URL_TEMPLATE.format(
            base_url=CID_BASE_URL,
            db_name=OBJECTS_DB,
            priref=linked_priref
        )
        try:
            xml_text = fetch_item_xml(session, linked_priref)
            stats.cid_fetch_ok += 1
            time.sleep(0.25)
        except requests.RequestException as e:
            stats.cid_fetch_fail += 1
            logger.warning(
                "%s - could not fetch reverse link %s (treating as in scope) error=%s",
                priref,
                linked_priref,
                e,
            )
            write_dead_letter(
                stats,
                DEAD_LETTER_PATH,
                stage="cid_reverse",
                priref=linked_priref,
                error=f"media check fetch failed for reverse link of {priref}: {e}",
                cid_url=cid_url,
                max_error_snippet=MAX_ERROR_SNIPPET,
            )
            in_scope += 1
            continue

        try:
            has_media = object_has_media(xml_text)
            stats.xml_parse_ok += 1
        except ET.ParseError as e:
            stats.xml_parse_fail += 1
            logger.warning(
                "%s - could not parse reverse link %s (treating as in scope) error=%s",
                priref,
                linked_priref,
                e,
            )
            write_dead_letter(
                stats,
                DEAD_LETTER_PATH,
                stage="cid_reverse",
                priref=linked_priref,
                error=f"media check parse failed for reverse link of {priref}: {e}",
                cid_url=cid_url,
                max_error_snippet=MAX_ERROR_SNIPPET,
            )
            in_scope += 1
            continue

        if has_media:
            in_scope += 1
        else:
            out_of_scope += 1
            logger.info(
                "%s - reverse link %s has no media (out of index scope)",
                priref,
                linked_priref,
            )

    return len(linked_prirefs), in_scope, out_of_scope


# =========================
# Elasticsearch helpers
# =========================


def search_works_by_object_number(es, object_number: str) -> list[dict]:
    query = {
        "nested": {
            "path": "media_objects",
            "query": {"term": {"media_objects.object_number": object_number}},
        }
    }
    response = es.search(
        index=ES_INDEX,
        query=query,
        size=50,
        source=["screencraft_priref", "title"],
    )
    hits = []
    for hit in response["hits"]["hits"]:
        src = hit.get("_source") or {}
        hits.append(
            {
                "id": hit["_id"],
                "priref": str(src.get("screencraft_priref") or hit["_id"]),
                "title": src.get("title"),
            }
        )
    return hits


# =========================
# Candidate processing
# =========================


def prepare_work_update(
    session: requests.Session, stats: CleanupStats, priref: str, logger
) -> Optional[dict]:
    """Fetch a work record from CID and convert to a fresh ES document."""
    cid_url = CID_ITEM_URL_TEMPLATE.format(
        base_url=CID_BASE_URL,
        db_name=WORKS_DB,
        priref=priref
    )
    try:
        xml_text = fetch_work_xml(session, priref)
        stats.cid_fetch_ok += 1
        time.sleep(0.25)
    except requests.RequestException as e:
        stats.cid_fetch_fail += 1
        logger.error(
            "%s - could not fetch work XML from CID API url=%s error=%s",
            priref,
            cid_url,
            e,
        )
        write_dead_letter(
            stats,
            DEAD_LETTER_PATH,
            stage="cid_fetch",
            priref=priref,
            error=str(e),
            cid_url=cid_url,
            max_xml_snippet=MAX_XML_SNIPPET,
            max_doc_snippet=MAX_DOC_SNIPPET,
            max_error_snippet=MAX_ERROR_SNIPPET,
        )
        return None

    try:
        doc = generic_xml_to_document(xml_text, priref, root_tag=ROOT_XML_TAG)
    except ValueError as e:
        stats.xml_parse_fail += 1
        logger.error(str(e))
        write_dead_letter(
            stats,
            DEAD_LETTER_PATH,
            stage="xml_parse",
            priref=priref,
            error=str(e),
            cid_url=cid_url,
            xml_text=xml_text,
            max_xml_snippet=MAX_XML_SNIPPET,
            max_doc_snippet=MAX_DOC_SNIPPET,
            max_error_snippet=MAX_ERROR_SNIPPET,
        )
        return None

    doc_priref = doc.get("screencraft_priref")
    if str(doc_priref) != priref:
        stats.xml_parse_fail += 1
        message = (
            f"{priref} - work record priref mismatch: "
            f"expected {priref}, got {doc_priref}"
        )
        logger.error(message)
        write_dead_letter(
            stats,
            DEAD_LETTER_PATH,
            stage="xml_parse",
            priref=priref,
            error=message,
            cid_url=cid_url,
            xml_text=xml_text,
            max_xml_snippet=MAX_XML_SNIPPET,
            max_doc_snippet=MAX_DOC_SNIPPET,
            max_error_snippet=MAX_ERROR_SNIPPET,
        )
        return None

    return doc


def process_candidates(
    session: requests.Session,
    es,
    stats: CleanupStats,
    prirefs: list[str],
    logger,
) -> list[dict]:
    actions: list[dict] = []
    seen_stale: set[str] = set()

    for count, priref in enumerate(prirefs, 1):
        if count % PROGRESS_EVERY == 0:
            logger.info("Progress: processed %d/%d candidates", count, len(prirefs))

        cid_url = CID_ITEM_URL_TEMPLATE.format(
            base_url=CID_BASE_URL,
            db_name=OBJECTS_DB,
            priref=priref
        )
        try:
            xml_text = fetch_item_xml(session, priref)
            stats.cid_fetch_ok += 1
            time.sleep(0.25)
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
                DEAD_LETTER_PATH,
                stage="cid_fetch",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                http_status=status_code,
                max_xml_snippet=MAX_XML_SNIPPET,
                max_doc_snippet=MAX_DOC_SNIPPET,
                max_error_snippet=MAX_ERROR_SNIPPET,
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
                DEAD_LETTER_PATH,
                stage="cid_fetch",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                max_xml_snippet=MAX_XML_SNIPPET,
                max_doc_snippet=MAX_DOC_SNIPPET,
                max_error_snippet=MAX_ERROR_SNIPPET,
            )
            continue

        try:
            object_number, work_link = extract_object_fields(xml_text, priref)
            stats.xml_parse_ok += 1
        except ValueError as e:
            stats.xml_parse_fail += 1
            logger.error(str(e))
            write_dead_letter(
                stats,
                DEAD_LETTER_PATH,
                stage="xml_parse",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                xml_text=xml_text,
                max_xml_snippet=MAX_XML_SNIPPET,
                max_doc_snippet=MAX_DOC_SNIPPET,
                max_error_snippet=MAX_ERROR_SNIPPET,
            )
            continue

        if not work_link:
            stats.no_work_link += 1
            continue

        try:
            hits = search_works_by_object_number(es, object_number)
        except Exception as e:
            stats.es_search_fail += 1
            logger.exception(
                "%s - Elasticsearch search failed for object_number=%s",
                priref,
                object_number,
            )
            write_dead_letter(
                stats,
                DEAD_LETTER_PATH,
                stage="es_search",
                priref=priref,
                error=f"Elasticsearch search failed: {e}",
                document={"object_number": object_number, "work_link": work_link},
                max_doc_snippet=MAX_DOC_SNIPPET,
                max_error_snippet=MAX_ERROR_SNIPPET,
            )
            continue

        if not hits:
            stats.es_no_hits += 1
            continue

        correct = [h for h in hits if h["priref"] == work_link]
        stale = [h for h in hits if h["priref"] != work_link]

        if not correct:
            stats.anomalies += 1
            logger.warning(
                "%s - %d work doc(s) carry object_number=%s but none match "
                "the live CID work link %s; hits=%s",
                priref,
                len(hits),
                object_number,
                work_link,
                [h["priref"] for h in hits],
            )
            write_dead_letter(
                stats,
                DEAD_LETTER_PATH,
                stage="es_review",
                priref=priref,
                error=(
                    "stale work docs found but none matches the live CID "
                    f"work link {work_link}"
                ),
                cid_url=cid_url,
                document={
                    "object_number": object_number,
                    "work_link": work_link,
                    "hits": hits,
                },
                max_doc_snippet=MAX_DOC_SNIPPET,
                max_error_snippet=MAX_ERROR_SNIPPET,
            )
            continue

        if not stale:
            stats.es_correct_only += 1
            continue

        for hit in stale:
            stale_priref = hit["priref"]
            if stale_priref in seen_stale:
                continue
            seen_stale.add(stale_priref)
            stats.stale_docs += 1

            total, in_scope, out_of_scope = count_in_scope_reverse_links(
                session, stats, stale_priref, logger
            )

            stats.reverse_links += total
            stats.reverse_links_in_scope += in_scope
            stats.reverse_links_out_of_scope += out_of_scope

            if in_scope > 0:
                doc = prepare_work_update(session, stats, stale_priref, logger)
                if doc is None:
                    continue
                stats.stale_updated += 1
                actions.append(
                    {
                        "_op_type": "index",
                        "_index": ES_INDEX,
                        "_id": stale_priref,
                        "_source": doc,
                    }
                )
                logger.info(
                    "%s - stale work %s (%s) still linked by %d in-scope "
                    "screencraft record(s) in CID (%d total, %d without "
                    "media); refreshing ES doc",
                    priref,
                    stale_priref,
                    hit["title"],
                    in_scope,
                    total,
                    out_of_scope,
                )
            else:
                stats.stale_deleted += 1
                actions.append(
                    {
                        "_op_type": "delete",
                        "_index": ES_INDEX,
                        "_id": stale_priref,
                    }
                )
                logger.info(
                    "%s - stale work %s (%s) has no in-scope screencraft "
                    "links left in CID (%d total, %d without media); "
                    "scheduling ES doc deletion",
                    priref,
                    stale_priref,
                    hit["title"],
                    total,
                    out_of_scope,
                )

    return actions


# =========================
# Bulk execution
# =========================


def execute_actions(es, actions: list[dict], stats: CleanupStats, logger) -> None:
    if not actions:
        logger.info("No cleanup actions to execute")
        return

    for ok, item in streaming_bulk(
        client=es,
        actions=iter(actions),
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
        stage = "es_update" if op_type == "index" else "es_delete"

        if ok:
            if op_type == "delete" and result.get("result") == "not_found":
                stats.es_fail += 1
                logger.warning(
                    "%s - document not found for deletion (already gone)",
                    priref,
                )
                write_dead_letter(
                    stats,
                    DEAD_LETTER_PATH,
                    stage=stage,
                    priref=priref,
                    error="document not found for deletion",
                    http_status=result.get("status"),
                    max_error_snippet=MAX_ERROR_SNIPPET,
                )
                continue
            stats.es_ok += 1
            logger.info(
                "Elasticsearch %s ok priref=%s result=%s",
                op_type,
                priref,
                result.get("result"),
            )
        else:
            stats.es_fail += 1
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
                DEAD_LETTER_PATH,
                stage=stage,
                priref=priref,
                error=f"Elasticsearch bulk {op_type} failed",
                http_status=result.get("status"),
                es_error=es_error,
                max_error_snippet=MAX_ERROR_SNIPPET,
            )


# =========================
# CLI
# =========================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect and clean stale Work documents in dpi_screencraft_works."
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
        help="Comma-separated list of screencraft object prirefs to check "
        "directly (max 1000). Mutually exclusive with --prirefs-csv and "
        "date arguments",
    )
    group.add_argument(
        "--prirefs-csv",
        default=None,
        type=validate_prirefs_csv,
        help="Path to CSV file with a single column of prirefs (no limit). "
        "Mutually exclusive with --prirefs and date arguments",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full analysis and report planned updates/deletions "
        "without writing to Elasticsearch",
    )

    return parser.parse_args()


# =========================
# Main
# =========================


def main() -> int:
    args = parse_args()
    logger = setup_logger("screencraft_work_cleanup", LOG_PATH)

    stats = CleanupStats()
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
            query_label="cleanup",
            http_timeout=HTTP_TIMEOUT,
            logger=logger,
        )

    stats.prirefs_total = max(stats.prirefs_total, len(prirefs))
    stats.prirefs_unique = max(stats.prirefs_unique, len(prirefs))

    try:
        ping_es(es, ES_URL, logger)

        actions = process_candidates(session, es, stats, prirefs, logger)

        if args.dry_run:
            logger.info(
                "DRY RUN - %d cleanup action(s) would be performed (index=%s):",
                len(actions),
                ES_INDEX,
            )
            for action in actions:
                if action["_op_type"] == "index":
                    logger.info(
                        "  would update priref=%s (refresh from CID)",
                        action["_id"],
                    )
                else:
                    logger.info("  would delete priref=%s", action["_id"])
            logger.info("DRY RUN complete - no documents were changed.")
        else:
            execute_actions(es, actions, stats, logger)

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
