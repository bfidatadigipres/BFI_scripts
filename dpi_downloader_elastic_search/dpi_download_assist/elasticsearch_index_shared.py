#!/usr/bin/env python3
"""
Shared utilities for Elasticsearch indexing scripts.
Provides HTTP session management, ES client setup, bulk indexing pipeline,
dead-letter queue, logging helpers, and CID priref fetching.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import date, datetime, timezone, timedelta
from typing import Any, Callable, Iterator, Optional

import requests
import defusedxml.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from xmljson import parker

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# =========================
# Logging setup
# =========================


def setup_logger(name: str, log_path: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    return logger


# =========================
# Date / time helpers
# =========================


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_date(date_str: str) -> str:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{date_str}'. Expected format: YYYY-MM-DD"
        ) from e
    return date_str


MAX_DIRECT_PRIREFS = 1000
CID_REQUEST_DELAY = 0.25


def validate_prirefs(priref_string: str) -> list[str]:
    prirefs = [p.strip() for p in priref_string.split(",")]
    if not prirefs or any(not p for p in prirefs):
        raise argparse.ArgumentTypeError(
            f"Invalid prirefs '{priref_string}'. "
            "Each priref must be non-empty numeric value, comma-separated"
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


def validate_prirefs_csv(csv_path: str) -> list[str]:
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


def daterange(start_date: date, end_date: date) -> Iterator[date]:
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


# =========================
# Text / snippet helpers
# =========================


def truncate_text(value: Optional[str], max_len: int) -> Optional[str]:
    if value is None:
        return None
    if len(value) <= max_len:
        return value
    return value[:max_len] + f"... [truncated {len(value) - max_len} chars]"


def safe_json_dumps(value: Any, max_len: int) -> Optional[str]:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = repr(value)
    return truncate_text(text, max_len)


# =========================
# HTTP / ES clients
# =========================


def build_requests_session(
    http_timeout: tuple[int, int] = (10, 60),
    http_retries: int = 5,
    http_backoff: float = 1.0,
    pool_connections: int = 20,
    pool_maxsize: int = 50,
) -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=http_retries,
        connect=http_retries,
        read=http_retries,
        status=http_retries,
        backoff_factor=http_backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def build_es_client(
    es_url: str,
    request_timeout: int = 60,
    max_retries: int = 5,
    retry_on_timeout: bool = True,
) -> Elasticsearch:
    return Elasticsearch(
        es_url,
        request_timeout=request_timeout,
        max_retries=max_retries,
        retry_on_timeout=retry_on_timeout,
    )


def ping_es(es: Elasticsearch, es_url: str, logger: logging.Logger) -> None:
    try:
        info = es.info()
        logger.info(
            "Connected to Elasticsearch at %s; cluster_name=%s version=%s",
            es_url,
            info.get("cluster_name"),
            info.get("version", {}).get("number"),
        )
    except Exception as e:
        raise RuntimeError(
            f"Could not connect to Elasticsearch at {es_url}: {e}"
        ) from e


# =========================
# Stats tracking
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

    def log_summary(self, logger: logging.Logger) -> None:
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


# =========================
# Dead-letter queue
# =========================


def write_dead_letter(
    stats: Stats,
    dead_letter_path: str,
    stage: str,
    priref: str,
    error: str,
    *,
    cid_url: Optional[str] = None,
    http_status: Optional[int] = None,
    xml_text: Optional[str] = None,
    document: Optional[dict] = None,
    es_error: Optional[Any] = None,
    max_xml_snippet: int = 2000,
    max_doc_snippet: int = 5000,
    max_error_snippet: int = 5000,
) -> None:
    record = {
        "timestamp": utc_now_iso(),
        "stage": stage,
        "priref": priref,
        "error": truncate_text(error, max_error_snippet),
        "cid_url": cid_url,
        "http_status": http_status,
        "xml_snippet": (truncate_text(xml_text, max_xml_snippet) if xml_text else None),
        "document_snippet": (
            safe_json_dumps(document, max_doc_snippet) if document is not None else None
        ),
        "es_error": (
            safe_json_dumps(es_error, max_error_snippet)
            if es_error is not None
            else None
        ),
    }

    with open(dead_letter_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    stats.dead_letter_written += 1


# =========================
# CID priref fetching
# =========================


def fetch_text(
    session: requests.Session,
    url: str,
    http_timeout: tuple[int, int] = (10, 60),
) -> str:
    response = session.get(url, timeout=http_timeout)
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
    build_url: Callable[[str, str], str],
    output_file_path: str,
    *,
    query_label: str = "",
    direct_query_max_days: int = 2,
    http_timeout: tuple[int, int] = (10, 60),
    logger: logging.Logger,
) -> list[str]:
    logger.info(
        "Fetching prirefs from CID for date range %s to %s",
        date_from,
        date_to,
    )

    span_days = date_span_days_inclusive(date_from, date_to)

    logger.info(
        "Resolved CID priref date span: %d day(s); direct-query threshold=%d day(s)",
        span_days,
        direct_query_max_days,
    )

    raw_prirefs: list[str] = []

    with open(output_file_path, "w", encoding="utf-8") as txtfile:
        txtfile.write(
            f"# CID priref output for run\n"
            f"# date_from={date_from}\n"
            f"# date_to={date_to}\n"
            f"# span_days={span_days}\n\n"
        )

    label = query_label or "query"

    if span_days <= direct_query_max_days:
        logger.info(
            "Using direct CID query mode for %d day(s) (<= %d)",
            span_days,
            direct_query_max_days,
        )
        window_from = format_yyyy_mm_dd(parse_yyyy_mm_dd(date_from) - timedelta(days=1))
        window_to = format_yyyy_mm_dd(parse_yyyy_mm_dd(date_to) + timedelta(days=1))
        url = build_url(window_from, window_to)
        logger.info("CID URL: %s", url)
        text = fetch_text(session, url, http_timeout)
        append_priref_block(output_file_path, label, date_from, date_to, text)
        raw_prirefs.extend(line.strip() for line in text.splitlines() if line.strip())
    else:
        logger.info(
            "Using per-day CID query mode because span is %d day(s) (> %d)",
            span_days,
            direct_query_max_days,
        )
        start = parse_yyyy_mm_dd(date_from)
        end = parse_yyyy_mm_dd(date_to)

        for day in daterange(start, end):
            window_from = format_yyyy_mm_dd(day - timedelta(days=1))
            window_to = format_yyyy_mm_dd(day + timedelta(days=1))
            url = build_url(window_from, window_to)
            logger.info("CID URL for %s: %s", day_str := format_yyyy_mm_dd(day), url)
            text = fetch_text(session, url, http_timeout)
            append_priref_block(output_file_path, label, window_from, window_to, text)
            raw_prirefs.extend(
                line.strip() for line in text.splitlines() if line.strip()
            )

    logger.info("Prirefs written to %s", output_file_path)
    stats.prirefs_total = len(raw_prirefs)

    seen: set[str] = set()
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


def fetch_prirefs_single(
    session: requests.Session,
    stats: Stats,
    search: str,
    build_url: Callable[[str], str],
    output_file_path: str,
    *,
    http_timeout: tuple[int, int] = (10, 60),
    logger: logging.Logger,
) -> list[str]:
    logger.info("Fetching prirefs with custom search query")
    url = build_url(search)
    logger.info("CID custom URL: %s", url)
    text = fetch_text(session, url, http_timeout)

    with open(output_file_path, "w", encoding="utf-8") as txtfile:
        txtfile.write(f"# Custom search: {search}\n\n")
        txtfile.write(text.rstrip() + "\n")

    raw_prirefs = [line.strip() for line in text.splitlines() if line.strip()]

    logger.info("Prirefs written to %s", output_file_path)
    stats.prirefs_total = len(raw_prirefs)

    seen: set[str] = set()
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


# =========================
# Document / XML helpers
# =========================


def validate_xml_root(xml_text: str, priref: str, root_tag: str) -> None:
    if root_tag not in xml_text:
        snippet = xml_text[:500].replace("\n", " ")
        raise ValueError(
            f"{priref} - invalid XML (no <{root_tag}> element). "
            f"Response starts: {snippet}"
        )


def xml_to_document(xml_text: str, priref: str, root_tag: str = "item") -> dict:
    validate_xml_root(xml_text, priref, root_tag)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise ValueError(f"{priref} - XML parse error: {e}") from e

    doc = parker.data(root)

    if not isinstance(doc, dict):
        raise ValueError(
            f"{priref} - converted document is not a dict, got {type(doc).__name__}"
        )

    return doc


# =========================
# Bulk indexing pipeline
# =========================


def make_bulk_action(es_index: str, priref: str, doc: dict) -> dict:
    return {
        "_op_type": "index",
        "_index": es_index,
        "_id": priref,
        "_source": doc,
    }


def action_generator(
    session: requests.Session,
    prirefs: list[str],
    stats: Stats,
    *,
    es_index: str,
    fetch_xml: Callable[[requests.Session, str], str],
    xml_to_doc: Callable[[str, str], dict],
    dead_letter_path: str,
    cid_item_url_template: str,
    progress_every: int = 100,
    max_xml_snippet: int = 2000,
    max_doc_snippet: int = 5000,
    max_error_snippet: int = 5000,
    logger: logging.Logger,
) -> Iterator[dict]:
    for count, priref in enumerate(prirefs, 1):
        if count % progress_every == 0:
            logger.info("Progress: prepared %d/%d prirefs", count, len(prirefs))

        cid_url = cid_item_url_template.format(priref=priref)
        xml_text = None

        try:
            xml_text = fetch_xml(session, priref)
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
                dead_letter_path,
                stage="cid_fetch",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                http_status=status_code,
                max_xml_snippet=max_xml_snippet,
                max_doc_snippet=max_doc_snippet,
                max_error_snippet=max_error_snippet,
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
                dead_letter_path,
                stage="cid_fetch",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                max_xml_snippet=max_xml_snippet,
                max_doc_snippet=max_doc_snippet,
                max_error_snippet=max_error_snippet,
            )
            continue

        try:
            doc = xml_to_doc(xml_text, priref)
            stats.xml_parse_ok += 1
        except ValueError as e:
            stats.xml_parse_fail += 1
            logger.error(str(e))
            write_dead_letter(
                stats,
                dead_letter_path,
                stage="xml_parse",
                priref=priref,
                error=str(e),
                cid_url=cid_url,
                xml_text=xml_text,
                max_xml_snippet=max_xml_snippet,
                max_doc_snippet=max_doc_snippet,
                max_error_snippet=max_error_snippet,
            )
            continue
        except Exception as e:
            stats.xml_parse_fail += 1
            logger.exception("%s - unexpected XML conversion error", priref)
            write_dead_letter(
                stats,
                dead_letter_path,
                stage="xml_parse",
                priref=priref,
                error=f"unexpected XML conversion error: {e}",
                cid_url=cid_url,
                xml_text=xml_text,
                max_xml_snippet=max_xml_snippet,
                max_doc_snippet=max_doc_snippet,
                max_error_snippet=max_error_snippet,
            )
            continue

        stats.docs_prepared += 1
        yield make_bulk_action(es_index, priref, doc)


def bulk_index(
    es: Elasticsearch,
    actions: Iterator[dict],
    stats: Stats,
    *,
    dead_letter_path: str,
    chunk_size: int = 200,
    max_chunk_bytes: int = 5 * 1024 * 1024,
    max_retries: int = 3,
    initial_backoff: int = 2,
    max_backoff: int = 60,
    progress_every: int = 100,
    max_error_snippet: int = 5000,
    logger: logging.Logger,
) -> None:
    for ok, item in streaming_bulk(
        client=es,
        actions=actions,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        raise_on_error=False,
        raise_on_exception=False,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
    ):
        op_type, result = next(iter(item.items()))
        priref = result.get("_id")

        if ok:
            stats.es_index_ok += 1
            if stats.es_index_ok % progress_every == 0:
                logger.info(
                    "Elasticsearch indexed %d docs so far; latest priref=%s "
                    "result=%s version=%s",
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
                dead_letter_path,
                stage="es_index",
                priref=priref,
                error=f"Elasticsearch bulk {op_type} failed",
                http_status=result.get("status"),
                es_error=es_error,
                max_error_snippet=max_error_snippet,
            )
