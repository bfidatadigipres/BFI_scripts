#!/usr/bin/env python3

"""
Unified BFI client library for the Axiell Adlib API.

This module replaces both ``adlib_v3.py`` and ``adlib_v3_sess.py``. It is
session based and routes every GET/POST/lock call through the central
``adlib_gateway`` service so that all scripts (local and remote) issue single,
queued calls to the Axiell API instead of hammering the SQL backend in
parallel.

Pure helpers (XML payload building, record field extraction) run locally and
never touch the network. Only the transport functions (``get``, ``post``,
``post_with_verify``, ``check``, ``write_lock``, ``unlock_record``,
``get_grouped_items``) are proxied through the gateway.

Environment variables
---------------------
ADLIB_GATEWAY_URL      Base URL of the gateway, e.g. http://gw-host:8080
ADLIB_GATEWAY_TOKEN    Shared secret for the gateway Authorization header
ADLIB_CALLER           Optional caller name override (default: script/pid@host)
ADLIB_GATEWAY_TIMEOUT  Per-call client timeout in seconds (default: 900)

2026
"""

import json
import os
import socket
import sys
import time
from datetime import datetime, timedelta
from random import uniform
from typing import Any, Dict, List, Optional, Union

import requests
import xmltodict
from requests import Session

HEADERS = {"Content-Type": "text/xml"}
TIMEOUT = 100

GATEWAY_TIMEOUT = int(os.environ.get("ADLIB_GATEWAY_TIMEOUT", "1200"))
_CLIENT_RETRIES = 6
_BACKOFF_BASE = 1.0
_BACKOFF_MAX = 30.0

#: Failure strings returned by the Axiell/SQL backend when a command could not
#: be completed. Detection now lives in the gateway (which owns the recycle),
#: but the client keeps the list so ``post()`` can still signal a soft failure.
FAILURE_STRINGS = [
    "A severe error occurred on the current command.",
    "Execution Timeout Expired. The timeout period elapsed",
]


class GatewayError(Exception):
    """Raised when the gateway cannot be reached or rejects a request."""


class GatewayAuthError(GatewayError):
    """Raised when the gateway rejects the shared token (401/403)."""


class GatewayBadRequest(GatewayError):
    """Raised when the gateway rejects the envelope (400)."""


class GatewayUnavailable(GatewayError):
    """Raised when the gateway stays unreachable/overloaded after retries."""


# --------------------------------------------------------------------------
# Session handling
# --------------------------------------------------------------------------
_default_session: Optional[Session] = None


def create_session() -> Session:
    """
    Return the process-wide client session used to talk to the gateway.

    Kept for backwards compatibility with scripts that call
    ``adlib.create_session()``. Sessions to the *upstream* Axiell API are now
    owned by the gateway, not the client.
    """
    global _default_session
    if _default_session is None:
        _default_session = Session()
    return _default_session


def _resolve_session(session: Optional[Session]) -> Session:
    return session if session is not None else create_session()


def _caller_id() -> str:
    override = os.environ.get("ADLIB_CALLER")
    if override:
        return override
    script = os.path.basename(sys.argv[0]) or "python"
    return f"{script}/{os.getpid()}@{socket.gethostname()}"


def _gateway_url() -> str:
    url = os.environ.get("ADLIB_GATEWAY_URL", "").rstrip("/")
    if not url:
        raise GatewayError(
            "ADLIB_GATEWAY_URL is not set. The unified adlib client routes "
            "all calls through the gateway; set it to e.g. "
            "http://gw-host:8080"
        )
    return url


def _auth_headers() -> Dict[str, str]:
    token = os.environ.get("ADLIB_GATEWAY_TOKEN", "")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


def _sleep_backoff(attempt: int) -> None:
    delay = min(_BACKOFF_MAX, _BACKOFF_BASE * (2**attempt))
    time.sleep(delay + uniform(0, _BACKOFF_BASE))


# --------------------------------------------------------------------------
# Gateway transport
# --------------------------------------------------------------------------
def _gateway_call(
    envelope: Dict[str, Any], session: Optional[Session] = None
) -> Dict[str, Any]:
    """
    POST an operation envelope to the gateway and return its JSON body.

    Retries with exponential backoff + jitter on transport errors and on
    gateway 503 (queue full / wait timeout, honouring Retry-After). Raises on
    auth/bad-request errors immediately.
    """
    sess = _resolve_session(session)
    url = _gateway_url() + "/request"
    headers = _auth_headers()
    envelope = dict(envelope)
    envelope.setdefault("caller", _caller_id())

    last_error: Optional[str] = None
    for attempt in range(_CLIENT_RETRIES):
        try:
            resp = sess.post(
                url, json=envelope, headers=headers, timeout=GATEWAY_TIMEOUT
            )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as err:
            last_error = str(err)
            _sleep_backoff(attempt)
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError as err:
                raise GatewayError(f"Gateway returned non-JSON body: {err}")

        if resp.status_code in (401, 403):
            raise GatewayAuthError(f"Gateway rejected credentials ({resp.status_code})")
        if resp.status_code == 400:
            raise GatewayBadRequest(f"Gateway rejected envelope: {resp.text}")
        if resp.status_code == 503:
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                time.sleep(min(int(retry_after), _BACKOFF_MAX))
            else:
                _sleep_backoff(attempt)
            last_error = "gateway busy (503)"
            continue

        last_error = f"gateway HTTP {resp.status_code}: {resp.text[:200]}"
        _sleep_backoff(attempt)

    raise GatewayUnavailable(
        f"Gateway unavailable after {_CLIENT_RETRIES} attempts: {last_error}"
    )


# --------------------------------------------------------------------------
# Transport operations (proxied through the gateway)
# --------------------------------------------------------------------------
def check(api: str, session: Optional[Session] = None) -> Dict[str, Any]:
    """Check the API responds by requesting its version."""
    query = {"command": "getversion", "limit": 0, "output": "jsonv1"}
    return get(api, query, session)


def get(
    api: str, query: Dict[str, Any], session: Optional[Session] = None
) -> Dict[str, Any]:
    """
    Send a GET request through the gateway and return the parsed JSON body.

    Retries and the app-pool recycle are handled gateway-side; the client
    raises ``GatewayError`` if the upstream call ultimately fails.
    """
    body = _gateway_call({"op": "get", "api": api, "params": query}, session)
    if not body.get("ok"):
        raise GatewayError(f"GET to {api} failed at gateway: {body.get('error')}")
    return json.loads(body["text"])


def post(
    api: str,
    payload: str,
    database: str,
    method: str,
    session: Optional[Session] = None,
) -> Union[Dict[str, Any], bool, None]:
    """
    Send a POST request through the gateway.

    Returns the parsed record dict on success, ``False`` if the backend
    reported a severe/timeout failure (the gateway will already have triggered
    a recycle), or ``None`` on other failures.
    """
    params = {
        "command": method,
        "database": database,
        "xmltype": "grouped",
        "output": "jsonv1",
    }
    body = _gateway_call(
        {"op": "post", "api": api, "params": params, "payload": payload},
        session,
    )
    if not body.get("ok"):
        print(f"adlib.post(): gateway reported failure: {body.get('error')}")
        return None
    return _parse_post_text(body.get("text", ""))


def _parse_post_text(text: str) -> Union[Dict[str, Any], bool, None]:
    """
    Parse an Axiell POST response body exactly as the legacy libraries did.

    Fixes the legacy ``adlib_v3.post`` bug where the ``"error"`` branch
    referenced an undefined ``record`` variable.
    """
    if _is_failure_response(text):
        return False
    if "recordList" in text:
        record = json.loads(text)
        try:
            inner = record["adlibJSON"]["recordList"]["record"]
            if isinstance(inner, list):
                return inner[0]
            return inner
        except (KeyError, IndexError, TypeError):
            return record
    if "@attributes" in text:
        return json.loads(text)
    if "error" in text:
        return None
    return None


def _is_failure_response(text: str) -> bool:
    return any(warning in text for warning in FAILURE_STRINGS)


def post_with_verify(
    api: str,
    payload: str,
    database: str,
    method: str,
    session: Optional[Session] = None,
    search_value: str = "",
    max_retries: int = 3,
    retry_delay: int = 10,
) -> Optional[Dict[str, Any]]:
    """
    POST a record and verify it via GET, executed atomically by the gateway.

    The whole POST -> verify -> retry loop runs as a single queued write
    operation on the gateway, so verification cannot be interleaved by other
    writers and no orphan records are created. Returns the parsed record dict
    on success, or ``None`` after all retries are exhausted.

    Accepts both legacy call styles: ``adlib_v3`` passed ``search_value`` as
    the 5th positional argument, while ``adlib_v3_sess`` passed ``session``
    there. A string in the ``session`` position is treated as ``search_value``.
    """
    if isinstance(session, str):
        if not search_value:
            search_value = session
        session = None

    body = _gateway_call(
        {
            "op": "post_with_verify",
            "api": api,
            "payload": payload,
            "database": database,
            "method": method,
            "search_value": search_value,
            "max_retries": max_retries,
            "retry_delay": retry_delay,
        },
        session,
    )
    if not body.get("ok") and body.get("error"):
        print(f"post_with_verify(): gateway error: {body.get('error')}")
    return body.get("record")


def write_lock(
    api: str, priref: str, database: str, session: Optional[Session] = None
) -> Optional[bool]:
    """Apply a writing lock to a record before updating it."""
    body = _gateway_call(
        {
            "op": "lock",
            "api": api,
            "priref": priref,
            "database": database,
            "command": "lockrecord",
        },
        session,
    )
    return bool(body.get("locked"))


def unlock_record(
    api: str, priref: str, database: str, session: Optional[Session] = None
) -> Optional[bool]:
    """Release a record lock (used when a write fails after locking)."""
    body = _gateway_call(
        {
            "op": "lock",
            "api": api,
            "priref": priref,
            "database": database,
            "command": "unlockrecord",
        },
        session,
    )
    return bool(body.get("locked"))


def get_grouped_items(
    api: str, database: str, session: Optional[Session] = None
) -> Union[Dict[str, List[str]], tuple]:
    """
    Return the field groupings for a database by fetching getmetadata (XML).
    """
    query = {"command": "getmetadata", "database": database, "limit": 0}
    body = _gateway_call(
        {"op": "get", "api": api, "params": query, "raw": True}, session
    )
    if not body.get("ok"):
        return None, None
    metadata = xmltodict.parse(body.get("text", ""))
    if not isinstance(metadata, dict):
        return None, None
    grouped: Dict[str, List[str]] = {}
    mdata = metadata["adlibXML"]["recordList"]["record"]
    for num in range(0, len(mdata)):
        try:
            group = mdata[num]["group"]
            field_name = mdata[num]["fieldName"]["value"][0]["#text"]
            grouped.setdefault(group, [])
            if field_name not in grouped[group]:
                grouped[group].append(field_name)
        except (KeyError, IndexError, TypeError):
            pass
    return grouped


# --------------------------------------------------------------------------
# Retrieval helper (search building + transport)
# --------------------------------------------------------------------------
def retrieve_record(
    api: str,
    database: str,
    search: str,
    limit: Union[int, str],
    session: Optional[Session] = None,
    fields: Optional[List[str]] = None,
) -> tuple:
    """
    Retrieve data from CID using the Axiell search API.

    Accepts both legacy call styles: the old ``adlib_v3`` passed ``fields`` as
    the 5th positional argument, while ``adlib_v3_sess`` passed ``session``
    there. A list in the ``session`` position is treated as ``fields``.
    """
    if isinstance(session, list):
        fields = session
        session = None

    if search.startswith("priref="):
        search_new = search
    elif database == "items":
        search_new = f"(record_type=ITEM) and {search}"
    elif database == "works":
        search_new = f"(record_type=WORK) and {search}"
    elif database == "manifestations":
        search_new = f"(record_type=MANIFESTATION) and {search}"
    else:
        search_new = search

    query: Dict[str, Any] = {
        "database": database,
        "search": search_new,
        "limit": limit,
        "output": "jsonv1",
    }
    if fields:
        query["fields"] = ", ".join(fields)

    record = get(api, query, session)
    if not record:
        return None, None
    if record["adlibJSON"]["diagnostic"]["hits"] == 0:
        return 0, None
    if "recordList" not in str(record):
        try:
            hits = int(record["adlibJSON"]["diagnostic"]["hits"])
            return hits, record
        except (IndexError, KeyError, TypeError) as err:
            print(err)
            return 0, record

    hits = int(record["adlibJSON"]["diagnostic"]["hits"])
    return hits, record["adlibJSON"]["recordList"]["record"]


def _time_window_last_mins(kind: str, minutes: int = 1) -> str:
    """
    Return a search clause matching records created/modified in the last
    ``minutes`` minutes. ``kind`` is 'creation' or 'modification'.
    """
    now = datetime.now()
    window_start = now - timedelta(minutes=minutes)
    return f"{kind} > '{window_start.strftime('%Y-%m-%d %H:%M:%S')}'"


# --------------------------------------------------------------------------
# Record field extraction (pure, local)
# --------------------------------------------------------------------------
def retrieve_field_name(record: Dict[str, Any], fieldname: str) -> List[Any]:
    """
    Extract all values for ``fieldname`` from a record, handling language
    spans and grouped sub-records. ``record`` is normally
    ``['adlibJSON']['recordList']['record'][0]``.
    """
    field_list: List[Any] = []
    try:
        for field in record[f"{fieldname}"]:
            if isinstance(field, str):
                field_list.append(field)
            elif "'@lang'" in str(field):
                field_list.append(field["value"][0]["spans"][0]["text"])
            else:
                field_list.append(field["spans"][0]["text"])
    except TypeError:
        field_list = traverse_sub_records(record, fieldname)
    except KeyError:
        field_list = group_check(record, fieldname)
    except Exception as err:
        print(err)

    if not isinstance(field_list, list):
        return [field_list]
    return field_list


def traverse_sub_records(record: Dict[str, Any], field: str) -> List[str]:
    """Where group nesting exists, search other layers for ``field``."""
    field_list: List[str] = []
    for sub_rec in record:
        if field in str(record[sub_rec]):
            new_rec = record[sub_rec]
            if isinstance(field, list):
                for f in new_rec:
                    if isinstance(f, str):
                        field_list.append(f)
                    elif "'@lang'" in str(f):
                        field_list.append(f["value"][0]["spans"][0]["text"])
                    else:
                        field_list.append(f["spans"][0]["text"])
            elif "'@lang'" in str(field):
                field_list.append(field["value"][0]["spans"][0]["text"])
            else:
                field_list.append(field["spans"][0]["text"])
    return field_list


def retrieve_facet_list(record: Dict[str, Any], fname: str) -> List[str]:
    """Retrieve the list of facet values for ``fname``."""
    facets = []
    for value in record["adlibJSON"]["facetList"][0]["values"]:
        facets.append(value[fname]["spans"][0]["text"])
    return facets


def group_check(
    record: Dict[str, Any], fname: str
) -> Union[List[str], List[Dict[str, Any]], str, None]:
    """Return the group(s) that contain field key ``fname``."""
    groups = dict([(k, v) for k, v in record.items() if f"{fname}" in str(v)])
    fieldnames: List[str] = []
    if len(groups) == 1:
        first_key = next(iter(groups))
        for entry in groups[f"{first_key}"]:
            for key, val in entry.items():
                if str(key) == str(fname):
                    if "@lang" in str(val):
                        try:
                            fieldnames.append(val[0]["value"][0]["spans"][0]["text"])
                        except (IndexError, KeyError):
                            pass
                    else:
                        try:
                            fieldnames.append(val[0]["spans"][0]["text"])
                        except (IndexError, KeyError):
                            pass
        if fieldnames:
            return fieldnames
    elif len(groups) > 1:
        all_vals: List[Dict[str, Any]] = []
        for kname in groups:
            for key, val in groups[f"{kname}"][0].items():
                if key == fname:
                    all_vals.append({fname: val})
        if len(all_vals) == 1:
            if "@lang" in str(all_vals):
                try:
                    return all_vals[0][fname][0]["value"][0]["spans"][0]["text"]
                except (KeyError, IndexError):
                    print(f"Failed to extract value: {all_vals}")
                    return None
            try:
                return all_vals[0][fname][0]["spans"][0]["text"]
            except (KeyError, IndexError):
                print(f"Failed to extract value: {all_vals}")
                return None
        return all_vals
    return None


# --------------------------------------------------------------------------
# XML payload building (pure, local)
# --------------------------------------------------------------------------
def escape_xml(s: str) -> str:
    """Escape characters that would break an XML POST to CID."""
    if not isinstance(s, str):
        return s
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def create_record_data(
    api: str,
    database: str,
    priref_or_session: Any,
    priref: Optional[Union[str, int]] = None,
    data: Optional[List[Dict[str, str]]] = None,
) -> str:
    """
    Build a grouped adlibXML record payload for ``priref`` from ``data``.

    Supports both legacy signatures:
      * ``create_record_data(api, database, priref, data)``      (adlib_v3)
      * ``create_record_data(api, database, session, priref, data)`` (sess)
    The third argument is treated as a session when it is a ``Session``.
    """
    session = None
    if isinstance(priref_or_session, Session):
        session = priref_or_session
        real_priref = priref
        real_data = data
    else:
        real_priref = priref_or_session
        real_data = priref if data is None else data

    if real_data is None:
        real_data = []
    if not isinstance(real_data, list):
        real_data = [real_data]

    grouped = get_grouped_items(api, database, session)
    new_grouping: Dict[str, List[Dict[str, str]]] = {}
    non_grouped_items: List[Dict[str, str]] = []

    for item in real_data:
        group_found = False
        for group_key, gfields in grouped.items():
            new_grouping.setdefault(group_key, [])
            access_record = {k: item[k] for k in item if k in gfields}
            if access_record:
                new_grouping[group_key].append(access_record)
                group_found = True
                break
        if not group_found:
            non_grouped_items.append(item)

    record_data: Dict[str, List[List[Dict[str, str]]]] = {}
    for group_key, records in new_grouping.items():
        if not records:
            continue
        record_data[group_key] = []
        current_block: List[Dict[str, str]] = []
        seen_keys: set = set()
        for record_item in records:
            item_keys = set(record_item.keys())
            if item_keys & seen_keys:
                record_data[group_key].append(current_block)
                current_block = []
                seen_keys = set()
            current_block.append(record_item)
            seen_keys.update(item_keys)
        if current_block:
            record_data[group_key].append(current_block)

    output_list = [f"<priref>{real_priref or 0}</priref>"]
    for ng_item in non_grouped_items:
        for key, value in ng_item.items():
            output_list.append(f"<{key}>{escape_xml(value)}</{key}>")
    for group_key, blocks in record_data.items():
        for block in blocks:
            output_list.append(f"<{group_key}>")
            for record_item in block:
                for key, value in record_item.items():
                    output_list.append(f"<{key}>{escape_xml(value)}</{key}>")
            output_list.append(f"</{group_key}>")

    payload = "".join(output_list)
    return (
        f"<adlibXML><recordList><record>{payload}</record>" f"</recordList></adlibXML>"
    )


def create_grouped_data(
    priref: Optional[str],
    grouping: str,
    field_pairs: List[Union[List[Dict[str, str]], Dict[str, str]]],
) -> Optional[str]:
    """
    Build a grouped adlibXML payload for appending repeated field groups to an
    existing record identified by ``priref``.
    """
    if not priref:
        return None

    payload_mid = ""
    for lst in field_pairs:
        mid = ""
        if isinstance(lst, list):
            for grouped in lst:
                for key, value in grouped.items():
                    mid += f"<{key}><![CDATA[{value}]]></{key}>"
        elif isinstance(lst, dict):
            for key, value in lst.items():
                mid += f"<{key}><![CDATA[{value}]]></{key}>"
        payload_mid += f"<{grouping}>" + mid + f"</{grouping}>"

    if len(priref) > 0:
        return (
            f"<adlibXML><recordList><record priref='{priref}'>"
            f"{payload_mid}</record></recordList></adlibXML>"
        )
    return payload_mid


def add_quality_comments(
    api: str, priref: str, comments: str, session: Optional[Session] = None
) -> bool:
    """Attach a dated quality comment to an item record via updaterecord."""
    date_now = str(datetime.now())[:10]
    payload = (
        f"<adlibXML><recordList><record priref='{priref}'>"
        f"<quality_comments>"
        f"<quality_comments><![CDATA[{comments}]]></quality_comments>"
        f"<quality_comments.date>{date_now}</quality_comments.date>"
        f"<quality_comments.writer>datadigipres</quality_comments.writer>"
        f"</quality_comments></record></recordList></adlibXML>"
    )
    rec = post(api, payload, "items", "updaterecord", session)
    if rec is None or rec is False:
        return False
    if "error" in str(rec):
        return False
    return True


# --------------------------------------------------------------------------
# Legacy compatibility shims (recycle is now handled gateway-side)
# --------------------------------------------------------------------------
def check_response(rec: str, api: str) -> Optional[bool]:
    """
    Return ``True`` if ``rec`` contains a known backend failure string.

    The app-pool recycle is now performed centrally by the gateway, so this
    function only detects failures; it no longer triggers a recycle.
    """
    if _is_failure_response(str(rec)):
        return True
    return None


def recycle_api(api: str) -> None:
    """
    No-op kept for import compatibility.

    Recycling the Axiell application pool is now performed by the gateway,
    which pauses all queued dispatch, issues the recycle trigger and waits,
    exactly once, on behalf of every caller.
    """
    print("adlib.recycle_api(): recycling is handled centrally by the gateway")
