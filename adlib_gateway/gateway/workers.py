#!/usr/bin/env python3

"""
Gateway worker pool and Axiell executor.

Owns the single upstream ``requests.Session`` (connection reuse), performs
GET/POST/lock/composite-verify operations against the Axiell API with retry +
exponential backoff + jitter, and centrally coordinates the IIS application
pool recycle (pausing all dispatch while it happens).

Framework agnostic: this module never imports Flask, so the executor and the
recycle logic can be unit tested directly.

2026
"""

import json
import threading
import time
from datetime import datetime, timedelta
from random import uniform
from typing import Any, Dict, Optional, Tuple

import requests

from config import Config

HEADERS = {"Content-Type": "text/xml"}
_PRIREF_MARKER = "{'@attributes': {'priref':"


class UpstreamError(Exception):
    """Raised when the upstream Axiell call fails after all retries."""


class RecycleCoordinator:
    """
    Ensures the IIS app-pool recycle happens at most once at a time and pauses
    all worker dispatch while the backend recovers.
    """

    def __init__(
        self,
        cfg: Config,
        session: requests.Session,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self._cfg = cfg
        self._session = session
        self._stop = stop_event or threading.Event()
        self._lock = threading.Lock()
        self._go = threading.Event()
        self._go.set()
        self._last_recycle = 0.0
        self.count = 0

    @property
    def paused(self) -> bool:
        return not self._go.is_set()

    def wait_if_paused(self) -> None:
        self._go.wait()

    def maybe_recycle(self, api: str, text: str) -> bool:
        """
        If ``text`` contains a known backend failure string, trigger a recycle
        (once) and block until recovery completes. Returns True if a failure
        was detected.
        """
        if not any(f in text for f in self._cfg.failure_strings):
            return False
        with self._lock:
            if time.monotonic() - self._last_recycle < self._cfg.recycle_pause_s:
                return True
            self._go.clear()
            try:
                self._do_recycle(api)
                self.count += 1
                self._last_recycle = time.monotonic()
                self._interruptible_sleep(self._cfg.recycle_pause_s)
            finally:
                self._go.set()
        return True

    def _do_recycle(self, api: str) -> None:
        try:
            self._session.get(
                api,
                headers=HEADERS,
                params=self._cfg.recycle_search,
                timeout=self._cfg.api_timeout_s,
            )
        except requests.RequestException as err:
            print(f"recycle trigger failed: {err}")

    def _interruptible_sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while time.monotonic() < end and not self._stop.is_set():
            time.sleep(min(0.5, max(0.0, end - time.monotonic())))


def _time_window(kind: str, minutes: int) -> str:
    start = datetime.now() - timedelta(minutes=minutes)
    return f"{kind} > '{start.strftime('%Y-%m-%d %H:%M:%S')}'"


def _verify_query(database: str, search: str) -> Dict[str, Any]:
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
    return {
        "database": database,
        "search": search_new,
        "limit": 1,
        "output": "jsonv1",
    }


def _parse_post_text(text: str, failure_strings) -> Any:
    if any(w in text for w in failure_strings):
        return False
    if "recordList" in text:
        record = json.loads(text)
        try:
            inner = record["adlibJSON"]["recordList"]["record"]
            return inner[0] if isinstance(inner, list) else inner
        except (KeyError, IndexError, TypeError):
            return record
    if "@attributes" in text:
        return json.loads(text)
    if "error" in text:
        return None
    return None


def _parse_retrieve(text: str) -> Tuple[int, Any]:
    if not text:
        return 0, None
    try:
        dct = json.loads(text)
        hits = int(dct["adlibJSON"]["diagnostic"]["hits"])
    except (ValueError, KeyError, TypeError):
        return 0, None
    if "recordList" not in text:
        return hits, dct
    try:
        return hits, dct["adlibJSON"]["recordList"]["record"]
    except (KeyError, TypeError):
        return hits, None


class WorkerPool:
    """Runs writer and reader worker threads over their round-robin queues."""

    def __init__(
        self,
        cfg: Config,
        write_q,
        read_q,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._cfg = cfg
        self._write_q = write_q
        self._read_q = read_q
        self._session = session or requests.Session()
        self._stop = threading.Event()
        self._recycle = RecycleCoordinator(cfg, self._session, self._stop)
        self._threads = []
        self.completed = 0

    @property
    def recycle(self) -> RecycleCoordinator:
        return self._recycle

    def start(self) -> None:
        for _ in range(self._cfg.writer_workers):
            self._spawn(self._write_q, "writer")
        for _ in range(self._cfg.reader_workers):
            self._spawn(self._read_q, "reader")

    def _spawn(self, queue, name: str) -> None:
        t = threading.Thread(target=self._run, args=(queue,), name=name, daemon=True)
        t.start()
        self._threads.append(t)

    def stop(self, join_timeout: float = 5.0) -> None:
        self._stop.set()
        self._write_q.close()
        self._read_q.close()
        for t in self._threads:
            t.join(timeout=join_timeout)

    def _run(self, queue) -> None:
        while not self._stop.is_set():
            self._recycle.wait_if_paused()
            item = queue.get(timeout=0.5)
            if item is None:
                continue
            future, descriptor = item
            if future.cancelled():
                continue
            try:
                result = self.execute(descriptor)
                future.set_result(result)
            except Exception as err:  # noqa: BLE001 - surfaced to the client
                future.set_exception(err)
            finally:
                self.completed += 1

    # -- dispatch ----------------------------------------------------------
    def execute(self, d: Dict[str, Any]) -> Dict[str, Any]:
        op = d.get("op")
        try:
            if op == "get":
                return self._op_get(d)
            if op == "post":
                return self._op_post(d)
            if op == "lock":
                return self._op_lock(d)
            if op == "post_with_verify":
                return self._op_post_with_verify(d)
            return {"ok": False, "error": f"unknown op: {op}"}
        except UpstreamError as err:
            return {"ok": False, "error": f"upstream failed: {err}"}
        except Exception as err:  # noqa: BLE001
            return {"ok": False, "error": f"{type(err).__name__}: {err}"}

    # -- upstream HTTP with retry/backoff/jitter ---------------------------
    def _http(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[bytes] = None,
    ) -> Tuple[int, str]:
        last: Optional[Exception] = None
        for attempt in range(self._cfg.upstream_retries):
            if self._stop.is_set():
                break
            try:
                resp = self._session.request(
                    method,
                    url,
                    headers=HEADERS,
                    params=params,
                    data=data,
                    timeout=self._cfg.api_timeout_s,
                )
            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as err:
                last = err
                self._backoff(attempt)
                continue

            if resp.status_code >= 500:
                last = Exception(f"HTTP {resp.status_code}")
                self._backoff(attempt)
                continue

            self._recycle.maybe_recycle(url, resp.text)
            return resp.status_code, resp.text

        raise UpstreamError(str(last) if last else "no attempt made")

    def _backoff(self, attempt: int) -> None:
        if self._stop.is_set():
            return
        delay = min(self._cfg.backoff_max_s, self._cfg.backoff_base_s * (2**attempt))
        time.sleep(delay + uniform(0, self._cfg.backoff_base_s))

    def _interruptible_sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while time.monotonic() < end and not self._stop.is_set():
            time.sleep(min(0.25, max(0.0, end - time.monotonic())))

    # -- operations --------------------------------------------------------
    def _op_get(self, d: Dict[str, Any]) -> Dict[str, Any]:
        status, text = self._http("GET", d["api"], params=d.get("params"))
        if status != 200:
            return {
                "ok": False,
                "status_code": status,
                "error": f"HTTP {status}",
                "text": text,
            }
        return {"ok": True, "status_code": status, "text": text}

    def _op_post(self, d: Dict[str, Any]) -> Dict[str, Any]:
        data = self._encode(d.get("payload", ""))
        status, text = self._http("POST", d["api"], params=d.get("params"), data=data)
        return {
            "ok": status == 200,
            "status_code": status,
            "text": text,
            "error": None if status == 200 else f"HTTP {status}",
        }

    def _op_lock(self, d: Dict[str, Any]) -> Dict[str, Any]:
        params = {
            "database": d["database"],
            "command": d["command"],
            "priref": str(d["priref"]),
            "output": "jsonv1",
        }
        try:
            status, text = self._http("POST", d["api"], params=params)
        except UpstreamError as err:
            return {"ok": False, "locked": False, "error": str(err)}
        return {
            "ok": True,
            "locked": status == 200,
            "status_code": status,
            "text": text,
        }

    def _op_post_with_verify(self, d: Dict[str, Any]) -> Dict[str, Any]:
        api = d["api"]
        database = d["database"]
        method = d["method"]
        search_value = d.get("search_value", "")
        max_retries = int(
            d.get("max_retries", self._cfg.verify.max_retries)
            or self._cfg.verify.max_retries
        )
        retry_delay = int(
            d.get("retry_delay", self._cfg.verify.retry_delay_s)
            or self._cfg.verify.retry_delay_s
        )
        params = {
            "command": method,
            "database": database,
            "xmltype": "grouped",
            "output": "jsonv1",
        }
        data = self._encode(d.get("payload", ""))

        for _ in range(1, max_retries + 1):
            if self._stop.is_set():
                return {"ok": False, "record": None, "error": "stopping"}
            text = ""
            try:
                _status, text = self._http("POST", api, params=params, data=data)
            except UpstreamError:
                text = ""

            result = _parse_post_text(text, self._cfg.failure_strings) if text else None
            if result and _PRIREF_MARKER in str(result):
                return {"ok": True, "record": result}

            self._interruptible_sleep(retry_delay)

            kind = "modification" if method == "updaterecord" else "creation"
            window = _time_window(kind, self._cfg.verify.verify_window_minutes)
            search = f"{window} and {search_value}" if search_value else window
            query = _verify_query(database, search)
            try:
                _gstatus, gtext = self._http("GET", api, params=query)
                hits, records = _parse_retrieve(gtext)
            except UpstreamError:
                hits, records = 0, None
            if hits and hits > 0 and records:
                rec = records[0] if isinstance(records, list) else records
                return {"ok": True, "record": rec}

        return {"ok": True, "record": None}

    @staticmethod
    def _encode(payload: Any) -> bytes:
        return payload.encode("utf-8") if isinstance(payload, str) else payload
