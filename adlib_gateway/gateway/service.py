#!/usr/bin/env python3

"""
Gateway service: the framework-agnostic core wired to Flask in ``app.py``.

Responsibilities:
  * bearer-token authorisation
  * target allowlisting (prevents the gateway becoming an open proxy)
  * admission control (bounded queues -> 503 + Retry-After)
  * per-caller round-robin submission to the writer/reader pools
  * blocking the caller until its queued operation completes (bounded wait)
  * structured request logging for observability/tuning

Kept free of Flask imports so it can be unit tested directly.

2026
"""

import hmac
import logging
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeout
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

from config import Config
from scheduler import RoundRobinQueue
from workers import WorkerPool

VALID_OPS = {"get", "post", "lock", "post_with_verify"}
READ_OPS = {"get"}
DEFAULT_RETRY_AFTER = "10"

logger = logging.getLogger("adlib_gateway")


class GatewayService:
    def __init__(self, cfg: Config, session=None) -> None:
        self.cfg = cfg
        self.write_q = RoundRobinQueue(cfg.max_queue_depth)
        self.read_q = RoundRobinQueue(cfg.max_queue_depth)
        self.pool = WorkerPool(cfg, self.write_q, self.read_q, session=session)
        self.pool.start()
        self._started = time.monotonic()

    def stop(self) -> None:
        self.pool.stop()

    # -- security ----------------------------------------------------------
    def authorize(self, auth_header: str) -> bool:
        token = self.cfg.token
        if not token:
            return True
        if not auth_header:
            return False
        parts = auth_header.split()
        supplied = (
            parts[1]
            if len(parts) == 2 and parts[0].lower() == "bearer"
            else auth_header
        )
        return hmac.compare_digest(supplied, token)

    def target_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname
        if not host:
            return False
        if not self.cfg.allowed_target_hosts:
            return True
        return host in self.cfg.allowed_target_hosts

    # -- request handling --------------------------------------------------
    def handle(
        self, envelope: Dict[str, Any], auth_header: str = ""
    ) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        """Return ``(http_status, body_dict, extra_headers)``."""
        if not self.authorize(auth_header):
            return 401, {"ok": False, "error": "unauthorized"}, {}

        op = envelope.get("op")
        if op not in VALID_OPS:
            return 400, {"ok": False, "error": f"unknown op: {op}"}, {}

        api = envelope.get("api")
        if not api or not isinstance(api, str):
            return 400, {"ok": False, "error": "missing 'api' target"}, {}
        if not self.target_allowed(api):
            return (
                400,
                {"ok": False, "error": f"target not allowed: {api}"},
                {},
            )

        caller = str(envelope.get("caller") or "unknown")
        queue = self.read_q if op in READ_OPS else self.write_q
        depth = queue.depth()

        future: Future = Future()
        submitted = time.monotonic()
        if not queue.submit(caller, (future, envelope)):
            self._log(caller, op, api, envelope, depth, 0.0, "queue-full", 503)
            return (
                503,
                {"ok": False, "error": "queue full"},
                {"Retry-After": DEFAULT_RETRY_AFTER},
            )

        try:
            budget = self.cfg.max_queue_wait_s + self.cfg.op_budget_s
            result = future.result(timeout=budget)
            waited = time.monotonic() - submitted
            outcome = "ok" if result.get("ok") else "soft-fail"
            self._log(caller, op, api, envelope, depth, waited, outcome, 200)
            return 200, result, {}
        except FuturesTimeout:
            future.cancel()
            waited = time.monotonic() - submitted
            self._log(caller, op, api, envelope, depth, waited, "timeout", 503)
            return (
                503,
                {"ok": False, "error": "queue wait timeout"},
                {"Retry-After": DEFAULT_RETRY_AFTER},
            )
        except Exception as err:  # noqa: BLE001
            waited = time.monotonic() - submitted
            self._log(caller, op, api, envelope, depth, waited, "error", 502)
            return 502, {"ok": False, "error": str(err)}, {}

    def _log(
        self,
        caller: str,
        op: str,
        api: str,
        envelope: Dict[str, Any],
        depth: int,
        waited: float,
        outcome: str,
        status: int,
    ) -> None:
        logger.info(
            "caller=%s op=%s db=%s depth=%d wait=%.3fs outcome=%s status=%d "
            "target=%s",
            caller,
            op,
            envelope.get("database", "-"),
            depth,
            waited,
            outcome,
            status,
            api,
        )

    # -- health ------------------------------------------------------------
    def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "uptime_s": int(time.monotonic() - self._started),
            "writer_workers": self.cfg.writer_workers,
            "reader_workers": self.cfg.reader_workers,
            "queue": {
                "write_depth": self.write_q.depth(),
                "read_depth": self.read_q.depth(),
                "write_by_caller": self.write_q.depths_by_caller(),
                "read_by_caller": self.read_q.depths_by_caller(),
            },
            "paused_for_recycle": self.pool.recycle.paused,
            "recycles": self.pool.recycle.count,
            "completed": self.pool.completed,
        }
