"""Service layer: auth, allowlist, validation, admission, health."""

import json

from conftest import FakeResponse, FakeUpstreamSession, make_config
from service import GatewayService

API = "http://cid.test/api"


def _service(token="secret", allowed=None, **cfg_overrides):
    cfg = make_config(**cfg_overrides)
    import os

    if token:
        os.environ[cfg.token_env] = token
    else:
        os.environ.pop(cfg.token_env, None)
    if allowed is not None:
        cfg.allowed_target_hosts = allowed
    session = FakeUpstreamSession(
        lambda m, u, **k: FakeResponse(
            200, json.dumps({"adlibJSON": {"diagnostic": {"hits": 0}}})
        )
    )
    svc = GatewayService(cfg, session=session)
    return svc


def _teardown(svc):
    svc.stop()


def test_unauthorized_without_token():
    svc = _service(token="secret")
    try:
        status, body, _ = svc.handle({"op": "get", "api": API}, "")
        assert status == 401
    finally:
        _teardown(svc)


def test_authorized_with_bearer_token():
    svc = _service(token="secret")
    try:
        status, body, _ = svc.handle(
            {"op": "get", "api": API, "params": {}}, "Bearer secret"
        )
        assert status == 200
        assert body["ok"] is True
    finally:
        _teardown(svc)


def test_unknown_op_rejected():
    svc = _service(token=None)
    try:
        status, body, _ = svc.handle({"op": "nope", "api": API}, "")
        assert status == 400
    finally:
        _teardown(svc)


def test_missing_api_rejected():
    svc = _service(token=None)
    try:
        status, _body, _ = svc.handle({"op": "get"}, "")
        assert status == 400
    finally:
        _teardown(svc)


def test_target_not_in_allowlist_rejected():
    svc = _service(token=None, allowed=["other.test"])
    try:
        status, body, _ = svc.handle({"op": "get", "api": API}, "")
        assert status == 400
        assert "not allowed" in body["error"]
    finally:
        _teardown(svc)


def test_non_http_scheme_rejected():
    svc = _service(token=None)
    try:
        status, _b, _ = svc.handle({"op": "get", "api": "file:///etc/passwd"}, "")
        assert status == 400
    finally:
        _teardown(svc)


def test_queue_full_returns_503_with_retry_after():
    svc = _service(token=None, max_queue_depth=0)
    try:
        status, body, headers = svc.handle({"op": "get", "api": API}, "")
        assert status == 503
        assert "Retry-After" in headers
    finally:
        _teardown(svc)


def test_health_reports_queue_and_recycle_state():
    svc = _service(token=None)
    try:
        health = svc.health()
        assert health["status"] == "ok"
        assert "queue" in health
        assert health["writer_workers"] >= 1
    finally:
        _teardown(svc)
