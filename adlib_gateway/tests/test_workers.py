"""Executor tests: get/post/lock/post_with_verify + recycle, with a fake
upstream session (no network, no sleeps)."""

import json

from conftest import FakeResponse, FakeUpstreamSession, make_config
from scheduler import RoundRobinQueue
from workers import WorkerPool

API = "http://cid.test/api"


def _pool(responder, **cfg_overrides):
    cfg = make_config(**cfg_overrides)
    session = FakeUpstreamSession(responder)
    pool = WorkerPool(cfg, RoundRobinQueue(10), RoundRobinQueue(10), session=session)
    return pool, session


def test_op_get_success():
    body = json.dumps({"adlibJSON": {"diagnostic": {"hits": 0}}})
    pool, _ = _pool(lambda m, u, **k: FakeResponse(200, body))
    result = pool.execute({"op": "get", "api": API, "params": {"a": 1}})
    assert result["ok"] is True
    assert json.loads(result["text"]) == json.loads(body)


def test_op_get_retries_then_succeeds():
    calls = {"n": 0}

    def responder(method, url, **kwargs):
        calls["n"] += 1
        if calls["n"] < 2:
            raise __import__("requests").exceptions.ConnectionError("boom")
        return FakeResponse(200, '{"ok": true}')

    pool, _ = _pool(responder, upstream_retries=3)
    result = pool.execute({"op": "get", "api": API, "params": {}})
    assert result["ok"] is True
    assert calls["n"] == 2


def test_op_get_upstream_exhausted_returns_not_ok():
    import requests

    def responder(method, url, **kwargs):
        raise requests.exceptions.ConnectionError("down")

    pool, _ = _pool(responder, upstream_retries=2)
    result = pool.execute({"op": "get", "api": API, "params": {}})
    assert result["ok"] is False
    assert "error" in result


def test_op_post_parses_record():
    rec = {
        "@attributes": {"priref": "123"},
        "object_number": [{"spans": [{"text": "N_1"}]}],
    }
    body = json.dumps({"adlibJSON": {"recordList": {"record": rec}}})
    pool, _ = _pool(lambda m, u, **k: FakeResponse(200, body))
    result = pool.execute(
        {
            "op": "post",
            "api": API,
            "payload": "<x/>",
            "params": {"command": "createrecord"},
        }
    )
    assert result["ok"] is True
    assert "recordList" in result["text"]


def test_op_lock_returns_locked_true():
    pool, _ = _pool(lambda m, u, **k: FakeResponse(200, "{}"))
    result = pool.execute(
        {
            "op": "lock",
            "api": API,
            "priref": "123",
            "database": "items",
            "command": "lockrecord",
        }
    )
    assert result["locked"] is True


def test_post_with_verify_success_first_attempt():
    rec = {"@attributes": {"priref": "999"}}
    body = json.dumps({"adlibJSON": {"recordList": {"record": rec}}})
    pool, session = _pool(lambda m, u, **k: FakeResponse(200, body))
    result = pool.execute(
        {
            "op": "post_with_verify",
            "api": API,
            "payload": "<x/>",
            "database": "works",
            "method": "createrecord",
            "search_value": "object_number=N_9",
        }
    )
    assert result["ok"] is True
    assert result["record"]["@attributes"]["priref"] == "999"
    # only the POST was needed
    assert len([c for c in session.calls if c[0] == "POST"]) == 1


def test_post_with_verify_falls_back_to_get_verification():
    """POST returns no priref, but the verify GET finds the record."""
    post_body = json.dumps({"adlibJSON": {"diagnostic": {"hits": 0}}})
    found = {"@attributes": {"priref": "555"}}
    get_body = json.dumps(
        {"adlibJSON": {"diagnostic": {"hits": 1}, "recordList": {"record": [found]}}}
    )

    def responder(method, url, **kwargs):
        if method == "POST":
            return FakeResponse(200, post_body)
        return FakeResponse(200, get_body)

    pool, session = _pool(responder)
    result = pool.execute(
        {
            "op": "post_with_verify",
            "api": API,
            "payload": "<x/>",
            "database": "items",
            "method": "createrecord",
            "search_value": "object_number=N_5",
        }
    )
    assert result["record"]["@attributes"]["priref"] == "555"
    assert any(c[0] == "GET" for c in session.calls)


def test_post_with_verify_exhausts_returns_none_record():
    empty = json.dumps({"adlibJSON": {"diagnostic": {"hits": 0}}})
    pool, _ = _pool(
        lambda m, u, **k: FakeResponse(200, empty),
        verify={"max_retries": 2, "retry_delay_s": 0},
    )
    result = pool.execute(
        {
            "op": "post_with_verify",
            "api": API,
            "payload": "<x/>",
            "database": "works",
            "method": "createrecord",
            "search_value": "x=1",
        }
    )
    assert result["record"] is None


def test_recycle_triggered_on_failure_string():
    calls = {"n": 0}

    def responder(method, url, **kwargs):
        calls["n"] += 1
        # first POST -> severe error, everything afterwards -> healthy
        if method == "POST" and calls["n"] == 1:
            return FakeResponse(200, "A severe error occurred on the current command.")
        return FakeResponse(200, '{"adlibJSON": {"diagnostic": {"hits": 0}}}')

    pool, session = _pool(responder, recycle_pause_s=0)
    pool.execute(
        {
            "op": "post",
            "api": API,
            "payload": "<x/>",
            "params": {"command": "updaterecord"},
        }
    )
    assert pool.recycle.count == 1
    # a recycle GET (params is the raw recycle search string) was issued
    assert any(
        c[0] == "GET" and isinstance(c[2].get("params"), str) for c in session.calls
    )


def test_unknown_op_returns_error():
    pool, _ = _pool(lambda m, u, **k: FakeResponse(200, "{}"))
    result = pool.execute({"op": "bogus", "api": API})
    assert result["ok"] is False
