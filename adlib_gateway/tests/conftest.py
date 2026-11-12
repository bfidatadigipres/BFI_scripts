"""Shared test setup: sys.path wiring plus lightweight fakes."""

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "client"))
sys.path.insert(0, os.path.join(ROOT, "gateway"))


class FakeResponse:
    def __init__(self, status_code=200, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    def json(self):
        import json

        return json.loads(self.text)


class FakeUpstreamSession:
    """Stands in for requests.Session against the Axiell API."""

    def __init__(self, responder):
        self.responder = responder
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responder(method, url, **kwargs)

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)


def make_config(**overrides):
    from config import Config, VerifyConfig

    verify_overrides = overrides.pop("verify", {})
    base = dict(
        upstream_retries=2,
        backoff_base_s=0,
        backoff_max_s=0,
        api_timeout_s=1,
        recycle_pause_s=0,
        max_queue_depth=50,
        max_queue_wait_s=5,
        op_budget_s=5,
    )
    base.update(overrides)
    cfg = Config(**base)
    verify_base = dict(max_retries=2, retry_delay_s=0, verify_window_minutes=1)
    verify_base.update(verify_overrides)
    cfg.verify = VerifyConfig(**verify_base)
    return cfg


@pytest.fixture
def fake_gateway(monkeypatch):
    """Patch the client's session.post to a programmable gateway responder."""
    import adlib

    state = {"responses": [], "requests": []}

    class _Sess:
        def post(self, url, json=None, headers=None, timeout=None):
            state["requests"].append({"url": url, "json": json, "headers": headers})
            if not state["responses"]:
                return FakeResponse(500, '{"ok": false}')
            item = state["responses"].pop(0)
            if isinstance(item, Exception):
                raise item
            return item

    monkeypatch.setenv("ADLIB_GATEWAY_URL", "http://gw.test:8080")
    monkeypatch.setenv("ADLIB_GATEWAY_TOKEN", "secret")
    monkeypatch.setattr(adlib, "_default_session", _Sess())
    return state
