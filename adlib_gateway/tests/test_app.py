"""End-to-end Flask app tests with a fake upstream session."""

import json
import os

import pytest

flask = pytest.importorskip("flask")

from conftest import FakeResponse, FakeUpstreamSession, make_config  # noqa: E402
from app import create_app  # noqa: E402

API = "http://cid.test/api"


@pytest.fixture
def client():
    os.environ["ADLIB_GATEWAY_TOKEN"] = "secret"
    cfg = make_config(
        max_queue_depth=50,
        max_queue_wait_s=5,
        op_budget_s=5,
        allowed_target_hosts=["cid.test"],
    )
    session = FakeUpstreamSession(
        lambda m, u, **k: FakeResponse(
            200, json.dumps({"adlibJSON": {"diagnostic": {"hits": 0}}})
        )
    )
    app = create_app(config=cfg, session=session)
    app.testing = True
    yield app.test_client()
    app.config["SERVICE"].stop()


def _auth(token="secret"):
    return {"Authorization": f"Bearer {token}"}


def test_healthz(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert "queue" in data


def test_request_requires_auth(client):
    resp = client.post("/request", json={"op": "get", "api": API})
    assert resp.status_code == 401


def test_request_wrong_token_rejected(client):
    resp = client.post(
        "/request", json={"op": "get", "api": API}, headers=_auth("nope")
    )
    assert resp.status_code == 401


def test_request_get_roundtrip(client):
    resp = client.post(
        "/request",
        json={
            "op": "get",
            "api": API,
            "params": {"command": "getversion"},
            "caller": "pytest/1@host",
        },
        headers=_auth(),
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert json.loads(body["text"])["adlibJSON"]["diagnostic"]["hits"] == 0


def test_request_rejects_bad_json(client):
    resp = client.post(
        "/request",
        data="not json",
        headers=_auth(),
        content_type="application/json",
    )
    assert resp.status_code == 400


def test_request_rejects_disallowed_target(client):
    resp = client.post(
        "/request",
        json={"op": "get", "api": "http://evil.test/api"},
        headers=_auth(),
    )
    assert resp.status_code == 400
    assert "not allowed" in resp.get_json()["error"]
