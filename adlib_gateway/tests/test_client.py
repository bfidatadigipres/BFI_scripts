"""Client library tests: envelope building, response parsing, legacy
signature compatibility, and gateway-hop retry. The gateway HTTP call is
faked via the ``fake_gateway`` fixture (no network)."""

import json

import pytest
from conftest import FakeResponse

import adlib
from adlib import GatewayError
from requests import Session

API = "http://cid.test/api"


def gw_get(axiell_body_obj):
    """Wrap an Axiell response the way the gateway returns it for op=get."""
    return FakeResponse(
        200,
        json.dumps(
            {"ok": True, "status_code": 200, "text": json.dumps(axiell_body_obj)}
        ),
    )


def gw_post(axiell_text):
    return FakeResponse(
        200, json.dumps({"ok": True, "status_code": 200, "text": axiell_text})
    )


def test_get_returns_parsed_json(fake_gateway):
    fake_gateway["responses"].append(gw_get({"adlibJSON": {"x": 1}}))
    out = adlib.get(API, {"command": "getversion"})
    assert out == {"adlibJSON": {"x": 1}}
    sent = fake_gateway["requests"][0]["json"]
    assert sent["op"] == "get"
    assert sent["api"] == API
    assert sent["caller"]


def test_auth_header_sent(fake_gateway):
    fake_gateway["responses"].append(gw_get({"ok": 1}))
    adlib.get(API, {})
    assert fake_gateway["requests"][0]["headers"]["Authorization"] == ("Bearer secret")


def test_get_raises_when_gateway_reports_not_ok(fake_gateway):
    fake_gateway["responses"].append(
        FakeResponse(200, json.dumps({"ok": False, "error": "upstream down"}))
    )
    with pytest.raises(GatewayError):
        adlib.get(API, {})


def test_client_retries_on_503_then_succeeds(fake_gateway):
    fake_gateway["responses"].append(
        FakeResponse(503, '{"error":"busy"}', headers={"Retry-After": "0"})
    )
    fake_gateway["responses"].append(gw_get({"adlibJSON": {"ok": 1}}))
    out = adlib.get(API, {})
    assert out == {"adlibJSON": {"ok": 1}}
    assert len(fake_gateway["requests"]) == 2


def test_missing_gateway_url_raises(monkeypatch):
    monkeypatch.delenv("ADLIB_GATEWAY_URL", raising=False)
    with pytest.raises(GatewayError):
        adlib.get(API, {})


def test_post_parses_record(fake_gateway):
    rec = {"@attributes": {"priref": "42"}}
    text = json.dumps({"adlibJSON": {"recordList": {"record": rec}}})
    fake_gateway["responses"].append(gw_post(text))
    out = adlib.post(API, "<x/>", "works", "createrecord")
    assert out == rec


def test_post_returns_false_on_failure_string(fake_gateway):
    fake_gateway["responses"].append(
        gw_post("A severe error occurred on the current command.")
    )
    out = adlib.post(API, "<x/>", "works", "updaterecord")
    assert out is False


def test_post_returns_none_on_error_body(fake_gateway):
    """Regression: legacy adlib_v3 referenced an undefined 'record' here."""
    fake_gateway["responses"].append(gw_post('{"error": "bad"}'))
    out = adlib.post(API, "<x/>", "works", "createrecord")
    assert out is None


def test_post_with_verify_returns_record(fake_gateway):
    rec = {"@attributes": {"priref": "7"}}
    fake_gateway["responses"].append(
        FakeResponse(200, json.dumps({"ok": True, "record": rec}))
    )
    out = adlib.post_with_verify(
        API, "<x/>", "works", "createrecord", search_value="object_number=N_7"
    )
    assert out == rec
    sent = fake_gateway["requests"][0]["json"]
    assert sent["op"] == "post_with_verify"
    assert sent["search_value"] == "object_number=N_7"


def test_post_with_verify_positional_string_is_search_value(fake_gateway):
    """Legacy adlib_v3 passed search_value as the 5th positional arg."""
    fake_gateway["responses"].append(
        FakeResponse(200, json.dumps({"ok": True, "record": None}))
    )
    adlib.post_with_verify(API, "<x/>", "works", "createrecord", "obj=N_1")
    sent = fake_gateway["requests"][0]["json"]
    assert sent["op"] == "post_with_verify"
    assert sent["search_value"] == "obj=N_1"


def test_write_lock_returns_true(fake_gateway):
    fake_gateway["responses"].append(
        FakeResponse(200, json.dumps({"ok": True, "locked": True}))
    )
    assert adlib.write_lock(API, "42", "items") is True


def test_retrieve_record_hits_and_records(fake_gateway):
    rec = [{"@attributes": {"priref": "1"}}]
    body = {"adlibJSON": {"diagnostic": {"hits": 1}, "recordList": {"record": rec}}}
    fake_gateway["responses"].append(gw_get(body))
    hits, records = adlib.retrieve_record(API, "items", "object_number=N_1", 1)
    assert hits == 1
    assert records == rec


def test_retrieve_record_list_fifth_positional_is_fields(fake_gateway):
    """Legacy adlib_v3 passed fields (a list) as the 5th positional arg."""
    body = {"adlibJSON": {"diagnostic": {"hits": 0}}}
    fake_gateway["responses"].append(gw_get(body))
    adlib.retrieve_record(API, "items", "object_number=N_1", 1, ["priref"])
    sent = fake_gateway["requests"][0]["json"]
    assert "priref" in sent["params"]["fields"]


def test_retrieve_record_prefixes_record_type(fake_gateway):
    fake_gateway["responses"].append(gw_get({"adlibJSON": {"diagnostic": {"hits": 0}}}))
    adlib.retrieve_record(API, "works", "id=X", 1)
    sent = fake_gateway["requests"][0]["json"]
    assert sent["params"]["search"].startswith("(record_type=WORK)")


def test_create_record_data_v3_positional(monkeypatch):
    monkeypatch.setattr(
        adlib, "get_grouped_items", lambda *a, **k: {"group1": ["field1"]}
    )
    xml = adlib.create_record_data(API, "items", "123", [{"field1": "v"}])
    assert "<priref>123</priref>" in xml
    assert "<group1><field1>v</field1></group1>" in xml


def test_create_record_data_sess_positional(monkeypatch):
    monkeypatch.setattr(
        adlib, "get_grouped_items", lambda *a, **k: {"group1": ["field1"]}
    )
    sess = Session()
    xml = adlib.create_record_data(API, "items", sess, "123", [{"field1": "v"}])
    assert "<priref>123</priref>" in xml
    assert "<group1><field1>v</field1></group1>" in xml


def test_escape_xml():
    assert adlib.escape_xml("a & b < c > d") == "a &amp; b &lt; c &gt; d"


def test_create_grouped_data():
    xml = adlib.create_grouped_data("9", "Label", [{"label.type": "main"}])
    assert xml.startswith("<adlibXML><recordList><record priref='9'>")
    assert "<Label><label.type><![CDATA[main]]></label.type></Label>" in xml


def test_retrieve_field_name_plain_span():
    rec = {"object_number": [{"spans": [{"text": "N_1"}]}]}
    assert adlib.retrieve_field_name(rec, "object_number") == ["N_1"]


def test_retrieve_field_name_string_value():
    rec = {"priref": ["12345"]}
    assert adlib.retrieve_field_name(rec, "priref") == ["12345"]


def test_group_check_single_group():
    rec = {"Title": [{"title": [{"spans": [{"text": "A"}]}]}]}
    assert adlib.group_check(rec, "title") == ["A"]


def test_check_response_detects_failure():
    assert (
        adlib.check_response(
            "Execution Timeout Expired. The timeout period elapsed", API
        )
        is True
    )


def test_recycle_api_is_noop(capsys):
    adlib.recycle_api(API)
    assert "gateway" in capsys.readouterr().out
