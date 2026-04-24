#!/usr/bin/env python3

import os
import sys
import pytest
import requests
import json
from pathlib import Path

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib


CASES = json.loads((Path(__file__).parent / "data" / "test_data.json").read_text())


def test_check(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = '{"adlibJSON": {"version": [{"spans": [{"text": "AxiellWebApi-Git, Version=3.9.1.3853"}]}]}}'
    mocker.patch("requests.request", return_value=mock_response)

    api_url = "fake_api"
    result = adlib.check(api_url)

    assert "adlibJSON" in result
    assert "version" in result["adlibJSON"]


def test_get(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = '{"adlibJSON": {"version": [{"spans": [{"text": "AxiellWebApi-Git, Version=3.9.1.3853"}]}]}}'
    mocker.patch("adlib_v3.requests.request", return_value=mock_response)

    api = ""
    query = {"command": "getversion", "limit": 0, "output": "jsonv1"}

    result = adlib.get(api, query)

    assert isinstance(result, dict)
    assert "adlibJSON" in result


@pytest.mark.parametrize(
    "exceptions",
    [
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
    ],
)
def test_get_exceptions(mocker, exceptions):
    mocker.patch("requests.request", side_effect=exceptions)

    api = ""
    query = {"command": "getversion", "limit": 0, "output": "jsonv1"}
    with pytest.raises(Exception):
        adlib.get(api, query)


def test_get_invalid_query(mocker):
    mocker.patch("requests.request", side_effect=requests.exceptions.JSONDecodeError)
    api = "***"
    query = None
    with pytest.raises(Exception):
        adlib.get(api, query)


def test_retrieve_record(mocker):
    mock_get = mocker.patch("adlib_v3.get")
    mock_get.return_value = {
        "adlibJSON": {
            "recordList": {
                "record": [
                    {
                        "@attributes": {
                            "priref": "12345678",
                            "created": "2025-10-01T06:02:40",
                            "modification": "2025-10-01T22:27:26",
                            "selected": "False",
                            "deleted": "False",
                        },
                        "copy_status": [
                            {
                                "@lang": "neutral",
                                "value": [
                                    {"spans": [{"text": "M"}]},
                                    {"spans": [{"text": "Master"}]},
                                ],
                            }
                        ],
                        "file_type": [{"spans": [{"text": "MPEG-TS"}]}],
                        "Acquired_filename": [
                            {
                                "digital.acquired_filename": [
                                    {
                                        "spans": [
                                            {
                                                "text": "/mnt/qnap_04/STORA/2025/09/25/five/11-30-00-1253-01-10-00/stream.mpeg2.ts"
                                            }
                                        ]
                                    }
                                ]
                            }
                        ],
                        "Part_of": [
                            {
                                "part_of.title": [{"spans": [{"text": "Vanessa"}]}],
                                "part_of_reference": [
                                    {
                                        "broadcast_channel": [
                                            [{"spans": [{"text": "Channel 5 HD"}]}]
                                        ],
                                        "object_number": [
                                            [{"spans": [{"text": "N-10768674"}]}]
                                        ],
                                        "priref": [{"spans": [{"text": "159193157"}]}],
                                    }
                                ],
                            }
                        ],
                        "Reproduction": [
                            {
                                "imagen.media_identifier": [{"spans": []}],
                                "reproduction.reference": [
                                    {
                                        "reference_number": [
                                            {
                                                "spans": [
                                                    {"text": "N_10768675_01of01.ts"}
                                                ]
                                            }
                                        ]
                                    }
                                ],
                            }
                        ],
                        "Title": [{"title": [{"spans": [{"text": "Vanessa"}]}]}],
                        "grouping": [
                            {
                                "spans": [
                                    {
                                        "text": "test"
                                    }
                                ]
                            }
                        ],
                        "input.date": [{"spans": [{"text": "2025-10-01"}]}],
                        "input.name": [{"spans": [{"text": "user"}]}],
                        "input.notes": [
                            {
                                "spans": [
                                    {
                                        "text": "test"
                                    }
                                ]
                            }
                        ],
                        "item_type": [
                            {
                                "@lang": "neutral",
                                "value": [
                                    {"spans": [{"text": "DIGITAL"}]},
                                    {"spans": [{"text": "Digital"}]},
                                ],
                            }
                        ],
                        "object_number": [{"spans": [{"text": "N_12345"}]}],
                        "priref": [{"spans": [{"text": "179722376"}]}],
                        "record_type": [
                            {
                                "@lang": "neutral",
                                "value": [
                                    {"spans": [{"text": "ITEM"}]},
                                    {"spans": [{"text": "Item"}]},
                                    {"spans": [{"text": "Item"}]},
                                ],
                            }
                        ],
                    }
                ]
            },
            "diagnostic": {
                "hits": 215,
                "xmltype": "Grouped",
                "hits_on_display": 1,
                "search": '(record_type=ITEM) and input.date="2025-10-01',
                "sort": None,
                "first_item": 1,
                "forward": 0,
                "backward": 0,
                "limit": 1,
                "dbname": "collect",
                "dsname": "film",
                "cgistring": {"database": "items"},
                "link_resolve_time": {
                    "value": "5.0002",
                    "unit": "mS",
                    "culture": "en-US",
                },
                "response_time": {"value": "42", "unit": "mS", "culture": "en-US"},
            },
        }
    }
    hits, records = adlib.retrieve_record(
        api="fake_api",
        database="items",
        search=f"priref=12345678",
        limit=1,
        fields=None,
    )

    # print(records)
    assert hits == 215
    # assert isinstance(records, list)
    assert records[0]["priref"][0]["spans"][0]["text"] == "179722376"

    mock_get.assert_called_once()
    called_args = mock_get.call_args[0][1]
    assert called_args["database"] == "items"


def test_retrieve_invalid_record(mocker):
    mock_get = mocker.patch("adlib_v3.get")
    mock_get.return_value = {
        "adlibJSON": {
            "diagnostic": {
                "hits": 0,
                "xmltype": "Grouped",
                "hits_on_display": 0,
                "search": '(record_type=ITEM) and input.date="2025-10-01" and priref="1234"',
                "sort": None,
                "first_item": 1,
                "forward": 0,
                "backward": 0,
                "limit": 1,
                "dbname": "collect",
                "dsname": "film",
                "cgistring": {"database": "items"},
                "xml_creation_time": {"value": "0", "unit": "mS", "culture": "en-US"},
                "response_time": {"value": "12", "unit": "mS", "culture": "en-US"},
            }
        }
    }
    hits, records = adlib.retrieve_record(
        api="fake_api", database="****", search=f"priref=1234", limit=1, fields=None
    )

    print(records)
    assert hits == 0
    assert type(records) is type(None)

    mock_get.assert_called_once()
    called_args = mock_get.call_args[0][1]
    assert called_args["database"] == "****"


def test_get_grouped_items(mocker):

    mock_response = mocker.Mock()
    mock_response.text = "<fake>xml</fake>"

    mocker.patch("requests.request", return_value=mock_response)

    fake_metadata = {
        "adlibXML": {
            "recordList": {
                "record": [
                    {"group": "GroupA", "fieldName": {"value": [{"#text": "Field1"}]}},
                    {"group": "GroupA", "fieldName": {"value": [{"#text": "Field2"}]}},
                    {"group": "GroupB", "fieldName": {"value": [{"#text": "Fieldx"}]}},
                ]
            }
        }
    }
    mocker.patch("adlib_v3.xmltodict.parse", return_value=fake_metadata)

    result = adlib.get_grouped_items("http://fake-api", "test_db")

    expected = {"GroupA": ["Field1", "Field2"], "GroupB": ["Fieldx"]}

    assert result == expected


def test_get_grouped_items_invalid(mocker):

    mock_response = mocker.Mock()
    mock_response.text = "<fake>xml</fake>"

    mocker.patch("requests.request", return_value=mock_response)

    mocker.patch("adlib_v3.xmltodict.parse", return_value="this is a string")

    result = adlib.get_grouped_items("http://fake-api", "test_db")

    assert result == (None, None)


def test_get_grouped_item_exce(mocker):

    mock_response = mocker.Mock()
    mock_response.text = "<fake>xml</fake>"

    mocker.patch("requests.request", return_value=mock_response)

    fake_metadata = {"adlibXML": {"recordList": {"record": None}}}
    mocker.patch("adlib_v3.xmltodict.parse", return_value=fake_metadata)

    with pytest.raises(TypeError):
        adlib.get_grouped_items("http://fake-api", "test_db")




def test_check_response_call(mocker):
    api = "https://test_api"
    rec = "A severe error occurred on the current command."

    mock_recycle = mocker.patch("adlib_v3.recycle_api")
    result = adlib.check_response(rec, api)

    mock_recycle.assert_called_once_with(api)
    assert result is True


def test_check_response_call_no_fails(mocker):
    api = "https://test_api"
    rec = "Everything is fine"

    mock_recycle = mocker.patch("adlib_v3.recycle_api")
    result = adlib.check_response(rec, api)

    mock_recycle.assert_not_called()
    assert result is None


def test_retrieve_facet_list(mocker):

    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = '{"adlibJSON": {"version": [{"spans": [{"text": "AxiellWebApi-Git, Version=3.9.1.3853"}]}]}}'
    mocker.patch("adlib_v3.requests.request", return_value=mock_response)

    result = "{'adlibJSON': {'facetList': [{'facet': 'dataType', 'values': [{'term': {'spans': [{'text': 'FolderData'}]}, 'lang': '', 'hits': 82407, 'priref': 1}, {'term': {'spans': [{'text': 'ReturnItems'}]}, 'lang': '', 'hits': 80528, 'priref': 33}, {'term': {'spans': [{'text': 'PickItems'}]}, 'lang': '', 'hits': 60983, 'priref': 32}, {'term': {'spans': [{'text': 'VideoCopy'}]}, 'lang': '', 'hits': 21022, 'priref': 15}, {'term': {'spans': [{'text': 'OffAir'}]}, 'lang': '', 'hits': 19534, 'priref': 2}, {'term': {'spans': [{'text': 'TransportIn'}]}, 'lang': '', 'hits': 17709, 'priref': 35}, {'term': {'spans': [{'text': 'TransportOut'}]}, 'lang': '', 'hits': 17265, 'priref': 34}, {'term': {'spans': [{'text': 'DataMigration'}]}, 'lang': '', 'hits': 11467, 'priref': 23}, {'term': {'spans': [{'text': 'VideoEncoding'}]}, 'lang': '', 'hits': 9009, 'priref': 20}, {'term': {'spans': [{'text': 'PreparationProjection'}]}, 'lang': '', 'hits': 7865, 'priref': 29}, {'term': {'spans': [{'text': 'PreparationScanning'}]}, 'lang': '', 'hits': 6137, 'priref': 28}, {'term': {'spans': [{'text': 'FilmCleaning'}]}, 'lang': '', 'hits': 5935, 'priref': 12}, {'term': {'spans': [{'text': 'IngestData'}]}, 'lang': '', 'hits': 4968, 'priref': 22}, {'term': {'spans': [{'text': 'DigitalQualityControl'}]}, 'lang': '', 'hits': 4394, 'priref': 6}, {'term': {'spans': [{'text': 'ServiceOnReturn'}]}, 'lang': '', 'hits': 3915, 'priref': 31}, {'term': {'spans': [{'text': 'Transcoding'}]}, 'lang': '', 'hits': 3832, 'priref': 21}, {'term': {'spans': [{'text': 'Inspection'}]}, 'lang': '', 'hits': 2762, 'priref': 25}, {'term': {'spans': [{'text': 'DataMigrationLTO'}]}, 'lang': '', 'hits': 2269, 'priref': 24}, {'term': {'spans': [{'text': 'TechnicalAcceptance'}]}, 'lang': '', 'hits': 1923, 'priref': 3}, {'term': {'spans': [{'text': 'AudioEncoding'}]}, 'lang': '', 'hits': 1613, 'priref': 19}, {'term': {'spans': [{'text': '2K4KScanning'}]}, 'lang': '', 'hits': 1194, 'priref': 18}, {'term': {'spans': [{'text': 'HDScanning'}]}, 'lang': '', 'hits': 1065, 'priref': 17}, {'term': {'spans': [{'text': 'TechnicalSelection'}]}, 'lang': '', 'hits': 968, 'priref': 26}, {'term': {'spans': [{'text': 'PreparationOther'}]}, 'lang': '', 'hits': 884, 'priref': 30}, {'term': {'spans': [{'text': 'Disposal'}]}, 'lang': '', 'hits': 397, 'priref': 38}, {'term': {'spans': [{'text': 'VideoQualityControl'}]}, 'lang': '', 'hits': 360, 'priref': 4}, {'term': {'spans': [{'text': 'PreparationPrinting'}]}, 'lang': '', 'hits': 340, 'priref': 27}, {'term': {'spans': [{'text': 'AudioQualityControl'}]}, 'lang': '', 'hits': 330, 'priref': 5}, {'term': {'spans': [{'text': 'DigitalImageGrading'}]}, 'lang': '', 'hits': 216, 'priref': 8}, {'term': {'spans': [{'text': 'FilmPrinting'}]}, 'lang': '', 'hits': 206, 'priref': 13}, {'term': {'spans': [{'text': 'FilmProcessing'}]}, 'lang': '', 'hits': 190, 'priref': 14}, {'term': {'spans': [{'text': 'AnalogImageGrading'}]}, 'lang': '', 'hits': 154, 'priref': 7}, {'term': {'spans': [{'text': 'AudioCopy'}]}, 'lang': '', 'hits': 71, 'priref': 16}, {'term': {'spans': [{'text': 'DigitalImageRestoration'}]}, 'lang': '', 'hits': 60, 'priref': 9}, {'term': {'spans': [{'text': 'NewTitleCreation'}]}, 'lang': '', 'hits': 26, 'priref': 11}, {'term': {'spans': [{'text': 'SDScanning'}]}, 'lang': '', 'hits': 19, 'priref': 39}, {'term': {'spans': [{'text': 'LoansOut'}]}, 'lang': '', 'hits': 4, 'priref': 37}, {'term': {'spans': [{'text': 'SilentInterTitleRestoration'}]}, 'lang': '', 'hits': 1, 'priref': 10}]}], 'diagnostic': {'hits': 372022, 'xmltype': 'Grouped', 'hits_on_display': 372022, 'search': 'dataType>0', 'sort': None, 'first_item': 1, 'forward': 0, 'backward': 0, 'limit': -1, 'dbname': 'workflow', 'dsname': '', 'cgistring': {'database': 'workflow'}, 'xml_creation_time': {'value': '0', 'unit': 'mS', 'culture': 'en-US'}, 'response_time': {'value': '6153', 'unit': 'mS', 'culture': 'en-US'}}}}"
    mocker.patch("adlib_v3.get", return_value=result)

    results = adlib.retrieve_facet_list(eval(result), "term")

    assert "FolderData" in results


@pytest.mark.parametrize(
    "exceptions",
    [
        TypeError,
    ],
)
def test_invalid_retrieve_facet_list(exceptions):

    with pytest.raises(exceptions):
        adlib.retrieve_facet_list(None, "***")


@pytest.mark.parametrize(
    "fake_record_input, fieldname_input, expected_output",
    [
        (
            "[{'@attributes': {'priref': '3542959', 'created': '2025-11-06T19:35:07', 'modification': '2025-11-06T19:35:07', 'selected': 'False', 'deleted': 'False'}, 'other.format.lref': [{'spans': [{'text': '402003'}]}] }]",
            "other.format.lref",
            ["402003"],
        )
    ],
)
def test_retrieve_field_name(fake_record_input, fieldname_input, expected_output):

    result = adlib.retrieve_field_name(eval(fake_record_input)[0], fieldname_input)
    assert "402003" in result
    assert isinstance(result, list)
    assert expected_output == result


def test_invalid_retrieve_field_name():
    record = "[{'@attributes': {'priref': '3542959', 'created': '2025-11-06T19:35:07', 'modification': '2025-11-06T19:35:07', 'selected': 'False', 'deleted': 'False'}, 'other.format.lref': [{'spans': [{'text': '402003'}]}], }]"

    result = adlib.retrieve_field_name(eval(record)[0], "None")

    assert None in result
    assert isinstance(result, list)


@pytest.mark.parametrize(
    "record_input, fieldname_input, expected_output",
    [(c["input"], c["fieldname_input"], c["expected_output"]) for c in CASES],
)
def test_group_check(record_input, fieldname_input, expected_output):

    results = adlib.group_check(record_input, fieldname_input)
    assert isinstance(results, list)
    assert expected_output[0] in results
    assert expected_output == results


@pytest.mark.parametrize("exceptions", [TypeError])
def test_invalid_group_check(exceptions):
    # record value
    with pytest.raises(exceptions):
        adlib.group_check()

def test_create_record_data(monkeypatch):
    api = "https://test_api"
    database = "db_test"
    record = [
        {"user": "user1"},
        {"input.date": "1999-01-01"},
    ]

    def fake_get_grouped_items(api, database):
        return {"group1": ["user", "input.date"]}

    def fake_get_fragment(data):
        return ["<user>uesr1</user>", "<input.date>1999-01-01</input.date>"]

    monkeypatch.setattr("adlib.get_grouped_items", fake_get_grouped_items)

    record_data_xml = adlib.create_record_data(api, database, "", record)

    assert isinstance(record_data_xml, str)
    assert (
        record_data_xml
        == "<adlibXML><recordList><record><priref>0</priref><user>uesr1</user><input.date>1999-01-01</input.date></record></recordList></adlibXML>"
    )

@pytest.mark.parametrize(
    "priref, grouping, field_pairs, outcome",
    [
        (
            "11111",
            "Title",
            "[{'title': 'title number 1'}, {'title': 'title number 2'}]",
            "<adlibXML><recordList><record priref='11111'><Title><title><![CDATA[title number 1]]></title></Title><Title><title><![CDATA[title number 2]]></title></Title></record></recordList></adlibXML>",
        ),
        (
            "2222",
            "File_Type",
            "[{'title': 'title number 1', 'file_type': 'mov'}, {'title': 'title number 2', 'file_type': 'tiff'}]",
            "<adlibXML><recordList><record priref='2222'><File_Type><title><![CDATA[title number 1]]></title><file_type><![CDATA[mov]]></file_type></File_Type><File_Type><title><![CDATA[title number 2]]></title><file_type><![CDATA[tiff]]></file_type></File_Type></record></recordList></adlibXML>",
        ),
        (
            "86754689",
            "other.lref.video",
            "[{'title': 'title number 1', 'other.lref.video': 'mov'}, {'title': 'title number 2', 'other.lref.video': 'tiff'}]",
            "<adlibXML><recordList><record priref='86754689'><other.lref.video><title><![CDATA[title number 1]]></title><other.lref.video><![CDATA[mov]]></other.lref.video></other.lref.video><other.lref.video><title><![CDATA[title number 2]]></title><other.lref.video><![CDATA[tiff]]></other.lref.video></other.lref.video></record></recordList></adlibXML>",
        ),
    ],
)
def test_create_grouped_data(priref, grouping, field_pairs, outcome):

    result = adlib.create_grouped_data(priref, grouping, eval(field_pairs))

    assert isinstance(result, str)
    assert result == outcome


def test_post(mocker):
    mock_reponse = mocker.Mock()
    mock_reponse.text = (
        json_response
    ) = """
    {
        "adlibJSON": {
            "recordList": {
                "record": [{"id": "12345", "name": "test record"}]

            }


        }

    }"""

    mock_request = mocker.patch("adlib_v3.requests.request", return_value=mock_reponse)
    mock_check = mocker.patch("adlib_v3.check_response", return_value=False)

    result = adlib.post(
        "https://fake-api.com", "<xml>payload</xml>", "test_db", "create"
    )

    assert result == {"id": "12345", "name": "test record"}
    mock_request.assert_called_once_with(
        "POST",
        "https://fake-api.com",
        headers={"Content-Type": "text/xml"},
        params={
            "command": "create",
            "database": "test_db",
            "xmltype": "grouped",
            "output": "jsonv1",
        },
        data=b"<xml>payload</xml>",
        timeout=100,
    )
    mock_check.assert_called_once_with(json_response, "https://fake-api.com")


def test_add_quality_comments(mocker):
    post_results = """
 {
        "adlibJSON": {
            "recordList": {
                "record": [{"id": "12345", "name": "test record"}]
            }
        }
    }
                   """
    mocker.patch("adlib_v3.post", return_value=post_results)

    result = adlib.add_quality_comments("", "12345", "moooooo")

    assert result is True


def test_invalid_add_quality_comments(mocker):
    mocker.patch("adlib_v3.post", return_value=None)

    result = adlib.add_quality_comments("https://fake-api", "12345", "moooooo")

    assert result is False


@pytest.mark.parametrize("exceptions", [Exception])
def test_add_quality_comments_exceptions(exceptions):
    with pytest.raises(exceptions):
        adlib.add_quality_comments("https://fake-api", "12345", "moooooo")



@pytest.mark.parametrize("error_status_code", [
    405, 403, 500
])
def test_invalid_unlock_record(mocker, error_status_code):
    mock_response = mocker.Mock()
    mock_response.status_code = error_status_code
    mocker.patch("adlib_v3.requests.post", return_value=mock_response)

    result = adlib.unlock_record("http://api", "12334", "db")

    assert result is False

def test_connection_error_unlock_record(mocker):

    mocker.patch("adlib_v3.requests.post", side_effect=requests.exceptions.ConnectionError())
    result = adlib.unlock_record("http://invalid_api", "1234", "db")

    assert result is None

def test_connection_error_log_unlock(mocker):
    mocker.patch("adlib_v3.requests.post", side_effect=requests.exceptions.ConnectionError())
    mock_print=mocker.patch("builtins.print")

    adlib.unlock_record("http://invalid_api", "1234", "db")

    assert "1234" in mock_print.call_args.args[0]
    assert "failed" in mock_print.call_args.args[0]

def test_unlock_record(mocker):
    mock_response=mocker.Mock()
    mock_response.status_code = 200
    mock_response.text="""
            {"adlibJSON":{"diagnostic":{"hits":0,"xmltype":"Unstructured","hits_on_display":0,"search":null,"sort":null,"message":"Record '1234' in database 'db' unlocked","first_item":1,"forward":0,"backward":0,"limit":0,"xml_creation_time":{"value":"0","unit":"mS","culture":"en-US"}}}}
    """
    mock_post=mocker.patch("adlib_v3.requests.post", return_value=mock_response)
    mock_print=mocker.patch("builtins.print")

    result = adlib.unlock_record("http://valid_api", "1234", "db")

    assert result is True
    assert  '"Record \'1234\' in database \'db\' unlocked"' in mock_print.call_args.args[0]
    assert mock_post.call_args.args[0] == "http://valid_api"
    assert mock_post.call_args.kwargs["params"]["command"] == "unlockrecord"
    assert mock_post.call_args.kwargs["params"]["priref"] == "1234"
    assert "db" in mock_post.call_args.kwargs["params"]['database']


@pytest.mark.parametrize("status_error_code", [
    402, 405, 500
])
def test_invalid_write_lock(mocker, status_error_code):
    mock_response = mocker.Mock()
    mock_response.status_code = status_error_code
    mocker.patch("adlib_v3.requests.post", return_value=mock_response)

    result = adlib.write_lock("http://api", "1234", "db")
    assert result is False

def test_connection_error_write_record(mocker):

    mocker.patch("adlib_v3.requests.post", side_effect=requests.exceptions.ConnectionError())
    result = adlib.write_lock("http://invalid_api", "1234", "db")

    assert result is None

def test_connection_error_log_lock(mocker):
    mocker.patch("adlib_v3.requests.post", side_effect=requests.exceptions.ConnectionError())
    mock_print=mocker.patch("builtins.print")

    adlib.write_lock("http://invalid_api", "1234", "db")

    assert "1234" in mock_print.call_args.args[0]
    assert "Lock record wasn't applied to record 1234" in mock_print.call_args.args[0]
    

@pytest.mark.parametrize("type_api, results, error_message", [
    ("""

       {"adlibJSON":{"diagnostic":{"hits":1,search":null,"sort":null,"message":"Record '1234' has  been set for user 'user'","first_item":1,"forward":0,"backward":0,"limit":0,"xml_creation_time":{"value":"0","unit":"mS","culture":"en-US"}}}}

    """, "Record '1234' has  been set for user 'user'", False), 
    ("""
{"adlibJSON":{"recordList":{"record":[]},"diagnostic":{"hits":1,"search":null,"sort":null,"error":{"message":"User 'user' attempts to lock record, with priref '1234' in database 'collect' but the record is already locked by 'user'"},"first_item":1,"forward":0,"backward":0,"limit":0,"xml_creation_time":{"value":"0","unit":"mS","culture":"en-US"}}}}
""", "User 'user' attempts to lock record, with priref '1234' in database 'collect' but the record is already locked by 'user'", True)
])
def test_write_lock(mocker, type_api, results, error_message):
    mock_response=mocker.Mock()
    mock_response.status_code = 200
    mock_response.text=type_api
    mock_post = mocker.patch("adlib_v3.requests.post", return_value=mock_response)
    mock_print = mocker.patch("builtins.print")

    result = adlib.write_lock("http://valid_api", "1234", "db")
    assert result is True
    assert mock_post.call_args.args[0] == "http://valid_api"
    assert "lockrecord" in mock_post.call_args.kwargs["params"]['command']
    assert "1234" in mock_post.call_args.kwargs["params"]['priref']
    assert "db" in mock_post.call_args.kwargs["params"]['database']

    if error_message:
        assert "error" in mock_print.call_args.args[0]

    assert results in mock_print.call_args.args[0]