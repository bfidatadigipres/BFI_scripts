import os
import sys
import pytest
import requests
import json
from pathlib import Path

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib



import adlib_v3_sess as adlib_sess
import pytest
import requests
import json
from pathlib import Path

CASES = json.loads((Path(__file__).parent / "data" / "test_data.json").read_text())


def test_check(mocker):
    expected_output = {"adlibJSON": {"version": [{"spans": [{"text": "AxiellWebApi-Git, Version=3.9.1.3853"}]}]}}
    mock_get = mocker.patch("adlib_v3_sess.get", return_value=expected_output)

    api_url = "http://api"
    result = adlib_sess.check(api_url)

    assert result["adlibJSON"]["version"][0]["spans"][0]["text"] == "AxiellWebApi-Git, Version=3.9.1.3853"
    assert mock_get.call_args.args[0] == api_url
    assert mock_get.call_args.args[1] == {"command": "getversion", "limit": 0, "output": "jsonv1"}

def test_retrieve_record(mocker):
    mock_get = mocker.patch("adlib_v3_sess.get")
    mock_get.return_value = {'adlibJSON': {'recordList': {'record': [{'@attributes': {'priref': '12345678', 'created': '2025-10-01T06:02:40', 'modification': '2025-10-01T22:27:26', 'selected': 'False', 'deleted': 'False'},  'copy_status': [{'@lang': 'neutral', 'value': [{'spans': [{'text': 'M'}]}, {'spans': [{'text': 'Master'}]}]}],  'file_type': [{'spans': [{'text': 'MPEG-TS'}]}], 'Acquired_filename': [{'digital.acquired_filename': [{'spans': [{'text': '/mnt/qnap_04/STORA/2025/09/25/five/11-30-00-1253-01-10-00/stream.mpeg2.ts'}]}]}], 'Part_of': [{'part_of.title': [{'spans': [{'text': 'Vanessa'}]}], 'part_of_reference': [{'broadcast_channel': [[{'spans': [{'text': 'Channel 5 HD'}]}]], 'object_number': [[{'spans': [{'text': 'N-10768674'}]}]], 'priref': [{'spans': [{'text': '159193157'}]}]}]}], 'Reproduction': [{'imagen.media_identifier': [{'spans': []}], 'reproduction.reference': [{'reference_number': [{'spans': [{'text': 'N_10768675_01of01.ts'}]}]}]}], 'Title': [{'title': [{'spans': [{'text': 'Vanessa'}]}]}], 'grouping': [{'spans': [{'text': 'Digital Acquisition: Off-Air TV Recording: Automated'}]}], 'input.date': [{'spans': [{'text': '2025-10-01'}]}], 'input.name': [{'spans': [{'text': 'datadigipres'}]}], 'input.notes': [{'spans': [{'text': 'STORA off-air television capture - automated bulk documentation'}]}],'item_type': [{'@lang': 'neutral', 'value': [{'spans': [{'text': 'DIGITAL'}]}, {'spans': [{'text': 'Digital'}]}]}], 'object_number': [{'spans': [{'text': 'N-10768675'}]}], 'priref': [{'spans': [{'text': '159193158'}]}], 'record_type': [{'@lang': 'neutral', 'value': [{'spans': [{'text': 'ITEM'}]}, {'spans': [{'text': 'Item'}]}, {'spans': [{'text': 'Item'}]}]}]}]}, 'diagnostic': {'hits': 215, 'xmltype': 'Grouped', 'hits_on_display': 1, 'search': '(record_type=ITEM) and input.date="2025-10-01', 'sort': None, 'first_item': 1, 'forward': 0, 'backward': 0, 'limit': 1, 'dbname': 'collect', 'dsname': 'film', 'cgistring': {'database': 'items'}, 'link_resolve_time': {'value': '5.0002', 'unit': 'mS', 'culture': 'en-US'},'response_time': {'value': '42', 'unit': 'mS', 'culture': 'en-US'}}}}
    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_get
    mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

    hits, records = adlib_sess.retrieve_record(
        api = 'fake_api',
        database='items',
        search = f"priref=12345678",
        limit=1,
        session=mock_session,
        fields=None
    )
    
    assert hits == 215
    assert records[0]['priref'][0]['spans'][0]['text'] == '159193158'

    mock_get.assert_called_once()
    called_args = mock_get.call_args[0][1]
    assert called_args['database'] == 'items'

def test_retrieve_invalid_record(mocker):
    mock_get = mocker.patch("adlib_v3_sess.get")
    mock_get.return_value = {'adlibJSON': {'diagnostic': {'hits': 0, 'xmltype': 'Grouped', 'hits_on_display': 0, 'search': '(record_type=ITEM) and input.date="2025-10-01" and priref="1234"', 'sort': None, 'first_item': 1, 'forward': 0, 'backward': 0, 'limit': 1, 'dbname': 'collect', 'dsname': 'film', 'cgistring': {'database': 'items'}, 'xml_creation_time': {'value': '0', 'unit': 'mS', 'culture': 'en-US'}, 'response_time': {'value': '12', 'unit': 'mS', 'culture': 'en-US'}}}}

    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_get
    mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

    hits, records = adlib_sess.retrieve_record(
        api = 'fake_api',
        database='****',
        search = f"priref=1234",
        limit=1,
        session=mock_session,
        fields=None
    )
    
    print(records)
    assert hits == 0
    assert type(records) is type(None)

    mock_get.assert_called_once()
    called_args = mock_get.call_args[0][1]
    assert called_args['database'] == '****'

def test_get(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = '{"adlibJSON": {"version": [{"spans": [{"text": "AxiellWebApi-Git, Version=3.9.1.3853"}]}]}}'
    mock_session = mocker.Mock()
    mock_session.get.return_value = mock_response
    mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

    api = "https://api"
    query = {"command": "getversion", "limit": 0, "output": "jsonv1"}
    result = adlib_sess.get(api, query, mock_session)

    assert isinstance(result, dict)
    assert "adlibJSON" in result
    assert mock_session.get.call_args.args[0] == api

@pytest.mark.parametrize("exceptions", [
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.HTTPError
])
def test_get_exceptions(mocker,exceptions):
    mocker.patch("requests.Session.get", side_effect=exceptions)

    api = ''
    query =  {"command": "getversion", "limit": 0, "output": "jsonv1"}
    with pytest.raises(Exception):
        adlib_sess.get(api, query)

def test_get_invalid_query(mocker):
    mocker.patch("requests.request", side_effect=requests.exceptions.JSONDecodeError)
    api = "***"
    query = None
    with pytest.raises(Exception):
        adlib_sess.get(api, query)

@pytest.mark.parametrize("expected_method_input", [
    ("updaterecord"),
    ("insertrecord")

])
def test_post(mocker, expected_method_input):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = json_response = '''
    {
        "adlibJSON": {
            "recordList": {
                "record": [{"id": "12345", "name": "test record"}]
            
            }
        
        
        }

    }'''
    mock_check = mocker.patch("adlib_v3_sess.check_response", return_value = False)
    mock_session = mocker.Mock()
    mock_session.post.return_value = mock_response
    mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

    result = adlib_sess.post('https://fake-api.com', '<xml>payload</xml>', 'test_db', expected_method_input, mock_session)

    assert result == {'id': '12345', 'name': 'test record'}
    mock_session.post.assert_called_once_with(
    "https://fake-api.com",
    headers={"Content-Type": "text/xml"},
    params={
        "command": expected_method_input,
        "database": "test_db",
        "xmltype": "grouped",
        "output": "jsonv1"
    },
    data=b"<xml>payload</xml>",
    timeout=100
)
    mock_check.assert_called_once_with(json_response, "https://fake-api.com")

# # invalid post
@pytest.mark.parametrize("exceptions", [
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.HTTPError
])
def test_post_exceptions(mocker,exceptions):
    mocker.patch("requests.Session.post", side_effect=exceptions)
    mock_session = mocker.Mock()
    mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

    api = ''
    payload=""
    with pytest.raises(Exception):
        adlib_sess.post(api, payload, "", "any", mock_session)

@pytest.mark.parametrize("fake_record_input, fieldname_input, expected_output", [
    ("[{'@attributes': {'priref': '3542959', 'created': '2025-11-06T19:35:07', 'modification': '2025-11-06T19:35:07', 'selected': 'False', 'deleted': 'False'}, 'other.format.lref': [{'spans': [{'text': '402003'}]}] }]", "other.format.lref",  ["402003"])
])
def test_retrieve_field_name(fake_record_input, fieldname_input, expected_output):

    result = adlib_sess.retrieve_field_name(eval(fake_record_input)[0], fieldname_input)
    assert '402003' in result
    assert isinstance(result, list) 
    assert expected_output == result


def test_invalid_retrieve_field_name():
    record = "[{'@attributes': {'priref': '3542959', 'created': '2025-11-06T19:35:07', 'modification': '2025-11-06T19:35:07', 'selected': 'False', 'deleted': 'False'}, 'other.format.lref': [{'spans': [{'text': '402003'}]}], }]"

    result = adlib_sess.retrieve_field_name(eval(record)[0],'None')

    assert None in result
    assert isinstance(result, list) 


@pytest.mark.parametrize("record_input, fieldname_input, expected_output", 
                         [(c['input'], c['fieldname_input'], c['expected_output']) for c in CASES]
)
        
def test_group_check(record_input, fieldname_input, expected_output):

    results = adlib_sess.group_check(record_input, fieldname_input)
    assert isinstance(results, list)
    assert expected_output[0] in results 
    assert expected_output == results

@pytest.mark.parametrize("exceptions", [
    TypeError
])
def test_invalid_group_check(exceptions):
    # record value
    with pytest.raises(exceptions):
        adlib_sess.group_check()



def test_get_grouped_items(mocker):

   mock_response = mocker.Mock()
   mock_response.text = "<fake>xml</fake>"
   mock_response.get.return_value = mock_response
   mock_session = mocker.Mock()
   mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)


   fake_metadata = {
       "adlibXML" : {
           "recordList": {
               "record": [
                   {"group": "GroupA", "fieldName": {"value": [{"#text": "Field1"}]}},
                   {"group": "GroupA", "fieldName": {"value": [{"#text": "Field2"}]}},
                   {"group": "GroupB", "fieldName": {"value": [{"#text": "Fieldx"}]}},
               ]
           }
       }
   }
   mocker.patch("adlib.xmltodict.parse", return_value=fake_metadata)

   result = adlib_sess.get_grouped_items("http://fake-api", "test_db", mock_session)

   expected = {
       "GroupA": ["Field1", "Field2"],
       "GroupB": ["Fieldx"]
   }

   assert result == expected


def test_get_grouped_items_invalid(mocker):

   mock_response = mocker.Mock()
   mock_response.text = "<fake>xml</fake>"
   mock_response.get.return_value = mock_response
   mock_session = mocker.Mock()
   mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

   mocker.patch("adlib.xmltodict.parse", return_value="this is a string")

   result = adlib_sess.get_grouped_items("http://fake-api", "test_db", mock_session)

   assert result == (None, None)


def test_get_grouped_item_exce(mocker):

   mock_response = mocker.Mock()
   mock_response.text = "<fake>xml</fake>"
   mock_response.get.return_value = mock_response
   mock_session = mocker.Mock()
   mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

   fake_metadata = {
       "adlibXML" : {
           "recordList": {
               "record": None
           }
       }
   }
   mocker.patch("adlib.xmltodict.parse", return_value=fake_metadata)

   with pytest.raises(TypeError):
       adlib_sess.get_grouped_items("http://fake-api", "test_db", mock_session)



def test_create_record_data(monkeypatch, mocker):
    api = 'https://test_api'
    database = 'db_test'
    mock_session = mocker.Mock()
    mocker.patch("adlib_v3_sess.create_session", return_value=mock_session)

    record_data = [
        {"user": "user1"},
        {"input.date": "1999-01-01"},
        {"input.time": "00:00:00"},
        {"input.notes": "nooob"},
        {"priref": "12345"},
        {"reference_number": "N_1234657_01of01.mov"},
        {"imagen.media.original_filename": "N_1234657_01of01.mov"},
        {"object.object_number": "N-1234657"},
    ]

    def fake_get_grouped_items(api, database, mock_session):
        return {'cast': ['cast.credit_credited_name', 'cast.activity.sequence', 'cast.name', 'cast.credit_type', 'cast.credit_on_screen', 'cast.section', 'cast.cagroup', 'cast.sequence', 'cast.name.lref'], 'credits': ['credit.name', 'credit.name.lref', 'credit.credited_name', 'credit.type', 'credit.sequence', 'credit.activity.sequence', 'credit.cagroup', 'credit.on_screen', 'credit.section'], 'Production': ['creator.lref', 'creator.role', 'creator'], 'utb': ['utb.fieldname', 'utb.content'], 'Title_date': ['title_date_start', 'title_date.type'], 'Production_date': ['production.date.end', 'production.date.start'], 'Description': ['description', 'description.type.lref', 'description.type', 'description.date'], 'Content_person': ['content.person.name.lref', 'content.person.name'], 'Content_subject': ['content.subject.lref', 'content.subject'], 'Application_restriction': ['application_restriction'], 'Title': ['title.article', 'title.qualifier', 'title', 'title.type', 'title.language'], 'Part_of': ['part_of_reference.lref', 'part_of_reference'], 'Parts': ['parts_reference'], 'Inscription': ['inscription.language'], 'Related_object': ['related_object.reference.lref', 'related_object.title'], 'Label': ['label.source', 'label.text', 'label.type'], 'ACCESS_RIGHTS': ['record_access.rights', 'record_access.user'], 'Content_genre': ['content.genre.lref', 'content.genre'], 'Edit': ['edit.date', 'edit.time'], 'Notes': ['notes']}
    
    
    monkeypatch.setattr("adlib_v3_sess.get_grouped_items", fake_get_grouped_items)



    record_data_xml = adlib_sess.create_record_data(api, database, mock_session, "", record_data)


    assert isinstance(record_data_xml, str)
    assert record_data_xml == "<adlibXML><recordList><record><priref>0</priref><user>user1</user><input.date>1999-01-01</input.date><input.time>00:00:00</input.time><input.notes>nooob</input.notes><priref>12345</priref><reference_number>N_1234657_01of01.mov</reference_number><imagen.media.original_filename>N_1234657_01of01.mov</imagen.media.original_filename><object.object_number>N-1234657</object.object_number></record></recordList></adlibXML>"

@pytest.mark.parametrize("expected_input, expected_output", [
    ("priref&", "priref&amp;"),
    ("priref>18000", "priref&gt;18000"),
    ("priref<18000","priref&lt;18000"),
    ("priref='N-12345'","priref=&apos;N-12345&apos;")
])
def test_escape_xml(expected_input, expected_output):
    
    result_xml = adlib_sess.escape_xml(expected_input)

    assert expected_output == result_xml


@pytest.mark.parametrize("priref, grouping, field_pairs, outcome", [
    ("11111", 'Title', "[{'title': 'title number 1'}, {'title': 'title number 2'}]", "<adlibXML><recordList><record priref='11111'><Title><title><![CDATA[title number 1]]></title></Title><Title><title><![CDATA[title number 2]]></title></Title></record></recordList></adlibXML>"),
    ('2222', 'File_Type', "[{'title': 'title number 1', 'file_type': 'mov'}, {'title': 'title number 2', 'file_type': 'tiff'}]", "<adlibXML><recordList><record priref='2222'><File_Type><title><![CDATA[title number 1]]></title><file_type><![CDATA[mov]]></file_type></File_Type><File_Type><title><![CDATA[title number 2]]></title><file_type><![CDATA[tiff]]></file_type></File_Type></record></recordList></adlibXML>"),
    ('86754689', 'other.lref.video', "[{'title': 'title number 1', 'other.lref.video': 'mov'}, {'title': 'title number 2', 'other.lref.video': 'tiff'}]", "<adlibXML><recordList><record priref='86754689'><other.lref.video><title><![CDATA[title number 1]]></title><other.lref.video><![CDATA[mov]]></other.lref.video></other.lref.video><other.lref.video><title><![CDATA[title number 2]]></title><other.lref.video><![CDATA[tiff]]></other.lref.video></other.lref.video></record></recordList></adlibXML>"),
    
])
def test_create_grouped_data(priref, grouping, field_pairs, outcome):
    
    result = adlib_sess.create_grouped_data(priref,grouping, eval(field_pairs))

    assert isinstance(result, str)
    assert result == outcome

def test_add_quality_comments(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = """
 {
        "adlibJSON": {
            "recordList": {
                "record": [{"id": "12345", "name": "test record"}] 
            }     
        }
    }
                   """

    mock_session = mocker.Mock()
    mock_session.post.return_value = mock_response

    result = adlib_sess.add_quality_comments("", "12345", "moooooo", mock_session)

    assert result is True


def test_invalid_add_quality_comments(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = """
{"adlibJSON":{"diagnostic":{"hits":0,"xmltype":"Unstructured","hits_on_display":0,"search":null,"sort":null,"error":{"message":"Priref 12345 is outside the boundaries of the dataset range (150000001 - 170000006) for database 'collect>film'"},"first_item":1,"forward":0,"backward":0,"limit":0,"xml_creation_time":{"value":"0","unit":"mS","culture":"en-US"},"response_time":{"value":"2","unit":"mS","culture":"en-US"}}}}
"""
    mock_session = mocker.Mock()
    mock_session.post.return_value = mock_response

    result = adlib_sess.add_quality_comments("https://fake-api", "12345", "moooooo", mock_session)

    assert result is False

@pytest.mark.parametrize("exceptions", [
    Exception
])
def test_add_quality_comments_exceptions(exceptions):
    with pytest.raises(exceptions):
        adlib_sess.add_quality_comments("https://fake-api", "12345", "moooooo")

def test_check_response_call(mocker):
    api = 'https://test_api'
    rec = 'A severe error occurred on the current command.'

    mock_recycle = mocker.patch("adlib_v3_sess.recycle_api")
    result = adlib_sess.check_response(rec, api)

    mock_recycle.assert_called_once_with(api)
    assert result is True


def test_check_response_call_no_fails(mocker):
    api = 'https://test_api'
    rec = 'Everything is fine'

    mock_recycle = mocker.patch("adlib_v3_sess.recycle_api")
    result = adlib_sess.check_response(rec, api)

    mock_recycle.assert_not_called()
    assert result is None
