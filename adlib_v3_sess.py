#!/usr/bin/env python3

'''
Python interface for Adlib API v3.7.17094.1+
(http://api.adlibsoft.com/site/api)

2024
'''

import json
import datetime
from requests import Session, exceptions
from lxml import etree, html
from dicttoxml import dicttoxml
import xmltodict
from tenacity import retry, stop_after_attempt

HEADERS = {
    'Content-Type': 'text/xml'
}


def check(api):
    '''
    Check API responds
    '''
    query = {
        'command': 'getversion',
        'limit': 0,
        'output': 'jsonv1'
    }

    return get(api, query)


def create_session():
    '''
    Start a requests session and return
    '''
    session = Session()
    return session


def retrieve_record(api, database, search, limit, session=None, fields=None):
    '''
    Retrieve data from CID using new API
    '''
    query = {
        'database': database,
        'search': search,
        'limit': limit,
        'output': 'jsonv1'
    }

    if fields:
        field_str = ', '.join(fields)
        query['fields'] = field_str

    record = get(api, query, session)
    if not record:
        return None, None
    elif record['adlibJSON']['diagnostic']['hits'] == 0:
        return 0, None
    elif 'recordList' not in str(record):
        try:
            hits = record['adlibJSON']['diagnostic']['hits']
            return hits, record
        except (IndexError, KeyError, TypeError) as err:
            print(err)
            return 0, record

    hits = record['adlibJSON']['diagnostic']['hits']
    return hits, record['adlibJSON']['recordList']['record']


@retry(stop=stop_after_attempt(10))
def get(api, query, session):
    '''
    Send a GET request
    '''
    if not session:
        session = create_session()
    try:
        req = session.get(api, headers=HEADERS, params=query)
        if req.status_code != 200:
            raise Exception
        dct = json.loads(req.text)
        return dct
    except exceptions.Timeout as err:
        print(err)
        raise Exception
    except exceptions.ConnectionError as err:
        print(err)
        raise Exception
    except exceptions.HTTPError as err:
        print(err)
        raise Exception


def post(api, payload, database, method, session=None):
    '''
    Send a POST request
    '''
    params = {
        'command': method,
        'database': database,
        'xmltype': 'grouped',
        'output': 'jsonv1'
    }
    payload = payload.encode('utf-8')

    if not session:
        session = create_session()

    if method == 'insertrecord':
        try:
            response = session.post(api, headers=HEADERS, params=params, data=payload, timeout=1200)
        except exceptions.Timeout as err:
            print(err)
            raise Exception
        except exceptions.ConnectionError as err:
            print(err)
            raise Exception
        except exceptions.HTTPError as err:
            print(err)
            raise Exception

    if method == 'updaterecord':
        try:
            response = session.post(api, headers=HEADERS, params=params, data=payload, timeout=1200)
        except exceptions.Timeout as err:
            print(err)
            raise Exception
        except exceptions.ConnectionError as err:
            print(err)
            raise Exception
        except exceptions.HTTPError as err:
            print(err)
            raise Exception

    print("-------------------------------------")
    print(f"adlib_v3.POST(): {response.text}")
    print("-------------------------------------")
    if 'recordList' in response.text:
        record = json.loads(response.text)
        try:
            if isinstance(record['adlibJSON']['recordList']['record'], list):
                return record['adlibJSON']['recordList']['record'][0]
            else:
                return record['adlibJSON']['recordList']['record']
        except (KeyError, IndexError, TypeError):
            return record
    elif '@attributes' in response.text:
        record = json.loads(response.text)
        return record
    elif 'error' in response.text:
        return None

    return None


def retrieve_field_name(record, fieldname):
    '''
    Retrieve record, check for language data
    Alter retrieval method. record ==
    ['adlibJSON']['recordList']['record'][0]
    '''
    field_list = []

    try:
        for field in record[f'{fieldname}']:
            if isinstance(field, str):
                field_list.append(field)
            elif "'@lang'" in str(field):
                field_list.append(field['value'][0]['spans'][0]['text'])
            else:
                field_list.append(field['spans'][0]['text'])
    except KeyError:
        field_list = group_check(record, fieldname)

    if not isinstance(field_list, list):
        return [field_list]
    return field_list


def retrieve_facet_list(record, fname):
    '''
    Retrieve list of facets
    '''
    facets = []
    for value in record['adlibJSON']['facetList'][0]['values']:
        facets.append(value[fname]['spans'][0]['text'])
    
    return facets


def group_check(record, fname):
    '''
    Get group that contains field key
    '''
    group_check = dict([ (k, v) for k, v in record.items() if f'{fname}' in str(v) ])
    fieldnames = []
    if len(group_check) == 1:
        first_key = next(iter(group_check))
        for entry in group_check[f'{first_key}']:
            for key, val in entry.items():
                if str(key) == str(fname):
                    if '@lang' in str(val):
                        try:
                            fieldnames.append(val[0]['value'][0]['spans'][0]['text'])
                        except (IndexError, KeyError):
                            pass
                    else:
                        try:
                            fieldnames.append(val[0]['spans'][0]['text'])
                        except (IndexError, KeyError):
                            pass
        if fieldnames:
            return fieldnames

    elif len(group_check) > 1:
        all_vals = []
        for kname in group_check:
            for key, val in group_check[f'{kname}'][0].items():
                if key == fname:
                    dictionary = {}
                    dictionary[fname] = val
                    all_vals.append(dictionary)
        if len(all_vals) == 1:
            if '@lang' in str(all_vals):
                try:
                    return all_vals[0][fname][0]['value'][0]['spans'][0]['text']
                except KeyError:
                    print(f"Failed to extract value: {all_vals}")
                    return None
            else:
                try:
                    return all_vals[0][fname][0]['spans'][0]['text']
                except KeyError:
                    print(f"Failed to extract value: {all_vals}")
                    return None
        else:
            return all_vals
    else:
        return None


def get_grouped_items(api, database, session):
    '''
    Check dB for groupings and ensure
    these are added to XML configuration
    '''
    query = {
        'command': 'getmetadata',
        'database': database,
        'limit': 0
    }
    if not session:
        session = create_session()
    result = session.get(api, headers=HEADERS, params=query)
    metadata = xmltodict.parse(result.text)
    if not isinstance(metadata, dict):
        return None, None

    grouped = {}
    mdata = metadata['adlibXML']['recordList']['record']
    for num in range(0, len(mdata)):
        try:
            group = mdata[num]['group']
            field_name = mdata[num]['fieldName']['value'][0]['#text']
            if group in grouped.keys():
                grouped[group].append(field_name)
            else:
                grouped[group] = [field_name]
        except KeyError:
            pass

    return grouped


def create_record_data(api, database, session, priref, data=None):
    '''
    Create a record from supplied dictionary (or list of dictionaries)
    '''
    if not isinstance(data, list):
        data = [data]

    # Take data and group where matched to grouped dict
    grouped = get_grouped_items(api, database, session)
    remove_list = []
    for key, value in grouped.items():
        new_grouping = {}
        for item in data:
            for k in item.keys():
                if k in value:
                    if key in new_grouping.keys():
                        new_grouping[key].update(item)
                        remove_list.append(item)
                    else:
                        new_grouping[key] = item
                        remove_list.append(item)
        if new_grouping:
            print(f"Adjusted grouping data: {new_grouping}")
            data.append(new_grouping)

    if remove_list:
        for rm in remove_list:
            if rm in data:
                data.remove(rm)

    frag = get_fragments(data)
    if not frag:
        return False

    record = etree.XML('<record></record>')
    if not priref:
        record.append(etree.fromstring('<priref>0</priref>'))
    else:
        record.append(etree.fromstring(f'<priref>{priref}</priref>'))
    for i in frag:
        record.append(etree.fromstring(i))

    # Convert XML object to string
    payload = etree.tostring(record)
    payload = payload.decode('utf-8')

    return f'<adlibXML><recordList>{payload}</recordList></adlibXML>'


def get_fragments(obj):
    '''
    Validate given XML string(s), or create valid XML
    fragment from dictionary / list of dictionaries
    Attribution @ Edward Anderson
    '''

    if not isinstance(obj, list):
        obj = [obj]

    data = []
    for item in obj:

        if isinstance(item, str):
            sub_item = item
        else:
            sub_item = dicttoxml(item, root=False, attr_type=False)

        # Append valid XML fragments to `data`
        try:
            list_item = html.fragments_fromstring(sub_item, parser=etree.XMLParser(remove_blank_text=True))
            for itm in list_item:
                xml = etree.fromstring(etree.tostring(itm))
                data.append(etree.tostring(xml))
        except Exception as err:
            raise TypeError(f'Invalid XML:\n{sub_item}') from err

    return data


def add_quality_comments(api, priref, comments, session=None):
    '''
    Receive comments string
    convert to XML quality comments
    and updaterecord with data
    '''

    p_start = f"<adlibXML><recordList><record priref='{priref}'><quality_comments>"
    date_now = str(datetime.datetime.now())[:10]
    p_comm = f"<quality_comments><![CDATA[{comments}]]></quality_comments>"
    p_date = f"<quality_comments.date>{date_now}</quality_comments.date>"
    p_writer = "<quality_comments.writer>datadigipres</quality_comments.writer>"
    p_end = "</quality_comments></record></recordList></adlibXML>"
    payload = p_start + p_comm + p_date + p_writer + p_end

    if not session:
        session = create_session()

    response = session.post(
        api,
        headers={'Content-Type': 'text/xml'},
        params={'database': 'items', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'jsonv1'},
        data=payload,
        timeout=1200)
    if "error" in str(response.text):
        return False
    else:
        return True

