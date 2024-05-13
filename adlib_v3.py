#!/usr/bin/env python3

'''
Python interface for Adlib API v3.7.17094.1+
(http://api.adlibsoft.com/site/api)

Joanna White
2024
'''

import os
import json
import requests
import datetime
from lxml import etree, html
from dicttoxml import dicttoxml

CID_API = os.environ['CID_API4']
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


def retrieve_record(api, database, search, limit, fields=None):
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

    record = get(api, query)
    print("***************")
    print(record)
    if 'recordList' not in str(record):
        try:
            hits = record['adlibJSON']['diagnostic']['hits']
            if hits == 0:
                return 0, None
            else:
                return hits, record
        except (IndexError, KeyError, TypeError):
            return 0, record

    hits = record['adlibJSON']['diagnostic']['hits']
    return hits, record['adlibJSON']['recordList']['record']


def get(api, query):
    '''
    Send a GET request
    '''
    try:
        req = requests.request('GET', api, headers=HEADERS, params=query)
        dct = json.loads(req.text)
        if 'recordList' in dct:
            dct = dct['adlibJSON']['recordList']['record']
    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
        print(err)
        dct = {}

    return dct


def post(api, payload, database, method):
    '''
    Send a POST request
    If using updaterecord consider if the record
    would benefit from a lock/unlock functionc
    '''
    params = {
        'command': method,
        'database': database,
        'xmltype': 'grouped',
        'output': 'jsonv1'
    }
    payload = payload.encode('utf-8')

    if method == 'insertrecord':
        try:
            response = requests.request('POST', api, headers=HEADERS, params=params, data=payload, timeout=1200)
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
            print(err)
            return None
        print(response.text)
        if "['adlibJSON']['recordList']['record']" in str(response.text):
            records = json.loads(response.text)
            if isinstance(records['adlibJSON']['recordList']['record'], list):                 
                return records['adlibJSON']['recordList']['record'][0]
            else:
                return records['adlibJSON']['recordList']['record']

    if method == 'updaterecord':
        try:
            response = requests.request('POST', api, headers=HEADERS, params=params, data=payload, timeout=1200)
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
            print(err)
            return None

        if '@attributes' in response.text:
            records = json.loads(response.text)
            return records

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
            elif '@lang' in str(field) or 'lang' in str(field):
                field_list.append(field['value'][0]['spans'][0]['text'])
            else:
                field_list.append(field['spans'][0]['text'])
    except KeyError:
        field_list = group_check(record, fieldname)

    if not isinstance(field_list, list):
        return [field_list]
    return field_list


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


def create_record_data(priref, data=None):
    '''
    Create a record from given XML string or dictionary (or list of dictionaries)
    '''

    if not isinstance(data, list):
        data = [data]

    frag = get_fragments(data)
    if not frag:
        return False

    record = etree.XML('<record></record>')
    for i in frag:
        record.append(etree.fromstring(i))

    if not priref:
        record.append(etree.fromstring('<priref>0</priref>'))
    else:
        record.append(etree.fromstring(f'<priref>{priref}</priref>'))

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


def add_quality_comments(api, priref, comments):
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

    response = requests.request(
        'POST',
        api,
        headers={'Content-Type': 'text/xml'},
        params={'database': 'items', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'jsonv1'},
        data=payload,
        timeout=1200)
    if "error" in str(response.text):
        return False
    else:
        return True

