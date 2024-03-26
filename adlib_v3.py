#!/usr/bin/env python3

'''
Python interface for Adlib API v3.7.17094.1+
(http://api.adlibsoft.com/site/api)

Joanna White
2024
'''

import os
import json
from lxml import etree, html
import requests
from dicttoxml import dicttoxml

CID_API = os.environ['CID_API4']
HEADERS = {
    'Content-Type': 'text/xml'
}


def retrieve_record(database, search, limit, fields=None):
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

    record = get(query)
    if 'recordList' not in str(record):
        return (record['adlibJSON']['diagnostic']['hits'], None)

    hits = len(record['adlibJSON']['recordList']['record'])
    return (hits, record['adlibJSON']['recordList']['record'])


def get(query):
    '''
    Send a GET request
    '''

    try:
        req = requests.request('GET', CID_API, headers=HEADERS, params=query)
        dct = json.loads(req.text)
    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
        print(err)
        dct = {}

    return dct


def post(payload, database, method, priref):
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

    if method == 'insertrecord':
        try:
            response = requests.request('POST', CID_API, headers=HEADERS, params=params, data=payload, timeout=1200)
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
            print(err)
            return None

        if 'recordList' in response.text:
            records = json.loads(response.text)
            return records['adlibJSON']['recordList']['record'][0]
        return None

    if method == 'updaterecord':
        lock = _lock(priref, database)
        if lock is False:
            return None
        try:
            response = requests.request('POST', CID_API, headers=HEADERS, params=params, data=payload, timeout=1200)
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
            print(err)
            return None

        if 'recordList' in response.text:
            records = json.loads(response.text)
            unlock = _unlock(priref, database)
            if unlock is False:
                raise Exception(f"Failed to unlock record following update {priref}")
            return records
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
            if '@lang' in str(field) or 'lang' in str(field):
                field_list.append(field['value'][0]['spans'][0]['text'])
            else:
                field_list.append(field['spans'][0]['text'])
    except KeyError:
        field_list.append(group_check(record, fieldname))

    return field_list


def group_check(record, fname):
    '''
    Get group that contains field key
    '''
    group_check = dict([ (k, v) for k, v in record.items() if f'{fname}' in str(v) ])

    if len(group_check) == 1:
        first_key = next(iter(group_check))
        for key, val in group_check[f'{first_key}'][0].items():
            if str(key) == str(fname):
                if '@lang' in str(val):
                    try:
                        return val[0]['value'][0]['spans'][0]['text']
                    except KeyError:
                        print(f"Failed to extract value: {val}")
                        return None
                else:
                    try:
                        return val[0]['spans'][0]['text']
                    except KeyError:
                        print(f"Failed to extract value: {val}")
                        return None
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


def _lock(priref, database):
    '''
    Lock item record for update
    '''
    try:
        response = requests.request(
            'POST',
            CID_API,
            params={'database': database, 'command': 'lockrecord', 'priref': f'{priref}', 'output': 'jsonv1'}
        )
        return True
    except Exception:
        return False


def _unlock(priref, database):
    '''
    Unlock item record if failed update
    '''
    try:
        response = requests.request(
            'POST',
            CID_API,
            params={'database': database, 'command': 'unlockrecord', 'priref': f'{priref}', 'output': 'jsonv1'},
        )
        return True
    except Exception:
        return False
