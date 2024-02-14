#!/usr/bin/env python3

'''
Python interface for Adlib API v3.7.17094.1+
(http://api.adlibsoft.com/site/api)

Joanna White
2024
'''

import os
import sys
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
    if not record:
        return None

    return record['adlibJSON']['recordList']['record']


def get(query):
    '''
    Send a GET request
    '''

    try:
       req = requests.request('GET', CID_API, headers=HEADERS, params=query)
       dct = json.loads(req.text)
    except Exception as err:
       dct = {}

    return dct


def post(self, params=None, payload=False, sync=True):
    '''
    Send a POST request
    '''
    if params is None:
        params={}
    # Add payload data to request
    if payload:
        response = self.session.post(self.url, params=params, data={'data': payload})
    else:
        response = self.session.post(self.url, params=params)

    # Wait for response
    if sync:
        return self._validate(response)
    else:
        return True



def retrieve_attribute(record, fieldname):
    '''
    Retrieve data from @attributes
    '''
    return record['@attributes'][f'{fieldname}']


def retrieve_field_name(record, fieldname):
    '''
    Retrieve record, check for language data
    Alter retrieval method. record ==
    ['adlibJSON']['recordList']['record'][0]
    '''
    field_list = []

    for field in record[f'{fieldname}']:
        if '@lang' in str(field):
            field_list.append(field['value'][0]['spans'][0]['text'])
        else:
            field_list.append(field['spans'][0]['text'])

    return field_list


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
            raise TypeError(f'Invalid XML:\n{s}') from err

    return data


