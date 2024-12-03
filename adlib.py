#!/usr/bin/env python3

'''
Python interface for [Adlib API]
(http://api.adlibsoft.com/site/api)
Edward Anderson, 2017

Converted for Python3
2021
'''

import re
import sys
import json
from lxml import etree, html
from requests import Session, exceptions
from dicttoxml import dicttoxml


class Database:
    '''
    This object initiates a single http session with
    an Adlib API endpoint and presents GET and POST
    methods for querying records and writing data.
    Returns a Result() object.
    '''

    def __init__(self, url):
        self.url = url
        self.session = Session()
        self.default_parameters = {'limit': 10}

    def _validate(self, response):
        try:
            data = etree.fromstring(response.content)
            return Result('xml', data)
        except Exception:
            pass

        try:
            data = json.loads(response.text)
            return Result('json', data)
        except Exception:
            pass

        return False

    def get(self, params=None):
        '''
        Send a GET request
        '''
        # Addition to hand default change to None
        if params is None:
            params={}
        # Append default parameters
        for i in self.default_parameters:
            if i not in params:
                params[i] = self.default_parameters[i]

        try:
            response = self.session.get(self.url, params=params, timeout=60)
        except exceptions.Timeout:
            raise

        return self._validate(response)

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

    def search(self, database, query_str):
        '''
        Quickly obtain all the prirefs which match given query
        '''

        q = {'database': database,
             'search': query_str,
             'fields': 'priref',
             'limit': '0'}

        return self.get(q)

    def records(self, database, query_str, fields=None, group=10):
        '''
        Individually fetch the JSON data for each record in query
        '''

        def chunker(seq, size):
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        def get_individual_records(prirefs, fields, group):
            for block in chunker(prirefs, group):
                grouped_prirefs = ','.join(block)

                q = {'database': database,
                     'search': f'priref={grouped_prirefs}',
                     'limit': group,
                     'output': 'json'}

                if fields:
                    q['fields'] = ','.join(fields)

                r = self.get(q)
                yield r

        prirefs = list()

        result = self.search(database, query_str)
        for r in result.records:
            p = r.get('priref')
            prirefs.append(p)

        if not fields:
            fields = ''

        hits = result.hits
        results = get_individual_records(prirefs, fields, group)

        return Collection(hits, results)


class Result:
    '''
    This object represents an Adlib API response which
    has been parsed into components according to the
    schema (xml/json) specified in the query.
    '''

    def __init__(self, language, data):
        self.language = language
        self.data = data
        self.diagnostic = self._diagnostics()
        self.error = self._errors()

        self.records = []
        self.hits = None

        if not self.error:
            try:
                self.records = self._records()
            except Exception:
                pass

            self.hits = int(self._hits())

    def _diagnostics(self):
        if self.language == 'xml':
            return self.data.xpath('//adlibXML/diagnostic')[0]
        elif self.language == 'json':
            return self.data['adlibJSON']['diagnostic']

    def _errors(self):
        if self.language == 'xml':
            if self.diagnostic.find('error') is not None:
                return True
            else:
                return False
        elif self.language == 'json':
            if 'error' in self.diagnostic:
                return True
            else:
                return False

    def _hits(self):
        if self.language == 'xml':
            return self.diagnostic.xpath('hits')[0].text
        elif self.language == 'json':
            return self.diagnostic['hits']

    def _records(self):
        if self.language == 'xml':
            return self.data.xpath('//adlibXML/recordList/record')
        elif self.language == 'json':
            return self.data['adlibJSON']['recordList']['record']


class Collection:
    '''
    This is a container for multiple Result() objects
    '''

    def __init__(self, hits, results):
        self.hits = hits
        self.records = self._records(results)
        self.results = self._requests(results)

    def _records(self, results):
        for i in results:
            if i.records:
                yield from i.records
            else:
                return

    def _requests(self, results):
        for i in results:
            if i.hits is not None:
                yield i
            else:
                return


class Cursor:
    '''
    This object facilitates record editing
    '''

    def __init__(self, adlib_db, databases=None, language=0):
        self.db = adlib_db
        self.metadata = {}
        self.field_groups = {}

        if databases:
            self._getmetadata(databases, language=language)

    def _boilerplate(self, fragment):
        '''
        Wrap XML fragment in boilerplate tags
        '''

        return f'<adlibXML><recordList>{fragment}</recordList></adlibXML>'

    def _fields_from_xpath(self, database, xpath_expression):
        '''
        Return a list of valid fields from the given XPath expression
        '''

        # Fetch all group->field mappings for current database
        if database not in self.metadata:
            self._getmetadata([database])

        # Extract field names from given XPath expression
        fields = re.findall(r'([A-Za-z._]+)', xpath_expression)
        fields = [f for f in fields if f in self.metadata[database]]

        return fields

    def _fragment(self, obj):
        '''
        Validate given XML string(s), or create valid XML
        fragment from dictionary / list of dictionaries
        '''

        if not isinstance(obj, list):
            obj = [obj]

        data = []
        for i in obj:

            # Parse instance as XML string
            if isinstance(i, str):
                s = i

            # Parse instance as dictionary
            else:
                s = dicttoxml(i, root=False, attr_type=False)

            # Append valid XML fragments to `data`
            try:
                l = html.fragments_fromstring(s, parser=etree.XMLParser(remove_blank_text=True))
                for i in l:
                    xml = etree.fromstring(etree.tostring(i))
                    data.append(etree.tostring(xml))
            except Exception as e:
                raise TypeError(f'Invalid XML:\n{s}') from e

        return data

    def _get(self, database, priref, fields):
        '''
        Download a single record and clean it by removing all attributes from topnode
        '''

        # Prepare parameters
        d = {'database': database,
            f'search': 'priref={int(priref)}'}

        if fields:
            d['fields'] = ','.join(fields)

        # Download data
        result = self.db.get(d)
        if result.hits != 1:
            return False

        record = result.records[0]

        # Parse record
        r = record.xpath('//record')[0]

        # Clear attributes in <record> element
        for i in r.xpath('//record[@*]'):
            i.attrib.clear()

        return r

    def _getmetadata(self, databases=None, language=0):
        '''
        Download database- and field- related metadata
        '''
        if databases is None:
            databases=[]
        # Get a list of the available databases
        d = {'command': 'listdatabases',
             'limit': 0,
             'output': 'json'}

        result = self.db.get(d)
        for r in result.records:
            dbase = r['database'][0]
            write = r['writeAllowed'][0]
            self.metadata[dbase] = {'writeAllowed': write}

        o = databases or self.metadata

        # Get field configuration metadata for each database
        for dbase in o:
            d = {'command': 'getmetadata',
                 'database': dbase,
                 'limit': 0}

            result = self.db.get(d)
            for r in result.records:
                data = {}
                for element in r:
                    if list(element):
                        value = element.xpath(f'value[@lang="{language}"]')
                        if value:
                            data[element.tag] = value[0].text
                    else:
                        data[element.tag] = element.text

                field = data['displayName']
                self.metadata[dbase][field] = data

        for d in self.metadata[dbase]:
            try:
                group = self.metadata[dbase][d]['group']
                if group in self.field_groups:
                    self.field_groups[group].append(d)
                else:
                    self.field_groups[group] = [d]
            except Exception:
                pass

    def _group_data(self, database, data):
        '''
        Wrap field-value dictionary in Group tags
        '''

        # Fetch all group->field mappings for current database
        if database not in self.metadata:
            self._getmetadata([database])

        if 'priref' not in self.metadata[database]:
            self._getmetadata([database])

        grouped_data = []
        for d in data:
            keys = list(d.keys())
            keys_joined = ', '.join(keys)

            # Verify that all keys are in database
            if all(key in self.metadata[database] for key in keys):

                # Verify that fields are correctly grouped
                try:
                    g = list(set([self.metadata[database][k]['group'] for k in keys]))
                    if len(g) == 1:
                        grouped_data.append({g[0]: d})
                    else:
                        # Given keys are Grouped, but erroneously in the same group
                        raise ValueError(f'Fields [{keys_joined}] are not part of the same Group')
                except Exception:
                    if len(keys) == 1:
                        grouped_data.append(d)
                    else:
                        raise ValueError(f'One or more of fields [{keys_joined}] is not Grouped')

            else:
                # Given keys are erroneously in the same group
                raise ValueError(f'One or more of fields [{keys_joined}] is not used in database [{database}]')

        return grouped_data

    def _write(self, database, payload, method=None, output=None, params=None):
        '''
        POST payload string to record
        '''
        if params is None:
            params=[]
        # Wrap payload in boilerplate
        payload = self._boilerplate(payload)

        # Prepare parameters
        w = {'database': database,
             'command': method,
             'xmltype': 'grouped'}

        # Append extra parameters
        for i in params:
            w[i] = params[i]

        if output:
            w['output'] = output

        # POST
        response = self.db.post(params=w, payload=payload)
        return response

    def create_occurrences(self, database, priref, data=None, prepend=False, output=None):
        '''
        Add new occurrence(s) of field(s) to a record.
        Use:
            * Data can be passed as an XML string, either `grouped` or `ungrouped`:
                Ungrouped: `data='<content.subject>Women in film</content.subject>'`
                Grouped:   `data='<Content_subject>
                                    <content.subject>Horse racing</content.subject>
                                  </Content_subject>'`
            * Or as a dictionary
                - `{'content.subject':'Horse racing'}`
            * Or a list of dictionaries
                - data=[{'content.subject':'Women', 'content.subject.lref':28807},
                        {'content.subject':'Horse racing'}]
            Note: Group tags are automatically wrapped around field-value pairs passed as dictionaries
        '''
        fields = set()

        # Process XML string
        if isinstance(data, str):
            xml = html.fragments_fromstring(data)
            for i in xml:
                if len(list(i)) > 0:
                    for e in i:
                        fields.add(e.tag)
                else:
                    fields.add(i.tag)

        # Process dictionaries
        else:
            # Handle all objects as list
            if not isinstance(data, list):
                data = [data]

            # Extract field(s)
            for d in data:
                for k in d:
                    fields.add(k)

            # Replace given `data` with Group-serialised data
            grouped_data = self._group_data(database, data)
            data = grouped_data

        # Create XML fragment
        f = self._fragment(data)
        if not f:
            return False

        # GET target record
        record = self._get(database, priref, fields)

        # Ensure <priref> is present in <record>
        if record.xpath('priref') is None:
            record.append(etree.fromstring(f'<priref>{priref}</priref>'))

        # Append/prepend new occurrence(s) data to record
        if prepend:
            for i in reversed(f):
                record.insert(0, etree.fromstring(i))
        else:
            for i in f:
                record.append(etree.fromstring(i))

        # Convert record to XML string payload
        edited_record = etree.tostring(record)
        edited_record = edited_record.decode('utf-8')
        # Write data
        response = self._write(database, edited_record, output=output)
        return response

    def delete_occurrences(self, database, priref, xpath_expression, fields=None, output=None):
        '''
        Remove element(s) from record XML which match the given XPath expression
        '''
        if fields is None:
            fields=[]

        if not fields:
            fields = self._fields_from_xpath(database, xpath_expression)

        record = self._get(database, priref, fields)

        matches = record.xpath(xpath_expression)
        for m in matches:
            m.clear()

        edited_record = etree.tostring(record)
        edited_record = edited_record.decode('utf-8')

        return self._write(database, edited_record, output=output)

    def count_occurrences(self, database, xpath_expression, priref=None, record=None, fields=None):
        '''
        Return the count of matches for given XPath expression
        '''
        if fields is None:
            fields=[]

        if not fields:
            fields = self._fields_from_xpath(database, xpath_expression)

        if priref is not None:
            record = self._get(database, priref, fields=fields)

        count = record.xpath(f'count({xpath_expression})')
        return int(count)

    def append_to_occurrences(self, database, priref, xpath_expression, data=None, fields=None, output=None):
        '''
        Add an element to occurrence(s) specified by XPath expression
        '''
        if fields is None:
            fields=[]

        if not fields:
            fields = self._fields_from_xpath(database, xpath_expression)

        record = self._get(database, priref, fields=fields)

        elements = self._fragment(data)

        matches = record.xpath(xpath_expression)
        for m in matches:
            for e in elements:
                m.append(etree.fromstring(e))

        edited_record = etree.tostring(record)
        edited_record = edited_record.decode('utf-8')

        return self._write(database, edited_record, output=output)

    def create_record(self, database, data=None, output=None, params=None, write=True):
        '''
        Create a record from given XML string or dictionary (or list of dictionaries)
        '''
        if params is None:
            params={}

        # Handle all objects as list
        if not isinstance(data, list):
            data = [data]

        # Create XML snippet
        fragment = self._fragment(data)
        if not fragment:
            return False

        # Create root
        record = etree.XML('<record></record>')

        # Append fragment elements to root
        for i in fragment:
            record.append(etree.fromstring(i))

        # Insert `priref=0` element
        record.append(etree.fromstring('<priref>0</priref>'))

        # Convert XML object to string
        payload = etree.tostring(record)
        payload = payload.decode('utf-8')
        print(payload)
        print(params)

        # POST record
        if write:
            response = self._write(database, payload, method='insertrecord', output=output, params=params)
            return response
        else:
            return payload

    def create_record_data(self, priref, data=None):
        '''
        Create a record from given XML string or dictionary (or list of dictionaries)
        '''

        # Handle all objects as list
        if not isinstance(data, list):
            data = [data]

        # Create XML snippet
        fragment = self._fragment(data)
        if not fragment:
            return False

        # Create root
        record = etree.XML('<record></record>')

        # Append fragment elements to root
        for i in fragment:
            record.append(etree.fromstring(i))

        if not priref:
            # Insert `priref=0` element
            record.append(etree.fromstring('<priref>0</priref>'))
        else:
            # Insert `priref` element
            record.append(etree.fromstring(f'<priref>{priref}</priref>'))

        # Convert XML object to string
        payload = etree.tostring(record)
        payload = payload.decode('utf-8')

        return f'<adlibXML><recordList>{payload}</recordList></adlibXML>'

    def update_record(self, priref, database, data=None, output=None, params=None, write=True):
        '''
        Update a record from given XML string or dictionary (or list of dictionaries)
        Could overwrite some field contents, use with care
        '''
        if params is None:
            params={}

        # Handle all objects as list
        if not isinstance(data, list):
            data = [data]

        # Create XML snippet
        fragment = self._fragment(data)
        if not fragment:
            return False

        # Create root
        record = etree.XML('<record></record>')

        # Append fragment elements to root
        for i in fragment:
            record.append(etree.fromstring(i))

        # Insert priref element
        record.append(etree.fromstring(f'<priref>{priref}</priref>'))

        # Convert XML object to string
        payload = etree.tostring(record)
        payload = payload.decode('utf-8')
        print(payload)
        # POST record
        if write:
            try:
                response = self._write(database, payload, method='updaterecord', output=output, params=params)
                return response
            except Exception as err:
                return payload

    def edit_record(self, database, priref, value, xpath_expression, fields=None, output=None, params=None):
        '''
        Replace a value selected by the given XPath expression with [value]
        '''
        if fields is None:
            fields=[]
        if params is None:
            params={}
        if not fields:
            fields = self._fields_from_xpath(database, xpath_expression)

        record = self._get(database, priref, fields=fields)

        elements = record.xpath(xpath_expression)
        for e in elements:
            e.text = value

        edited_record = etree.tostring(record)
        edited_record = edited_record.decode('utf-8')

        response = self._write(database, edited_record, output=output, params=params)
        return response

