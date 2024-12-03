#!/usr/bin/env python3

'''
Module used by workflow scripts submitta.py
for F47 record creation
'''

# Public packages
import os
import sys
from datetime import datetime

# Local packages
sys.path.append(os.environ['CODE'])
sys.path.append(os.environ['WORKFLOW'])
import adlib_v3 as adlib
import records

# Global variable
CID_API = os.environ['CID_API4']


class Activities():
    # Class updates complete and tested
    def __init__(self):
        self.payloads = {}

        query = {'database': 'workflow',
                 'search': 'dataType>0',
                 'limit': -1,
                 'facets': 'dataType',
                 'facetlimit': 100,
                 'output': 'jsonv1'}

        records = adlib.get(CID_API, query)
        dt_list = adlib.retrieve_facet_list(records, 'term')

        for data_type in dt_list:
            search = f'dataType={data_type} and payloadDatabase>0'
            fields = ['payloadDatabase', 'description']
            hits, record = adlib.retrieve_record(CID_API, 'workflow', search, '1', fields)
            if hits is None:
                continue
            if hits == 0:
                continue
            payload_database = adlib.retrieve_field_name(record[0], 'payloadDatabase')[0]
            label = adlib.retrieve_field_name(record[0], 'description')[0]

            d = {'label': label,
                 'dataType': data_type}

            if payload_database in self.payloads:
                self.payloads[payload_database].append(d)
            else:
                self.payloads[payload_database] = [d]

    def get(self, activity):
        '''
        Return payload and dataType for given activity label
        '''
        for payload in self.payloads:
            for d in self.payloads[payload]:
                if d['label'] == activity:
                    detail = {'payloadDatabase': payload,
                              'dataType': d['dataType'],
                              'description': activity}

                    return detail


class Task():
    '''
    Suite of Workflow jobs and activities
    '''

    def __init__(self, items=None, **kwargs):
        try:
            [int(i) for i in items]
        except Exception:
            raise TypeError('Item IDs must be prirefs')

        self.items = items
        self.job_number = None
        self.last_activity_priref = None
        self.priref = None
        self.profiles = {
                            'workflow': {
                                'status': 'Started',
                                'dataType': 'FolderData',
                                'recordType': 'Folder'
                            },
                            'objectList': {
                                'description': 'objects',
                                'recordType': 'ObjectList',
                                'status': 'Started'
                            },
                            'object': {
                                'payloadDatabase': 'collect.inf',
                                'recordType': 'Object',
                                'status': 'Started'
                            }
                        }

        # Changed transpor.entry line to map to 'entry' instead of 'despatch' on 2020-07-13
        # To fix priref range problem in Workflow payload database (transpor.entry)
        self.database_map = {'conserva.inf': 'conservation',
                             'reprordr.inf': 'reproduction',
                             'request.inf': 'request',
                             'transpor.despatch': 'despatch',
                             'transpor.entry': 'entry'}

        self.make_topnode(**kwargs)
        objectList_priref = self.make_objectList(self.priref)
        self.make_objects(objectList_priref, items=self.items)

    def _date_time(self):
        date = str(datetime.now())[:10]
        time = str(datetime.now())[11:19]
        return date, time

    def build_record(self, data):
        record = records.Record()

        data['priref'] = '0'
        data['input.name'] = 'collectionssystems'
        data['input.date'] = self._date_time()[0]
        data['input.time'] = self._date_time()[1]

        for i in data:
            record.append(field=records.Field(name=i, text=data[i]))

        return record

    def write_record(self, database='workflow', record=None):
        data = record.to_xml(to_string=True)
        payload = f'<adlibXML><recordList>{data}</recordList></adlibXML>'
        response = adlib.post(CID_API, payload, database, 'insertrecord')
        print(response)
        return response

    def make_topnode(self, **kwargs):
        wf = dict(self.profiles['workflow'])

        for k in kwargs:
            wf[k] = kwargs[k]

        wf['topNode'] = 'x'

        record = self.build_record(wf)
        response = self.write_record(record=record)

        self.job_number = int(adlib.retrieve_field_name(response, 'jobnumber')[0])
        self.priref = int(adlib.retrieve_field_name(response, 'priref')[0])
        self.last_activity_priref = self.priref

    def make_objectList(self, parent):
        ol = dict(self.profiles['objectList'])
        ol['parent'] = str(self.last_activity_priref)

        record = self.build_record(ol)
        response = self.write_record(record=record)

        priref = int(adlib.retrieve_field_name(response, 'priref')[0])
        return priref

    def make_objects(self, objectList_priref, items=None):
        count = 0
        if items is not None:
            for item in items:
                d = dict(self.profiles['object'])

                d['description'] = get_object_number(item)
                d['parent'] = str(objectList_priref)
                d['payloadLink'] = str(item)

                record = self.build_record(d)

                try:
                    response = self.write_record(record=record)
                except Exception:
                    continue
                # Unsure about this piece, need to get hit response from self.write_record
                if response['@attributes']:
                    count += 1
                else:
                    continue

                # Hits not returned from write_record
                # count += response.hits

            if count == len(items):
                return True

        return False

    def add_activity(self, activity, items=None, **payload_kwargs):
        a = activity_map.get(activity)
        if not a:
            raise Exception('''Unknown activity label: "{}"
                               or activity is not supported'''.format(activity))

        # Payload record
        db = self.database_map[a['payloadDatabase']]
        p = self.build_record(payload_kwargs)
        response = self.write_record(database=db, record=p)
        payload_priref = int(adlib.retrieve_field_name(response, 'priref')[0])

        # Workflow record
        d = dict(self.profiles['workflow'])
        d['recordType'] = 'WorkFlow'
        d['payloadLink'] = str(payload_priref)

        # payloadDatabase and dataType details
        for i in a:
            d[i] = a[i]

        d['parent'] = str(self.last_activity_priref)
        wf = self.write_record(record=self.build_record(d))
        wf_priref = int(adlib.retrieve_field_name(wf, 'priref')[0])
        self.last_activity_priref = wf_priref

        ol_priref = self.make_objectList(parent=str(self.last_activity_priref))
        status = self.make_objects(str(ol_priref), items)

        if status:
            return True
        else:
            return False


class Batch():
    '''
    Create a nested tree of Workflow activities for a list of items.

    Instantiate with, for example:

        # List of item prirefs
        l = [123, 456]

        # Dictionary containing 'activities', 'topNode' and 'payload'
          and associated case-sensitive fields and their values

        d = {'activities': [
               'Pick items',
               'Return items'],

             'topNode': {
               'activity.code.lref': '108964',
               'purpose': 'Preservation'},

             'payload': {
               'Pick items': {
                 'destination': 'PBK06B03000000'}}
            }

        b = Batch(items=l, **d)

    Note for the future: consider refactoring kwargs['payload'] into a
                         list and getting rid of kwargs['activities'];
                         just remember that the order of activities is
                         important
    '''

    def __init__(self, items=None, **kwargs):
        if not items:
            raise Exception('Required: list of item prirefs')

        if 'activities' not in kwargs:
            raise Exception('Required: list of case-sensitive activities in kwargs')

        if 'topNode' not in kwargs:
            raise Exception('Required: dictionary of topNode field-values in kwargs')

        if 'payload' not in kwargs:
            raise Exception('Required: dictionary of payload field-values in kwargs')

        self.task = Task(items, **kwargs['topNode'])

        overall_status = []
        for a in kwargs['activities']:
            status = self.task.add_activity(a, items=items, **kwargs['payload'][a])
            overall_status.append(status)

        self.priref = self.task.priref
        if all(overall_status):
            self.successfully_completed = True
        else:
            self.successfully_completed = False


class F47Batch():
    '''
    Create a tree of Workflow activities specific to F47 video encoding:
      - Pick
      - Encode
      - Return

    To use:

        # Items
        item_prirefs = [123, 567]

        # Job metadata
        topnode_metadata = {'description': 'F47 / Ofcom / etc',
                            'completion.date': '2019-08-16'}

        # Create
        b = F47BatchDev(l, **topnode_metadata)
    '''

    def __init__(self, items=None, **kwargs):
        # Default metadata
        d = {
              'activities': [
                'Pick items',
                'Video Encoding',
                'Return items'
              ],
              'topNode': {
                'activity.code.lref': '108964',
                'purpose': 'Preservation',
                'request_type': 'VIDEOCOPY',
                'final_destination': 'F47',
                'request.details': 'Transfer to preservation and proxy formats',
                'assigned_to': 'Television Operations'
              },
              'payload': {
                'Pick items': {
                  'destination': 'PBK06B03000000'
                },
                'Video Encoding': {
                  'handling.name': 'Television Operations'
                },
                'Return items': {}
              }
            }

        # Add any additional metadata to the topNode
        for k in kwargs:
            d['topNode'][k] = kwargs[k]

        # Create
        self.batch = Batch(items, **d)

    @property
    def successfully_completed(self):
        return self.batch.successfully_completed


class twoInchBatch():
    '''
    Create a tree of Workflow activities specific to VT10 video encoding:
      - Pick
      - Encode
      - Return

    To use:

        # Items
        item_prirefs = [123, 567]

        # Job metadata
        topnode_metadata = {'description': '2inch / Ofcom / etc',
                            'completion.date': '2021-06-01'}

        # Create
        b = VT10Batch(l, **topnode_metadata)
    '''

    def __init__(self, items=None, **kwargs):
        # Default metadata
        d = {
              'activities': [
                'Pick items',
                'Video Encoding',
                'Return items'
              ],
              'topNode': {
                'activity.code.lref': '108964',
                'purpose': 'Preservation',
                'request_type': 'VIDEOCOPY',
                'final_destination': '2inch - Video Copying',
                'request.details': 'Transfer to preservation formats',
                'assigned_to': 'Television Operations'
              },
              'payload': {
                'Pick items': {
                  'destination': 'PBK03A06000000'
                },
                'Video Encoding': {
                  'handling.name': 'Television Operations'
                },
                'Return items': {}
              }
            }

        # Add any additional metadata to the topNode
        for k in kwargs:
            d['topNode'][k] = kwargs[k]

        # Create
        self.batch = Batch(items, **d)

    @property
    def successfully_completed(self):
        return self.batch.successfully_completed

class VT10Batch():
    '''
    Create a tree of Workflow activities specific to VT10 video encoding:
      - Pick
      - Encode
      - Return

    To use:

        # Items
        item_prirefs = [123, 567]

        # Job metadata
        topnode_metadata = {'description': 'VT10 / Ofcom / etc',
                            'completion.date': '2021-06-01'}

        # Create
        b = VT10BatchDev(l, **topnode_metadata)
    '''

    def __init__(self, items=None, **kwargs):
        # Default metadata
        d = {
              'activities': [
                'Pick items',
                'Video Encoding',
                'Return items'
              ],
              'topNode': {
                'activity.code.lref': '108964',
                'purpose': 'Preservation',
                'request_type': 'VIDEOCOPY',
                'final_destination': 'VTR 10 - Video Copying',
                'request.details': 'Transfer to preservation formats',
                'assigned_to': 'Television Operations'
              },
              'payload': {
                'Pick items': {
                  'destination': 'PBK03A06000000'
                },
                'Video Encoding': {
                  'handling.name': 'Television Operations'
                },
                'Return items': {}
              }
            }

        # Add any additional metadata to the topNode
        for k in kwargs:
            d['topNode'][k] = kwargs[k]

        # Create
        self.batch = Batch(items, **d)

    @property
    def successfully_completed(self):
        return self.batch.successfully_completed

class D3Batch():
    '''
    Create a tree of Workflow activities specific to D3 video encoding (currently modelled from VT10):
      - Pick
      - Encode
      - Return

    To use:

        # Items
        item_prirefs = [123, 567]

        # Job metadata
        topnode_metadata = {'description': 'D3 / Ofcom / etc',
                            'completion.date': '2021-06-01'}

        # Create
        b = D3BatchDev(l, **topnode_metadata)
    '''

    def __init__(self, items=None, **kwargs):
        # Default metadata
        d = {
              'activities': [
                'Pick items',
                'Video Encoding',
                'Return items'
              ],
              'topNode': {
                'activity.code.lref': '108964',
                'purpose': 'Preservation',
                'request_type': 'VIDEOCOPY',
                'final_destination': 'D3 - Video Copying',
                'request.details': 'Transfer to preservation and proxy formats',
                'assigned_to': 'Television Operations'
              },
              'payload': {
                'Pick items': {
                  'destination': 'PBK03A06000000'
                },
                'Video Encoding': {
                  'handling.name': 'Television Operations'
                },
                'Return items': {}
              }
            }

        # Add any additional metadata to the topNode
        for k in kwargs:
            d['topNode'][k] = kwargs[k]

        # Create
        self.batch = Batch(items, **d)

    @property
    def successfully_completed(self):
        return self.batch.successfully_completed


def get_object_number(priref):
    search = f'priref={priref}'
    record = adlib.retrieve_record(CID_API, 'items', search, '1', ['object_number'])[1]
    ob_num = adlib.retrieve_field_name(record[0], 'object_number')[0]
    return ob_num


def get_priref(object_number):
    search = f'object_number="{object_number}"'
    record = adlib.retrieve_record(CID_API, 'items', search, '1', ['priref'])[1]
    priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    return priref


def count_jobs_submitted(search):
    print(search)
    hits = adlib.retrieve_record(CID_API, 'workflow', search, '-1')[0]
    if hits is None:
        raise Exception(f'Workflow search failed to access API: {search}')
    return hits


try:
    activity_map = Activities()
except Exception as exc:
    print(exc)
    raise Exception('Unable to build map of Workflow databases')
