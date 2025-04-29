#!/usr/bin/env python3

'''
Module for Splitting scripts to
retrieve carrier information inc
siblings, cousins, partWholes,
package, can data, segments etc.

Note:
Content class is out of use in
post-Imagen DPI workflows

Refactored for Python3
June 2022
'''

# Public packages
import re
import os
import sys
import string
import logging
import datetime
import requests
from PIL import Image

# Private packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
LOGS = os.environ['LOG_PATH']
LOG_PATH = os.path.join(LOGS, 'splitting_models.log')
DPI_PATH = os.environ['DPI_API']
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)

# Setup logging, overwrite each time
logger = logging.getLogger('split_qnap_test')
hdlr = logging.FileHandler(LOG_PATH)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def star(f):
    ''' Allows arg unpacking in lambda '''
    return lambda args: f(*args)


class Item():
    ''' Useful interactions with an item record '''

    def __init__(self, priref):
        self.priref = priref
        self.object_number = self._object_number()
        self.containers = Containers(priref)
        self.reels = max(len(i) for i in self.containers.identifiers.values() if i)

    def _object_number(self):
        ''' Resolve <object_number> id '''
        r = cid_get('items', f'priref={self.priref}', 'object_number')
        logger.info("Object number: %s", r.records)
        return r.records[0]['object_number'][0]

    def data(self):
        ''' Fetch the complete record '''
        r = cid_get('items', f'priref={self.priref}', '')
        logger.info("data: %s", r.records)
        return r.records[0]

    def siblings(self):
        ''' Fetch identifiers of items in same manifestation '''
        q = f'(part_of_reference->(parts_reference.lref={self.priref}))'
        r = cid_get('items', q, 'priref')
        siblings_priref = [int(i['priref'][0]) for i in r.records if i]
        logger.info("Siblings: %s", siblings_priref)
        return siblings_priref

    def cousins(self):
        ''' Fetch identifiers of items in any other manifestation in same work '''
        q = f'(part_of_reference->(part_of_reference->(parts_reference->(parts_reference.lref={self.priref}))))'
        r = cid_get('items', q, 'priref')
        cousins_priref = [int(i['priref'][0]) for i in r.records if int(i['priref'][0]) not in self.siblings()]
        logger.info("Cousins: %s", cousins_priref)
        return cousins_priref


class Containers():
    ''' Fetch and wrangle physical label identifiers for the given item priref '''

    def __init__(self, priref):
        self.packages = self._packages(priref)
        self.cans = self._cans(priref)
        self.identifiers = {'packages': self.packages, 'cans': self.cans}

    def _packages(self, priref):
        ''' Obtain <package_number> identifiers '''
        q = f'collcopy.number->object.number.lref={priref}'
        r = cid_get('packages', q, 'package_number')
        return [str(i['package_number'][0]) for i in r.records if i]

    def _cans(self, priref):
        ''' Wrangle <can_ID> identifier '''
        q = f'priref={priref} and can_ID=*'
        r = cid_get('items', q, 'can_ID')
        logger.info("Cans: %s", r.records[0]['can_ID'][0])
        if r.hits == 0:
            return []

        can_id = str(r.records[0]['can_ID'][0])

        # N-item, eg 5074964Aa becomes 5074964AA
        if (can_id[-2] == 'A' and can_id[-1].islower()):
            return [f'{can_id[:-1]}A']
        # N-reel, eg 5074964A becomes 5074964AA
        if (can_id[:-1].isnumeric() and can_id[-1].isupper()):
            whole = string.ascii_uppercase.index(can_id[-1]) + 1
            return [ f"{can_id[:-1]}{i}{can_id[-1]}" for i in string.ascii_uppercase[:whole] ]
        # No partWhole, all other instances returned as is
        return [can_id]


class PhysicalIdentifier():
    ''' Determine class of label - package number or can ID '''

    def __init__(self, identifier):
        self.label = identifier
        self._types = set()
        logger.info(self.label)

        # Determine if identifier is used in CID
        d = {'items': 'can_ID', 'containers': 'name'}
        for db in d:
            print(db)
            q = f'{d[db]}="{identifier}"'
            r = cid_get(db, q, 'priref')
            if r.records:
                self._types.add(d[db])
                print(f'* Self_types = {self._types}')

        # If ID is unused, does it follow pattern?
        if not self._types:
            # <can_ID> Regex checks for two capital letters
            if re.match(r'^.*[A-Z]{2,3}$', identifier):
                self._types.add('can_ID')
            # <package_number> Regex looks for seven characters numbers/uppercase any order
            elif re.match(r'^[A-Z0-9]{7}$', identifier):
                self._types.add('package_number')

    @property
    def type(self):
        i = len(self._types)
        if i == 1:
            return list(self._types)[0]
        if i > 1:
            # Forces type can_ID over package_number
            return 'can_ID'
        else:
            raise Exception('Unable to determine identifier type from CID')


class Carrier():
    ''' Model data for a carrier from its physical <package_number> or <can_ID> label '''

    def __init__(self, **identifiers):
        self.identifiers = identifiers
        self.partwhole = self._partwhole()
        self._items = None

        # Resolve can_ID as package if insufficient data
        if self.partwhole is None and 'can_ID' in self.identifiers:
            packages = set()
            for r in self.items:
                try:
                    carriers = r['Parts']
                except KeyError as exc:
                    str_except = r['object_number'][0]
                    raise Exception(f'Unable to determine partWhole from can_ID - could not use package instead because item {str_except} not linked to a package') from exc

                for c in carriers:
                    p = c['parts_reference'][0]['current_location.name'][0]['name'][0]
                    packages.add(str(p))

            if len(packages) == 1:
                self.identifiers['name'] = list(packages)[0]
                self.partwhole = self._partwhole()

        self._validate()

    def _validate(self):
        ''' Check the data and the model '''

        if not self.items:
            raise Exception('Carrier has no documented items')
        if len(self.items) > 1 and self.partwhole[1] > 1:
            raise Exception('Multi-item carrier should not have multiple reels')

    def _partwhole(self):
        ''' Determine partwhole from identifier '''

        if 'name' in self.identifiers and 'can_ID' in self.identifiers:
            print('* Both Can_ID and Package detected as identifier type...')
            raise Exception('* Both Can_ID and Package detected as identifier type...')
        if 'name' in self.identifiers:
            print('* This is a Container, according to the model, querying CID for part/whole...')
            q_str = self.identifiers['name']
            q = f'current_location.name="{q_str}"'
            r = cid_get('carriersfull', q, 'carrier_part.number,carrier_part.total_numbers')
            if len(r.records) > 1:
                total_recs = len(r.records)
                wholes_all = []
                for num in range(0, total_recs):
                    data = r.records[num]
                    print(f'* Querying CID for multiple part returns / whole data: {q}')
                    try:
                        part = int(data['carrier_part.number'][0])
                        whole = int(data['carrier_part.total_numbers'][0])
                        wholes_all.append(whole)
                        print(f'* Part {part} of {whole}')
                    except Exception as exc:
                        print('* Insufficient reel data in package record')
                        raise Exception('* Insufficient reel data in package record') from exc
                if wholes_all.count(wholes_all[0]) != len(wholes_all):
                    raise Exception(f'* Whole numbers do not match for all returned partWholes: {wholes_all}')

            data = r.records[0]
            print(f'* Querying CID for part / whole data: {q}')

            try:
                part = int(data['carrier_part.number'][0])
                whole = int(data['carrier_part.total_numbers'][0])
                print(f'* Part {part} of {whole}')
            except Exception as exc:
                print('* Insufficient reel data in package record')
                raise Exception('* Insufficient reel data in package record') from exc
            return [part, whole]

        elif 'can_ID' in self.identifiers:
            print('* This is a Can ID, according to the model, converting can_ID letters to numerical value...')
            print(self.identifiers['can_ID'])
            if re.match(r'.*[A-Z][A-Z]$', self.identifiers['can_ID']):
                # Reads can_ID final two letters, returns numerical value, eg AB = ['1','2']
                return [ string.ascii_uppercase.index(i) + 1 for i in [s for s in self.identifiers['can_ID'][-2:]] ]

    def _find_items(self):
        ''' Make query string required to find items '''
        if 'name' in self.identifiers:
            return f"(parts_reference->(current_location.name={self.identifiers['name']}))"

        if 'can_ID' in self.identifiers:
            if self.partwhole:
                # Multi-item tape
                if self.partwhole[-1] == 1:
                    q = self.identifiers['can_ID'][:-1] + '*'
                else:
                    q = self.identifiers['can_ID'][:-2] + self.identifiers['can_ID'][-1]
            else:
                q = self.identifiers['can_ID']

            return f'(can_ID="{q}")'

    @property
    def items(self):
        ''' Fetch all item data '''
        if self._items is None:
            q = f'{self._find_items()} sort can_ID,priref ascending'
            r = cid_get('items', q, '')
            self._items = r.records

        return self._items

    def _field_value(self, field, value_instance=None):
        ''' Helper function for extracting field values from records '''
        values = []

        for r in self.items.records:
            try:
                if field in r:
                    if value_instance:
                        values.append(r[field][0]['value'][value_instance])
                    else:
                        values.append(r[field][0])
            except KeyError:
                pass

        return values

    @property
    def duration(self):
        ''' Sum all known durations of carried items'''
        try:
            total = sum(float(i) for i in self._field_value('video_duration'))
            return round(total, 2)
        except Exception as exc:
            print(exc)

    @property
    def video_format(self):
        ''' Return list of video formats '''
        try:
            return list(set(self._field_value('video_format', value_instance=1)))
        except IndexError:
            pass

    @property
    def status(self):
        ''' Return list of copy statuses '''
        try:
            return list(set(self._field_value('copy_status', value_instance=1)))
        except Exception as exc:
            print(exc)

    @property
    def segments(self):
        ''' Return dict of priref/segments '''
        data = self._segmentation()

        # Validate segmentation
        if (len(self.items) > 1 or self.partwhole[1] > 1):
            missing = [i['object_number'][0] for i in self.items if 'video_part' not in i]
            if missing:
                print(f'* Insufficient video_part data in items: {",".join(missing)}')
                raise Exception(f"* Insufficient video_part data in items: {','.join(missing)}")

        if (len(self.items) > 1 and self.partwhole == [1, 1]):
            print(data)
            # JMW - reworked for Py3 dictionaries, using star function
            in_sort_segments = sorted(data.items(), key=star(lambda k,v: (v[0][0], k)))
            out_sort_segments = sorted(data.items(), key=star(lambda k,v: (v[-1][-1], k)))
            print(f"Segments:\n{in_sort_segments}\n{out_sort_segments}")
            if not in_sort_segments == out_sort_segments:
                raise Exception('Illegal video_part structure')
            # Sort segments
            data = in_sort_segments

        if self.partwhole[1] > 1:
            try:
                # Select tape's timecode parts
                me = self.partwhole[0]
                p = int(self.items[0]['priref'][0])
                data = {p: [data[p][me-1]]}
            except Exception as exc:
                obj = self.items[0]['object_number'][0]
                raise Exception(f'Insufficient video_part data for reel {me} in item {obj}') from exc
        print(data)
        return data

    def _segmentation(self):
        ''' Wrangle segmentation information in seconds for items carried '''

        def seconds(time_str):
            ''' Total seconds from mm.ss string '''
            if time_str.count('.') == 1:
                m, s = [int(i) for i in time_str.split('.')]
            elif time_str.count(':') == 2:
                h, m, s = [int(i) for i in time_str.split(':')]
                m += h * 60
            else:
                m, s = [int(time_str), 0]
            return int(datetime.timedelta(minutes=m, seconds=s).total_seconds())

        manifest = {}

        for i in self.items:
            item_priref = int(i['priref'][0])
            item_obj = i['object_number'][0]

            if 'video_part' not in i:
                continue

            parts = []
            sections = []

            video_parts = i['video_part']
            for p in video_parts:
                if p.count('-') != 1:
                    raise Exception(f'Invalid video_part format in item {item_obj}')

                try:
                    a, b = [seconds(n) for n in p.split('-')]
                except Exception as exc:
                    raise Exception(f'Illegal video_part segment in item: {item_obj}') from exc

                if a > b:
                    raise Exception(f'Invalid video_part data in item {item_obj}')

                segment = (a, b)

                # Detect time decrements as tape divisions
                if parts:
                    if a < parts[-1][-1]:
                        # Multi-reeler
                        if self.partwhole[1] > 1:
                            sections.append(parts)
                            parts = []
                        else:
                            # Single-reeler shouldn't have decrement in video_part sequence
                            raise Exception(f'Unexpected video_part discontinuation in item {item_obj}')

                parts.append(segment)

            if parts:
                sections.append(parts)

            manifest[item_priref] = sections

        return manifest


class Content():
    ''' Wrappers for obtaining a/v content from <collect> priref identifiers '''

    def __init__(self, priref):
        self.priref = priref
        self.imagen_record = self._record()
        self.media = self._media()
        self.umids = [{i: r[i][0] for i in r if i.endswith('_umid')}
                      for r in self.media]

        self.image_urls = [f"{DPI_PATH}{i['imagen.media.largeimage_umid']}" for i in self.umids]

        self.images = [Image.open(requests.get(i, stream=True, verify=False).raw)
                       for i in self.image_urls]

    def _record(self):
        q = f'priref={self.priref}'
        f = 'imagen.record_identifier'
        r = cid_get('internalobject', q, f)
        id_ = str(r.records[0]['Imagen'][0][f][0])
        return id_

    def _media(self):
        q = f'imagen.media.identifier={self.imagen_record}'
        r = cid_get('media', q, '')
        return r.records


def cid_get(database, search, fields):
    ''' Simple query wrapper '''

    d = {'database': database,
         'search': search,
         'fields': fields,
         'output': 'json',
         'limit': '0'}

    try:
        result = CID.get(d)
        print(result)
    except Exception as exc:
        raise Exception from exc

    return result
