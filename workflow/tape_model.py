#!/usr/bin/env python3

'''
Updated for Python3/AdlibV3.
Tested for selecta.py
'''

# Public imports
import os
import sys

# Local imports
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global variables
CID_API = os.environ['CID_API4']


class Tape():

    def __init__(self, object_number=None, priref=None):
        if priref:
            search = f'priref={priref}'
            record = adlib.retrieve_record(CID_API, 'items', search, '1', ['object_number'])[1]
            try:
                object_number = adlib.retrieve_field_name(record[0], 'object_number')[0]
            except Exception:
                raise Exception('Unable to model carrier for item: {}'.format(priref))

        self.package_number = self.get_package_number(object_number)
        self.can_id = self.get_can_id(object_number)
        self.identifiers = {'can_ID': self.can_id, 'package_number': self.package_number}

        self.items = None
        self.works = None

        self._items()
        self.objects = self.get_identifiers()

    def _items(self):
        if self.package_number:
            query = f'parts_reference->current_location.name->name="{self.package_number}"'
        elif self.can_id:
            if self.can_id[-1] in ['A','B','C','D','E','F','G','H','I',
                                   'J','K','L','M','N','O','P','Q','R',
                                   'S','T','U','V','W','X','Y','Z']:
                query = f'can_ID="{self.can_id}*"'
            else:
                query = f'can_ID="{self.can_id}"'
        else:
            return None

        search = f'item_type=Video and {query}'
        fields = [
            'video_duration',
            'video_format',
            'object_number',
            'priref',
            'copy_status',
            'video_part'
        ]
        results = adlib.retrieve_record(CID_API, 'items', search, '100', fields)[1]
        if results:
            self.items = results
        else:
            self.items = None

    def _works(self):
        if not self.items:
            return None

        if self.works:
            return

        works = []
        for pr in self.objects:
            i = pr['priref']
            query = f'Df=work and (parts_reference->(parts_reference.lref={i}))'
            record = adlib.retrieve_record(CID_API, 'works', query, '100', ['object_number', 'priref'])[1]
            if record:
                o = adlib.retrieve_field_name(record[0], 'object_number')[0]
                p = int(adlib.retrieve_field_name(record[0], 'priref')[0])
                d = {'object_number': o, 'priref': p}
                works.append(d)
            else:
                pass

        if works:
            self.works = works
        else:
            self.works = []

    def get_identifiers(self):
        if not self.items:
            return None

        ids = []
        for r in self.items:
            try:
                o = adlib.retrieve_field_name(r, 'object_number')[0]
                p = int(adlib.retrieve_field_name(r, 'priref')[0])
                d = {'object_number': o, 'priref': p}
                ids.append(d)
            except Exception:
                pass

        return ids

    def get_package_number(self, object_number):
        search = f'part_of_reference="{object_number}"'
        record = adlib.retrieve_record(CID_API, 'carriersfull', search, '1', ['current_location.barcode'])[1]
        if record:
            package = adlib.retrieve_field_name(record[0], 'current_location.barcode')[0]
            return str(package)
        else:
            return None

    def get_can_id(self, object_number):
        search = f'object_number="{object_number}"'
        record = adlib.retrieve_record(CID_API, 'items', search, '1', ['can_ID'])[1]
        if record:
            item_can_id = adlib.retrieve_field_name(record[0], 'can_ID')[0]
            if item_can_id is None:
                return None
            tape_can_id = ''.join(x for x in item_can_id if not x.islower())
            return str(tape_can_id)
        else:
            return None

    def duration(self):
        if not self.items:
            return None

        total = 0

        for r in self.items:
            try:
                item_duration = float(adlib.retrieve_field_name(r, 'video_duration')[0])
                total += item_duration
            except Exception:
                return None

        if total:
            return int(total)
        else:
            return None

    def format(self):
        if not self.items:
            return None

        formats = set()
        for r in self.items:
            try:
                item_format = r['video_format'][0]['value'][1]['spans'][0]['text']
                formats.add(item_format)
            except Exception:
                pass

        if len(formats) == 1:
            return str(list(formats)[0])
        else:
            return None

    def status(self):
        if not self.items:
            return None

        states = set()
        for r in self.items:
            try:
                s = r['copy_status'][0]['value'][1]['spans'][0]['text']
                states.add(s)
            except Exception:
                pass

        return ','.join(sorted(states))

    def segmentation(self):
        if not self.items:
            return None

        all = []
        for rec in self.items:
            vps = adlib.retrieve_field_name(rec, 'video_part')
            for v in vps:
                all.append(v)
        return all

    def content_dates(self):
        self._works()
        if not self.works:
            return None

        years = set()
        work_prirefs = []
        work_prirefs = [i['priref'] for i in self.works]

        for i in work_prirefs:
            search = f'priref={i} and title_date_start>0'
            record = adlib.retrieve_record(CID_API, 'works', search, '1', ['title_date_start'])[1]

            if record:
                date = adlib.retrieve_field_name(record[0], 'title_date_start')[0]
                year = int(date[:4])
                years.add(year)
            else:
                return None

        if years:
            return sorted(list(years))
        else:
            return None

    def location(self):
        if self.package_number:
            search = f'name="{self.package_number}"'
            record = adlib.retrieve_record(CID_API, 'locations', search, '1')[1]

            if record:
                location = str(adlib.retrieve_field_name(record[0], 'part_of')[0])
                return location
            else:
                pass

    def origin(self):
        # Is carrier a preferred migration source?
        this_format = self.format()
        if not this_format:
            return None

        rank = ['CD Video',
                'CD-R',
                'DVD',
                'DVD-R',
                'DVD+R',
                'Audio tape',
                'Audio CD',
                'Digital Audio Tape',
                'VHS cassette',
                'Super VHS',
                'Betamax',
                'Umatic (unspecified)',
                'Umatic Low Band',
                'Umatic High Band',
                'Umatic High Band SP',
                'Shibaden',
                '1-inch (unspecified)',
                '1-inch A Format',
                '1-inch C-Format',
                '1-inch IVC',
                '2-inch Quadruplex',
                '2-inch Quadruplex High Band',
                'MII',
                'Betacam',
                'Betacam SP',
                'Betacam SX',
                'D1',
                'D2',
                'D5',
                'D5 HD',
                'DV-Cam',
                'Mini DV',
                'D3',
                'Digital Betacam',
                'HD Cam',
                'HD Cam SR',
                'High Definition Tape']

        migrate_this = []
        for i in self.objects:
            priref = i['priref']
            # Item has digital master sibling or is in DPI?
            q = f'priref={priref} and (part_of_reference->(parts_reference->(reproduction.reference->imagen.media.original_filename=* or (item_type=Digital and copy_status=Master))))'   
            digitised = self._check('items', q)
            print(digitised)
            if digitised is not None:
                if digitised >= 1:
                    migrate_this.append(False)
                    continue

            # Analyse video siblings JWM {priref} may want changing to {0} in search
            search = f'copy_status="Master","Status pending" and item_type=Video and (part_of_reference->(parts_reference.lref={priref})) and not priref={priref}'
            hits, recs = adlib.retrieve_record(CID_API, 'items', search, '1', ['video_format', 'object_number'])
            if hits is None:
                raise Exception("CID API could not be reached with Items search:\n{seach}")
            # No master siblings, therefore migrateable
            if hits == 0:
                migrate_this.append(True)
                break

            # Compare format rank of siblings
            for sib in recs:
                print(f"** Sib: {sib}")
                try:
                    # Untested until siblings appear
                    sib_format = sib['video_format'][0]['value'][1]['spans'][0]['text']
                    print(sib_format)
                except Exception:
                    migrate_this.append(True)
                    continue

                try:
                    rank.index(sib_format)
                except Exception:
                    continue

                if rank.index(this_format) >= rank.index(sib_format):
                    migrate_this.append(True)
                else:
                    migrate_this.append(False)

        return any(migrate_this)

    def _check(self, database, query):
        hits = adlib.retrieve_record(CID_API, database, query, '-1')[0]
        if hits is None:
            raise Exception(f'CID API could not be reached with {database} search:\n{search}')
        if hits > 0:
            return hits
        else:
            pass

    def _count_manifestations(self, query):
        for i in self.objects:
            priref = i['priref']
            search = f'Df=manifestation and parts_reference.lref={priref} and ({query})'

            match = self._check('manifestations', search)
            if match:
                yield 1

    def cousins(self):
        for i in self.objects:
            priref = i['priref']
            query = f'part_of_reference->(part_of_reference->(parts_reference->(parts_reference.lref={priref})))'
            if self._check('items', query) > 1:
                return True

        return False

    def siblings(self):
        for i in self.objects:
            priref = i['priref']
            query = f'part_of_reference->parts_reference.lref={priref}'
            if self._check('items', query) > 1:
                return True

        return False

    def digital_siblings(self):
        for i in self._count_manifestations('parts_reference->(item_type=Digital)'):
            return True
        return False

    def film_siblings(self):
        for i in self._count_manifestations('parts_reference->(item_type=Film)'):
            return True
        return False

    def video_siblings(self):
        for i in self._count_manifestations('parts_reference->(item_type=Video)'):
            return True
        return False
