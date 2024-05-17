import os
from adlib import adlib

CID_API = os.environ['CID_API4']
cid = adlib.Database(url=CID_API)

class Tape():

    def __init__(self, object_number=None, priref=None):
        if priref:
            q = {'database': 'items',
                 'search': 'priref={}'.format(priref),
                 'fields': 'object_number',
                 'output': 'json'}

            try:
                object_number = cid.get(q).records[0]['object_number'][0]
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
            # Deprecated - uses the old CD data model, replaced in barcode project January 2020
            #query = 'collcopy.number->package_number="{}"'.format(self.package_number)
            query = 'parts_reference->current_location.name->name="{}"'.format(self.package_number)
        elif self.can_id:
            if self.can_id[-1] in ['A','B','C','D','E','F','G','H','I',
                                   'J','K','L','M','N','O','P','Q','R',
                                   'S','T','U','V','W','X','Y','Z']:
                query = 'can_ID="{}*"'.format(self.can_id)
            else:
                query = 'can_ID="{}"'.format(self.can_id)
        else:
            return None

        q = {'database': 'items',
             'search': 'item_type=Video and {}'.format(query),
             'fields': 'video_duration,video_format,object_number,priref,copy_status,video_part',
             'limit': '100',
             'output': 'json'}

        try:
            result = cid.get(q)
            self.items = result.records
        except Exception:
            self.items = None

    def _works(self):
        if not self.items:
            return None

        if self.works:
            return

        works = []

        item_prirefs = [i['priref'] for i in self.objects]
        for i in item_prirefs:
            query = 'Df=work and (parts_reference->(parts_reference.lref={}))'.format(i)

            q = {'database': 'works',
                 'search': query,
                 'fields': 'object_number,priref',
                 'limit': '100',
                 'output': 'json'}

            try:
                result = cid.get(q)
                o = result.records[0]['object_number'][0]
                p = int(result.records[0]['priref'][0])
                d = {'object_number': o, 'priref': p}
                works.append(d)
            except Exception:
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
                o = r['object_number'][0]
                p = int(r['priref'][0])
                d = {'object_number': o, 'priref': p}
                ids.append(d)
            except Exception:
                pass

        return ids

    def get_package_number(self, object_number):
        #query = 'collcopy.number->object.number->object_number="{}"'.format(object_number)
        query = 'part_of_reference="{}"'.format(object_number)

        #q = {'database': 'packages',
        q = {'database': 'carriersfull',
             'search': query,
             #'fields': 'package_number',
             #'fields': 'current_location.barcode',
             'output': 'json'}

        try:
            result = cid.get(q)
            package = result.records[0]['current_location.barcode'][0]
            return str(package)
        except Exception:
            return None

    def get_can_id(self, object_number):
        query = 'object_number="{}"'.format(object_number)

        q = {'database': 'items',
             'search': query,
             'fields': 'can_ID',
             'output': 'json'}

        try:
            result = cid.get(q)
            item_can_id = result.records[0]['can_ID'][0]
            tape_can_id = ''.join(x for x in item_can_id if not x.islower())
            return str(tape_can_id)
        except Exception:
            return None

    def duration(self):
        if not self.items:
            return None

        total = 0

        for r in self.items:
            try:
                item_duration = float(r['video_duration'][0])
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
                item_format = r['video_format'][0]['value'][1]
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
                s = r['copy_status'][0]['value'][1]
                states.add(s)
            except Exception:
                pass

        return ','.join(sorted(states))

    def segmentation(self):
        if not self.items:
            return None

        return all(['video_part' in i for i in self.items])

    def content_dates(self):
        self._works()
        if not self.works:
            return None

        years = set()

        work_prirefs = [i['priref'] for i in self.works]

        for i in work_prirefs:
            query = 'priref={} and title_date_start>0'.format(i)

            q = {'database': 'works',
                 'search': query,
                 'fields': 'title_date_start',
                 'output': 'json'}

            try:
                result = cid.get(q)
                date = result.records[0]['Title_date'][0]['title_date_start'][0]
                year = int(date[:4])
                years.add(year)
            except Exception:
                return None

        if years:
            return sorted(list(years))
        else:
            return None

    def location(self):
        if self.package_number:
            # Deprecated - refers to old data model, replaced in January 2020 in barcode project
            #query = 'package_number="{}"'.format(self.package_number)
            query = 'name="{}"'.format(self.package_number)

            #q = {'database': 'packages',
            q = {'database': 'locations',
                 'search': query,
                 #'fields': 'current.location',
                 'fields': 'part_of',
                 'output': 'json'}

            try:
                r = cid.get(q)
                location = str(r.records[0]['part_of'][0])
                return location
            except Exception:
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
            q = 'priref={} and (part_of_reference->(parts_reference->(reproduction.reference->imagen.media.original_filename=* or (item_type=Digital and copy_status=Master))))'.format(priref)
            digitised = self._check('items', q)
            if digitised is not None:
                if digitised >= 1:
                    migrate_this.append(False)
                    continue

            # Analyse video siblings
            query = {'database': 'items',
                     'search': 'copy_status="Master","Status pending" and item_type=Video and (part_of_reference->(parts_reference.lref={0})) and not priref={0}'.format(priref),
                     'fields': 'video_format,object_number',
                     'output': 'json'}

            # No master siblings, therefore migrateable
            video_siblings = cid.get(query)
            if video_siblings.hits == 0:
                migrate_this.append(True)
                break

            # Compare format rank of siblings
            for sib in video_siblings.records:
                try:
                    sib_format = sib['video_format'][0]['value'][1]
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
        q = {'database': database,
             'search': query,
             'limit': '-1',
             'output': 'json'}

        try:
            r = cid.get(q)
            return r.hits
        except Exception:
            pass

    def _count_manifestations(self, query):
        for i in self.objects:
            priref = i['priref']
            search = 'Df=manifestation and parts_reference.lref={} and ({})'.format(priref, query)

            match = self._check('manifestations', search)
            if match:
                yield 1

    def cousins(self):
        for i in self.objects:
            priref = i['priref']
            query = 'part_of_reference->(part_of_reference->(parts_reference->(parts_reference.lref={})))'.format(priref)
            if self._check('items', query) > 1:
                return True

        return False

    def siblings(self):
        for i in self.objects:
            priref = i['priref']
            query = 'part_of_reference->parts_reference.lref={}'.format(priref)
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


