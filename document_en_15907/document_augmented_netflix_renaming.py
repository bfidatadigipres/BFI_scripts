#!/usr/bin/ python3

'''
Script to receive IMP
Netflix folder named with UID.
UID placed into Item record
digital.acquired_filename field
of CID item record.

Receives IMP folder complete with
MXF video and audio files, and XML
metadata files.

Steps:
1. Finds match for IMP folder name
   within watch folder, from CID item
   record, ie 'N-123456'
2. Creates N_123456_ filename prefix
3. Opens each XML in folder looking
   for string match to '<PackingList'
4. Iterates <PackingList><AssetList><Asset>
   blocks extracting <OriginalFilmName language='en'>
   into list of items
5. Numbers each following order retrieved,
   ie, if 6 assets 'N_123456_01of06' for first item
   and 'N_123456_06of06' for last item, adds to dict.
   Checks same amount of items in PKL as in folder.
6. Iterates dictionary adding original filename and
   new name to CID item record 'digital.acquired_filename'
   field, which allows repeated entries. Formatting:
   "<Original Filename> - Renamed to: N_123456_01of06.mxf"
7. Open each XML and write content to the label.text
   and label.type field (possibly new field)
8. XML and MKF contents of IMP folder are renamed
   as per dictionary and moved to autoingest new
   black_pearl_ingest_netflix path (to be confirmed)
   where new put scripts ensure file is moved to
   the netflix01 bucket.

NOTE: The CID item record digital.acquired_filename
      entry is essential to let the CID media record
      have the digital.acquired_filename populated
      for the XML/MXF original name. So step 6 should
      be robust with good reporting if failures occur.
      Notes of original filename/new name must be logged
      as a back up to this.

FUTURE: This script will need to make new CID item
      item records in the future for ProRes and subtitle
      data, and link to the parent manifestation.

Joanna White
2023
'''

# Public packages
import os
import sys
import json
import logging
import datetime
import requests
import xmltodict
import yaml

# Local packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
STORAGE = os.environ.get('QNAP_IMAGEN')
ADMIN = os.environ.get('ADMIN')
LOGS = os.path.join(ADMIN, 'Logs')
CODE = os.environ.get('CODE_PATH')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Date variables
TODAY = datetime.date.today()
TWO_WEEKS = TODAY - datetime.timedelta(days=14)
START = f"{TWO_WEEKS.strftime('%Y-%m-%d')}T00:00:00"
END = f"{TODAY.strftime('%Y-%m-%d')}T23:59:00"
TITLE_DATA = ''
UPDATE_AFTER = '2022-07-01T00:00:00'

# Setup logging
LOGGER = logging.getLogger('document_augmented_netflix_renaming')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_netflix_renaming.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_check(imp_fname):
    '''
    Sends CID request for series_id data
    '''
    hit_count = ""
    priref = ""
    query = {'database': 'items',
             'search': f'digital.acquired_filename="{imp_name}"',
             'limit': '1',
             'output': 'json',
             'fields': 'priref, object_number'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        print(f"cid_check(): Unable to match IMP with Item record: {imp_name} {err}")
        query_result = None
    try:
        priref = query_result.records[0]['priref'][0]
        print(f"cid_check(): Series priref: {priref}")
    except (IndexError, KeyError, TypeError) as err:
        priref = ''
    try:
        ob_num = query_result.records[0]['object_number'][0]
        print(f"cid_check(): Series priref: {ob_num}")
    except (IndexError, KeyError, TypeError) as err:
        ob_num = ''s

    return priref, ob_num


def main():
    '''
    Check watch folder for IMP folder
    look to match IMP folder name with
    CID item record.
    Where matched, process contents
    read PKL XML for part whole order
    and check contents match Asset list.
    '''
    folder_list = [x for x in os.listdir(STORAGE) if os.path.isdir(os.path.join(STORAGE, x))]
    if len(folder_list) == 0:
        LOGGER.info("Netflix IMP renaming script. No folders found.")
        sys.exit()

    for folder in folder_list:
        fpath = os.path.join(STORAGE, folder)
        priref, ob_num = cid_check(folder.strip())
        if not priref:
            LOGGER.warning("Cannot find CID Item record for this folder: %s", fpath)
            continue

        LOGGER.info("Folder matched to CID Item record: %s | %s", folder, priref)
        xml_list = [x for x in os.listdir(fpath) if x.endswith(('.xml', '.XML'))]
        mxf_list = [x for x in os.listdir(fpath) if x.endswith(('.mxf', '.MXF'))]
        all_items = [x for x in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, x))]
        total_files = len(xml_list) + len(mxf_list)
        if total_files != len(all_items):
            LOGGER.warning("Folder contains files that are not XML or MXF: %s", fpath)
            continue

        packing_list = ''
        pkl_dct = {}
        for xml in xml_list:
            with open(os.path.join(fpath, xml), 'r') as file:
                info = file.read()
                if '<PackingList' in str(info):
                    packing_list = os.path.join(fpath, xml)
                    pkl_dct = xmltodict.parse(f"""{info.strip()}""", process_namespaces=True, namespaces=namespaces)

        if not packing_list or pkl_dct:
            LOGGER.warning("Packing list not found among XML files. Cannot process files partWholes: %s", fpath)
            continue
        # Iterate the 'Asset' blocks in 'AssetList' and build list of filenames
        # Ensure count of all assets matches total_files
        # Build dict of original filename = and new filename (based on CID ob_num)
        # Write all dict names to digital.acquired_filename in CID item record
        # Rename all files in IMP folder
        # Move to local autoingest black_pearl_ingest_netflix (subfolder for netflix01 bucket put)
        '''
        # Make new item records here (get title, etc from CID item record, parent priref)
        record, item = build_defaults()
        priref_item = create_item(priref_man, data_dct, record, item)
        if len(priref_item) == 0:
             LOGGER.warning("Monograph item record creation failed, skipping all further stages")
             continue
        print(f"PRIREF FOR NEW ITEM: {priref_item}")
        '''


def build_defaults():
    '''
    Build record and item defaults
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': 'Netflix metadata integration - automated bulk documentation'},
               {'record_access.user': 'BFIiispublic'},
               {'record_access.rights': '0'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'grouping.lref': '400947'},
               {'language.lref': '71429'},
               {'language.type': 'DIALORIG'}])

    item = ([{'record_type': 'ITEM'},
             {'item_type': 'DIGITAL'},
             {'copy_status': 'M'},
             {'copy_usage.lref': '131560'},
             {'file_type.lref': '401103'}, # IMP
             {'code_type.lref': '400945'}, # Mixed
             {'accession_date': str(datetime.datetime.now())[:10]},
             {'acquisition.method.lref': '132853'}, # Donation - with written agreement ACQMETH
             {'acquisition.source.lref': '143463'}, # Netflix
             {'acquisition.source.type': 'DONOR'}])

    return (record, series_work, work, work_restricted, manifestation, item)


def create_digital_original_filenames(filename_dct):
    '''
    WIP - NEW FUNCTION UNTESTED
    Create entries for digital.acquired_filename
    and append to the CID item record.
    '''
    name_updates = []
    for key, val in filename_dct.items():
        name_updates.append({'digital.acquired_filename': f'{key} - Renamed to: {val}'})

    return name_update


def append_original_names(priref, name_udpates):
    '''
    UPDATED - UNTESTED
    Appending digital acquired filenames to
    the CID item record
    '''

    # Append cast/credit and edit name blocks to work_append_dct
    item_append_dct = []
    item_append_dct.extend(name_updates)
    item_edit_data = ([{'edit.name': 'datadigipres'},
                       {'edit.date': str(datetime.datetime.now())[:10]},
                       {'edit.time': str(datetime.datetime.now())[11:19]},
                       {'edit.notes': 'Netflix automated digital acquired filename update'}])

    item_append_dct.extend(item_edit_data)
    LOGGER.info("** Appending data to work record now...")
    print("*********************")
    print(item_append_dct)
    print("*********************")

    result = item_append(priref, item_append_dct)
    if result:
        print(f"Item appended successful! {priref}")
        LOGGER.info("Successfully appended IMP digital.acquired_filenames to Item record %s", priref)
        return True
    else:
        LOGGER.warning("Failed to append IMP digital.acquired_filenames to Item record %s", priref)
        print(f"CID item record append FAILED!! {priref}")
        return False


def item_append(priref, item_dct=None):
    '''
    Items passed in item_dct for amending to CID item record
    '''
    print(item_dct)
    if item_dct is None:
        item_dct = []
        LOGGER.warning("item_append(): item_dct passed to function as None")
    try:
        result = CUR.update_record(priref=priref,
                                   database='items',
                                   data=item_dct,
                                   output='json',
                                   write=True)
        print("*** CID item record append result:")
        print(result)
        return True
    except Exception as err:
        LOGGER.warning("item_append(): Unable to append work data to CID item record %s", err)
        print(err)
        return False


def append_url_data(work_priref, man_priref, data=None):
    '''
    KEEPING THIS IN CASE XML WRITE REQUIRED
    FOR append_original_names() MODULE
    '''

    if 'watch_url' in data:
        # Write to manifest
        payload_mid = f"<URL>{data['watch_url']}</URL><URL.description>Netflix viewing URL</URL.description>"
        payload_head = f"<adlibXML><recordList><record priref='{man_priref}'><URL>"
        payload_end = "</URL></record></recordList></adlibXML>"
        payload = payload_head + payload_mid + payload_end

        write_lock('manifestations', man_priref)
        post_response = requests.post(
            CID_API,
            params={'database': 'manifestations', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'json'},
            data={'data': payload})

        if "<error><info>" in str(post_response.text):
            LOGGER.warning("cid_media_append(): Post of data failed: %s - %s", man_priref, post_response.text)
            unlock_record('manifestations', man_priref)
        else:
            LOGGER.info("cid_media_append(): Write of access_rendition data appear successful for Priref %s", man_priref)

        # Write to work
        payload_head = f"<adlibXML><recordList><record priref='{work_priref}'><URL>"
        payload = payload_head + payload_mid + payload_end

        write_lock('works', work_priref)
        post_response = requests.post(
            CID_API,
            params={'database': 'works', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'json'},
            data={'data': payload})

        if "<error><info>" in str(post_response.text):
            LOGGER.warning("cid_media_append(): Post of data failed: %s - %s", work_priref, post_response.text)
            unlock_record('works', work_priref)
        else:
            LOGGER.info("cid_media_append(): Write of access_rendition data appear successful for Priref %s", work_priref)


def create_item(man_priref, work_dict, record_defaults, item_default):
    '''
    WIP - NEEDS AMENDING
    Create item record for priref
    or subtitles and link to manifestation
    '''
    item_id = ''
    item_object_number = ''
    item_values = []
    item_values.extend(record_defaults)
    item_values.extend(item_default)
    item_values.append({'part_of_reference.lref': man_priref})
    if 'title' in work_dict:
        item_values.append({'title': work_dict['title']})
        item_values.append({'title.language': 'English'})
        item_values.append({'title.type': '05_MAIN'})
    if 'title_article' in work_dict:
        item_values.append({'title.article': work_dict['title_article']})
    print(item_values)
    try:
        i = CUR.create_record(database='items',
                              data=item_values,
                              output='json',
                              write=True)

        if i.records:
            try:
                item_id = i.records[0]['priref'][0]
                item_object_number = i.records[0]['object_number'][0]
                print(f'* Item record created with Priref {item_id} Object number {item_object_number}')
                LOGGER.info('Item record created with priref %s', item_id)
            except Exception as err:
                LOGGER.warning("Item data could not be retrieved from the record: %s", err)

    except Exception as err:
        LOGGER.critical('PROBLEM: Unable to create Item record for <%s> manifestation', man_priref)
        print(f"** PROBLEM: Unable to create Item record attached to manifestation: {man_priref}\nError: {err}")

    return item_object_number, item_id


def write_lock(database, priref):
    '''
    Apply a writing lock to the person record before updating metadata to Headers
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': database, 'command': 'lockrecord', 'priref': f'{priref}', 'output': 'json'})
    except Exception as err:
        LOGGER.warning("Lock record wasn't applied to record %s\n%s", priref, err)


def unlock_record(database, priref):
    '''
    Only used if write fails and lock was successful, to guard against file remaining locked
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': database, 'command': 'unlockrecord', 'priref': f'{priref}', 'output': 'json'})
    except Exception as err:
        LOGGER.warning("Post to unlock record failed. Check record %s is unlocked manually\n%s", priref, err)



if __name__ == '__main__':
    main()
