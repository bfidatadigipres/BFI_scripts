#!/usr/bin/ python3

'''
Script to look for files named 'EDIT_{source_item}',
create new CID item record with VIEW specifics, rename
file then move to autoingest path

NOTES: Integrated with adlib_v3 for test

Joanna White
2024
'''

# Public packages
import os
import sys
import json
import logging
import datetime
import requests

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global variables
STORAGE = os.path.join(os.environ['QNAP_11'], 'access_edits')
AUTOINGEST = os.path.join(os.environ['QNAP_11'], 'autoingest/ingest/autodetect')
LOGS = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Setup logging
LOGGER = logging.getLogger('document_access_edits')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_access_edits.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check for downtime control
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            LOGGER.info("Script run prevented by downtime_control.json. Script exiting")
            sys.exit("Script run prevented by downtime_control.json. Script exiting")


def main():
    '''
    Iterate access_edits folder working through edited
    files prefixed 'EDIT_'
    '''
    file_list = [x for x in os.listdir(STORAGE) if x.startswith('EDIT_')]
    if len(file_list) == 0:
        sys.exit()

    LOGGER.info("======== Document Access Edits scripts start =====================")
    for file in file_list:
        fpath = os.path.join(STORAGE, file)
        LOGGER.info("File found to process: %s", fpath)
        if not os.path.isfile(fpath):
            LOGGER.warning("Skipping: File type has not been recoginsed.")
            continue

        # Get source Item record ob_num from filename
        source_file = file.split('EDIT_')[1].split('_')[:-1]
        source_ob_num = '-'.join(source_file)
        search = f"object_number='{source_ob_num}'"
        hits, source_record = adlib.retrieve_record('items', search, '0', fields=None)
        if hits == '0':
            LOGGER.warning("Skipping: Unable to match source object number %s to CID item record", source_ob_num)
            continue

        # Build new CID item record from existing data and make CID item record
        source_priref = source_record[0]['priref'][0]
        item_dct = make_item_record_dict(source_priref, file, source_record)
        LOGGER.info(item_dct)
        item_xml = adlib.create_record_data('', item_dct)
        priref, ob_num = push_record_create(item_xml, 'items', 'insertrecord')
        if priref is None:
            LOGGER.warning("Skipping: CID item record creation failed: %s", item_xml)
            continue

        # Rename/move to autoingest
        part_whole = file.split('EDIT_')[1].split('_')[-1]
        new_fname = f"{ob_num.replace('-', '_')}_{part_whole}"
        new_fpath = os.path.join(AUTOINGEST, new_fname)
        LOGGER.info("Renaming file:\n%s\n%s", fpath, new_fpath)

        os.rename(fpath, new_fpath)
        if os.path.exists(new_fpath):
            LOGGER.info("New file successfully renamed and moved to autoingest path")
        else:
            LOGGER.warning("Failed to rename/move file: %s", fpath)

    LOGGER.info("======== Document Access Edits scripts end =======================")


def make_item_record_dict(priref, file, record):
    '''
    Get CID item record for source and borrow data
    for creation of new CID item record
    '''
    ext = file.split('.')[-1].upper()

    item = []
    item.extend(defaults())

    if 'Title' in str(record):
        title = adlib.retrieve_field_name('title', record)
        item.append({'title': title})
        if 'title.article' in str(record):
            item.append({'title.article': adlib.retrieve_field_name('title.article', record)})
        item.append({'title.language': 'English'})
        item.append({'title.type': '05_MAIN'})
    else:
        LOGGER.warning("No title data retrieved. Aborting record creation")
        return None
    if 'part_of_reference' in str(record):
        item.append({'part_of_reference.lref': adlib.retrieve_field_name('part_of_reference.lref', record)})
    else:
        LOGGER.warning("No part_of_reference data retrieved. Aborting record creation")
        return None
    if 'grouping.lref' in str(record):
        item.append({'grouping.lref': adlib.retrieve_field_name('grouping.lref', record)})
    if 'language' in str(record):
        item.append({'language': adlib.retrieve_field_name('language', record)})
        item.append({'language.type': adlib.retrieve_field_name('language.type', record)})

    item.append({'digital.acquired_filename': file})
    item.append({'digital.acquired_fileame.type': 'File'})
    item.append({'file_type': ext})
    item.append({'scan.type': 'Progressive'})
    item.append({'source_item.lref': priref})
    item.append({'quality_comments.date': str(datetime.datetime.now())[:10]})
    item.append({'quality_comments': 'Viewing copy creted from digital master which has been ingested for access instances. \
                                      The file may have had adverts, bars and tones cut out, or other fixes applied.'})
    item.append({'quality_comments.writer': 'BFI National Archive'})

    return item


def defaults():
    '''
    Build defaults for new CID item records
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': 'DMS access edit integration - automated bulk documentation'},
               {'record_access.user': 'BFIiispublic'},
               {'record_access.rights': '0'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'language.lref': '74129'},
               {'language.type': 'DIALORIG'},
               {'record_type': 'ITEM'},
               {'item_type': 'DIGITAL'},
               {'copy_status': 'V'},
               {'copy_usage.lref': '131560'},
               {'access_conditions': 'Before reusing BFI National Archive digital collections please ensure the required clearances from copyright holders, contributors or other stakeholders have been obtained for specific use.'},
               {'access_contitions.date': str(datetime.datetime.now())[:10]}])

    return record


def push_record_create(payload, database, method):
    '''
    Use requests.request to push data to the
    CID API as grouped XML
    '''
    hdrs = {'Content-Type': 'text/xml'}
    prms = {
        'command': method,
        'database': database,
        'xmltype': 'grouped',
        'output': 'json'
    }

    try:
        response = requests.request('POST', CID_API, headers=hdrs, params=prms, data=payload, timeout=1200)
        print(response.text)
    except Exception as err:
        LOGGER.critical("push_record_create(): Unable to create %s record with %s and payload: \n%s", database, method, payload)
        print(err)
        return None, None

    if 'recordList' in response.text:
        records = json.loads(response.text)
        priref = records['adlibJSON']['recordList']['record'][0]['priref'][0]
        object_number = records['adlibJSON']['recordList']['record'][0]['object_number'][0]
        return priref, object_number
    return None, None


if __name__ == '__main__':
    main()
