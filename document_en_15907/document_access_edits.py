#!/usr/bin/ spython3

'''
Script to look for files named 'EDIT_{source_item}', 
create new CID item record with VIEW specifics, rename
file then move to autoingest path

Joanna White
2024
'''

# Public packages
import os
import sys
import json
import shutil
import logging
import requests
import datetime
import subprocess

# Local packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
STORAGE = os.path.join(os.environ['QNAP_11'], 'access_edits')
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


def cid_check(object_number):
    '''
    Looks up object_number and retrieves title
    and other data for new timed text record
    '''
    query = {'database': 'items',
             'search': f'object_number="{object_number}"',
             'limit': '1',
             'output': 'json'}
    try:
        query_result = CID.get(query)
        return query_result.records
    except Exception as err:
        print(f"cid_check(): Unable to match supplied name with CID Item record: {object_number} {err}")

    return None


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
        source_record = cid_check(source_ob_num)
        if source_record is None:
            LOGGER.warning("Skipping: Unable to match source object number %s to CID item record", source_ob_num)
            continue

        # Build new CID item record from existing data
        source_priref = source_record[0]['priref'][0]
        item_dct = make_item_record_dict(source_priref, file, source_record)
        LOGGER.info(item_dct)
        



def make_item_record_dict(priref, file, record):
    '''
    Get CID item record for source and borrow data
    for creation of new CID item record
    '''
    item = []
    record_default = defaults()
    item.extend(record_default)
    item.append({'record_type': 'ITEM'})
    item.append({'item_type': 'DIGITAL'})
    item.append({'copy_status': 'M'})
    item.append({'copy_usage.lref': '131560'})
    item.append({'accession_date': str(datetime.datetime.now())[:10]})

    if 'Title' in str(record):
        mov_title = record[0]['Title'][0]['title'][0]
        item.append({'title': f"{mov_title} ({arg})"})
        if 'title.article' in str(record):
            item.append({'title.article': record[0]['Title'][0]['title.article'][0]})
        item.append({'title.language': 'English'})
        item.append({'title.type': '05_MAIN'})
    else:
        LOGGER.warning("No title data retrieved. Aborting record creation")
        return None
    if 'Part_of' in str(record):
        item.append({'part_of_reference.lref': record[0]['Part_of'][0]['part_of_reference'][0]['priref'][0]})
    else:
        LOGGER.warning("No part_of_reference data retrieved. Aborting record creation")
        return None
    item.append({'related_object.reference.lref': priref})
    item.append({'related_object.notes': f'{arg} for'})
    if 'SDR' in arg:
        item.append({'file_type.lref': '397457'}) # Unsure how to set file type here also, MOV?
    elif 'Audio Description' in arg:
        item.append({file_type: 'MOV'}) # Unsure how to set file type here - MOV?
    if 'acquisition.date' in str(record):
        item.append({'acquisition.date': record[0]['acquisition.date'][0]})
    if 'acquisition.method' in str(record):
        item.append({'acquisition.method.lref': record[0]['acquisition.method.lref'][0]})
    if 'Acquisition_source' in str(record):
        item.append({'acquisition.source.lref': record[0]['Acquisition_source'][0]['acquisition.source.lref'][0]})
        item.append({'acquisition.source.type': record[0]['Acquisition_source'][0]['acquisition.source.type'][0]['value'][0]})
    item.append({'access_conditions': 'Access requests for this collection are subject to an approval process. '\
                                      'Please raise a request via the Collections Systems Service Desk, describing your specific use.'})
    item.append({'access_conditions.date': str(datetime.datetime.now())[:10]})
    if 'grouping' in str(record):
        item.append({'grouping': record[0]['grouping'][0]})
    if 'language' in str(record):
        item.append({'language': record[0]['language'][0]['language'][0]})
        item.append({'language.type': record[0]['language'][0]['language.type'][0]['value'][0]})
    if len(file) > 1:
        item.append({'digital.acquired_filename': file})

    return item


def defaults():
    '''
    Build defaults for new CID item records
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': 'Amazon metadata integration - automated bulk documentation'},
               {'record_access.user': 'BFIiispublic'},
               {'record_access.rights': '0'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'System Management'},
               {'record_access.rights': '3'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Information Specialist'},
               {'record_access.rights': '3'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Digital Operations'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Documentation'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Curator'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Special Collections'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Librarian'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': '$REST'},
               {'record_access.rights': '1'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'grouping.lref': '400947'}, # JMW Will need replacing when new grouping made for Amazon
               {'language.lref': '74129'},
               {'language.type': 'DIALORIG'},
               {'record_type': 'ITEM'},
               {'item_type': 'DIGITAL'},
               {'copy_status': 'M'},
               {'copy_usage.lref': '131560'},
               {'file_type.lref': '397457'}, # ProRes 422 HQ Interlaced (can't find progressive)
               {'accession_date': str(datetime.datetime.now())[:10]}])

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
