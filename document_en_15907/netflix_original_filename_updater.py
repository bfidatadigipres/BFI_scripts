#!/usr/bin/ python3

'''
Script to search in CID item
records for Netflix groupings
where digital.acquired_filename
field is populated with 'File'
entries.

Check in CID media records
for matching assets, and if present
map the original filename to the
digital.acquired_filename field
in CID digital media record

Joanna White
2023
'''

# Public packages
import os
import sys
import logging

# Local packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
STORAGE_PTH = os.environ.get('TRANSCODING')
NETFLIX_PTH = os.environ.get('NETFLIX_PATH')
NET_INGEST = os.environ.get('NETFLIX_AUTOINGEST')
AUTOINGEST = os.path.join(STORAGE_PTH, NET_INGEST)
STORAGE = os.path.join(STORAGE_PTH, NETFLIX_PTH)
ADMIN = os.environ.get('ADMIN')
LOGS = os.path.join(ADMIN, 'Logs')
CODE = os.environ.get('CODE_PATH')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Setup logging
LOGGER = logging.getLogger('netflix_original_filename_updater')
HDLR = logging.FileHandler(os.path.join(LOGS, 'netflix_original_filename_updater.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_check_items():
    '''
    Sends CID request for digital.acquired_filename
    block for iteration
    '''
    query = {'database': 'items',
             'search': f'(grouping.lref="400947" and file_type="IMP")',
             'limit': '0',
             'output': 'json',
             'fields': 'object_number, digital.acquired_filename'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        print(f"cid_check_items(): Unable to retrieve any Netflix groupings from CID item records: {err}")
        query_result = None
    try:
        return_count = query_result.hits
        print(f"{return_count} CID item records found")
    except (IndexError, TypeError, KeyError):
        pass

    return query_result.records


def cid_check_media(priref):
    '''
    Sends CID request for object number
    checks if filename already populated
    '''
    query = {'database': 'media',
             'search': f'object.object_number.lref="{priref}"',
             'limit': '0',
             'output': 'json',
             'fields': 'priref, digital.acquired_filename'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        print(f"cid_check_media(): Unable to find CID digital media record match: {priref} {err}")
        query_result = None
    try:
        priref = query_result.records[0]['priref'][0]
        print(f"cid_check_media(): Priref matched: {priref}")
    except (IndexError, KeyError, TypeError):
        priref = ''
    try:
        file_name = query_result.records[0]['Acquired_filename'][0]['digital.acquired_filename'][0]['Value'][0]
        print(f"cid_check_media(): File name: {file_name}")
    except (IndexError, KeyError, TypeError):
        file_name = ''

    return priref, file_name


def main():
    '''
    Look for all Netflix items
    recently created (date period
    needs defining) and check CID
    media record has original filename
    populated for all IMP items.
    '''

    records = cid_check_items()
    priref_list = []
    for record in records:
        priref_list.append(record['priref'][0])
    print(priref_list)




if __name__ == '__main__':
    main()
