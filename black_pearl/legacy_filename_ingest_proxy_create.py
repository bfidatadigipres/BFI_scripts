#!/usr/bin/env python3

'''
Launched from shell script to multiple jobs for parallel processing

1. Receive file path and validate path is a file/exists
2. Take filename and look up in CSV_PATH, if not there/no priref skip.
3. If priref associated with filename, check CID for item record
   and extract item_type, file_type, imagen.media.original_filename:
   a. If item_type = Digital, file_type = MP4, and i.m.o_f is empty:
    - Ingest MP4 to DPI, create new CID media record
    - Create JPEG images thumbnail/largeimage
    - Copy MP4 with correct filename to proxy path
    - Write access rendition values to new CID media record
    - Move MP4 to completed folder for deletion
   b. If i_t = Digital, f_t = non MP4, i.m.o_f is populated:
    - Check CID media record for access rendition values
    - If present, no further actions move MP4 to 'already_ingested' folder
    - If not present then create JPEG images and move MP4 to proxy path
    - Update existing CID digital media record with access rendition vals
   c. If i_t is not Digital
    - Skip with note in logs, as CID item record data is inaccruate
   d. If i_t = Digital, f_t is empty:
    - Skip with note in logs, that CID item record not sufficient

This script needs to:
- BP Put of individual items
- BP validation of said individual item
- Create CID Digital Media record
- Create JPEG file/thumbnail/largeimage from blackdetected MP4 scan
- Append data to CID media records

Files not to be handled via the regular autoingest/black_pearl scripting
and therefore not to use the autoingest folder structures.

Joanna White
2024
'''

# Global imports
import re
import os
import csv
import sys
import json
import pandas
import string
import shutil
import logging
import requests
import datetime
import subprocess
from ds3 import ds3, ds3Helpers

# Private imports
sys.path.append(os.environ['CODE'])
import adlib

# Global paths
QNAP = os.environ['QNAP_REND1']
FILE_PATH = os.path.join(QNAP, 'filename_updater/')
COMPLETED = os.path.join(FILE_PATH, 'completed/')
INGEST = os.path.join(FILE_PATH, 'for_ingest/')
PROXY_CREATE = os.path.join(FILE_PATH, 'proxy_create/')
CSV_PATH = os.path.join(os.environ['ADMIN'], 'legacy_MP4_file_list.csv')
LOG_PATH = os.environ['LOG_PATH']
CID_API = os.environ['CID_API3']
BUCKET = 'preservation01'
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
JSON_END = os.environ['JSON_END_POINT']

# Setup logging
LOGGER = logging.getLogger('legacy_filename_updater')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'legacy_filename_updater.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check control_json isn't False
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['autoingest']:
            print('* Exit requested by downtime_control.json. Script exiting')
            sys.exit('Exit requested by downtime_control.json. Script exiting')


def read_csv_match_file(file):
    '''
    Make set of all entries
    with title as key, and value
    to contain all other entries
    as a list (use pandas)
    '''

    data = pandas.read_csv(CSV_PATH)
    data_dct = data.to_dict(orient='list')
    length = len(data_dct['fname'])

    for num in range(1, length):
        if file in data_dct['fname'][num]:
            return data_dct['priref'][num], data_dct['ob_num'][num]


def check_cid_record(priref, file):
    '''
    Search for ob_num of file name
    and check MP4 in file_type for
    returned record
    '''
    search = f"priref='{priref}'"
    query = {
        'database': 'items',
        'search': search,
        'limit': '0',
        'output': 'json',
        'fields': 'item_type, file_type, imagen.media.original_filename, reference_number'
    }

    try:
        result = CID.get(query)
        print(result.records[0])
    except Exception as err:
        print(f"Unable to retrieve CID Item record {err}")

    item_type = file_type = original_fname = ''
    if 'item_type' in str(result.records[0]):
        item_type = result.records[0]['item_type'][0]['value'][0]
    if 'file_type' in str(result.records[0]):
        file_type = result.records[0]['file_type'][0]
    if 'imagen.media.original_filename' in str(result.records[0]):
        original_fname = file_type = result.records[0]['imagen.media.original_filename'][0]
    if 'reference_number' in str(result.records[0]):
        ref_num = result.records[0]['reference_number'][0]

    return item_type, file_type, original_fname, ref_num


def check_media_record(fname):
    '''
    Check if CID media record
    already created for filename
    '''
    search = f"imagen.media.original_filename='{fname}'"
    query = {
        'database': 'media',
        'search': search,
        'limit': '0',
        'output': 'json',
        'fields': 'imagen.media.hls_umid, access_rendition.mp4'
    }

    try:
        result = CID.get(query)
        print(result.records[0])
    except Exception as err:
        print(f"Unable to retrieve CID Media record {err}")

    media_hls = access_mp4 = ''
    if 'imagen.media.hls_umid' in str(result.records[0]):
        media_hls = result.records[0]['imagen.media.hls_umid'][0]
    if 'access_rendition.mp4' in str(result.records[0]):
        access_mp4 = result.records[0]['Access_rendition'][0]['access_rendition.mp4'][0]

    return media_hls, access_mp4


def check_bp_status(fname, bucket_list):
    '''
    Look up filename in BP to avoid
    multiple ingests of files
    '''

    for bucket in bucket_list:
        query = ds3.HeadObjectRequest(bucket, fname)
        result = CLIENT.head_object(query)

        if 'DOESNTEXIST' in str(result.result):
            continue

        try:
            md5 = result.response.msg['ETag']
            length = result.response.msg['Content-Length']
            if int(length) > 1 and len(md5) > 30:
                return True
        except (IndexError, TypeError, KeyError) as err:
            print(err)


def get_media_ingests(object_number):
    '''
    Use object_number to retrieve all media records
    '''

    dct = {'database': 'media',
           'search': f'object.object_number="{object_number}"',
           'fields': 'imagen.media.original_filename',
           'limit': 0,
           'output': 'json'}

    original_filenames = []
    try:
        result = CID.get(dct)
        print(f'\t* MEDIA_RECORDS test - {result.hits} media records returned with matching object_number')
        print(result.records)
        for r in result.records:
            filename = r['imagen.media.original_filename']
            print(f"File found with CID record: {filename}")
            original_filenames.append(filename[0])
    except Exception as err:
        print(err)

    return original_filenames


def main():
    '''
    Find all files in filename_updater/ folder
    and call up CID to identify whether assets
    need ingesting or access copy work
    '''

    files = [ x for x in os.listdir(FILE_PATH) if os.path.isfile(os.path.join(FILE_PATH, x)) ]
    if not files:
        sys.exit()

    LOGGER.info("============== Legacy filename updater START ==================")
    LOGGER.info("Files located in filename_updated/ folder: %s", ', '.join(files))
    alpha = string.ascii_uppercase

    for file in files:
        fpath = os.path.join(FILE_PATH, file)
        fname, ext = file.split('.')
        print(fname)

        # Find match in CSV
        match_dict = read_csv_match_file(file)
        print(match_dict)
        if match_dict is None:
            LOGGER.warning("File not found in CSV: %s", file)
            continue
        if '#VALUE!' in str(match_dict):
            LOGGER.warning("Skipping: Priref or object_number value missing for file %s", file)
            continue

        object_number, priref = match_dict

        # Look up CID item record for file_type = MP4
        if len(object_number) == 0:
            LOGGER.warning("Skipping. Object number couldn't be created from file renaming.")
            continue
        item_type, file_type, original_fname, reference_num = check_cid_record(object_number, file)
        if not item_type or file_type:
            LOGGER.warning("Skipping. No CID item record found for object number %s", object_number)
            continue
        if item_type != 'DIGITAL':
            LOGGER.warning("Skipping. Incorrect CID item record attached to MP4 file, not DIGITAL item_type")
            continue
        LOGGER.info("CID item record found with MP4 file-type for %s", object_number)

        ingest = proxy = False
        # Begin assessment of actions
        if file_type == 'MP4' and original_fname == '':
            ingest = True
            proxy = True
        elif file_type == 'MP4' and len(original_fname) > 3:
            access_hls, access_mp4 = check_media_record(original_fname)
            if len(access_hls) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_hls)
            elif len(access_mp4) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_mp4)
            else:
                LOGGER.info("No access copy found for file. Moving MP4 for proxy")
                proxy = True
        elif file_type != 'MP4' and len(original_fname) > 3:
            LOGGER.info("File type for CID item record is not MP4: %s", file_type)
            access_hls, access_mp4 = check_media_record(original_fname)
            if len(access_hls) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_hls)
            elif len(access_mp4) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_mp4)
            else:
                LOGGER.info("No access copy found for file. Moving MP4 for proxy")
                proxy = True
        elif file_type != 'MP4' and original_fname = '':
            # Would we ingest this if file_type doesn't match?
            proxy = True
        else:
            continue

        # Start ingest

        # Start MP4 move/JPEG creation

    LOGGER.info("============== Legacy filename updater END ====================")




if __name__ == '__main__':
    main()

