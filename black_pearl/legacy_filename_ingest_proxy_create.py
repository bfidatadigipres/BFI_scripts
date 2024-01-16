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
FILE_PATH = os.path.join(QNAP, os.environ['FILENAME_UPDATE'])
AUTOINGEST = os.path.join(QNAP, os.environ['AUTODETECT'])
LOG_PATH = os.environ['LOG_PATH']
CID_API = os.environ['CID_API3']
BUCKET = 'preservation01'
CONTROL_JSON = os.path.join(LOGS_PATH 'downtime_control.json')
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


def check_cid_record(ob_num, file):
    '''
    Search for ob_num of file name
    and check MP4 in file_type for
    returned record
    '''
    pass


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
    }

    try:
        result = CID.get(query)
        if result.hits:
            return True
    except Exception as err:
        print(f"Unable to retrieve CID Media record {err}")
    return False


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
    and check all '-' are '_' and any 'A' or 'B'
    are converted to 01of02 and 02of02 etc.
    '''

    files = os.listdir(FILE_PATH)
    if not files:
        sys.exit()

    LOGGER.info("============== Legacy filename updater START ==================")
    LOGGER.info("Files located in filename_updated/ folder: %s", ', '.join(files))
    alpha = string.ascii_uppercase

    for file in files:
        fpath = os.path.join(FILE_PATH, file)
        fname, ext = file.split('.')
        print(fname)
        # Change N- for N_
        if fname.startswith('N-'):
            fname = fname.replace('-', '_')
        print(fname)

        # Check if 01of** format in name
        count = fname.split('_')
        new_fname = ob_num = ''
        if len(count) >= 3 and 'of' in count[-1]:
            LOGGER.info("PartWhole formatting present for file %s", file)
            print(f"PartWhole present: {fname}")
            ob_num = '-'.join(count[:-1])
            new_fname = fname
        elif len(count) >= 3:
            LOGGER.info("Too many '_' in filename and no partWhole 'of' present %s", file)
            continue
        # Check of 'AB' format indicates part whole
        fname = fname.upper()
        if len(count) == 2:
            letters = re.findall(r'[A-Z]', fname, re.I)
            print(letters)
            if 'N' != letters[0]:
                LOGGER.warning("First letter of name found is not 'N': %s. Skipping.", file)
                continue
            if len(letters) == 2:
                part = alpha.find(letters[1]) + 1
                if part != 1:
                    LOGGER.warning("Skipping: Filename found that is not formatted correctly: %s", file)
                    continue
                part_whole = f'_01of01'
                new_fname = f"{fname[:-1]}{part_whole}"
                ob_num = fname[:-1].replace('_', '-')
            if len(letters) == 3:
                part1 = alpha.find(letters[1]) + 1
                part2 = alpha.find(letters[2]) + 1
                part_whole = f'_{str(part1).zfill(2)}of{str(part2).zfill(2)}'
                new_fname = f"{fname[:-2]}{part_whole}"
                ob_num = fname[:-2].replace('_', '-')
            if len(letters) =< 1 or len(letter) > 3:
                LOGGER.warning("Unanticipated file format: %s. Skipping.", file)
                continue

        # Look up CID item record for file_type = MP4
        if len(ob_num) == 0:
            LOGGER.warning("Skipping. Object number couldn't be created from file renaming.")
            continue
        match = check_cid_record(ob_num, file)
        if not match:
            LOGGER.warning("Skipping. No CID item record found for object number %s", ob_num)
        LOGGER.info("CID item record found with MP4 file-type for %s", ob_num)

        # Rename and move to Autoinges
        new_file = f"{new_fname}.{ext}"
        new_fpath = os.path.join(FILE_PATH, new_file)
        LOGGER.info("Filename changed from %s to correct formatting %s", file, new_file)
        try:
            os.rename(fpath, new_fpath)
        except Exception as err:
            LOGGER.warning("Could not rename file: %s -> %s", file, new_file)
            print(err)
            continue
        if os.path.exists(new_fpath):
            LOGGER.info("File renamed and moving to Autoingest: %s", new_fpath)
            shutil.move(new_fpath, AUTOINGEST)
        if os.path.exists(os.path.join(AUTOINGEST, new_file)):
            LOGGER.info("File successfully moved to autoingest.")
            LOGGER.info("--------------------------------------")
        else:
            LOGGER.error("Error file has not moved to autoingest.")
            LOGGER.info("----------------------------------------")

    LOGGER.info("============== Legacy filename updater END ====================")




if __name__ == '__main__':
    main()

