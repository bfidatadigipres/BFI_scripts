#!/usr/bin/env python3

'''
Fix C- N-prefix files in processing/segmented where CID Item creation has failed:
1. Traverse subfolders in segmented folder
2. find C- N- files
3. Checks how old the modification time of file is (over 24 hours)
4. Calls document_item.new_or_existing_no_segments_mopup() supplies H22/OFCOM grouping
5. Looks for existing CID item record, if not there creates a new CID items record
6. Returns new object_number
7. Checks part_of_reference in carrier dB for number of hits returned:
   - if one, assumes it's a single part and makes file name with object_number_01of01.MKV
   - if more than one hit, calls up models.Carrier, iterates list looking for object_number
     match, where found extracts partWholes and outputs to a log (for test period)
   - if non, exits with note log
8. Files are renamed and left in folder for aspect.py to pick up

Stephen McConnachie
June 2021

Refactored for Py3
Joanna White
May 2023
'''

# Public packages
import os
import sys
import pytz
import json
import logging
from datetime import datetime, timezone

# Private packages
sys.path.append(os.environ['CODE'])
import document_item
import models
import adlib

# Logging
LOGS = os.environ['LOG_PATH']
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)

# Setup logging, overwrite each time
logger = logging.getLogger('split_mopup_segmented')
hdlr = logging.FileHandler(os.path.join(LOGS, 'split_mopup_segmented.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Targets (will look for 'segmented' subdirectory)
TARGETS = [
    '/mnt/qnap_10/processing/',
    '/mnt/qnap_h22/Public/processing/',
    '/mnt/qnap_08/processing/',
    '/mnt/qnap_video/Public/F47/processing/'
#    '/mnt/isilon/video_operations/processing/',
#    '/mnt/grack_f47/processing/'
]


def check_control():
    '''
    Check downtime control and stop script of False
    '''
    with open(os.path.join(LOGS, 'downtime_control.json')) as control:
        j = json.load(control)

        if not j['split_control_delete']:
            logger.info('Exit requested by downtime_control.json')
            sys.exit('Exit requested by downtime_control.json')
        if not j['split_control_h22']:
            logger.info('Exit requested by downtime_control.json')
            sys.exit('Exit requested by downtime_control.json')


def check_for_parts(ob_num):
    '''
    Call up CID for hits against
    part_of_reference for ob_num
    '''

    search = f'part_of_reference="{ob_num}"'
    query = {'database': 'carriers',
             'search': search,
             'limit': 0,
             'output': 'json',
             'fields': 'object_number'}

    try:
        result = CID.get(query)
    except Exception as err:
        logger.exception('CID check for carriers failed: %s', err)
        result = None

    if len(result.records[0]) == 1:
        if result.records[0]['object_number'][0]:
            print(result.records[0]['object_number'][0])
            return True
    elif len(result.records[0]) > 1:
        return "Model carrier check"
    else:
        return False


def check_cid():
    '''
    Check CID API responsive
    '''
    try:
        logger.info("Initialising CID session. Script will exit if CID offline")
        cur = adlib.Cursor(CID)
    except KeyError:
        logger.warning("Cannot establish CID session, exiting script.")
        sys.exit()


def main():
    '''
    Iterate targets looking for files that have not been
    renamed, check for CID item record, retrieve object
    number for naming. Check carriers dB for matching
    hits to source file name (N- C-). If hits == 1 then
    hardcode 01of01 filename. If hits > 1 use models.Carrier
    to generate part whole and print to log for test period
    '''
    logger.info("======================== SPLIT MOPUP START ==========================")
    check_control()
    check_cid()

    # Iterate targets
    for media_target in TARGETS:

        # Path to source media
        root = os.path.join(media_target, 'segmented')
        print(f"** Targeting: {root}")
        logger.info("** Targeting: %s", root)

        # List files in recursive sub-directories
        files = []
        for directory, _, filenames in os.walk(root):
            for filename in [f for f in filenames if f.startswith(('C-', 'N-'))]:
                files.append(os.path.join(directory, filename))

        # Process files sequentially
        for filepath in files:
            fpath, f = os.path.split(filepath)
            foldername = os.path.basename(fpath)
            object_number, extension = f.split('.')
            print(f'=== Current file: {filepath}')
            logger.info('%s\tProcessing file\t%s', filepath, object_number)

            # Test modified time of file against current date and time
            # Process file only if modified last 24 hrs ago
            now = datetime.now().astimezone()
            local_tz = pytz.timezone("Europe/London")
            file_mod_time = os.stat(filepath).st_ctime
            modified = datetime.fromtimestamp(file_mod_time, tz=timezone.utc)
            mod = modified.replace(tzinfo=pytz.utc).astimezone(local_tz)
            diff = now - mod
            seconds = diff.seconds
            hours = (seconds / 60) // 60
            logger.info('%s\tModified time is %s seconds ago. %s hours', filepath, seconds, hours)
            print(f'{filepath}\tModified time is {seconds} seconds ago')

            if seconds < 36000: # 10 hours
                logger.info('%s\tFile modified time is too recent, skipping file\t%s', filepath, os.path.basename(filepath))
                print(f'{filepath}\tFile modified time is too recent, skipping file\t{f}')
                continue

            # Use object number to query CID and get existing / create new derived MKV Item
            note = 'autocreated'
            # New block to ensure correct grouping supplied for Item record creation
            if '/mnt/qnap_10' in filepath or '/mnt/qnap_h22' in filepath:
                grouping = '398385'
            else:
                grouping = '397987'
            try:
                new_object = document_item.new_or_existing_no_segments_mopup(object_number, extension, grouping, note=note)
                logger.info('%s\tGetting MKV CID Item ref for file: %s', filepath, object_number)
                print(f'{filepath}\tGetting MKV CID Item ref for file: {object_number}')
            except Exception as err:
                document_message = f'{filepath}\tFailed to get Matroska Item ref: {object_number}\t{err}'
                logger.warning(document_message)
                print(document_message)
                continue

            if not new_object:
                object_message = f'{filepath}\tFailed to read object_number from MKV Item record'
                logger.warning(object_message)
                print(object_message)
                continue

            logger.info("%s\tFound new object number using document_item.new_or_existing_no_segments_mopup: %s", filepath, new_object)

            # Rename media object with N-* object_number and partWhole
            new_object_under = new_object.replace('-', '_')
            new_f = ''

            # New block here, databse=carriers&search=part_of_reference='{object_number}'
            parts_check = check_for_parts(object_number)
            if parts_check is True:
                print("Item has just one part")
                new_f = f'{new_object_under}_01of01.{extension}'
            elif parts_check == 'Model carrier check':
                print("Item has more than one part, check models.Carrier for partWhole")

                # Model identifier
                try:
                    i = models.PhysicalIdentifier(foldername)
                    logger.info(i)
                    style = i.type
                    logger.info('* Identifier is %s', style)
                except Exception as err:
                    logger.warning('models.py error: %s\t%s\t%s', filepath, foldername, err)
                    continue
                try:
                    carr = models.Carrier(**{style: foldername})
                    logger.info('* Carrier modelled ok')
                except Exception as err:
                    logger.warning('models.py error: %s\t%s\t%s', filepath, foldername, err)
                    continue
                part = str(carr.partwhole[0]).zfill(2)
                whole = str(carr.partwhole[1]).zfill(2)
                logger.info("**** %s\tPart whole from model.Carriers: %s of %s", filepath, part, whole)
                new_f = f'{new_object_under}_{part}of{whole}.{extension}'
                logger.info("**** %s\tFile would be renamed %s -> %s", filepath, f, new_f)
                continue
            else:
                print("Item parts couldn't be determined. Skipping")
                logger.info("Item parts couldn't be determined from carrier. Skipping.")
                continue
            if not new_f:
                logger.info("No object_number match in Carrier.items")
                continue

            dst = os.path.join(fpath, new_f)
            try:
                print(f'\t{filepath} --> {dst}')
                os.rename(filepath, dst)
                logger.info('%s\tRenamed Matroska file with MKV Item object_number and partWhole: %s --> %s', filepath, f, new_f)
                print(f'{filepath}\tRenamed Matroska file with MKV Item object_number and partWhole: {f} --> {new_f}')
            except Exception:
                logger.warning('%s\tFailed to rename Matroska file with MKV Item object_number and partWhole\t%s', filepath, f)
                print(f'{filepath}\tFailed to rename Matroska file with MKV Item object_number and partWhole\t{f}')
                continue

    logger.info("======================== SPLIT MOPUP END ============================")


if __name__ == '__main__':
    main()
