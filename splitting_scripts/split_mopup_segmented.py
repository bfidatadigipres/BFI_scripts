#!/usr/bin/env python3

'''
Fix C- N-prefix files in processing/segmented where CID Item creation has failed:
1. Traverse subfolders in segmented folder
2. find C- N- files
3. Checks how old the modification time of file is (over 24 hours)
4. Calls document_item.new_or_existing_no_segments_mopup() supplies H22/OFCOM grouping
5. Looks for existing CID item record, if not there creates a new CID items record
6. Returns new object_number
7. Calls up models with container/CAN ID folder name and retrieves
   correct part whole for the file being processed.
8. Files are renamed and left in folder for aspect.py to pick up

NOTE: Updated for Adlib V3

Stephen McConnachie
June 2021
Refactored 2023
'''

# Public imports
import os
import sys
import glob
import json
import logging
from datetime import datetime, timezone
import pytz

# Private imports
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import document_item
import models

# Logging
LOGS = os.environ['SCRIPT_LOG']
CID_API = os.environ['CID_API4']

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
    '/mnt/isilon/video_operations/processing/'
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


def check_cid():
    '''
    Check CID API responsive
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        logger.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def check_for_parts(ob_num):
    '''
    Call up CID for hits against
    part_of_reference for ob_num
    '''
    search = f'part_of_reference="{ob_num}"'
    hits, record = adlib.retrieve_record(CID_API, 'carriers', search, '0', ['object_number'])
    if not record:
        logger.exception('CID check for carriers failed: %s', search)
        return False
    print(record[0])
    if hits == 1 and 'object_number' in str(record[0]):
        print(adlib.retrieve_field_name(record[0], 'object_number')[0])
        return True
    elif hits > 1:
        return "Model carrier check"
    else:
        return False


def check_media_record(fname):
    '''
    Check if CID media record
    already created for filename
    '''
    search = f"imagen.media.original_filename='{fname}'"
    hits = adlib.retrieve_record(CID_API, 'media', search, '0')[0]

    if hits >= 1:
        return True
    else:
        print(f"Unable to retrieve CID Media record with search: {search}")
        return False


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
        processing = os.path.split(media_target)[0]
        autoingest = os.path.join(os.path.split(processing)[0], 'autoingest')
        print(f"** Targeting: {root}")
        logger.info("** Targeting: %s", root)
        logger.info("** Autoingest: %s", autoingest)

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
            file_mod_time = os.stat(filepath).st_mtime
            modified = datetime.fromtimestamp(file_mod_time, tz=timezone.utc)
            mod = modified.replace(tzinfo=pytz.utc).astimezone(local_tz)
            print(mod)
            print(now)
            diff = now - mod
            seconds = diff.seconds
            hours = (seconds / 60) // 60
            logger.info('%s\tModified time is %s seconds ago. %s hours', filepath, seconds, hours)
            print(f'{filepath}\tModified time is {seconds} seconds ago')

            if seconds < 36000:
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
                print(f'***** {filepath}\tGetting MKV CID Item ref for file: {object_number}')
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

            # Model identifier to obtain partWhole
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
                print("Item parts couldn't be determined. Skipping")
                logger.info("Item parts couldn't be determined from carrier. Skipping.")
                continue

            if not isinstance(carr.partwhole[0], int):
                print("Item parts couldn't be determined. Skipping")
                logger.info("Item parts couldn't be determined from carrier. Skipping.")
                continue

            part = str(carr.partwhole[0]).zfill(2)
            whole = str(carr.partwhole[1]).zfill(2)
            logger.info("**** %s\tPart whole from model.Carriers: %s of %s", filepath, part, whole)
            new_f = f'{new_object_under}_{part}of{whole}.{extension}'

            # Check if filename already exists in CID/autoingest (don't rename if duplicate)
            check_result = check_media_record(new_f)
            if check_result is True:
                logger.info("Skipping: Filename found to have persisted to DPI: %s", new_f)
                print(f"SKIPPING: Filename {new_f} persisted to BP, CID media record found")
                continue
            match = glob.glob(f"{autoingest}/**/*/{new_f}", recursive=True)
            if new_f in str(match):
                logger.info("Skipping - CID item record exists and file found in autoingest: %s", match[0])
                print(f"SKIPPING: CID item record exists and file found in autoingest: {match[0]}")
                continue

            logger.info("**** %s\tFile to be renamed %s -> %s", filepath, f, new_f)
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
