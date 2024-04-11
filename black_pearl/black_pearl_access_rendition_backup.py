#!/usr/bin/ python3

'''
Script to frequently back up
MP4 and JPG proxy files created as part
of autoingest to DPI.

Initially this script is to be
run with an open modification time but
later to be set to a given amount of
days that matches script run frequency.

NOTE: Assuming one bucket will suffice
      initially, to be expanded if needed.

Joanna White
2024
'''

import os
import sys
import json
import shutil
import logging
from datetime import datetime
from time import sleep
from ds3 import ds3, ds3Helpers

# Global vars
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.environ['CONTROL_JSON']
STORAGE = os.environ['QNAP_11']
INGEST_POINT = os.path.join(STORAGE, 'mp4_proxy_backup_ingest/')
MOD_MAX = 1000 # Modification time restriction
UPLOAD_MAX = 1099511627776 # 1TB max
BUCKET = 'access_renditions_backup01'

# Setup logging
LOGGER = logging.getLogger(f'black_pearl_access_rendition_backup')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'black_pearl_access_rendition_backup.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

START_FOLDERS = {
    'bfi': '201605',
    'eafa': '201605',
    'iwm': '201605',
    'lsa': '201605',
    'mace': '201605',
    'nefa': '201605',
    'nis': '201605',
    'nls': '201605',
    'nssaw': '201605',
    'nwfa': '201605',
    'sase': '201605',
    'thebox': '201605',
    'wfsa': '201605',
    'yfa': '201605'
}


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['black_pearl']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def get_size(fpath):
    '''
    Check the size of given folder path
    return size in kb
    '''
    if os.path.isfile(fpath):
        return os.path.getsize(fpath)

    try:
        byte_size = sum(os.path.getsize(os.path.join(fpath, f)) for f in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, f)))
    except OSError as err:
        LOGGER.warning("get_size(): Cannot reach folderpath for size check: %s\n%s", fpath, err)
        byte_size = None

    return byte_size


def check_mod_time(fpath):
    '''
    Compare modification times to ensure
    within mod max limit
    '''
    today = datetime.now()
    mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
    time_delta = today - mtime
    if time_delta.days > MOD_MAX:
        return False
    return True
    

def check_bp_status(fname):
    '''
    Look up filename in BP buckets
    to avoid multiple ingest of files
    '''
    
    query = ds3.HeadObjectRequest(BUCKET, fname)
    result = CLIENT.head_object(query)
    # Only return false if DOESNTEXIST is missing, eg file found
    if 'DOESNTEXIST' not in str(result.result):
        LOGGER.info("File %s found in Black Pearl bucket %s", fname, BUCKET)
        return False

    return True


def move_to_ingest_folder(new_path, file_list):
    '''
    File list to be formatted structured:
    'bfi/202402/filename1', 'bfi/202402/filename2'
    Runs while loop and moves upto 2TB folder size
    End when 2TB reached or files run out
    '''
    remove_list = []
    LOGGER.info("move_to_ingest_folder(): %s", INGEST_POINT)

    folder_size = get_size(INGEST_POINT)
    max_fill_size = UPLOAD_MAX - folder_size
    for fname in file_list:
        folderpath, file = os.path.split(fpath)
        fpath = os.path.join(STORAGE, fname)

        if not max_fill_size >= 0:
            LOGGER.info("move_to_ingest_folder(): Folder at capacity. Breaking move to ingest folder.")
            break

        file_size = get_size(fpath)
        max_fill_size -= file_size
        shutil.move(fpath, new_path)
        LOGGER.info("move_to_ingest_folder(): Moved file into new Ingest folder: %s - %s", new_path, file)
        remove_list.append(fname)

    for remove_file in remove_list:
        if remove_file in file_list:
            file_list.remove(remove_file)
    LOGGER.info("move_to_ingest_folder(): Revised file list in Black Pearl ingest folder: %s", file_list)

    return file_list


def delete_existing_proxy(folderpath, file_list):
    '''
    A proxy is being replaced so the
    existing version should be cleared
    '''
    if not file_list:
        LOGGER.info("No files being replaced at this time")
        return []
    for file in file_list:
        delete_path = os.path.join(folderpath, file)
        request = ds3.DeleteObjectRequest(BUCKET, delete_path)
        CLIENT.delete_object(request)
        sleep(20)
        success = check_bp_status(delete_path, BUCKET)
        if success:
            LOGGER.info("File %s deleted successfully", delete_path)
            file_list.remove(file)
        else:
            LOGGER.warning("Failed to delete file - %s", delete_path)
    return file_list


def main():
    '''
    Search through list of files in folder path
    Check for modification times newer than MOD_MAX
    Move upto 1TB of files (into folder structures)
    to INGEST_POINT and PUT to BP bucket. Folders not
    meeting 1TB max are fine to PUT.
    Move all contents of folder back to matching
    folder structures. Iterate to next folder path and
    check INGEST_POINT empty for next PUT.
    Initially running for mod days < 1000, will reduce
    to match script run time in future, approx 7 days.
    Need to consider removing duplicates and replacing
    with newer file versions...
    '''
    for key, value in START_FOLDERS.items():
        access_path = os.path.join(STORAGE, key)
        folder_list = os.listdir(access_path)
        folder_list = folder_list.sort()
        if folder_list[0] != value:
            LOGGER.warning('Problems with retrieved folder list for %s:\n%s', access_path, folder_list)
            continue
        file_list = []
        replace_list = []
        for folder in folder_list:
            files = os.listdir(os.path.join(access_path, folder))
            new_path = os.path.join(INGEST_POINT, folder),
            os.makedirs(new_path, mode=0o777, exist_ok=True)
            for file in files:
                if not check_mod_time(os.path.join(new_path, file)):
                    LOGGER.info("File %s mod time outside of maximum time %s", file, MOD_MAX)
                    continue
                if not check_bp_status(file):
                    file_list.append(f"{key}/{folder}/file")
                else:
                    file_list.append(f"{key}/{folder}/file")
                    replace_list.append(f"{key}/{folder}/file")
        new_path = os.path.join(INGEST_POINT, folder),
        os.makedirs(new_path, mode=0o777, exist_ok=True)
        success_list = delete_existing_proxy(f"{key}/{folder}/", replace_list)
        if success_list == []:
            LOGGER.info("All repeated files successfully deleted before replacement.")
        else:
            LOGGER.warning("Duplicate files remaining in Black Pearl: %s", replace_list)
        while file_list:
            empty_check = os.listdir(INGEST_POINT)
            if len(empty_check) != 0:
                LOGGER.warning("Exiting: Files in %s", INGEST_POINT)
                sys.exit()
            new_file_list = move_to_ingest_folder(new_path, file_list)
            job_list = put_dir(INGEST_POINT, BUCKET)
            LOGGER.info("PUT folder confirmation: %s", job_list)
            LOGGER.info("PUT items:\n%s", file_list)
            for entry in file_list:
                if entry in new_file_list:
                    continue
                shutil.move(os.path.join(INGEST_POINT, entry), os.path.join(STORAGE, entry))
                if os.path.isfile(os.path.join(STORAGE, entry)):
                    LOGGER.info("Moved ingested file back to QNAP-11 storage path.")
                else:
                    LOGGER.warning("Failed to move file back to STORAGE path. Script exiting!")
                    sys.exit()
            file_list = new_file_list
            sleep(3600)


def put_dir(directory_pth):
    '''
    Add the directory to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    JMW - Need to understand how to PUT so folder structures
    are maintained, eg bfi/202402/<file>
    '''
    try:
        put_job_ids = HELPER.put_all_objects_in_directory(source_dir=directory_pth, bucket=BUCKET, objects_per_bp_job=5000, max_threads=3)
    except Exception as err:
        LOGGER.error('Exception: %s', err)
        print('Exception: %s', err)
    LOGGER.info("PUT COMPLETE - JOB ID retrieved: %s", put_job_ids)
    job_list = []
    for job_id in put_job_ids:
        job_list.append(job_id)
    return job_list


if __name__ == "__main__":
    main()
