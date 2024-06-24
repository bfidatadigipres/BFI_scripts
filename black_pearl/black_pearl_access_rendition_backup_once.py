#!/usr/bin/ python3

'''
Script to frequently back up
MP4 and JPG proxy files created as part
of autoingest to DPI.

Initially this script is to be
run with an open modification time but
later to be set to a given amount of
days that matches script run frequency.

Joanna White
2024
'''

# Public imports
import os
import sys
import json
import shutil
import logging
from datetime import datetime
from time import sleep
from ds3 import ds3, ds3Helpers

# Local imports
import bp_utils
sys.path.append(os.environ['CODE'])
import utils

# Global vars
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.environ['CONTROL_JSON']
STORAGE = os.environ['TRANSCODING']
INGEST_POINT = os.path.join(STORAGE, 'mp4_proxy_backup_ingest/')
MOD_MAX = 2000 # Modification time restriction
UPLOAD_MAX = 1099511627776 # 1TB max
BUCKET = 'Access_Renditions_backup'

# Setup logging
LOGGER = logging.getLogger('black_pearl_access_rendition_backup')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'black_pearl_access_rendition_backup.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

START_FOLDERS = {
#    'bfi': '201605'
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

    # DOESNTEXIST means file not found
    if 'DOESNTEXIST' in str(result.result):
        return False
    # File found
    return True


def move_to_ingest_folder(new_path, file_list):
    '''
    File list to be formatted structured:
    'bfi/202402/filename1', 'bfi/202402/filename2'
    Runs while loop and moves upto 1TB folder size
    '''
    ingest_list = []
    LOGGER.info("move_to_ingest_folder(): %s", INGEST_POINT)

    folder_size = get_size(INGEST_POINT)
    max_fill_size = UPLOAD_MAX - folder_size

    for fname in file_list:
        fpath = os.path.join(STORAGE, fname)
        if not max_fill_size >= 0:
            LOGGER.info("move_to_ingest_folder(): Folder at capacity. Breaking move to ingest folder.")
            break

        file_size = get_size(fpath)
        max_fill_size -= file_size
        print(f"Moving file {fname} to {new_path}")
        shutil.move(fpath, new_path)
        ingest_list.append(fname)
    LOGGER.info("move_to_ingest_folder(): Ingest list: %s", ingest_list)

    return ingest_list


def delete_existing_proxy(file_list):
    '''
    A proxy is being replaced so the
    existing version should be cleared
    '''
    if not file_list:
        LOGGER.info("No files being replaced at this time")
        return []
    for file in file_list:
        request = ds3.DeleteObjectRequest(BUCKET, file)
        CLIENT.delete_object(request)
        sleep(10)
        success = check_bp_status(file)
        if success is False:
            LOGGER.info("File %s deleted successfully", file)
            file_list.remove(file)
        if success is True:
            LOGGER.warning("Failed to delete file - %s", file)
    return file_list


def main():
    '''
    Search through list of files in folder path
    Check for modification times newer than MOD_MAX
    Move to INGEST_POINT and PUT to BP bucket.
    Move all contents of folder back to matching
    folder structures. Iterate to next folder path and
    check INGEST_POINT empty for next PUT.
    '''

    LOGGER.info("====== BP Access Renditions back up script start ==================")
    for key, value in START_FOLDERS.items():
        access_path = os.path.join(STORAGE, key)
        LOGGER.info("** Access path selected: %s", access_path)
        folder_list = os.listdir(access_path)
        folder_list.sort()
        if folder_list[0] != value:
            LOGGER.warning('SKIPPING: First retrieved folder is not %s:\n%s', value, folder_list[0])
            continue

        # Iterate folders building lists
        file_list = []
        replace_list = []
        for folder in folder_list:
            check_control()

            LOGGER.info("** Working with access path date folder: %s", folder)
            new_path = os.path.join(INGEST_POINT, key, folder)
            os.makedirs(new_path, mode=0o777, exist_ok=True)
            LOGGER.info("Created new ingest path: %s", new_path)

            files = os.listdir(os.path.join(access_path, folder))
            LOGGER.info("Starting batch ingest of target files in date folder - modified within last %s days", MOD_MAX)
            for file in files:
                old_fpath = os.path.join(access_path, folder, file)
                if check_mod_time(old_fpath) is False:
                     LOGGER.info("File %s mod time outside of maximum days allowed for upload: %s", file, MOD_MAX)
                     continue
                if check_bp_status(f"{key}/{folder}/{file}") is False:
                    file_list.append(f"{key}/{folder}/{file}")
                else:
                    print(f"Already in Black Pearl: {file}")
                    local_md5 = utils.create_md5_65536(os.path.join(access_path, folder, file))
                    bp_md5 = bp_utils.get_bp_md5(f"{key}/{folder}/{file}", BUCKET)
                    if local_md5 != bp_md5:
                        print(f"MD5 mismatch between local and BP: {file}")
                        print(f"Local {local_md5} - {file}")
                        print(f"Remote {bp_md5} - {file}")
                        LOGGER.info("Overwriting item %s as MD5 files don't match:\n%s - Local MD5\n%s - Remote MD5", file, local_md5, bp_md5)
                        file_list.append(f"{key}/{folder}/{file}")
                        replace_list.append(f"{key}/{folder}/{file}")

            # Delete existing versions if being replaced
            if len(replace_list) > 0:
                # Delete existing versions if being replaced
                LOGGER.info("** Replacement files needed, original proxy files for deletion:\n%s", replace_list)
                print(len(replace_list))
                success_list = delete_existing_proxy(replace_list)
                if len(success_list) == 0:
                    LOGGER.info("All repeated files successfully deleted before replacement.")
                else:
                    LOGGER.warning("Duplicate files remaining in Black Pearl - removing from replace_list to avoid duplicate writes: %s", success_list)
                    for fail_item in success_list:
                        replace_list.remove(fail_item)

            # While files remaining in list, move to ingest folder, PUT, and remove again
            while file_list:
                check_control()
                empty_check = [ x for x in os.listdir(INGEST_POINT) if os.path.isfile(os.path.join(INGEST_POINT, x)) ]
                if len(empty_check) != 0:
                    LOGGER.warning("Exiting: Files found that weren't moved from ingest point previous run: %s", INGEST_POINT)
                    sys.exit("See logs for exit reason")

                # Returns list of ingested items and PUTs to BP before moving ingest items back to original path
                ingest_list = []
                ingest_list = move_to_ingest_folder(new_path, file_list)
                LOGGER.info("** Moving new set of PUT items:\n%s", ingest_list)

                job_list = put_dir(INGEST_POINT)
                if job_list:
                    LOGGER.info("** PUT folder confirmation: %s", job_list)
                    LOGGER.info("Moving files back to original qnap_access_renditions folders: %s", ingest_list)
                    success = move_items_back(ingest_list)
                else:
                    LOGGER.warning("Exiting: Failed to PUT data to Black Pearl. Clean up work needed")
                    sys.exit("Failed to PUT data to BP. See logs")
                if success:
                    new_file_list = []
                    set_ingest_list = set(ingest_list)
                    new_file_list = [ x for x in file_list if x not in set_ingest_list ]
                    LOGGER.info("Files successfully moved back to original path.\n")
                    if len(file_list) != (len(ingest_list) + len(new_file_list)):
                        LOGGER.info("Inbalance in list following set removal. Exiting")
                        sys.exit("Inbalance in lists for ingest folder/file lists")
                    file_list = new_file_list
                else:
                    LOGGER.warning("Problem moving files from ingest list:\n%s", ingest_list)
                    set_ingest_list = set(ingest_list)
                    files_stuck = [x for x in set_ingest_list if x not in os.listdir(new_path) ]
                    LOGGER.warning("Files that are stuck in folder:\n%s", files_stuck)
                    sys.exit(f"Please manually move files back to QNAP-11:\n{files_stuck}")

                # Sleep between 1TB PUTs
                LOGGER.info("Sleep 4hrs")
                sleep(14400)

    LOGGER.info("====== BP Access Renditions back up script end ====================")


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


def move_items_back(ingest_list):
    '''
    Receive PUT file list and move items back after
    confirmation of job PUT okay
    '''

    if len(ingest_list) == 0:
        sys.exit()

    for entry in ingest_list:
        print(f"Move: {os.path.join(INGEST_POINT, entry)} - to - {os.path.join(STORAGE, entry)}")
        LOGGER.info("Moving %s to original path %s", os.path.join(INGEST_POINT, entry), os.path.join(STORAGE, entry))
        try:
            shutil.move(os.path.join(INGEST_POINT, entry), os.path.join(STORAGE, entry))
        except Exception as err:
            print(err)
        if not os.path.isfile(os.path.join(STORAGE, entry)):
            LOGGER.warning("Failed to move file back to STORAGE path. Script exiting!")

    empty_check = [ x for x in os.listdir(INGEST_POINT) if os.path.isfile(os.path.join(INGEST_POINT, x)) ]
    if len(empty_check) != 0:
        return False
    else:
        return True


if __name__ == "__main__":
    main()

