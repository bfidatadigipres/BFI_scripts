#!/usr/bin/ python3c

'''
Script to frequently back up
MP4 and JPG proxy files created as part
of autoingest to DPI.

Targeting bfi/ subfolders only at this time:
- Checks for modifications in last MOD_MAX days
- Checks if file already in BP bucket
- If yes compares local/remote MD5
- If don't match pushes through to BP bucket along iwth
  items not found there already (new items)
- Deletes out of date duplicates (replace_list)
  Sleep for 30 mins before PUT of same files
- Pushes all replacement/new items to BP bucket

2024
'''

# Global imports
import os
import sys
import shutil
import logging
from datetime import datetime
from time import sleep
from ds3 import ds3, ds3Helpers
from typing import Final, Optional

# Local imports
import bp_utils
sys.path.append(os.environ['CODE'])
import utils

# Global vars
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.environ['CONTROL_JSON']
STORAGE = os.environ['TRANSCODING']
INGEST_POINT = os.path.join(STORAGE, 'mp4_proxy_backup_ingest_bfi/')
MOD_MAX = 30
UPLOAD_MAX = 1099511627776
BUCKET = 'Access_Renditions_backup'

# Setup logging
LOGGER = logging.getLogger('black_pearl_access_rendition_modified_backup')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'black_pearl_access_rendition_modified_backup.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

START_FOLDERS: Final = {
    'bfi': '201605'
}


def check_mod_time(fpath: str) -> bool:
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


def move_to_ingest_folder(new_path: str, file_list: list[str]):
    '''
    File list to be formatted structured:
    'bfi/202402/filename1', 'bfi/202402/filename2'
    Runs while loop and moves upto 1TB folder size
    '''
    ingest_list: list[str] = []
    LOGGER.info("move_to_ingest_folder(): %s", INGEST_POINT)

    folder_size = utils.get_size(INGEST_POINT)
    if folder_size is None:
        folder_size = 0
    max_fill_size  = UPLOAD_MAX - folder_size

    for fname in file_list:
        fpath: str = os.path.join(STORAGE, fname)
        if not max_fill_size >= 0:
            LOGGER.info("move_to_ingest_folder(): Folder at capacity. Breaking move to ingest folder.")
            break

        file_size = utils.get_size(fpath)
        if file_size is None:
            file_size = 0
        max_fill_size -= file_size
        print(f"Moving file {fname} to {new_path}")
        shutil.move(fpath, new_path)
        ingest_list.append(fname)
    LOGGER.info("move_to_ingest_folder(): Ingest list: %s", ingest_list)

    return ingest_list


def delete_existing_proxy(file_list: list[str]) -> list[str]:
    '''
    A proxy is being replaced so the
    existing version should be cleared
    '''
    if not file_list:
        LOGGER.info("No files being replaced at this time")
        return []
    for file in file_list:
        confirmed: Optional[ds3.DeleteObjectReponse] = bp_utils.delete_black_pearl_object(file, None, BUCKET)
        if confirmed:
            sleep(10)
            success: bool = bp_utils.check_no_bp_status(file, [BUCKET])
            if success is False:
                LOGGER.info("File %s deleted successfully", file)
                file_list.remove(file)
            if success is True:
                LOGGER.warning("Failed to delete file - %s", file)
        else:
            LOGGER.warning("Failed to delete asset: %s", file)

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
        access_path: str = os.path.join(STORAGE, key)
        LOGGER.info("** Access path selected: %s", access_path)
        folder_list = os.listdir(access_path)
        folder_list.sort()
        print(folder_list)
        if folder_list[0] != value:
            LOGGER.warning('SKIPPING: First retrieved folder is not %s:\n%s', value, folder_list[0])
            continue

        # Iterate folders building lists
        file_list: list[str] = []
        replace_list: list[str] = []
        for folder in folder_list:
            if not utils.check_control('black_pearl'):
                LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
                sys.exit('Script run prevented by downtime_control.json. Script exiting.')

            LOGGER.info("** Working with access path date folder: %s", folder)
            new_path = os.path.join(INGEST_POINT, key, folder)
            os.makedirs(new_path, mode=0o777, exist_ok=True)

            files: list[str] = os.listdir(os.path.join(access_path, folder))
            for file in files:
                old_fpath = os.path.join(access_path, folder, file)
                if file.endswith(('.mp4', '.MP4')):
                    continue
                if check_mod_time(old_fpath) is False:
                    continue
                if bp_utils.check_no_bp_status(f"{key}/{folder}/{file}", [BUCKET]) is True:
                    LOGGER.info("New item to write to BP: %s/%s/%s", key, folder, file)
                    print(f"New item to write to BP: {key}/{folder}/{file}")
                    file_list.append(f"{key}/{folder}/{file}")
                else:
                    print(f"Existing item to overwrite: {key}/{folder}/{file}")
                    LOGGER.info("Existing item to delete and write to BP: %s/%s/%s", key, folder, file)
                    replace_list.append(f"{key}/{folder}/{file}")

            # Checking for matching MD5 within replace list
            print(len(replace_list))
            remove_list: list = []
            if replace_list:
                for item in replace_list:
                    local_md5 = utils.create_md5_65536(os.path.join(STORAGE, item))
                    bp_md5 = bp_utils.get_bp_md5(item, BUCKET)
                    print(f"Local {local_md5} - {item}")
                    print(f"Remote {bp_md5} - {item}")
                    if local_md5 == bp_md5:
                        print(f"Removing from list MD5 match: {item}")
                        LOGGER.info("Skipping item %s as MD5 files match:\n%s - Local MD5\n%s - Remote MD5", item, local_md5, bp_md5)
                        remove_list.append(item)
                    elif bp_md5 is None:
                        LOGGER.info("MD5 for item was not found in Black Pearl. File in incorrect list 'replace_list'")
                    else:
                        LOGGER.info("MD5s do not match, queue for deletion:\n%s - Local MD4\n%s - Remote MD5", local_md5, bp_md5)
                        print(f"MD5 do not match - queued for deletion: {item}")

            for item in remove_list:
                replace_list.remove(item)

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
                sleep(1800)

            # While files remaining in list, move to ingest folder, PUT, and remove again
            for rep_item in replace_list:
                file_list.append(rep_item)
            while file_list:
                if not utils.check_control('black_pearl'):
                    LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
                    sys.exit('Script run prevented by downtime_control.json. Script exiting.')
                empty_check = [ x for x in os.listdir(INGEST_POINT) if os.path.isfile(os.path.join(INGEST_POINT, x)) ]
                if len(empty_check) != 0:
                    LOGGER.warning("Exiting: Files found that weren't moved from ingest point previous run: %s", INGEST_POINT)
                    sys.exit("See logs for exit reason")

                # Returns list of ingested items and PUTs to BP before moving ingest items back to original path
                ingest_list: list = []
                ingest_list = move_to_ingest_folder(new_path, file_list)
                LOGGER.info("** Moving new set of PUT items:\n%s", ingest_list)

                job_list: Optional[list[str]] = bp_utils.put_directory(INGEST_POINT, BUCKET)
                if not job_list:
                    LOGGER.warning("Exiting: Failed to PUT data to Black Pearl. Clean up work needed")
                    sys.exit("Failed to PUT data to BP. See logs")
                LOGGER.info("** PUT folder confirmation: %s", job_list)
                LOGGER.info("Moving files back to original qnap_access_renditions folders: %s", ingest_list)
                success = move_items_back(ingest_list)
                if success:
                    new_file_list: list[str] = []
                    set_ingest_list: set = set(ingest_list)
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


    LOGGER.info("====== BP Access Renditions back up script end ====================")


def move_items_back(ingest_list: list[str]) -> bool:
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

    empty_check: list[str] = [ x for x in os.listdir(INGEST_POINT) if os.path.isfile(os.path.join(INGEST_POINT, x)) ]
    if len(empty_check) != 0:
        return False
    else:
        return True


if __name__ == "__main__":
    main()
