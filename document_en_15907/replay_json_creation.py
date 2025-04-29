#!/usr/bin/env LANG=en_UK.UTF-8 /usr/local/bin/python3

'''
replay_json_creation.py

Script function:
1. Iterates watch folder looking for MKV files.
2. Extracts filename, and searches in CID API manifestations
   database for exact match in utb.content field
3. Where found (there may be multiple matches) extract
   part_of_reference and lref, time_code.start and end
   and add each records data to list of dicts
4. Convert each list into a JSON line, and add all to
   a JSON formatted file, saved as MKV filename.json
5. Move both the MKV and JSON to the BFI_replay folder.

2021
'''

import os
import sys
import json
import shutil
import logging
import datetime
import tenacity
from typing import Optional, Final, Any

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Global variables
WATCH_FOLDER = os.environ['BFI_REPLAY']
SPLIT_PATH = os.path.join(WATCH_FOLDER, 'split')
MULTI_SPLIT_PATH = os.path.join(WATCH_FOLDER, 'multiple_split')
NO_SPLIT_PATH = os.path.join(WATCH_FOLDER, 'no_split')
LOG_PATH = os.environ['LOG_PATH']
CID_API = os.environ['CID_API3']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]

# Setup logging
LOGGER = logging.getLogger('replay_json_creation')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'replay_json_creation.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


@tenacity.retry(stop=(tenacity.stop_after_delay(60) | tenacity.stop_after_attempt(10)))
def cid_retrieve(filename: str, search: str) -> Optional[tuple[dict[str, Any]]]:
    '''
    Receive filename and search in CID manifestations
    Return list of lists to main. Retry every minute for
    10 minutes in case of problem accessing CID API
    '''
    fields: list[str] = [
        'time_code.start',
        'time_code.end',
        'part_of_reference',
        'part_of_reference.lref'
    ]

    record = adlib.retrieve_record(CID_API, 'manifestations', search, '0', fields)[1]
    if not record:
        LOGGER.exception("cid_retrieve(): Unable to retrieve data for %s", filename)
        return None
    else:
        return record


def extract_prirefs(records: Optional[tuple[dict[str, Any]]]) -> Optional[list[str]]:
    '''
    Iterate returned CID hits for individual prirefs
    '''
    prirefs = []
    for rec in records:
        try:
            priref = adlib.retrieve_field_name(rec, 'priref')[0]
            prirefs.append(priref)
        except (KeyError, IndexError):
            pass

    return prirefs


def create_dictionary(records: Optional[tuple[dict[str, Any]]]) -> dict[str, str]:
    '''
    Extract data and list of dictionaries
    '''
    loop_dict = {}
    for rec in records:
        try:
            priref = adlib.retrieve_field_name(rec, 'priref')[0]
            print(priref)
        except (KeyError, IndexError):
            priref = ''
        try:
            ob_num = adlib.retrieve_field_name(rec, 'object_number')[0]
            print(ob_num)
        except (KeyError, IndexError):
            ob_num = ''
        try:
            parent_priref = adlib.retrieve_field_name(rec, 'part_of_reference.lref')[0]
        except (KeyError, IndexError):
            parent_priref = ''
        parent_rec = adlib.retrieve_record(CID_API, 'works', f'priref="{parent_priref}"', '1', ['object_number'])[1]
        if not parent_rec:
            continue
        try:
            parent_ob_num = adlib.retrieve_field_name(parent_rec, 'object_number')[0]
        except (KeyError, IndexError):
            parent_ob_num = ""
        # Check Part_of data present
        if (len(parent_ob_num) == 0 or len(parent_priref) == 0):
            continue
        try:
            time_code_start = adlib.retrieve_field_name(rec, 'time_code.start')[0]
            print(time_code_start)
        except (KeyError, IndexError) as err:
            time_code_start = ''
            print(err)
        try:
            time_code_end = adlib.retrieve_field_name(rec, 'time_code.end')[0]
            print(time_code_end)
        except (KeyError, IndexError) as err:
            time_code_end = ''
            print(err)
        # Check time code data present
        if (len(time_code_start) == 0 or len(time_code_end) == 0):
            continue

        loop_dict[f"{parent_ob_num}, {parent_priref}"] = f"{time_code_start}, {time_code_end}"

    return loop_dict


def main():
    '''
    Retrieve list of MKV names, match to utb in manifestation
    Return dictionary of dictionaries, containing part_of_ref
    and time_code start/end data for each matching manifestation.
    Pretty print to json file, named as MKV filename. Move to replay.
    '''

    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()
    if not utils.check_control('pause_scripts'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    LOGGER.info("------------- Replay JSON creation script START --------------")
    files = [x for x in os.listdir(WATCH_FOLDER) if x.startswith('N_') and x.endswith(('.mkv', '.MKV'))]
    if not len(files) > 0:
        LOGGER.info("No files located at this time, exiting.")
        LOGGER.info("------------- Replay JSON creation script END ----------------\n")
        sys.exit("No files located at this time, exiting.")
    LOGGER.info("Files located for time code extraction: %s", files)

    # Iterate each item in file list extracting timecode data
    for filename in files:
        LOGGER.info("------------- %s -------------", filename)
        filepath = os.path.join(WATCH_FOLDER, filename)

        if not os.path.exists(filepath):
            LOGGER.info("Skipping: File %s as it doesn't exist in watch folder", filename)
            continue

        local_logger(f"---- New file: {filename} ---------------")
        LOGGER.info("Retrieving Priref, Timecode and Part Of data from CID...")
        cid_data_all = cid_retrieve(filename, f"(utb.content='{filename}' WHEN utb.fieldname='bfi_replay_filename')")

        if not cid_data_all:
            local_logger(f"Problem retrieving data from CID, please check {filename} details.\n")
            LOGGER.warning("SKIPPING: No data retrieved from CID for file %s", filename)
            continue

        # Split workflow begins
        data = {}
        dct_data_compiled = {}
        prirefs = extract_prirefs(cid_data_all)

        for priref in prirefs:
            data = cid_retrieve(filename, f"priref='{priref}'")
            dct_data = create_dictionary(data)
            LOGGER.info("File %s priref: %s", filename, priref)
            for k, v in dct_data.items():
                if len(v) == 0:
                    continue
                dct_data_compiled[k] = v

        if len(dct_data_compiled) == 0:
            split = 'no_split'
        if len(dct_data_compiled) == 1:
            split = 'split'
        if len(dct_data_compiled) > 1:
            split = 'multiple_split'

        if split == 'no_split':
            local_logger("No splitting required for this file. Moving to no_split/ folder")
            LOGGER.info("No splitting required. Moving to no_split/ folder.")
            success = move_files(filepath, 'no_split')
            if success:
                local_logger("MKV Moved successfully.\n")
                LOGGER.info("Moved %s to no_split path successful", filename)
            else:
                local_logger("FAIL MKV MOVE: Leaving in place for repeat attempt next script run.\n")
                LOGGER.warning("%s - Failed to move to no_split/ folder. Retry next pass.", filename)
            continue

        local_logger("Making JSON file with CID data...")
        local_logger(dct_data_compiled)
        json_path = json_dump(dct_data_compiled, filename)
        LOGGER.info("Data retrieved from CID: %s", dct_data_compiled)

        if not os.path.exists(json_path):
            LOGGER.warning("SKIPPING: JSON path doesn't exist: %s", json_path)
            local_logger("FAIL: JSON file creation. Leaving MKV file in place for repeat attempt next script run.\n")
            continue

        LOGGER.info("New JSON file created: %s", json_path)
        json_size = os.path.getsize(json_path)
        if not json_size > 10:
            LOGGER.warning("SKIPPING MOVE: Data not written to JSON file correctly. Deleting JSON file for retry.")
            local_logger("FAIL: JSON file creation. Leaving MKV file in place for repeat attempt next script run.\n")
            os.remove(json_path)
            continue

        # Moving JSON file alongside when multiple items per tape?
        local_logger(f"JSON file created successfully, MKV and JSON being moved to {split}/ folder:")
        LOGGER.info("Moving %s to new BFI Replay %s path", filename, split)
        success = move_files(filepath, split)
        if success:
            local_logger("MKV Moved successfully.")
            LOGGER.info("Move successful.")
        else:
            local_logger(f"FAIL MKV MOVE: Manual move of MKV file required to {split}/ folder.")
            LOGGER.warning("%s - Failed to move to %s/ folder. Manual move required.", filename, split)

        LOGGER.info("Moving JSON file to new BFI Replay %s path", split)
        success2 = move_files(json_path, split)
        if success2:
            local_logger("JSON moved successfully.\n")
            LOGGER.info("JSON Move successful")
        else:
            LOGGER.warning("FAIL JSON MOVE: Failed to move JSON for file to %s/ folder.", split)
            local_logger(f"FAIL JSON MOVE: Manual move of JSON file required to {split}/ folder.\n")

    LOGGER.info("------------- Replay JSON creation script END ----------------\n")


def json_dump(data: str, filename: str) -> str:
    '''
    Split filename and make .json, dump text to json
    '''

    file = os.path.splitext(filename)[0]
    file_json = os.path.join(WATCH_FOLDER, f"{file}.json")
    try:
        with open(file_json, 'x') as new_file:
            new_file.close()
    except FileExistsError:
        return file_json

    with open(file_json, 'w') as json_file:
        json_file.write(json.dumps(data, indent=4))
        json_file.close()

    return file_json


def move_files(filepath: str, arg: str) -> bool:
    '''
    Move to split/no_split paths
    '''

    file = os.path.basename(filepath)

    if arg == 'no_split':
        new_path = os.path.join(NO_SPLIT_PATH, file)
    elif arg == 'split':
        new_path = os.path.join(SPLIT_PATH, file)
    elif arg == 'multiple_split':
        new_path = os.path.join(MULTI_SPLIT_PATH, file)
    LOGGER.info("move_files(): Moving %s to %s", file, new_path)

    try:
        shutil.move(filepath, new_path)
        return True
    except Exception as err:
        LOGGER.warning("move_files(): Move failed:\n%s", err)
        return False


def local_logger(data: str) -> None:
    '''
    Print local log to WATCH_FOLDER
    '''

    local_log = os.path.join(WATCH_FOLDER, 'BFI_replay_split_JSON_creation.log')
    now = datetime.datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')

    with open(local_log, 'a+') as log:
        log.write(f"{timestamp} -- {data}\n")
        log.close()


if __name__ == '__main__':
    main()
