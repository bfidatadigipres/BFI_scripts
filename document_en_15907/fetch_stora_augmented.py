#!/usr/bin/env python3

"""
Fetch JSON from augmented EPG metadata API, for use by document_augmented_stora.py

main():
1. Check if any dictionary paths are absent from STORA path, if not prepeneds "NO_RECORDING_{channel}"
2. Once a day after shows have completed (12:10am), call the API for yesterday's metadata, using fetch():
3. If first fetch fails, fetch(): will retry three times pausing up to ten minutes between each.
4. When downloaded calls json_split(): to split the JSON for each channel into it's time slots
move():
5. Called by main(): compares the STORA generated recording foldernames against split JSON file
6. Move matching JSON time slot file into folder for same time slot. Copy to a second if two found.
folder_check():
7. Called by move(): looks for folders without .json files in. If found renames the csv to end '.stora'

2020
"""

import datetime
import errno
import json
import logging
# Python packages
import os
import shutil
import sys
import time
from typing import Any, Final, Optional

# ENV packages
import requests
import tenacity

# Global variables
STORAGE_PATH: Final = os.environ["STORA_PATH"]
LOG_PATH: Final = os.environ["LOG_PATH"]
CODE_PATH: Final = os.environ["CODE"]
STORA_CONTROL: Final = os.path.join(CODE_PATH, "stora_control.json")
UNMATCHED_JSON: Final = os.path.join(STORAGE_PATH, "unmatched_jsons/")
TODAY: Final = datetime.date.today()
YESTERDAY: Final = TODAY - datetime.timedelta(days=3)
YESTERDAY_CLEAN: Final = YESTERDAY.strftime("%Y-%m-%d")
START: Final = f"{YESTERDAY_CLEAN}T00:00:00"
END: Final = f"{YESTERDAY_CLEAN}T23:59:00"
# If a different date period needs targeting use:
# START = '2024-10-19T00:00:00'
# END = '2024-10-19T23:59:00'
DATE_PATH: Final = START[0:4] + "/" + START[5:7] + "/" + START[8:10]
PATH: Final = os.path.join(STORAGE_PATH, DATE_PATH)
dct = {}

# Setup logging
logger = logging.getLogger("fetch_stora_augmented")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "fetch_stora_augmented.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Setup Rest API
URL = os.environ["PATV_URL"]
HEADERS = {"accept": "application/json", "apikey": os.environ["PATV_KEY"]}

CHANNEL = {
    "bbconehd": os.environ["PA_BBCONE"],
    "bbctwohd": os.environ["PA_BBCTWO"],
    "bbcthree": os.environ["PA_BBCTHREE"],
    "bbcfourhd": os.environ["PA_BBCFOUR"],
    "bbcnewshd": os.environ["PA_BBCNEWS"],
    "cbbchd": os.environ["PA_CBBC"],
    "cbeebieshd": os.environ["PA_CBEEBIES"],
    "itv1": os.environ["PA_ITV1"],
    "itv2": os.environ["PA_ITV2"],
    "itv3": os.environ["PA_ITV3"],
    "itv4": os.environ["PA_ITV4"],
    "itvbe": os.environ["PA_ITVBE"],
    "channel4": os.environ["PA_CHANNEL4"],
    "more4": os.environ["PA_MORE4"],
    "e4": os.environ["PA_E4"],
    "film4": os.environ["PA_FILM4"],
    "five": os.environ["PA_FIVE"],
    "5star": os.environ["PA_5STAR"],
}


def check_control() -> None:
    """
    Check control JSON for downtime request
    """
    with open(STORA_CONTROL) as control:
        j = json.load(control)
        if not j["stora_qnap04"]:
            logger.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")


@tenacity.retry(wait=tenacity.wait_random(min=5, max=30))
def check_api(value: str) -> Optional[bool]:
    """
    Run standard check with today's date on supplied channelID
    """
    params = {"channelId": f"{value}", "start": START, "end": END, "aliases": "True"}
    req = requests.request("GET", URL, headers=HEADERS, params=params, timeout=120)
    print(req)
    if req.status_code == 200:
        return True
    logger.info("PATV API return status code: %s", req.status_code)
    raise tenacity.TryAgain


def make_path(channel: dict[str, str], item: str) -> None:
    """
    Test path exists, if not makes new directory 'NO_RECORDING_*'
    """
    try:
        print(f"make_path(): Making path for {PATH}/NO_RECORDING_{item}")
        os.makedirs(f"{PATH}/NO_RECORDING_{item}")
    except OSError as err:
        if err.errno != errno.EEXIST:
            logger.warning(
                "Unable to create new 'NO_RECORDING_' channel extension fors %s",
                channel,
            )
            raise


@tenacity.retry(wait=tenacity.wait_random(min=5, max=30))
def fetch(value) -> Optional[dict[str, str]]:
    """
    Retrieval of EPG metadata here
    """
    try:
        params = {
            "channelId": f"{value}",
            "start": START,
            "end": END,
            "aliases": "True",
        }
        logger.info("fetch(): %s", params)
        req = requests.request("GET", URL, headers=HEADERS, params=params, timeout=120)
        jdct = json.loads(req.text)
    except Exception as err:
        print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
        logger.critical("**** PROBLEM: Cannot fetch EPG metadata. **** \n%s", err)
        jdct = None

    if "'message': 'Service error'" in str(jdct):
        logger.warning("Service is unreachable, pausing and retrying")
        raise tenacity.TryAgain
    if jdct is None:
        logger.warning("Failed to download EPG schedules, see log error.")
        raise tenacity.TryAgain
    return jdct


def json_split(jdct: dict[str, Any], key: str) -> None:
    """
    Splits dct into subdicts based on 'item' and dateTime being present
    """
    dump_to = os.path.join(PATH, key)
    if "item" not in jdct:
        return None
    for subdct in jdct["item"]:
        if "dateTime" in subdct:
            try:
                # Outputs split files in subdct when dateTime present to storage_path/dump_path/
                dt_item = subdct["dateTime"]
                fname = os.path.join(dump_to, f"info_{dt_item}.json")
                with open(fname, "w") as f:
                    json.dump({"item": [subdct]}, f, indent=4)
            except Exception as e:
                print(
                    f"json_split(): ** WARNING: Splitting script has failed to split and output to {fname} {e}"
                )
                logger.warning(
                    "** WARNING: Splitting script has failed to output to %s %s",
                    dump_to,
                    e,
                )
            else:
                logger.info(
                    "Splitting has been successful. Saving to top folder %s", dump_to
                )


def main() -> None:
    """
    Checks if all channel folders exist in storage_path
    Populates channel folders that do with cut up schedules
    Matches to programme folders where possible
    """
    check_control()
    logger.info(
        "========== Fetch augmented metadata script STARTED ==============================================="
    )

    fails = 0
    for item in CHANNEL.keys():
        item_path = os.path.join(PATH, item)
        print(item_path)
        if not os.path.exists(item_path):
            fails += 1
            make_path(CHANNEL, item)
            logger.warning(
                " *************** PATH CREATION -- NO_RECORDING_%s -- AS PATH IS ABSENT ***************** ",
                item,
            )
            print("Updating new path to channel dictionary")
            new_channel = f"NO_RECORDING_{item}"
            old_channel = item
            # New block to manage up to 3 missing channels
            if fails == 1:
                channel1 = dict(CHANNEL)
                channel1[new_channel] = channel1.pop(old_channel)
            if fails == 2:
                channel2 = dict(channel1)
                channel2[new_channel] = channel2.pop(old_channel)
            if fails == 3:
                channel3 = dict(channel2)
                channel3[new_channel] = channel3.pop(old_channel)
            if fails == 4:
                channel4 = dict(channel3)
                channel4[new_channel] = channel4.pop(old_channel)

    print(fails)
    # If metadata cannot be retrieved the script exits
    logger.info(
        "Requests will now attempt to retrieve the EPG channel metadata from start=%s to end=%s",
        START,
        END,
    )

    # New block to manage upto 3 missing channels, else exit
    if fails == 0:
        print(f"NO channels missing, using channel dictionary: {CHANNEL}")
        for key, value in CHANNEL.items():
            result = check_api(value)
            if not result:
                logger.warning(
                    "Unable to establish contact with PATV API for channel %s. Script exitings",
                    key,
                )
                sys.exit()
            jdct = fetch(value)
            retrieve_dct_data(key, value, jdct)
        for item in CHANNEL.keys():
            path_move = os.path.join(STORAGE_PATH, DATE_PATH, item)
            logger.info("Path for move actions: %s", path_move)
            move(path_move, item)
            folder_check(path_move)
    elif fails == 1:
        print(f"One channel missing, using channel1 dictionary: {channel1}")
        for key, value in channel1.items():
            result = check_api(value)
            if not result:
                logger.warning(
                    "Unable to establish contact with PATV API for channel %s. Script exitings",
                    key,
                )
                sys.exit()
            jdct = fetch(value)
            retrieve_dct_data(key, value, jdct)
        for item in channel1.keys():
            path_move = os.path.join(STORAGE_PATH, DATE_PATH, item)
            logger.info("Path for move actions: %s", path_move)
            move(path_move, item)
            folder_check(path_move)
    elif fails == 2:
        print(f"Two channels missing, using channel2 dictionary: {channel2}")
        for key, value in channel2.items():
            result = check_api(value)
            if not result:
                logger.warning(
                    "Unable to establish contact with PATV API for channel %s. Script exitings",
                    key,
                )
                sys.exit()
            jdct = fetch(value)
            retrieve_dct_data(key, value, jdct)
        for item in channel2.keys():
            path_move = os.path.join(STORAGE_PATH, DATE_PATH, item)
            logger.info("Path for move actions: %s", path_move)
            move(path_move, item)
            folder_check(path_move)
    elif fails == 3:
        print(f"Three channels missing, using channel3 dictionary: {channel3}")
        for key, value in channel3.items():
            result = check_api(value)
            if not result:
                logger.warning(
                    "Unable to establish contact with PATV API for channel %s. Script exitings",
                    key,
                )
                sys.exit()
            jdct = fetch(value)
            retrieve_dct_data(key, value, jdct)
        for item in channel3.keys():
            path_move = os.path.join(STORAGE_PATH, DATE_PATH, item)
            logger.info("Path for move actions: %s", path_move)
            move(path_move, item)
            folder_check(path_move)
    elif fails == 4:
        print(f"Four channels missing, using channel4 dictionary: {channel4}")
        for key, value in channel4.items():
            result = check_api(value)
            if not result:
                logger.warning(
                    "Unable to establish contact with PATV API for channel %s. Script exitings",
                    key,
                )
                sys.exit()
            jdct = fetch(value)
            retrieve_dct_data(key, value, jdct)
        for item in channel4.keys():
            path_move = os.path.join(STORAGE_PATH, DATE_PATH, item)
            logger.info("Path for move actions: %s", path_move)
            move(path_move, item)
            folder_check(path_move)
    else:
        logger.critical("Too many channels missing, script exiting.")
        sys.exit("Exiting because too many channels are absent - manual help needed!")

    logger.info(
        "========== Fetch augmented metadata script ENDED ================================================"
    )


def retrieve_dct_data(key: str, value: str, jdct=None) -> None:
    """
    Check if DCT data is None, if not instigate json_split
    """
    if jdct is None:
        logger.warning(
            "FAILED: First attempt to retrieve metadata. Second attempt will be made in 5 minutes."
        )
        time.sleep(300)
        jdct = fetch(value)
        if jdct is None:
            logger.warning(
                "FAILED: Second attempt to retrieve metadata. Third attempt will be made in 10 minutes."
            )
            time.sleep(600)
            jdct = fetch(value)
            if jdct is None:
                logger.critical(
                    "*** FAILED: Third attempt to retrieve metadata from EPG website. Script exiting ***"
                )
                sys.exit()
            else:
                logger.info(
                    "EPG metadata successfully retrieved. Starting split of JSON files"
                )
                json_split(jdct, key)
        else:
            logger.info(
                "EPG metadata successfully retrieved. Starting split of JSON files"
            )
            json_split(jdct, key)
    else:
        logger.info("EPG metadata successfully retrieved. Starting split of JSON files")
        json_split(jdct, key)


def move(path_move: str, item: str) -> None:
    """
    Handles move of JSON files
    into correct paths with
    matching datetime in dir name
    """
    file_trim = {}
    for file in os.scandir(path_move):
        if file.path.endswith(".json"):
            filename = os.path.basename(file)
            trim = filename[16:21].replace(":", "-")
            file_trim.update({trim: path_move + "/" + filename})
        else:
            print(f"move(): Skipping {file} as file is not JSON: {path_move}")
            logger.info(
                "Skipping file=%s as file is not a JSON: path=%s", file, path_move
            )

    # Look for JSON time matching programme folders (1 or 2, more critical)
    directory_list = os.listdir(path_move)
    for key, value in file_trim.items():
        matches = [x for x in directory_list if x.startswith(key)]
        if len(matches) == 0:
            continue
        elif len(matches) == 1:
            item_path = os.path.join(path_move, matches[0])
            logger.info("MATCH! File %s and directory %s match", value, item_path)
            print(
                f"move(): MATCH! File {value} -- trim {key} with {matches[0]} -- Moving file now"
            )
            try:
                shutil.move(value, item_path)
                logger.info("File moved successfully")
            except Exception as err:
                logger.warning(
                    (
                        "** WARNING: Unable to move file %s to folder %s",
                        value,
                        item_path,
                    ),
                    err,
                )
                print(
                    f"move(): Unable to move file {value} to folder {item_path} {err}"
                )
        elif len(matches) == 2:
            # First match
            item_path = os.path.join(path_move, matches[0])
            logger.info(
                "MATCHED TWICE! File %s and directory %s match", value, item_path
            )
            print(
                f"move(): MATCHED TWICE! File {value} -- trim {key} with {matches[0]} -- Copying JSON file to path now"
            )
            try:
                shutil.copy(value, item_path)
                logger.info("File moved successfully")
            except Exception as err:
                logger.warning(
                    (
                        "** WARNING: Unable to move file %s to folder %s",
                        value,
                        item_path,
                    ),
                    err,
                )
                print(
                    f"move(): Unable to move file {value} to folder {item_path} {err}"
                )
            # Second match
            item_path = os.path.join(path_move, matches[1])
            logger.info(
                "MATCHED TWICE! File %s and directory %s match", value, item_path
            )
            print(
                f"move(): MATCHED TWICE! File {value} -- trim {key} with {matches[1]} -- Moving file now"
            )
            try:
                shutil.move(value, item_path)
                logger.info("File moved successfully")
            except Exception as err:
                logger.warning(
                    (
                        "** WARNING: Unable to move file %s to folder %s",
                        value,
                        item_path,
                    ),
                    err,
                )
                print(
                    f"move(): Unable to move file {value} to folder {item_path} {err}"
                )
        else:
            logger.critical(
                "ERROR! More than two programmes have the same starting date in path: %s\n%s",
                path_move,
                matches,
            )
            continue

    # Remove json files not in programme folder and move to unmatched_jsons folder
    for file in os.scandir(path_move):
        filepath = os.path.join(path_move, file)
        if os.path.isfile(filepath):
            if filepath.endswith(".json"):
                filename = os.path.basename(filepath)
                print(f"move(): {filename} is not a directory")
                logger.info("%s path is not a directory.", filename)
                make_new_path = os.path.join(UNMATCHED_JSON, DATE_PATH, item)
                print(f"Make new path mkdirs: {make_new_path}")
                try:
                    os.makedirs(make_new_path, exist_ok=True)
                    print(f"Directory created successfully {make_new_path}")
                except OSError as error:
                    print("Unable to mkdir, probably already exists")
                    logger.warning(
                        "Make directory failed, path probably already exists %s", error
                    )
                try:
                    new_path = os.path.join(UNMATCHED_JSON, DATE_PATH, item, filename)
                    print(f"Moving {filepath} to new location at {new_path}")
                    logger.info(
                        "move(): Moving unmatched JSON from %s to new path %s",
                        filepath,
                        new_path,
                    )
                    shutil.move(filepath, new_path)
                except Exception as err:
                    logger.warning(
                        "move(): Unable to move %s to unmatched_json: %s", filepath, err
                    )


def folder_check(path_move: str) -> None:
    """
    Where folder missing .JSON
    rename info.csv to info.csv.stora
    """
    print(f"folder_check(): Searching for folders without any .json in {path_move}")
    logger.info(
        "folder_check(): Searching for folders without any .json in %s", path_move
    )
    for directory in os.scandir(path_move):
        dir_path = os.path.join(path_move, directory)
        if not os.path.isdir(dir_path):
            logger.info("Skipping, not a folder: %s", dir_path)
            continue
        if any(file.endswith(".json") for file in os.listdir(dir_path)):
            logger.info("Skipping path, as it contains a json: %s", dir_path)
        else:
            for file in os.listdir(dir_path):
                if file.startswith("info.csv"):
                    logger.info("folder_check(): CSV file to be renamed: %s", file)
                    old_name = os.path.join(dir_path, file)
                    filename = file[:8] + ".stora"
                    filename_new = os.path.join(dir_path, filename)
                    logger.info(
                        "**** folder_check(): Renaming <%s> to <%s> ****",
                        old_name,
                        filename_new,
                    )
                    os.rename(old_name, filename_new)


if __name__ == "__main__":
    main()
