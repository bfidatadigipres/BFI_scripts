#!/usr/bin/env LANG=en_UK.UTF-8 /usr/local/bin/python3

"""
Young Audience Content Fund rename and move to autoingest:
1. Iterate through files in Video Operations Completed/ folder
2. Where file endswith MXF or MOV extracts filename and searches in CID's
   `digital.acquired_filename` field for a wholename match (inc ext)
   a. Where found: extract object number from record and create N_123456_01of01
   (always 01of01 part whole) filename with correct file format extension
   b. Where not found: move file to 'CID_item_not_found' folder and append log.
   Script exits
3. Renames files and updates logs
4. Moves new file to Video_operations/finished/autoingest path

NOTE: Supports use of adlib_v3.py

2021
"""

import datetime
import logging

# Public packages
import os
import shutil
import sys

# Private packages
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Global path variables
YACF_PATH = os.environ["YACF_COMPLETE"]
YACF_NO_CID = os.path.join(YACF_PATH, "cid_item_not_found")
AUTOINGEST = os.environ["AUTOINGEST_YACF"]
LOG_PATH = os.environ["LOG_PATH"]
LOCAL_LOG = os.path.join(YACF_PATH, "YACF_renumbering.log")
CONTROL_JSON = os.path.join(LOG_PATH, "downtime_control.json")
CID_API = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("YACF_rename_move.log")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "YACF_rename_move.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# Global variables
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]
DATE_TIME = f"{TODAY_DATE} = {TODAY_TIME}"


def cid_retrieve(filename: str) -> tuple[str, str, str, str]:
    """
    Receive filename and search in CID items
    Return object number to main
    """
    search: str = f'digital.acquired_filename="{filename}"'
    record: str = adlib.retrieve_record(
        CID_API,
        "items",
        search,
        "0",
        ["priref", "object_number", "title", "title.article"],
    )[1]
    LOGGER.info("cid_retrieve(): Making CID query request with:\n %s", search)
    if not record:
        print(f"cid_retrieve(): Unable to retrieve data for {filename}")
        LOGGER.exception("cid_retrieve(): Unable to retrieve data for %s", filename)
        return None
    try:
        priref: str = adlib.retrieve_field_name(record[0], "priref")[0]
    except (KeyError, IndexError) as err:
        priref = ""
        LOGGER.warning("cid_retrieve(): Unable to access priref %s", err)
    try:
        ob_num: str = adlib.retrieve_field_name(record[0], "object_number")[0]
    except (KeyError, IndexError) as err:
        ob_num = ""
        LOGGER.warning("cid_retrieve(): Unable to access object_number: %s", err)
    try:
        title: str = adlib.retrieve_field_name(record[0], "title")[0]
    except (KeyError, IndexError) as err:
        title = ""
        LOGGER.warning("cid_retrieve(): Unable to access title: %s", err)
    try:
        title_article = adlib.retrieve_field_name(record[0], "title.article")[0]
    except (KeyError, IndexError) as err:
        title_article = ""
        LOGGER.warning("cid_retrieve(): Unable to access title article %s", err)

    return priref, ob_num, title, title_article


def main():
    """
    search in CID Item for digital.acquired_filename
    Retrieve object number and use to build new filename for YACF file
    Update local log for YACF monitoring
    Move file to autoingest path
    """
    LOGGER.info("=========== YACF script start ==========")
    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()
    if not utils.check_control("pause_scripts"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    for root, _, files in os.walk(YACF_PATH):
        for file in files:
            ### Check if file is in the correct path
            filepath = os.path.join(root, file)
            if not utils.check_storage(filepath):
                LOGGER.info(
                    f"The storage_control.json returned ‘False’ for path {DIGIOPS_PATH} Script is exiting"
                )
                sys.exit(
                    "Script run prevented by storage_control.json. Script exiting."
                )
            if "CID_item_not_found" in filepath:
                LOGGER.info(
                    "WRONG PATH: Skipping %s as item located in 'CID_item_not_found' folder",
                    file,
                )
                local_logger(
                    f"\nWRONG PATH: Skipping file {file}, as located in 'CID_item_not_found' folder"
                )
                continue
            if file.endswith((".MXF", ".mxf", ".MOV", ".mov")):
                LOGGER.info(
                    "----------------- New file found %s ----------------", file
                )
                LOGGER.info(
                    "Processing %s now. Looking in CID item records for filename", file
                )
                priref, ob_num, title, title_art = cid_retrieve(file)

                if len(ob_num) > 0:
                    LOGGER.info(
                        "CID item data retrieved - Priref: %s  Object_number: %s  Title: %s %s",
                        priref,
                        ob_num,
                        title_art,
                        title,
                    )
                    local_logger(
                        "\n------------------------- New file found ------------------------------"
                    )
                    local_logger(
                        f"Processing file: {file}. Retrieving CID item record data"
                    )
                    local_logger(
                        f"Data retrieved from CID Item:\nItem object number: {ob_num}\nTitle: {title_art} {title}"
                    )
                    local_logger(
                        f"** Renumbering file {file} with object number {ob_num}"
                    )
                    new_filepath, new_file = rename(filepath, ob_num)
                    if len(new_file) > 0:
                        local_logger(f"New filename generated: {new_file}")
                        local_logger(
                            f"File renumbered and filepath updated to: {new_filepath}"
                        )
                        success = move(new_filepath, "ingest")
                        if success:
                            local_logger(
                                f"File {new_file} relocated to Autoingest {DATE_TIME}"
                            )
                            local_logger(
                                "---------------- File process complete ----------------"
                            )
                        else:
                            LOGGER.warning(
                                "FILE %s DID NOT MOVE SUCCESSFULLY TO AUTOINGEST",
                                new_file,
                            )
                            local_logger(
                                f"ERROR MOVING FILE: Script could not move file to Autoingest. Please check file permissions. {DATE_TIME}"
                            )
                    else:
                        LOGGER.warning("Problem creating new number for %s", file)
                        local_logger(
                            f"ERROR RENAMING FILE: Problem found when attempting to rename file {file}"
                        )
                        local_logger(
                            "ERROR RENAMING FILE: Please check file has no permissions limitations, script will retry later"
                        )
                else:
                    LOGGER.info(
                        "File information not found in CID. Moving file to 'CID_item_not_found' folder"
                    )
                    local_logger(
                        f"\nNO CID DATA: File found {file} but no CID data retrieved"
                    )
                    local_logger(
                        "NO CID DATA: Moving file to 'CID_item_not_found' folder - please check filename and CID item record"
                    )
                    success2 = move(filepath, "fail")
                    if success2:
                        LOGGER.info("File moved successfully")
                        local_logger(
                            f"NO CID DATA: File {file} relocated to 'CID_item_not_found' folder {DATE_TIME}"
                        )
                    else:
                        LOGGER.warning(
                            "FILE %s DID NOT MOVE SUCCESSFULLY TO FOLDER", file
                        )
                        local_logger(
                            f"ERROR MOVING FILE: Script could not move file. Please check file permissions. Script will retry. {DATE_TIME}"
                        )
            else:
                LOGGER.info("Skipping. File found that is not MXF or MOV: %s", file)


def rename(filepath: str, ob_num: str) -> tuple[str, str]:
    """
    Receive original file path and rename filename
    based on object number, return new filepath, filename
    """
    new_filepath, new_filename = "", ""
    path, filename = os.path.split(filepath)
    ext: str = os.path.splitext(filename)
    new_name: str = ob_num.replace("-", "_")
    new_filename: str = f"{new_name}_01of01{ext[1]}"
    print(f"Renaming {filename} to {new_filename}")
    new_filepath = os.path.join(path, new_filename)

    try:
        os.rename(filepath, new_filepath)
    except OSError:
        LOGGER.warning("There was an error renaming %s to %s", filename, new_filename)

    return (new_filepath, new_filename)


def move(filepath: str, arg: str) -> bool:
    """
    Move existing filepaths to Autoingest
    """
    if os.path.exists(filepath) and "fail" in arg:
        print(f"move(): Moving {filepath} to {YACF_NO_CID}")
        try:
            shutil.move(filepath, YACF_NO_CID)
            return True
        except Exception as err:
            LOGGER.warning(
                "Error trying to move file %s to %s. Error: %s",
                filepath,
                YACF_NO_CID,
                err,
            )
            return False
    elif os.path.exists(filepath) and "ingest" in arg:
        print(f"move(): Moving {filepath} to {AUTOINGEST}")
        try:
            shutil.move(filepath, AUTOINGEST)
            return True
        except Exception:
            LOGGER.warning("Error trying to move file %s to %s", filepath, AUTOINGEST)
            return False
    else:
        return False


def local_logger(data: str) -> None:
    """
    Pretty printed log for human readable data
    Output local log data for Video Ops teams to monitor renaming process
    """
    if len(data) > 0:
        with open(LOCAL_LOG, "a+") as log:
            log.write(data + "\n")
            log.close()


if __name__ == "__main__":
    main()
