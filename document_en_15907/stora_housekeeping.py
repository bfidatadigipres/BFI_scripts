#!/usr/bin/env python3

"""
Script to handle clean up of the completed folders in STORA_PATH/*YEAR*
*** Must run from stora_housekeeping_start.sh which generates current file list ***
1. Extract date range for one week, ending day before yesterday and only target these paths
2. Examine each folder for presence of a file with .PROBLEM in name
3. Where found the folder is renamed prefixed "PROBLEM_" where it doesn't already exist
4. Examine each folder for presence of file 'stream.mpeg2.ts', where found skipped
   Else, where not found the folder assumed completed: CID record created and .ts file moved
   This folder can me moved to ARCHIVE_PATH
5. Clean up of folders at day, month (if last day of month) and year level (if last day of year).

Python 3.7 +
2021
"""

import datetime
import itertools
import logging
import os
import shutil
import sys

sys.path.append(os.environ["CODE"])
import utils

# Global paths
STORA_PATH = os.environ["STORA_PATH"]
ARCHIVE_PATH = os.path.join(STORA_PATH, "completed/")
TEXT_PATH = os.path.join(os.environ["CODE_BFI"], "document_en_15907/dump_text.txt")
LOG_PATH = os.environ["LOG_PATH"]

# Setup logging
logger = logging.getLogger("stora_housekeeping")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "stora_housekeeping.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Date variables for use in API calls (new year safe)
TODAY_DATE = datetime.date.today()
TODAY = str(TODAY_DATE)
YESTERDAY_DATE = TODAY_DATE - datetime.timedelta(days=7)
YESTERDAY = YESTERDAY_DATE.strftime("%Y-%m-%d")
MONTH = f"{YESTERDAY[0:4]}/{YESTERDAY[5:7]}"
STORA_PATH_MONTH = os.path.join(STORA_PATH, MONTH)
STORA_PATH_YEAR = os.path.join(STORA_PATH, MONTH[0:4])
# Alternative paths to clear backlog
# STORA_PATH_MONTH = os.path.join(STORA_PATH, '2023/02')
# STORA_PATH_YEAR = os.path.join(STORA_PATH, '2023')


def clear_folders(path):
    """
    Remove root folders that contain no directories or files
    """
    for root, dirs, files in os.walk(path):
        if not (files or dirs):
            print(f"*** Folder empty {root}: REMOVE ***")
            logger.info("*** FOLDER IS EMPTY: %s -- DELETING FOLDER ***", root)
            os.rmdir(root)
        else:
            logger.info("SKIPPING FOLDER %s -- THIS FOLDER IS NOT EMPTY", root)
            print(f"FOLDER {root} NOT EMPTY - this will not be deleted")


def clear_limited_folders(path):
    """
    Remove empty folders at one depth only
    protecting new folders ahead of recordings
    """
    folders = [x for x in os.listdir(path) if os.path.isdir(os.path.join(path, x))]
    for folder in folders:
        fpath = os.path.join(path, folder)
        if len(os.listdir(fpath)) == 0:
            print(f"*** Folder empty {folder}: REMOVE ***")
            logger.info("*** FOLDER IS EMPTY: %s -- DELETING FOLDER ***", fpath)
            os.rmdir(fpath)
        else:
            logger.info("SKIPPING FOLDER %s -- THIS FOLDER IS NOT EMPTY", fpath)
            print(f"FOLDER {folder} NOT EMPTY - this will not be deleted")


def date_gen(date_str):
    """
    Attributed to Ayman Hourieh, Stackoverflow question 993358
    Python 3.7+ only for this function - fromisoformat()
    """
    from_date = datetime.date.fromisoformat(date_str)
    while True:
        yield from_date
        from_date = from_date - datetime.timedelta(days=1)


def main():
    """
    Build date range (40 days prior to day before yesterday)
    Only move/clean up folders in target date range, protecting
    empty folders created ahead of today for future recordings
    """
    if not utils.check_control("power_off_all"):
        logger.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(STORA_PATH):
        logger.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    logger.info("=========== stora_housekeeping.py START ===========")
    period = []
    date_range = []
    period = itertools.islice(date_gen(YESTERDAY), 40)
    for date in period:
        date_range.append(date.strftime("%Y/%m/%d"))

    print(date_range)
    with open(TEXT_PATH, "r") as path:
        paths = path.readlines()
        for line in paths:
            line = line.rstrip("\n")
            if any(dt in line for dt in date_range):
                logger.info("Folder in date range to process: %s", line)

                # Skip immediately if stream found
                files = os.listdir(line)
                if "stream.mpeg2.ts" in files:
                    print(f"*** SKIPPING {line} as Stream.mpeg2.ts file here ***")
                    logger.warning(
                        "FOUND: stream.mpeg2.ts found %s, folder will be skipped", line
                    )
                    continue

                # Check for 'problem' files generated through CID API errors
                for file in files:
                    if file.endswith(".PROBLEM"):
                        print(f"***** Fault found in folder {line} *****")
                        logger.warning("PROBLEM file found %s, renaming folder", line)
                        if "PROBLEM_" in line:
                            print(f"Folder {line} already prepended PROBLEM_, skipping")
                            logger.info("%s already prepended PROBLEM_, skipping", line)
                            continue

                        split = line.split("/")
                        print(split)
                        split_head = split[:9]
                        split_tail = split[9:]
                        print(split_head, split_tail)
                        head_path = "/".join(split_head)
                        tail_path = "/".join(split_tail)
                        whole_path = os.path.join(head_path, tail_path)
                        print(f"Renaming folder PROBLEM_{tail_path}")
                        logger.info("Renaming folder PROBLEM_%s", tail_path)
                        os.rename(
                            whole_path, os.path.join(head_path, f"PROBLEM_{tail_path}")
                        )

                # Check if folder now updated problem and skip / else process move
                if "PROBLEM_" in line:
                    print(f"*** SKIPPING {line} as PROBLEM_ in foldername ***")
                    logger.warning(
                        "FOUND: PROBLEM_ found in foldername %s, folder will be skipped",
                        line,
                    )
                    continue
                else:
                    print(
                        f"MOVING folder - Problem or Stream.mpeg2.ts NOT found: {line}"
                    )
                    logger.info("MOVING FOLDER: No PROBLEM or STREAM found in %s", line)
                    line_split = line.split("/")
                    line_split = line_split[5:9]
                    line_join = "/".join(line_split)
                    new_path = os.path.join(ARCHIVE_PATH, line_join)
                    try:
                        os.makedirs(new_path, mode=0o777, exist_ok=True)
                        print(f"New path mkdir: {new_path}")
                    except OSError as error:
                        print(f"Unable to make new directory {new_path}")
                        logger.warning(
                            "Unable to make new directory: %s\n %s", new_path, error
                        )
                        continue
                    foldername = os.path.basename(line)
                    if os.path.exists(os.path.join(new_path, foldername)):
                        data = os.listdir(line)
                        for d in data:
                            try:
                                shutil.move(
                                    os.path.join(line, d),
                                    os.path.join(new_path, foldername),
                                )
                                print(
                                    f"Move path: {line}/{d} to {new_path}/{foldername}"
                                )
                                logger.info(
                                    "Moving folder: %s to %s",
                                    os.path.join(line, d),
                                    os.path.join(new_path, foldername),
                                )
                            except Exception:
                                print(
                                    f"Unable to move folder {line}/{d}, into {new_path}/{foldername}"
                                )
                                logger.exception(
                                    "Unable to move folder %s, into %s",
                                    os.path.join(line, d),
                                    os.path.join(new_path, foldername),
                                )
                                continue

                    try:
                        shutil.move(line, new_path)
                        print(f"Move path: {line} to {new_path}")
                        logger.info("Moving folder: %s to %s", line, new_path)
                    except Exception:
                        print(f"Unable to move folder {line}, into {new_path}")
                        logger.exception(
                            "Unable to move folder %s, into %s", line, new_path
                        )
                        continue

                # New block to move top level recording.log etc only if move above completes
                pth_split_old = os.path.split(line)[0]
                files = [
                    x
                    for x in os.listdir(pth_split_old)
                    if os.path.isfile(os.path.join(pth_split_old, x))
                ]
                pth_splt2 = pth_split_old.split("/")
                pth_splt2 = pth_splt2[5:]
                pth_join = "/".join(pth_splt2)
                new_move_path = os.path.join(ARCHIVE_PATH, pth_join)
                for file in files:
                    if "recording" in file:
                        shutil.move(os.path.join(pth_split_old, file), new_move_path)
                    elif "restart_" in file:
                        shutil.move(os.path.join(pth_split_old, file), new_move_path)
                    elif "schedule_" in file:
                        shutil.move(os.path.join(pth_split_old, file), new_move_path)

            else:
                logger.info("SKIPPING OUT OF RANGE FOLDER: %s", line)

    # Clear channel/programme folders in date range
    for date in date_range:
        clear_path = os.path.join(STORA_PATH, date)
        clear_folders(clear_path)

    # Clear month/year level folder, only if empty
    clear_limited_folders(STORA_PATH_MONTH)
    clear_limited_folders(STORA_PATH_YEAR)

    logger.info("=========== stora_housekeeping.py ENDS ============")


if __name__ == "__main__":
    main()
