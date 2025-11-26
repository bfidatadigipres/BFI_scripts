#!/usr/bin/env python3

"""
Code to rename TV programmes recorded 2022/2023
but never moved through to ingest:
- Stored in QNAP-05 in eg 2022-09-09/ folders
- Named eg, 'sky_news_2022-09-05_23-00-01.ts'
- Renamed to eg, 'N_10802345_01of24.ts'

CSV contains information for all files that need
renaming. Usually in batches of 24, sometimes 23

Once complete the folders are moved manually to
DPI autoingest on that qnap.

2025
"""

import logging
import os
import sys
import csv
from datetime import datetime

sys.path.append(os.environ.get("CODE"))
import utils

# global vars
LOG_PATH = os.environ.get("LOG_PATH")
SUPPLY = os.path.join(os.environ.get("QNAP_05"), "RETAIN_FOR_DPI_INGEST/")
CSV = os.path.join(os.environ.get("ADMIN"), "renumbering_document.csv")

# Setup logging
LOGGER = logging.getLogger("historical_tv_renaming")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "historical_tv_renaming.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def yield_row():
    """
    Iterate through rows of CSV
    """
    with open(CSV, "r") as rows:
        csv_reader = csv.reader(rows)

        for row in csv_reader:
            if row[0] == "Number":
                continue
            yield row


def main():
    """
    Iterate through rows, break up data and
    check path exists and rename
    """
    if not utils.check_storage(SUPPLY) or not utils.check_storage(CSV):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not utils.check_control("pause_scripts"):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    LOGGER.info("====================== File renaming start ==================== %s", str(datetime.now()))

    count = 0
    for row in yield_row():
        LOGGER.info("Row extracted for processing:\n%s", row)
        first_num = bdate = start = bst = channel = fpath = fname = ob_num = ""
        first_num = row[0]
        bdate = row[1]
        start = row[2]
        bst = row[3]
        channel = row[4]
        fpath = row[5]
        fname = row[6]
        if fname == "Missed recording":
            LOGGER.info("** Missed recording here - skipping")
            continue
        ob_num = row[7]
        LOGGER.info("** File being processed: %s - object number %s", fname, ob_num)

        if len(first_num) == 0:
            count += 1
            if count > full_range:
                sys.exit("Count over duration of whole!")
            new_fname = f"{ob_num}_{str(count).zfill(2)}of{str(full_range).zfill(2)}.ts"
        else:
            part_whole = first_num.split("_")[-1].rsplit(".ts")
            LOGGER.info("New part whole extracted: %s for file %s", part_whole, first_num)
            part, whole = part_whole[0].split("of")
            count = 1
            full_range = int(whole)
            new_fname = first_num

        old_name = os.path.join(fpath, fname)
        new_name = os.path.join(fpath, new_fname)
        LOGGER.info("Renaming file:\n%s\n%s", old_name, new_name)

        if not os.path.exists(old_name):
            LOGGER.warning("File path could not be found: %s", old_name)
            sys.exit("Path does not exist!")
        success = rename(old_name, new_name)
        if not success:
            LOGGER.warning("Exiting. Rename failed: %s", old_name)
            sys.exit("Rename failed!")
    LOGGER.info("====================== File renaming ended ==================== %s", str(datetime.now()))


def rename(fpath: str, new_fpath: str) -> bool:
    """
    Rename a file with new name
    """
    try:
        os.rename(fpath, new_fpath)
        if os.path.isfile(new_fpath):
            return True
    except OSError as err:
        LOGGER.warning(err)

    return False


if __name__ == "__main__":
    main()
