#!/usr/bin/env python3

"""
BAU fetch CSV from Tech Edge SFTP
for integration into Adverts project
and augmentation of CID record data

Must leave 10 day clearance for
all metadata augmentation to CSV
from TechEdge. If less than 10 days
then 'Missing' rows appear and BARB
data and Break codes are absent.

2025
"""

import os
import sys
import csv
import logging
import subprocess
from datetime import date, timedelta
from typing import Final, Iterator, Optional, List, Any

sys.path.append(os.environ.get("CODE"))
import utils

CODE_PATH = os.path.join(os.environ.get("CODE"), "document_en_15907/techedge")
sys.path.append(CODE_PATH)
import sftp_utils as ut

# Global variables
STORAGE_PATH: Final = os.environ["ADVERTS_PATH"]
LOG_PATH: Final = os.environ["LOG_PATH"]
CODE_PATH: Final = os.environ["CODE"]
CONTROL: Final = os.path.join(LOG_PATH, "downtime_control.json")
DEST: Final = os.path.join(
    os.environ.get("ADMIN"),
    "datasets/adverts_techedge_no_dupes/"
)

# Setup logging
LOGGER = logging.getLogger("retrieve_historical_data")
hdlr = logging.FileHandler(
    os.path.join(LOG_PATH, "retrieve_techedge_csv_data_and_clean.log")
)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)

OMIT = [
    "SKYADSMART000",
    "C4ADDRESSABLE",
    "ITVADDRESSABLE"
]

HEADER = [
    "Channel",
    "Date",
    "Start time",
    "Film Code",
    "Break Code",
    "Advertiser",
    "Brand",
    "Agency",
    "Holding Company",
    "BARB Prog Before",
    "BARB Prog After",
    "Sales House",
    "Major category",
    "Mid category",
    "Minor category",
    "All PIB rel",
    "All PIB pos",
    "Log Station (2010-)",
    "Impacts A4+"
]


def date_range(start_date: str, end_date: str) -> Iterator[str]:
    """
    Set date range, and yield one
    at a time back to main.
    Args received must be:
    datetime.date(2015, 1, 1)
    """

    days = int((end_date - start_date).days)
    for n in range(days):
        yield str(start_date + timedelta(n))


def check_for_existing(target_date: str) -> bool:
    """
    See if match already in ADVERTS path
    """
    for file in os.listdir(STORAGE_PATH):
        if file.startswith(target_date):
            return True

    return False


def yield_csv_rows(cpath: str) -> Iterator[List[str]]:
    """
    Open CSV path supplied and yield rows
    Args:
        cpath (str): Path to CSV
    """
    with open(cpath, "r", encoding="latin1") as data:
        rows = csv.reader(data)
        next(rows)
        for row in rows:
            print(row)
            yield row


def make_new_csv(csv_title: str) -> Optional[str]:
    """
    Create a new CSV
    return CSV path
    """
    cpath = os.path.join(DEST, csv_title)
    if os.path.exists(cpath):
        print(f"Path already exists: {cpath}")
        return None

    with open(cpath, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(HEADER)

    if os.path.exists(cpath):
        return cpath

    return None


def main() -> None:
    """
    Checks if all channel folders exist in storage_path
    Populates channel folders that do with cut up schedules
    Matches to programme folders where possible
    """

    if not utils.check_control("power_off_all"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_control("pause_scripts"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(STORAGE_PATH):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    end_date = date.today() - timedelta(days=10)
    start_date = end_date - timedelta(days=3)
    sftp = ut.sftp_connect()
    LOGGER.info(
        "========== Fetch adverts data & cleanse script STARTED ==================================="
    )

    for target_date in date_range(start_date, end_date):
        check = check_for_existing(target_date)
        if check is True:
            continue
        download_path = ut.get_metadata(target_date, sftp)
        if not download_path:
            LOGGER.warning("Match for date path was not found yet: %s", target_date)
            continue
        elif os.path.isfile(download_path):
            LOGGER.info("New download: %s", download_path)

        # Create cleaned version
        LOGGER.info("Creating cleaned up CSV version...")
        os.chmod(download_path, mode=0o777)
        clean_csv = make_new_csv(f"{target_date}_BFIExport.csv")
        if clean_csv is None:
            print(f"Missing destination path: {clean_csv}")
            continue
        os.chmod(clean_csv, mode=0o777)
        with open(clean_csv, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            for row in yield_csv_rows(download_path):
                print(type(row))
                match = [x for x in OMIT if x == row[3]]
                if match:
                    continue
                writer.writerow(row)
        try:
            count1 = subprocess.run(
                ["wc", "-l", download_path],
                capture_output=True,
                check=True,
                text=True,
                shell=False
            )
            count2 = subprocess.run(
                ["wc", "-l", clean_csv],
                capture_output=True,
                check=True,
                text=True,
                shell=False
            )
            LOGGER.info(
                "New CSV created: Downloaded line count %s / Clean up CSV line count %s",
                count1.stdout.split(" ")[0],
                count2.stdout.split(" ")[0]
            )
        except Exception as err:
            LOGGER.warning("Could not access CSV lengths: %s", err)
            print(err)

    LOGGER.info(
        "========== Fetch adverts data & cleanse script ENDED ===================================="
    )


if __name__ == "__main__":
    main()
