#!/usr/bin/env python3

"""
Fetch daily CSV from Tech Edge SFTP
for integration into Adverts project
and augmentation of CID record data

2025
"""

import json
import logging
import os
import sys
from datetime import date, timedelta
from time import sleep
from typing import Final

# Local import
CODE_PATH = os.path.join(os.environ.get("CODE"), "document_en_15907/techedge")
LIST = os.path.join(CODE_PATH, "date_list.json")
sys.path.append(CODE_PATH)
import sftp_utils as ut

sys.path.append(os.environ.get("CODE"))
import utils

# Global variables
STORAGE_PATH: Final = os.environ["ADVERTS_PATH"]
LOG_PATH: Final = os.environ["LOG_PATH"]
CODE_PATH: Final = os.environ["CODE"]
CONTROL: Final = os.path.join(LOG_PATH, "downtime_control.json")

# Setup logging
LOGGER = logging.getLogger("retrieve_historical_data")
hdlr = logging.FileHandler(
    os.path.join(LOG_PATH, "retrieve_historical_adverts_metadata.log")
)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def check_control() -> None:
    """
    Check control JSON for downtime request
    """
    with open(CONTROL) as control:
        j = json.load(control)
        if not j["pause_scripts"]:
            LOGGER.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")


def get_date():
    """
    Set date range, and yield one
    at a time back to main.
    Args received must be:
    datetime.date(2015, 1, 1)
    """

    with open(LIST, "r") as d:
        date_list = json.load(d)
        for date in date_list["dates"]:
            yield date


def check_for_existing(target_date):
    """
    See if match already in ADVERTS path
    """
    files = [x for x in os.listdir(STORAGE_PATH)]
    for file in files:
        if file.startswith(target_date):
            return True

    return False


def main() -> None:
    """
    Checks if all channel folders exist in storage_path
    Populates channel folders that do with cut up schedules
    Matches to programme folders where possible
    """

    if not utils.check_control("power_off_all"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(STORAGE_PATH):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    LOGGER.info(
        "========== Fetch historical adverts data script STARTED ==============================================="
    )
    sftp = ut.sftp_connect()
    for target_date in get_date():
        print(target_date)
        check = check_for_existing(target_date)
        if check is True:
            continue
        download_path = ut.get_metadata(target_date, sftp)
        if not download_path:
            LOGGER.warning("Match for date path was not found yet: %s", target_date)
        elif os.path.isfile(download_path):
            LOGGER.info("New download: %s", download_path)

    LOGGER.info(
        "========== Fetch historical adverts data script ENDED ================================================"
    )


if __name__ == "__main__":
    main()
