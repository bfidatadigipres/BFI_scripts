#!/usr/bin/env python3

"""
Fetch historical CSV from Tech Edge SFTP
for integration into Adverts project

2025
"""

import datetime
import json
import logging
import os
import sys
from typing import Final

# Local import
CODE_PATH = os.path.join(os.environ.get('CODE'), "document_en_15907/techedge")
sys.path.append(CODE_PATH)
import sftp_utils as ut

# Global variables
STORAGE_PATH: Final = os.environ["ADVERTS_PATH"]
LOG_PATH: Final = os.environ["LOG_PATH"]
CODE_PATH: Final = os.environ["CODE"]
CONTROL: Final = os.path.join(LOG_PATH, "downtime_control.json")
START = datetime.date(2024, 2, 25)
END = datetime.date(2025, 7, 29)

# Setup logging
logger = logging.getLogger("retrieve_historical_data")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "retrieve_historical_adverts_metadata.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def check_control() -> None:
    """
    Check control JSON for downtime request
    """
    with open(CONTROL) as control:
        j = json.load(control)
        if not j["pause_scripts"]:
            logger.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")


def date_range(start_date, end_date):
    """
    Set date range, and yield one
    at a time back to main.
    Args received must be:
    datetime.date(2015, 1, 1)
    """

    days = int((end_date - start_date).days)
    for n in range(days):
        yield str(start_date + datetime.timedelta(n))


def check_for_existing(target_date):
    """
    See if match already in ADVERTS path
    """
    files = [ x for x in os.listdir(STORAGE_PATH) ]
    for file in files:
        if file.startswith(target_date):
            return True

    return False


def main() -> None:
    """
    Checks if all channel folders exist in storage_path
    Populates channel folders that do with cut up schedules
    Matches to programme folders where possible
    check_control()
    """
    
    sftp = ut.sftp_connect()
    logger.info(
        "========== Fetch historical adverts data script STARTED ==============================================="
    )
    count = 0
    for target_date in date_range(START, END):
        count += 1
        if count > 13:
            sftp = ut.sftp_connect()
            count = 0

        check = check_for_existing(target_date)
        if check is True:
            continue
        download_path = ut.get_metadata(target_date, sftp)
        if not download_path:
            logger.warning("Match for date path was not found: %s", target_date)
        elif os.path.isfile(download_path):
            logger.info("New download: %s", download_path)
        else:
            logger.warning("*** Failed to download %s", target_date)
    logger.info(
        "========== Fetch historical adverts data script ENDED ================================================"
    )


if __name__ == "__main__":
    main()
