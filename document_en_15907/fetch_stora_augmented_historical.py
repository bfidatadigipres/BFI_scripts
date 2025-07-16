#!/usr/bin/env python3

"""
Fetch historical JSON from augmented EPG metadata API, for augmenting metadata for REDUX era records

main():


2025
"""

import datetime
import json
import logging
import os
import shutil
import sys
import time
from typing import Any, Final, Optional

import requests
import tenacity

# Global variables
STORAGE_PATH: Final = os.environ["HISTORICAL_PATH"]
LOG_PATH: Final = os.environ["LOG_PATH"]
CODE_PATH: Final = os.environ["CODE"]
CONTROL: Final = os.path.join(LOG_PATH, "downtime_control.json")
START = datetime.date(2015, 9, 9)
END = datetime.date(2022, 1, 20)

# Setup logging
logger = logging.getLogger("fetch_stora_augmented_historical")
hdlr = logging.FileHandler(
    os.path.join(LOG_PATH, "fetch_stora_augmented_historical.log")
)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Setup Rest API
URL = os.environ["PATV_URL"]
HEADERS = {"accept": "application/json", "apikey": os.environ["PATV_HISTORIC"]}

CHANNEL = {
    "bbconehd": os.environ.get("PA_BBCONE"),
    "bbctwohd": os.environ.get("PA_BBCTWO"),
    "bbcthree": os.environ.get("PA_BBCTHREE"),
    "bbcfourhd": os.environ.get("PA_BBCFOUR"),
    "bbcnewshd": os.environ.get("PA_BBCNEWS"),
    "cbbchd": os.environ.get("PA_CBBC"),
    "cbeebieshd": os.environ.get("PA_CBEEBIES"),
    "itv1": os.environ.get("PA_ITV1"),
    "itv2": os.environ.get("PA_ITV2"),
    "itv3": os.environ.get("PA_ITV3"),
    "itv4": os.environ.get("PA_ITV4"),
    "itvbe": os.environ.get("PA_ITVBE"),
    "citv": os.environ.get("PA_CITV"),
    "channel4": os.environ.get("PA_CHANNEL4"),
    "more4": os.environ.get("PA_MORE4"),
    "e4": os.environ.get("PA_E4"),
    "film4": os.environ.get("PA_FILM4"),
    "five": os.environ.get("PA_FIVE"),
    "5star": os.environ.get("PA_5STAR"),
}


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


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def check_api(start: str, end: str, value: str) -> Optional[bool]:
    """
    Run standard check with today's date on supplied channelID
    """
    params = {"channelId": f"{value}", "start": start, "end": end, "aliases": "True"}
    req = requests.request("GET", URL, headers=HEADERS, params=params, timeout=120)
    print(req.text)
    if req.status_code == 200:
        return True
    logger.info("PATV API return status code: %s", req.status_code)
    raise tenacity.TryAgain


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


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def fetch(value, start, end) -> Optional[dict[str, str]]:
    """
    Retrieval of EPG metadata here
    """
    try:
        params = {
            "channelId": f"{value}",
            "start": start,
            "end": end,
            "aliases": "True",
        }
        logger.info("fetch(): %s", params)
        req = requests.request("GET", URL, headers=HEADERS, params=params, timeout=120)
        jdct = json.loads(req.text)
        print(jdct)
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


def json_split(pth: str, jdct: dict[str, Any], key: str) -> None:
    """
    Splits dct into subdicts based on 'item' and dateTime being present
    """
    dump_to = os.path.join(pth, key)
    if "item" not in jdct:
        return None
    for subdct in jdct["item"]:
        if "dateTime" in subdct:
            try:
                # Outputs split files in subdct when dateTime present to storage_path/dump_path/
                dt_item = subdct["dateTime"]
                fname = os.path.join(dump_to, f"info_{dt_item}.json")
                print(fname)
                with open(fname, "w+") as f:
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
                    "Splitting has been successful. Saving to top folder %s", fname
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

    for target_date in date_range(START, END):
        date_start = f"{target_date}T00:00:00"
        date_end = f"{target_date}T23:59:00"
        folder_date = target_date.replace("-", "/")
        pth = os.path.join(STORAGE_PATH, folder_date)

        logger.info(
            "Requests will now attempt to retrieve the EPG channel metadata from start=%s to end=%s",
            date_start,
            date_end,
        )

        for key, value in CHANNEL.items():
            item_path = os.path.join(pth, key)
            print(f"Making path for: {item_path}")

            result = check_api(date_start, date_end, value)
            if not result:
                print(f"Cannot establish connectino with {key} for date {date_start}")
                logger.warning(
                    "Unable to establish contact with PATV API for channel %s. Script exitings",
                    key,
                )
                continue

            jdct = fetch(value, date_start, date_end)
            if not jdct["item"]:
                continue

            if not os.path.exists(item_path):
                os.makedirs(item_path, mode=0o777, exist_ok=True)
            retrieve_dct_data(date_start, date_end, key, value, pth, jdct)
            logger.info("Path for move actions: %s", item_path)
            # move(item_path, key)

    logger.info(
        "========== Fetch augmented metadata script ENDED ================================================"
    )


def move(path_move: str, item: str) -> None:
    """
    Handles move of JSON files
    into correct date/channel paths
    """
    file_trim = {}
    for file in os.listdir(path_move):
        if file.endswith(".json"):
            filename = os.path.basename(file)
            trim = filename[16:21].replace(":", "-")
            file_trim.update({trim: path_move + "/" + filename})
            print(file, file_trim)
        else:
            print(f"move(): Skipping {file} as file is not JSON: {path_move}")
            logger.info(
                "Skipping file=%s as file is not a JSON: path=%s", file, path_move
            )

    for key, value in file_trim.items():
        item_path = os.path.join(path_move, item)

        try:
            shutil.move(value, item_path)
            logger.info("File moved successfully: %s", item_path)
        except Exception as err:
            logger.warning(
                (
                    "** WARNING: Unable to move file %s to folder %s",
                    value,
                    item_path,
                ),
                err,
            )


def retrieve_dct_data(
    start: str, end: str, key: str, value: str, pth: str, jdct=None
) -> None:
    """
    Check if DCT data is None, if not instigate json_split
    """
    if jdct is None:
        logger.warning(
            "FAILED: First attempt to retrieve metadata. Second attempt will be made in 5 minutes."
        )
        time.sleep(300)
        jdct = fetch(start, end, value)
        if jdct is None:
            logger.warning(
                "FAILED: Second attempt to retrieve metadata. Third attempt will be made in 10 minutes."
            )
            time.sleep(600)
            jdct = fetch(start, end, value)
            if jdct is None:
                logger.critical(
                    "*** FAILED: Third attempt to retrieve metadata from EPG website. Script exiting ***"
                )
                sys.exit()
            else:
                logger.info(
                    "EPG metadata successfully retrieved. Starting split of JSON files"
                )
                json_split(pth, jdct, key)
        else:
            logger.info(
                "EPG metadata successfully retrieved. Starting split of JSON files"
            )
            json_split(pth, jdct, key)
    else:
        logger.info("EPG metadata successfully retrieved. Starting split of JSON files")
        json_split(pth, jdct, key)


if __name__ == "__main__":
    main()
