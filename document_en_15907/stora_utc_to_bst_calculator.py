#!/usr/bin/env python3

"""
SYS.ARGV[1] needs to be a path to a CSV file

This script requires CID CSV exports of STORA manifestations
that contain 'priref', 'transmission_start_time' and
'transmission_date'.

1. Iterate through the CSV building a concatenated 'UTC_timestamp'
   for all entries

2. Where a date/time fall within BST - pass date and time strings 
   to check_bst_adjustment() and get back adjusted date/time - use
   these to replace existing 'transmissions_start_time' and '_date' fields.

3. Populate new CSV with 'priref', original or new date/time fields
   and new 'UTC_timestamp' field. Return to CID team to ingest to CID.

CID field transmission_start_time formatted HH:MM:SS
CID transmission_date formatted YYYY-MM-DD

DR-573

2025
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import csv
from typing import Any, Final, Optional

FORMAT = "%Y-%m-%d %H:%M:%S"
LOGS = os.environ.get("LOG_PATH")

BST_DCT = {
    "2015": ["2015-03-29", "2015-10-25"],
    "2016": ["2016-03-27", "2016-10-30"],
    "2017": ["2017-03-26", "2017-10-29"],
    "2018": ["2018-03-25", "2018-10-28"],
    "2019": ["2019-03-31", "2019-10-27"],
    "2020": ["2020-03-29", "2020-10-25"],
    "2021": ["2021-03-28", "2021-10-31"],
    "2022": ["2022-03-27", "2022-10-30"],
    "2023": ["2023-03-26", "2023-10-29"],
    "2024": ["2024-03-31", "2024-10-27"],
    "2025": ["2025-03-30", "2025-10-26"]
}

# Setup logging
LOGGER = logging.getLogger("stora_utc_to_bst_calculator")
HDLR = logging.FileHandler(os.path.join(LOGS, "stora_utc_to_bst_calculator.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def yield_rows(csv_path):
    """
    Open and read a CSV, yielding
    one line at a time for processing
    """
    with open(csv_path, 'r') as data:
        row_data = csv.reader(data)
        for row in row_data:
            yield row


def check_bst_adjustment(utc_datetime_str: str) -> bool:
    """
    Determines if a given UTC datetime string falls within BST
    adds +1 where needed
    """

    try:
        dt_utc = datetime.strptime(utc_datetime_str, FORMAT).replace(tzinfo=timezone.utc)
        print(dt_utc)
    except ValueError as e:
        raise ValueError(f"Invalid datetime string format: {e}. Expected '%Y-%m-%d %H:%M:%S'")

    london_tz = ZoneInfo("Europe/London")
    dt_london = dt_utc.astimezone(london_tz)
    string_bst = datetime.strftime(dt_london, FORMAT)
    return string_bst.split(" ")


def main():
    """
    Receive CSV path
    Create new CSV inheriting name
    of supplied, but prepended to
    signal UCT updates complete
    Iterate through rows and update
    date/time to new UTC timestamp
    then calculate if BST updates needd
    """

    if not len(sys.argv) == 2:
        sys.exit("Exiting. Missing CSV path, please try again with filepath to CSV file")
    if not os.path.isfile(sys.argv[1]):
        sys.exit(f"Exiting. Supplied CSV path non readable in code: {sys.argv[1]}")

    # Get the new CSV path created
    root, csv = os.path.split(sys.argv[1])
    new_csv = f"utc_update_{csv}"
    new_csv_path = os.path.join(root, new_csv)
    check_file = make_new_csv(new_csv_path)
    if not check_file:
        sys.exit("Scripts failed to make new CSV to store changed date times in.")

    # Begin iterating supplied rows to make new CSV
    for row in yield_rows(sys.argv[1]):
        # Refresh all essential fields
        priref = utc_timestamp = utc_date = utc_time = csv_date = csv_time = ""

        priref = row[0]
        if not priref.isnumeric():
            continue

        check = check_for_priref(priref)
        if check:
            print(f"Already processed row {row}")
            continue

        # Start UTC manipulations
        utc_date = row[1]
        utc_time = row[2]

        if len(utc_date) > 3 and len(utc_time) > 4:
            utc_timestamp = f"{utc_date} {utc_time}"
        else:
            LOGGER.warning("Failed to process: %s", row)
            continue

        bst_data = check_bst_adjustment(utc_timestamp)
        if not bst_data:
            LOGGER.warning("Failed to process: %s", row)
            continue
        if bst_data[1] == utc_time:
            csv_date = utc_date
            csv_time = utc_time
        else:
            csv_date = bst_data[0]
            csv_time = bst_data[1]
        if not write_to_csv([priref, utc_timestamp, csv_time, csv_date], new_csv_path):
            LOGGER.warning("Failed to process: %s", row)


def check_for_priref(new_csv, priref):
    """
    Check derivative CSV for
    priref already processed
    """
    for row in yield_rows(new_csv):
        if priref == row[0]:
            return True


def make_new_csv(new_csv_path):
    """
    If not already created, make a new
    CSV and add four column headings
    to match CID fields for import
    """
    cols = ["priref", "utc_timestamp", "transmission_start_time", "transmission_date"]
    with open(new_csv_path, mode="w", newline="") as doc:
        write_data = csv.writer(doc)
        write_data.writerows(cols)

    if os.path.isfile(new_csv_path):
        return True


def write_to_csv(new_row_data, new_csv):
    """
    Write additional line to new CSV
    """
    with open(new_csv, "a") as doc:
        writer = csv.writer(doc)
        writer.writerow(new_row_data)


if __name__ == "__main__":
    main()