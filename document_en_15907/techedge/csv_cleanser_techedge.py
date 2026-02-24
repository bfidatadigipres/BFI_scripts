"""
Open CSV and parse each row
to check for dict of repeat
data and exclude from new
CSV entry.

Data cleansing prior to LLM
assessment of truncated and
incorrectly capitalised lines

2026
"""

import os
import sys
import csv
import logging
from datetime import datetime, timedelta
from typing import List, Optional

# Global vars
SOURCE = os.path.join(os.environ.get("ADMIN"), "datasets/adverts_techedge/")
DEST = os.path.join(os.environ.get("ADMIN"), "datasets/adverts_techedge_no_dupes/")
LOG_PATH = os.environ.get("LOG_PATH")
START_DATE = "2016-12-31"
END_DATE = "2026-02-20"
FMT = "%Y-%m-%d"

# Setup logging
LOGGER = logging.getLogger("csv_cleanser_techedge")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "csv_cleanser_techedge.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
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

def yield_csv_rows(cpath: str) -> List[str]:
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


def make_new_csv(csv_title):
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


def yield_date():
    """
    Work between
    start and end dates
    yielding next day to
    process CSV
    """
    start = datetime.strptime(START_DATE, FMT)
    yield_date = ""

    while END_DATE not in yield_date:
        new_date = start + timedelta(days=1)
        yield_date = datetime.strftime(new_date, FMT)
        start = new_date
        yield yield_date


def main():
    """
    Yield dates from range
    built path to CSV source
    and create new CSV
    iterate CSV content excluding
    OMITS list and write allowed
    data to new CSV
    """
    for dt in yield_date():
        csv1 = os.path.join(SOURCE, f"{dt}_BFIExport.csv")
        if not os.path.exists(csv1):
            print(f"Missing source path: {csv1}")
            continue

        csv2 = make_new_csv(f"{dt}_BFIExport.csv")
        if csv2 is None:
            print(f"Missing destination path: {csv2}")
            continue
        with open(csv2, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)

            for row in yield_csv_rows(csv1):
                print(type(row))
                match = [x for x in OMIT if x == row[3]]
                if match:
                    continue
                writer.writerow(row)



if __name__ == "__main__":
    main()
