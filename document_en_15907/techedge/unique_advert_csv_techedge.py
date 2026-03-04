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
import csv
import sys
import logging
import collections
from datetime import datetime, timedelta
from typing import List, Optional

# Global vars
SOURCE = os.path.join(os.environ.get("ADMIN"), "datasets/adverts_techedge_no_dupes/")
LOG_PATH = os.environ.get("LOG_PATH")
START_DATE = "2015-12-31"
END_DATE = "2026-02-14"
FMT = "%Y-%m-%d"

# Setup logging
LOGGER = logging.getLogger("unique_adverts_csv_techedge")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "unique_adverts_csv_techedge.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


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
    seen_keys = set()
    csv2 = os.path.join(SOURCE, f"Unique_adverts_BFIExport.csv")
    with open(csv2, "a", newline="", encoding="latin1") as f:
        writer = csv.writer(f)
        for dt in yield_date():
            csv1 = os.path.join(SOURCE, f"{dt}_BFIExport.csv")
            if not os.path.exists(csv1):
                print(f"Missing source path: {csv1}")
                continue
            print(f"\nPROCESSING: {csv1}")
            with open(csv1, newline="", encoding="latin1") as file:
                reader = csv.reader(file)
                for row in reader:
                    key = row[3]
                    if key not in seen_keys:
                        print(f"New unique key found: {key}:\n{row}")
                        seen_keys.add(key)
                        writer.writerow(row)


if __name__ == "__main__":
    main()
