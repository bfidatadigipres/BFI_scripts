#!/usr/bin/ python3

"""
Script to receive list of
'Latest = False' files found
within the access renditions
backup bucket.

Iterate list checking for versions
that are not the 'latest' or are
not flagged 'Latest', and delete
those using 'version_id' of each
file in case.

2026
"""

import logging
import os
import sys
import csv
from datetime import datetime
from typing import Final, Dict, List, Any, Generator

# Local imports
import bp_utils as bp
sys.path.append(os.environ["CODE"])
import utils

# Global vars
LOG_PATH: Final = os.environ["LOG_PATH"]
STORAGE: Final = os.environ["TRANSCODING"]
CSV_PTH: Final = os.path.join(STORAGE, "false_latest_flag.csv")
BUCKET: Final = "Access_Renditions_backup"

# Setup logging
LOGGER: Final = logging.getLogger("Access_rendition_deletion_clean_up")
HDLR: Final = logging.FileHandler(
    os.path.join(LOG_PATH, "Access_rendition_deletion_clean_up.log")
)
FORMATTER: Final = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def yield_csv_rows(cpath: str) -> Generator[List[str]]:
    """
    Open CSV path supplied and yield rows
    Args:
        cpath (str): Path to CSV
    """
    with open(cpath, "r", encoding="latin1") as data:
        rows = csv.reader(data)
        next(rows)
        for row in rows:
            yield row.split(",")


def extract_objects_sort(obj_list: list[dict]) -> tuple(Dict[str, str], Dict[str, Any]):
    """
    Iterate list of objects
    extracting version_id and
    creation_date. Sort into 
    oldest -> newest
    Return all but last (newest)
    for deletions
    """
    found_items = {}
    for obj in obj_list:
        version_id = creation_date = None
        data = obj.get("Blobs").get("ObjectList")
        version_id = data[0].get("VersionId")
        creation_date = obj.get("CreationDate")
        if version_id and creation_date:
            found_items[creation_date] = version_id

    sorted_dict = dict(sorted(found_items.items(), key=lambda item: item[0]))
    deleted_key = sorted_dict.popitem()
    print(f"Preserving newest item: {deleted_key}")
    print(f"Items for deletion: {sorted_dict}")

    return deleted_key, sorted_dict


def main() -> None:
    """
    Open CSV and iterate list
    of duplicate files not
    correctly deleted in bucket
    - Retrieve from API list of versions
    - Sort for all older creation dates
    - Delete this and leave just one
    - Check for Latest flag status of 'True'
      and where/if found retain this version
    - Log clean up procedures
    """
    
    if not utils.check_control("pause_all_code"):
        sys.exit("Code cannot run at this time.")
    
    for row in yield_csv_rows(CSV_PTH):
        LOGGER.info("Cleaning up first row entry: %s", row[0])
        try:
            fname = row[0]
        except IndexError as err:
            LOGGER.warning("No entry found in row: %s", err)
            continue

        obj_list = bp.get_object_list_items(fname)
        if obj_list is None or len(obj_list) == 0:
            LOGGER.info("Unable to retrieve data from Black Pearl on file: %s", fname)
            continue

        LOGGER.info("Retrieved %s items for file: %s", len(obj_list.get("ObjectList")), fname)
        if len(obj_list.get("ObjectList")) == 1:
            LOGGER.info("This file has just returned one version. Skipping")
            continue
        
        preserved_items, to_delete = extract_objects_sort(obj_list)
        LOGGER.info(
            "Preserving %s with creation date %s and version Id %s",
            fname, preserved_items[0], preserved_items[1]
        )
        LOGGER.info(
            "Item creation dates and version_ids for deletion:\n%s\n%s\n",
            ", ".join(to_delete.keys()),
            ", ".join(to_delete.values()),
        )
        """
        success = delete_existing_proxy(fname, to_delete, len(obj_list))
        if not success:
            LOGGER.warning("%s - Deletions not fully successful")
            continue

        LOGGER.info("Completed: Clean up of spare files for %s", fname)
        """


def delete_existing_proxy(fname: str, deletions: dict[str, str], total) -> bool:
    """
    A proxy is being replaced so the
    existing version should be cleared
    """

    if not deletions:
        LOGGER.info("No files being replaced at this time")
        return False

    count = 0   
    for key, val in deletions.items():
        LOGGER.info("Deletion stage received: %s | %s | %s", fname, key, val)
        confirmed = bp.delete_black_pearl_object(fname, val, BUCKET)
        print(type(confirmed))

        if confirmed:
            count += 1
            sleep(100)
            obj_list = bp.get_object_list_items(fname)
            check = int(total) - count
            if len(obj_list) != check:
                LOGGER.waring("** Potential deletion failure with version %s / %s", val, key)
            LOGGER.info("Successfully deletion of version %s created on %s", val, key)
    
    if count == (total - 1):
        return True

    return False


if __name__ == "__main__":
    main()