"""
Process historical MP4 files
Locate the file in subfolder
and rename N_12345_01of01.mp4
Move into local autoingest path

2026
"""

import os
import sys
import csv
import logging
from typing import List, Optional

sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils

# Global vars
STORAGE = os.environ.get("QNAP_05")
AUTOINGEST = os.path.join(os.environ.get("AUTOINGEST_QNAP05"), "ingest/autodetect")
LOG_PATH = os.environ.get("LOG_PATH")
CID_API = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("collect_asset_processing")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "collection_asset_processing.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def yield_csv_rows(cpath: str) -> List[str]:
    """
    Open CSV path supplied and yield rows
    Args:
        cpath (str): Path to CSV
    """
    with open(cpath, "r", encoding="latin1") as data:
        rows = csv.reader(data)
        for row in rows:
            yield row.split(",")


def make_file_path(fname: str, pth: str) -> str:
    """
    Builds file path and returns
    Args:
        fname (str): File name
        pth (str): Supplied field path
    """
    if "cid_server_files" in pth:
        return os.path.join(STORAGE, fname)


def transform_name(fname: str) -> Optional[str]:
    """
    Transform filename to correct DPI filename

    Args:
        fname (str): Filename as found in 
    Returns:
        Optional[str]: Transformed name or NoneType
    """
    if "-" in fname:
        fname = fname.replace("-", "_")
    if "01of01" not in fname:
        num, ext = fname.split(".")
        fname = f"{num}_01of01.{ext}"

    check = utils.check_filename(fname)
    if check is True:
        return fname
    return None


def check_item(priref: str, object_num: str) -> bool:
    """
    Use requests to retrieve priref/RNA data for item object number
    """
    search = f"priref='{priref}'"
    record = adlib.retrieve_record(CID_API, "items", search, "1")[1]
    if record is None:
        return None

    ob_num = adlib.retrieve_field_name(record[0], "object_number")[0]
    if not ob_num:
        return False

    if ob_num == object_num:
        return True

    return False


def main() -> None:
    """
    Iterate CSV and extract
    file name and path, from
    this build fpath.
    Move file to processing/
    where renaming occurs.
    Move renamed version to
    local autoingest.
    """
    
    if not utils.check_control("power_off_all"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(STORAGE):
        LOGGER.info("Script run prevented by Storage Control document. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")
    if not sys.argv[1]:
        sys.exit("No CSV path supplied, exiting.")
    
    LOGGER.info("==== Collections Asset renaming script START ========")
    for fname, folderpath, priref in yield_csv_rows(sys.argv[1]):
        fpath = make_file_path(fname, folderpath)
        if not os.path.exists(fpath):
            LOGGER.info("Skipping: Filename not found in path {fpath}")
            continue
        LOGGER.info("** New item: %s - %s", fname, fpath)

        # Make filename conversion / CID check
        new_fname = transform_name(fname)
        if not new_fname:
            LOGGER.warning("Skipping: Filename failed transformation {fname}")
            continue
        object_number = utils.get_object_number(new_fname)
        cid_check = check_item(priref, object_number)
        if cid_check is None:
            LOGGER.warning("Error retrieving data from CID with priref: %s", priref)
            continue
        if cid_check is False:
            LOGGER.warning("Object number %s did not match that found from priref %s", object_number, priref)
            continue

        LOGGER.info("Filename converted from %s to %s", fname, new_fname)
        LOGGER.info("CID record matched to priref %s/object number %s", priref, object_number)
        LOGGER.info("Moving %s to processing/ to rename")

        # Begin renaming move to AUTOINGEST
        new_fpath = os.path.join(AUTOINGEST, new_fname)
        if os.path.exists(new_fpath):
            LOGGER.warning("SKIPPING: New file path already exists:\n%s", new_fpath)
            continue
        LOGGER.info("Renaming existing file path to new file path: %s", new_fpath)

        try:
            os.rename(fpath, new_fpath)
        except (OSError, FileNotFoundError) as err:
            LOGGER.warning("ERROR RENAMING TO %s\n%s", new_fpath, err)
            continue
        if os.path.exists(fpath):
            LOGGER.warning("ERROR! Old file path still exists!\n%s", fpath)
            sys.exit("Aborting, in case of permission issues!")
        LOGGER.info("Successfully moved path to Autoingest with renaming:\n%s\n%s", fpath, new_fpath)

    LOGGER.info("==== Collections Asset renaming script COMPLETED ====")

if __name__ == "__main__":
    main()