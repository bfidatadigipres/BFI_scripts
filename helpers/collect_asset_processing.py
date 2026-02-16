"""
Process historical MP4 files
Locate the file in subfolder
and rename N_12345_01of01.mp4
Move into local autoingest path

NOTE: WAITING TO HEAR ABOUT DECISION
TO CLEAR OUT ALL EXISTING DIGITAL MEDIA
RECORDS. REFACTORING NEEDED IF THIS HAPPENS

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
AUTOINGEST = os.path.join(os.environ.get("AUTOINGEST_QNAP05"), "ingest/autodetect/legacy")
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


def check_item(search: str, database: str, field: str) -> Optional[str]:
    """
    Use requests to retrieve priref/RNA data for item object number
    """

    hits, record = adlib.retrieve_record(CID_API, database, search, "0")
    if hits == 0:
        "No hits"
    if record is None:
        return None

    fetched_field = adlib.retrieve_field_name(record[0], [field])[0]
    if not fetched_field:
        return ""

    return fetched_field


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

        # JMW CHECK HERE FOR NO HITS - CHANGE PATH FOR INGEST FROM LEGACY
        obj = check_item(f"priref='{priref}'", "items", "object_number")
        if cid_check is None:
            LOGGER.warning("Error retrieving data from CID with priref: %s", priref)
            continue
        if cid_check == "No hits":
            LOGGER.warning("No matching record found for priref %s", priref)
            continue
        if cid_check == "":
            LOGGER.warning("Object number %s could not be retrieved from priref %s", object_number, priref)
            continue
        if obj != object_number:
            LOGGER.warning("Object number %s does not match that from priref %s", object_number, obj)
            continue

        LOGGER.info("Filename will be converted from %s to %s", fname, new_fname)
        LOGGER.info("CID record matched to priref %s/object number %s", priref, obj)

        # Ident which autoingest path (with/without dm rcord)
        ref_num = check_item(f"object_number='{obj}'", "media", "reference_number")
        if ref_num is None:
            LOGGER.warning("Error retrieving data from CID with priref: %s", priref)
            continue
        if ref_num is "":
            LOGGER.warning("Object number %s could not be retrieved from priref %s", obj, priref)
            continue
        if ref_num == "No hits":
            LOGGER.warning("No matching Digital Media record found for object number %s", obj)
            continue
        if ref_num.strip() != fname:
            LOGGER.warning("Reference number %s does not match File name %s", ref_num, fname)
            continue
        imagen_name = check_item(f"object_number='{obj}'", "media", "imagen.media.original_filename")
        if imagen_name is None or imagen_name is "No hits":
            LOGGER.warning("Error retrieving data from CID with object_number: %s", obj)
            continue
        if len(imagen_name) > 0:
            LOGGER.warning("Imagen.media.original_filename present %s - do not ingest this twice!", imagen_name)
            continue

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
        if not os.path.exists(new_fpath):
            LOGGER.warning("New file path cannot be found in Autoingest:\n%s", new_fpath)
            sys.exit("Aborting, in case of permissions issues!")
        LOGGER.info("Successfully moved path to Autoingest with renaming:\n%s\n%s", fpath, new_fpath)

    LOGGER.info("==== Collections Asset renaming script COMPLETED ====")


if __name__ == "__main__":
    main()