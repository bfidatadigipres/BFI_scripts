#!/usr/bin/env python3

"""
*** THIS SCRIPT MUST RUN FROM SHELL LAUNCH PASSING PATH TO MD5 ***
Script to clean up Checksum  files that belong to filepaths deleted by autoingest,
writes checksums to CID media record priref before deleting files.

1. Receive path to MD5 file
2. Extract filename from title and check file is worth processing
3. Open copy of persistence_queue.csv using csv.DictReader()
4. Iterate through path keys looking for path match to filename and 'delete file'
5. Where there's a match look up work on CID and retrieve media record priref
6. Write checksum to CID media record in notes field
7. Where data written successfully delete checksum
8. Where there's no media record match check for CID item record, if not found move
   to 'unmatched_checksums' subfolder. If CID media match failed API read or CID
   item record matched then leave checksum in place for repeat attempt later

2021
Python3.8+
"""

import datetime
import logging

# Global packages
import os
import shutil
import sys
import typing

# Local packages
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Global variables
LOG_PATH: typing.Final = os.environ["LOG_PATH"]
CHECKSUM_PATH: typing.Final = os.path.join(LOG_PATH, "checksum_md5")
CONTROL_JSON: typing.Final = os.path.join(LOG_PATH, "downtime_control.json")
CID_API: typing.Final = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("checksum_clean_up")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "checksum_clean_up.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# Local date vars for script comparison/activation
TODAY_TIME: typing.Final = str(datetime.datetime.now())
TIME: typing.Final = TODAY_TIME[11:19]
DATE: typing.Final = datetime.date.today()
TODAY: typing.Final = str(DATE)


def name_split(filepath: str) -> str:
    """
    Splits name of checksum file to filename
    cuts '.md5' from the end to leave whole filename
    """
    fname = ""
    filename_ext = ""
    filename_ext = os.path.basename(filepath)
    fname = filename_ext[:-4]
    return fname


def checksum_split(data: str) -> tuple[str, str, str]:
    """
    Splits string and returns
    """
    md5 = path = date = ""
    try:
        data_split = data.split(" - ")
        md5 = data_split[0]
        path = data_split[1]
        date = data_split[2]
    except Exception as e:
        print(e)

    if len(date) > 0:
        return (md5, path, date)
    else:
        data_split = data.split("  ")
        md5 = data_split[0]
        path = data_split[1]
        date = ""
        return (md5, path, date)


def cid_retrieve(fname: str) -> tuple[str, str]:
    """
    Retrieve priref for media record from imagen.media.original_filename
    """
    priref = checksum_val = ""
    search = (
        f"(imagen.media.original_filename='{fname}') or (reference_number='{fname}')"
    )
    print(f"Media record search: {search}")
    record = adlib.retrieve_record(
        CID_API, "media", search, "0", ["priref", "checksum.value"]
    )[1]
    if record is None:
        return "", ""
    print(record)
    if "priref" in str(record):
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
    if "checksum.value" in str(record):
        checksum_val = adlib.retrieve_field_name(record[0], "checksum.value")[0]

    return priref, checksum_val


def check_for_item_record(fname):
    """
    Check for Item record for filename
    """
    ob_num = utils.get_object_number(fname)
    if ob_num is None:
        return None
    search = f"object_number='{ob_num}'"
    print(f"Item record search: {search}")
    hits = adlib.retrieve_record(CID_API, "collect", search, "0")[0]
    if hits is None:
        return None
    if hits >= 1:
        return True
    if hits == 0:
        return False


def read_checksum(path: str) -> list[str]:
    """
    Open text file at path
    Readlins() and store in variable
    """
    with open(path) as data:
        readme = data.readlines()
        return readme


def main():
    """
    Clean up scripts for checksum files that have been processed by autoingest
    writing text file dumps to media record in CID
    """
    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()
    if not utils.check_control("pause_scripts"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    if len(sys.argv) != 2:
        LOGGER.warning("SCRIPT NOT STARTING: MD5 path argument error: %s", sys.argv)
        sys.exit(f"Supplied argument error: {sys.argv}")

    filepath: str = sys.argv[1]
    fname: str = name_split(filepath)
    LOGGER.info("===== CHECKSUM CLEAN UP SCRIPT START: %s =====", fname)

    if not os.path.isfile(filepath):
        LOGGER.warning("%s -- MD5 path does not exist: %s", fname, filepath)
        sys.exit(f"Supplied path does not exist: {filepath}")

    if fname.endswith((".ini", ".DS_Store", ".mhl", ".json", ".swp")):
        LOGGER.info("%s -- Skipping as non media file detected.", fname)
        sys.exit(f"Supplied file is not a media file {fname}")

    LOGGER.info("%s -- Processing checksum", fname)
    priref, checksum_val = cid_retrieve(fname)
    if priref == "":
        LOGGER.warning(
            "Failed to match data to a CID Media record. Skipping file %s", fname
        )
        match = check_for_item_record(fname)
        if match is None:
            LOGGER.info(
                "API could not establish match with CID item record and file: %s", fname
            )
        if match is True:
            LOGGER.info("Skipping for now. CID Item record matched to file: %s", fname)
        if match is False:
            LOGGER.warning(
                "CID item record not found that matches file name: %s", fname
            )
            LOGGER.info("Moving MD5 file to subfolder 'unmatched_checksums'")
            # Move to subfolder for missing CID media and item records
            filepath_split = os.path.split(filepath)
            move_path = os.path.join(
                filepath_split[0], "unmatched_checksums/", filepath_split[1]
            )
            try:
                shutil.move(filepath, move_path)
            except FileNotFoundError:
                LOGGER.warning("Could not move filepath, missing: %s", filepath)
        sys.exit("Checksum could not be matched to CID media record")
    if len(checksum_val) == 0:
        LOGGER.info(
            "%s Media record found for associated checksum. Checksum no longer required.",
            fname,
        )
        LOGGER.info("Priref <%s> - Checksum value empty.", priref)
        if len(priref) > 0 and priref.isnumeric():
            LOGGER.info(
                "%s -- Priref retrieved: %s. Writing checksum to record", fname, priref
            )
            # Get checksum data and write to media record notes
            ck_data = read_checksum(filepath)
            if len(ck_data) == 0:
                LOGGER.warning(
                    "%s -- MD5 file is empty, exiting without writing checksum to CID. Deleting MD5.",
                    fname,
                )
                os.remove(filepath)  # MD5 file deletion
                LOGGER.info("===== CHECKSUM CLEAN UP SCRIPT COMPLETE %s =====", fname)
                sys.exit()
            ck_data = str(ck_data[0])
            checksum_data = checksum_split(ck_data)
            md5 = checksum_data[0]
            md5_path = checksum_data[1]
            md5_date = checksum_data[2]
            if "None" in str(md5):
                LOGGER.warning(
                    "%s -- MD5 is 'None', exiting without writing checksum to CID. Deleting MD5.",
                    fname,
                )
                os.remove(filepath)  # MD5 file deletion
                LOGGER.info("===== CHECKSUM CLEAN UP SCRIPT COMPLETE %s =====", fname)
                sys.exit()

            pre_data = f"<adlibXML><recordList><record><priref>{priref}</priref>"
            checksum1 = f"<Checksum><checksum.value>{md5}</checksum.value><checksum.type>MD5</checksum.type>"
            checksum2 = f'<checksum.date>{md5_date}</checksum.date><checksum.path>"{md5_path}"</checksum.path></Checksum>'
            checksum3 = f"<Edit><edit.name>datadigipres</edit.name><edit.date>{str(datetime.datetime.now())[:10]}</edit.date>"
            checksum4 = f"<edit.time>{str(datetime.datetime.now())[11:19]}</edit.time>"
            checksum5 = (
                "<edit.notes>Automated bulk checksum documentation.</edit.notes></Edit>"
            )
            post_data = "</record></recordList></adlibXML>"
            checksum = (
                pre_data
                + checksum1
                + checksum2
                + checksum3
                + checksum4
                + checksum5
                + post_data
            )
            print(checksum)
            try:
                LOGGER.info(
                    "%s -- Attempting to write checksum data to Checksum fields", fname
                )
                record = adlib.post(CID_API, checksum, "media", "updaterecord")
            except Exception as err:
                LOGGER.warning(
                    "%s -- Unable to append checksum to media record %s\n%s",
                    fname,
                    priref,
                    err,
                )

            if record is None:
                LOGGER.warning(
                    "%s -- FAIL: Checksum write to media record! Leaving to attempt again later:\n%s\n%s",
                    fname,
                    checksum,
                    record,
                )
            if md5 in str(record):
                LOGGER.info(
                    "%s -- Successfully written checksum data to Checksum fields! Deleting checksum file",
                    fname,
                )
                os.remove(filepath)
            if "error" in str(record):
                LOGGER.warning(
                    "%s -- FAIL: Checksum write to media record! Leaving to attempt again later:\n%s\n%s",
                    fname,
                    checksum,
                    record,
                )

        else:
            LOGGER.info("%s -- No priref retrieved, skipping", fname)
    elif len(checksum_val) > 20:
        LOGGER.info(
            "SKIPPING: %s Checksum already present in media record but md5 document still exists - deleting",
            checksum_val,
        )
        os.remove(filepath)
        sys.exit("Checksum already exists. Exiting.")
    else:
        LOGGER.info(
            "Failed to match data to a CID Media record %s. Skipping this file.", fname
        )

    LOGGER.info("===== CHECKSUM CLEAN UP SCRIPT COMPLETE %s =====", fname)


if __name__ == "__main__":
    main()
