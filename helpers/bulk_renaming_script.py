#!/usr/bin/env python3

"""
Code to rename files into one of two categories:
- N-123456.ext (correct N_ and add 01of01 partwhole)
- N-123456_01of01.ext (correct N_)

Supply path to files and capture as sys.argv[1]
Move renamed file to local autodetect path
For legacy Collections Search MP4 ingest to DPI

2025
"""

import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Generator

sys.path.append(os.environ.get("CODE"))
import utils

# Setup logging
LOGGER = logging.getLogger("bulk_renaming_script")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "bulk_renaming_script.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# global vars
AUTODETECT = os.path.join(os.environ.get("QNAP_05"), "autoingest/ingest/autodetect/")


def yield_file(fpath: str) -> Generator[str]:
    """
    Iterate through files and
    return one at a time to main
    """
    for root, _, files in os.walk(fpath):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path):
                yield root, file


def main():
    """
    Receive pth to folder
    check file content type
    and rename folders and move
    to local autodetect path
    """
    if len(sys.argv) < 2:
        sys.exit("Please supply path folder. Script exiting")
    fpath = sys.argv[1]
    if not utils.check_storage(fpath) or not utils.check_storage(AUTODETECT):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not os.path.exists(fpath):
        sys.exit(f"Script exiting. Path cannot be found by code: {fpath}")
    LOGGER.info("File path received: %s", fpath)

    for root, file in yield_file(fpath):
        LOGGER.info("** File being processed: %s", file)
        if not file.startswith("N-"):
            LOGGER.warning("Skipping: File does not start N-")
            continue
        part, whole = utils.check_part_whole(file)

        # File has part whole, just needs N- changing
        if isinstance(part, int):
            LOGGER.info(
                f"Part found: {str(part).zfill(2)} Whole found: {str(whole).zfill(2)}"
            )
            fparts = file.split("-")
            new_fname = f"N_{fparts[-1]}"
            new_fpath = os.path.join(root, new_fname)
            success = rename(os.path.join(root, file), new_fpath)
            if success:
                LOGGER.info("File renamed successfully")
                move_success = move_to_autoingest(new_fpath, new_fname)
                LOGGER.info("Moved successfully to autoingest: %s", move_success)
            else:
                LOGGER.warning("File rename failed: %s", os.path.join(root, file))
                continue

        # Part whole may be present but failed
        elif "of" in file.lower():
            LOGGER.warning(
                "Skipping file. Part whole extraction failed for some reason."
            )
            continue

        # No part whole, add one
        elif not part and not whole and "of" not in file.lower():
            LOGGER.info("File needs part whole adding 01of01")
            fsplit1 = file.split("-")
            if not len(fsplit1) == 2:
                LOGGER.warning("Skipping. Too many '-' in filename.")
                continue
            fsplit2 = fsplit1[-1].split(".")
            if not len(fsplit2) == 2:
                LOGGER.warning("Skipping. Too many '.' in filename.")
                continue
            new_fname = f"{fsplit1[0]}_{fsplit2[0]}_01of01.{fsplit2[-1]}"
            LOGGER.info("New file name generated: %s", new_fname)
            new_fpath = os.path.join(root, new_fname)
            success = rename(os.path.join(root, file), new_fpath)
            if success:
                LOGGER.info("File renamed successfully")
                move_success = move_to_autoingest(new_fpath, new_fname)
                LOGGER.info("Moved successfully to autoingest: %s", move_success)
            else:
                LOGGER.warning("File rename failed: %s", os.path.join(root, file))
                continue


def rename(fpath: str, new_fpath: str) -> bool:
    """
    Rename a file with new formatting
    """
    try:
        os.rename(fpath, new_fpath)
        if os.path.isfile(new_fpath):
            return True
    except OSError as err:
        LOGGER.warning(err)

    return False


def move_to_autoingest(fpath: str, file: str) -> str:
    """
    Take fpath, build autoingest path
    then move file to autoingest with check
    and return bool confirmation.
    """
    move_path = os.path.join(AUTODETECT, file)
    try:
        shutil.move(fpath, move_path)
        if os.path.exists(move_path):
            return "Moved to autoingest"
    except shutil.Error as err:
        LOGGER.warning("Move error: %s", err)

    return "Failed to move file."


if __name__ == "__main__":
    main()
