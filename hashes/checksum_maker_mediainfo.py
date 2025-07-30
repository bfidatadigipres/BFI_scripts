#!/usr/bin/env /usr/local/bin/python3

"""
** THIS SCRIPT MUST BE LAUNCHED WITHIN SHELL SCRIPT TO PROVIDE PATHS AS SYS.ARGV[1] **
Actions of the script:
1. Checks the path input is legitimate, then stores sys.argv[1] as variable 'filepath'.
2. Checks if file has a checksum already in existence in CHECKSUM_PATH, if yes exits,
   if no or 'None' in checksum continues.
3. Passes the filepath to the md5_65536() function.
    md5(file) chunk size 65536 (found to be fastest):
    i. Opens the input file in read only bytes.
    ii. Splits the file into chunks, iterates through 4096 bytes at a time.
    iii. Returns the MD5 checksum, formatted hexdigest / Returns None if exception raised
4. The MD5 checksum is passed to function that writes it to .md5 file along with path and date
5. 5 Mediainfo reports generated and placed in cid_mediainfo folder
6. tenacity decorators for part 3, 4 and 5 to ensure retries occur until no exception is raised.
7. Write paths for mediainfo files to CSV for management of ingest to CID/deletion

2021
"""

# External Libraries
import datetime
import logging
import os
import sys
from typing import Final, Optional
import tenacity

# Custom Libraries
sys.path.append(os.environ["CODE"])
import utils

# Global variables
LOG_PATH: Final = os.environ["LOG_PATH"]
CODE_PTH: Final = os.environ["CODE_DDP"]
CODE: Final = os.environ["CODE"]
TODAY: Final = str(datetime.date.today())
CONTROL_JSON: Final = os.environ["CONTROL_JSON"]
CHECKSUM_PATH: Final = os.path.join(LOG_PATH, "checksum_md5")
CHECKSUM_PATH2: Final = os.path.join(CODE_PTH, "Logs", "checksum_md5")
MEDIAINFO_PATH: Final = os.path.join(LOG_PATH, "cid_mediainfo")
MEDIAINFO_PATH2: Final = os.path.join(CODE_PTH, "Logs", "cid_mediainfo")

# Setup logging
LOGGER = logging.getLogger("checksum_maker_mediainfo")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "checksum_maker_mediainfo.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def checksum_exist(filename: str, checksum: str, filepath: str) -> str:
    """
    Create a new Checksum file and write MD5_checksum
    Return checksum path where successfully written
    """
    cpath: str = os.path.join(CHECKSUM_PATH, f"{filename}.md5")
    if os.path.isfile(cpath):
        checksum_pth = utils.checksum_write(cpath, checksum, filepath, filename)
    else:
        with open(cpath, "x") as fnm:
            fnm.close()
        checksum_pth = utils.checksum_write(cpath, checksum, filepath, filename)
    if not os.path.isfile(checksum_pth):
        return None

    return checksum_pth


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_output_md5(filepath: str, filename: str) -> Optional[str]:
    """
    Runs checksum generation/output to file as separate function allowing for easier retries
    """
    try:
        md5_checksum: Optional[str] = utils.create_md5_65536(filepath)
        LOGGER.info("%s - MD5 sum generated: %s", filename, md5_checksum)
        if "None" in str(md5_checksum):
            raise Exception
        return md5_checksum
    except Exception as e:
        LOGGER.exception(
            "%s - Failed to make MD5 checksum for %s\n%s", filename, filepath, e
        )
        raise Exception
    else:
        return None


def checksum_test(check: str) -> Optional[bool]:
    """
    Check for 'None' where checksum should be
    """
    try:
        if os.path.exists(os.path.join(CHECKSUM_PATH, check)):
            checksum_pth: str = os.path.join(CHECKSUM_PATH, check)

        with open(checksum_pth, "r") as file:
            line: str = file.readline()
            if line.startswith("None"):
                LOGGER.info("None entry found: %s", check)
                return True

    except Exception as e:
        LOGGER.info(e)
        return None


def main():
    """
    Argument passed from shell launch script to GNU parallel bash with Flock lock
    Decorator for two functions ensures retries if Exceptions raised
    """
    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit("Shell script failed to pass argument to Python script")
    if not utils.check_control("power_off_all"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(sys.argv[1]):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    filepath: str = sys.argv[1]
    filename: str = os.path.split(filepath)[-1]

    LOGGER.info("============ Python3 %s START =============", filepath)
    # Check if MD5 already generated for file using list comprehension
    check: list = [
        f
        for f in os.listdir(CHECKSUM_PATH)
        if f.startswith(filename)
        and not f.endswith(
            (".ini", ".DS_Store", ".mhl", ".json", ".tmp", ".dpx", ".DPX", ".swp")
        )
    ]
    if len(check) > 1:
        sys.exit(f"More than one checksum found with {filename}")

    # Check if existing MD5 starts with 'None'
    if len(check) == 1:
        checksum_present: bool = checksum_test(check[0])
        if checksum_present:
            sys.exit("Checksum already exists for this file, exiting.")

    # Make metadata then write checksum to path as filename.ext.md5
    bpi_path: str = get_bpi_folder(filepath)
    LOGGER.info("Black Pearl Ingest folder identified: %s", bpi_path)
    if not os.path.isfile(filepath):
        filepath: str = utils.local_file_search(bpi_path, filename)

    md5_checksum: Optional[str] = make_output_md5(filepath, filename)
    if md5_checksum is None:
        md5_checksum = make_output_md5(filepath, filename)
    elif "None" in str(md5_checksum):
        md5_checksum = make_output_md5(filepath, filename)

    if os.path.isfile(filepath):
        LOGGER.info("Attempting to make metadata file dumps")
        make_metadata(bpi_path, filepath, filename, MEDIAINFO_PATH)
        success = checksum_exist(filename, md5_checksum, filepath)
        if not success:
            LOGGER.warning("Failed to write checksum to filepath: %s", filepath)
        else:
            LOGGER.info("%s Checksum written to: %s", filename, success)
    else:
        LOGGER.warning("Metadata cannot be made - file absent from path: %s", filepath)

    LOGGER.info("=============== Python3 %s END ==============", filename)


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_metadata(bpi_path: str, fpath: str, fname: str, mediainfo_path: str) -> None:
    """
    Create mediainfo files
    Check before each run that file is still
    in same path, otherwise search in local
    black_pearl_ingest path for new path
    """
    # Run script from media files local directory

    if not os.path.isfile(fpath):
        fpath = utils.local_file_search(bpi_path, fname)
    path1 = utils.mediainfo_create("-f", "TEXT", fpath, mediainfo_path)
    if not os.path.isfile(fpath):
        fpath = utils.local_file_search(bpi_path, fname)
    path2 = utils.mediainfo_create("", "TEXT", fpath, mediainfo_path)
    if not os.path.isfile(fpath):
        fpath = utils.local_file_search(bpi_path, fname)
    path3 = utils.mediainfo_create("", "EBUCore", fpath, mediainfo_path)
    if not os.path.isfile(fpath):
        fpath = utils.local_file_search(bpi_path, fname)
    path4 = utils.mediainfo_create("", "PBCore2", fpath, mediainfo_path)
    if not os.path.isfile(fpath):
        fpath = utils.local_file_search(bpi_path, fname)
    path5 = utils.mediainfo_create("", "XML", fpath, mediainfo_path)
    if not os.path.isfile(fpath):
        fpath = utils.local_file_search(bpi_path, fname)
    path6 = utils.mediainfo_create("-f", "JSON", fpath, mediainfo_path)

    # Return path back to script directory
    LOGGER.info(
        "Written metadata to paths:\n%s\n%s\n%s\n%s\n%s\n%s",
        path1,
        path2,
        path3,
        path4,
        path5,
        path6,
    )


def get_bpi_folder(filepath: str) -> Optional[str]:
    """
    Identify and return the base
    black_pearl_ingest folder to
    aid finding of moved files
    """
    fpath = os.path.split(filepath)[0]
    if "black_pearl_" in fpath.split("/")[-1]:
        return fpath
    else:
        fpath2 = os.path.split(fpath)[0]
        if "black_pearl_" in fpath2.split("/")[-1]:
            return fpath2


if __name__ == "__main__":
    main()
