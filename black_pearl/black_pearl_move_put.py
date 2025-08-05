#! /usr/bin/env python3

"""
RETRIEVE PATH NAME SYS.ARGV[1] FROM CRON LAUNCH

Script to manage retrieval of Ingest jobs from Black Pearl ingest
folders and PUT data to Black Pearl tape library

Script actions:
1. Identify supply path and collection for bucket selection
2. Adds items found top level in black_pearl_(netflix_)ingest to dated ingest
   subfolder until the total size of the folder exceeds upload size,
   using while loop to count total subfolder size.
3. When exceeding upload size, the script takes subfolder contents
   and batch PUTs to Black Pearl using ds3 client.
4. Once complete iterate returned job ids, and request that a
   notification JSON is issued to validate PUT success.
5. Use receieved job_id to rename the PUT subfolder.

Notes: Threads hardcoded to 3 per script run / 5000 objects per job

2022
"""

import logging
import os
import shutil
import sys
from datetime import datetime
from typing import Optional

# Local import
import bp_utils as bp
import pytz

sys.path.append(os.environ["CODE"])
import utils

# Global vars
LOG_PATH = os.environ["LOG_PATH"]
CONTROL_JSON = os.environ["CONTROL_JSON"]
INGEST_CONFIG = os.environ["INGEST_SIZE"]

# Setup logging
log_name = sys.argv[1].replace("/", "_")
logger = logging.getLogger(f"black_pearl_move_put_{sys.argv[1]}")
HDLR = logging.FileHandler(
    os.path.join(LOG_PATH, f"black_pearl_move_put_{log_name}.log")
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)


def move_to_ingest_folder(
    folderpth: str,
    upload_size: int,
    autoingest: str,
    file_list: list[str],
    bucket_list: list[str],
) -> list[str]:
    """
    Runs while loop and moves upto 2TB folder size
    End when 2TB reached or files run out
    """
    remove_list = []
    print("Move to ingest folder found....")
    logger.info("move_to_ingest_folder(): Moving files to %s", folderpth)

    folder_size = utils.get_size(folderpth)
    if folder_size is None:
        folder_size = 0
    max_fill_size = upload_size - folder_size
    for file in file_list:
        if ".DS_Store" in file:
            continue
        if not max_fill_size >= 0:
            logger.info(
                "move_to_ingest_folder(): Folder at capacity. Breaking move to ingest folder."
            )
            break
        status = bp.check_no_bp_status(file, bucket_list)
        if status is False:
            print(f"bp.check_no_bp_status: {status}")
            logger.warning(
                "move_to_ingest_folder(): Skipping. File already found in Black Pearl: %s",
                file,
            )
            continue
        fpath = os.path.join(autoingest, file)
        file_size = utils.get_size(fpath)
        if file_size is None:
            file_size = 0
        max_fill_size -= file_size
        shutil.move(fpath, os.path.join(folderpth, file))
        logger.info(
            "move_to_ingest_folder(): Moved file into new Ingest folder: %s", file
        )
        remove_list.append(file)

    for remove_file in remove_list:
        if remove_file in file_list:
            file_list.remove(remove_file)
    logger.info(
        "move_to_ingest_folder(): Revised file list in Black Pearl ingest folder: %s",
        file_list,
    )

    return file_list


def create_folderpth(autoingest: str) -> str:
    """
    Create new folderpth for ingest
    """

    fname = format_dt()
    folderpth = os.path.join(autoingest, f"ingest_{fname}")
    try:
        os.mkdir(folderpth, mode=0o777)
    except OSError as err:
        logger.warning(
            "create_folderpth(): OS error when making directory: %s\n%s", folderpth, err
        )
        folderpth = ""

    return folderpth


def format_dt() -> str:
    """
    Return date correctly formatted
    """
    now = datetime.now(pytz.timezone("Europe/London"))
    return now.strftime("%Y-%m-%d_%H-%M-%S")


def check_folder_age(fname: str) -> int:
    """
    Retrieve date time stamp from folder
    Returns days in integer using timedelta days
    """
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    dt_str = fname[7:].split("_")
    dt_time = dt_str[1].replace("-", ":")
    new_name = f"{dt_str[0]} {dt_time}.000000"
    date_time = datetime.strptime(new_name, fmt)
    now = datetime.strptime(str(datetime.now()), fmt)
    difference = now - date_time

    return difference.days


def main():
    """
    Access Black Pearl ingest folders, move items into subfolder
    If subfolder size exceeds 'upload_size', trigger put_dir to send
    the contents to BP bucket in one block. If subfolder doesn't exceed
    'upload_size' (specified by INGEST_CONFIG) leave for next pass.
    """
    if not sys.argv[1]:
        sys.exit("Missing launch path, script exiting")

    upload_size = fullpath = autoingest = bucket_collection = ""
    if "netflix" in str(sys.argv[1]):
        fullpath = os.environ["PLATFORM_INGEST_PTH"]
        upload_size = 559511627776
        autoingest = os.path.join(fullpath, os.environ["BP_INGEST_NETFLIX"])
        bucket_collection = "netflix"
    elif "amazon" in str(sys.argv[1]):
        fullpath = os.environ["PLATFORM_INGEST_PTH"]
        upload_size = 559511627776
        autoingest = os.path.join(fullpath, os.environ["BP_INGEST_AMAZON"])
        bucket_collection = "amazon"
    else:
        # Retrieve an upload size limit in bytes
        data_sizes = utils.read_yaml(INGEST_CONFIG)
        hosts = data_sizes["Host_size"]
        for host in hosts:
            for key, val in host.items():
                if str(sys.argv[1]) in key:
                    fullpath = key
                    upload_size = int(val)

        if not utils.check_storage(fullpath):
            logger.info("Script run prevented by storage_control.json. Script exiting.")
            sys.exit("Script run prevented by storage_control.json. Script exiting.")
        autoingest = os.path.join(fullpath, os.environ["BP_INGEST"])
        bucket_collection = "bfi"
    print(f"*** Bucket collection: {bucket_collection}")
    print(f"Upload size: {upload_size} bytes")
    print(f"Fullpath: {fullpath} {autoingest}")

    if not os.path.exists(autoingest):
        logger.warning("Complication with autoingest path: %s", autoingest)
        sys.exit("Supplied argument did not match path")
    if not upload_size:
        logger.warning("Error retrieving upload size from DPI INGEST yaml")
        sys.exit()

    # Get current bucket name for bucket_collection type
    bucket, bucket_list = bp.get_buckets(bucket_collection)
    print(f"bp.get_buckets: {bucket} {bucket_list}")
    logger.info("Key bucket selected %s, bucket list %s", bucket, bucket_list)
    if "blobbing" in str(bucket):
        logger.warning("Blobbing bucket selected. Aborting PUT")
        sys.exit()

    # Get initial filenames / foldernames
    files = [
        f for f in os.listdir(autoingest) if os.path.isfile(os.path.join(autoingest, f))
    ]
    folders = [
        d for d in os.listdir(autoingest) if os.path.isdir(os.path.join(autoingest, d))
    ]
    if len(files) == 0 and len(folders) <= 1:
        print(f"Files found: {len(files)} - Folders found: {len(folders)}")
        sys.exit()

    logger.info("======== START Black Pearl ingest %s START ========", sys.argv[1])

    # If no files, check for part filled folder first then exit
    if not files:
        for folder in folders:
            if not utils.check_control("black_pearl"):
                logger.info(
                    "Script run prevented by downtime_control.json. Script exiting."
                )
                sys.exit(
                    "Script run prevented by downtime_control.json. Script exiting."
                )
            folderpth = os.path.join(autoingest, folder)
            if not folder.startswith("ingest_"):
                continue

            logger.info("** Ingest folder found (and no files present): %s", folderpth)
            job_list = []
            # Check how old ingest folder is, if over 1 day push anyway
            fname = os.path.split(folderpth)[1]
            days_old = check_folder_age(fname)
            logger.info("Folder %s is %s days old", folder, days_old)
            if days_old >= 1:
                logger.info(
                    "Ingest folder over %s days old - moving to Black Pearl ingest bucket %s.",
                    days_old,
                    bucket,
                )
                job_list = put_dir(folderpth, bucket)
            else:
                logger.info(
                    "Ingest folder not over 24 hours old. Leaving for more files to be added."
                )
                continue
            # Rename folder path with job_list so it is bypassed
            if job_list:
                logger.info(
                    "Job list retrieved for Black Pearl PUT, renaming folder: %s",
                    job_list,
                )
                success = pth_rename(folderpth, job_list)
                if not success:
                    logger.warning("Renaming of folderpath to job id failed.")
                    logger.warning(
                        "Please ensure this folder %s is renamed manually to %s",
                        folderpth,
                        job_list,
                    )
        logger.info("No files or folders remaining to be processed. Script exiting.")
        logger.info("======== END Black Pearl ingest %s END ========", sys.argv[1])
        sys.exit()

    while files:
        if not utils.check_control("black_pearl"):
            logger.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")
        folderpth = ""
        # Autoingest check for ingest_ path under 2TB
        folders = [
            d
            for d in os.listdir(autoingest)
            if os.path.isdir(os.path.join(autoingest, d)) and d.startswith("ingest_")
        ]
        if len(folders) >= 1:
            logger.info("One or more ingest folders found. Checking size of each")
            for folder in folders:
                folder_check_pth = os.path.join(autoingest, folder)
                logger.info(
                    "** Ingest folder found (and files present): %s", folder_check_pth
                )
                fsize = utils.get_size(folder_check_pth)
                if fsize < upload_size:
                    logger.info(
                        "Folder will have more files added to reach maximum upload size."
                    )
                    folderpth = folder_check_pth
                else:
                    logger.info(
                        "Already over maximum upload size, will not add more files: %s",
                        folder_check_pth,
                    )

        # If found ingest_ paths not selected for further ingest
        if folderpth == "":
            logger.info("No suitable ingest folder exists, creating new one...")
            folderpth = create_folderpth(autoingest)

        # Start move to folderpth now identified
        logger.info("Ingest folder selected: %s", folderpth)
        print(
            f"move_to_ingest_folder: {folderpth}, {autoingest}, {files}, {bucket_list}"
        )
        files_remaining = move_to_ingest_folder(
            folderpth, upload_size, autoingest, files, bucket_list
        )
        if files_remaining is None:
            logger.info("Problem with folder size extraction in get_size().")
            continue

        job_list = []
        fsize = utils.get_size(folderpth)
        print(
            f"Folder identified is {fsize} bytes, and upload size limit is {upload_size} bytes"
        )
        if len(os.listdir(folderpth)) == 0:
            logger.info(
                "Script exiting: Folderpath still remains empty after move_to_ingest function: %s",
                folderpth,
            )
            sys.exit()
        if fsize > upload_size:
            # Ensure ingest folder is now pushed to black pearl
            logger.info(
                "Starting move of folder path to Black Pearl ingest bucket %s", bucket
            )
            job_list = put_dir(folderpth, bucket)
        else:
            # Check how old ingest folder is, if over 1 day push anyway
            fname = os.path.split(folderpth)[1]
            days_old = check_folder_age(fname)
            logger.info("Folder %s is %s days old.", fname, days_old)
            logger.info(
                "Folder under min ingest size, checking how long since creation..."
            )
            if days_old >= 1:
                logger.info(
                    "Over one day old, moving to Black Pearl ingest bucket %s", bucket
                )
                job_list = put_dir(folderpth, bucket)
            else:
                logger.info("Skipping: Folder not over 1 day old.")
                files = None
                continue

        # Rename folder path with job_list so it is bypassed
        if job_list:
            success = pth_rename(folderpth, job_list)
            if not success:
                logger.warning("Renaming of folderpath to job id failed.")
                logger.warning(
                    "Please ensure this folder %s is renamed manually to %s",
                    folderpth,
                    job_list,
                )

        logger.info(
            "Successfully written data to BP. Job list for folder: %s", job_list
        )

        if not files_remaining:
            logger.info(
                "No files remaining in Black Pearl ingest folder, script exiting."
            )

        logger.info("More files to process, restarting move sequence.\n")
        files = files_remaining

    logger.info(f"======== END Black Pearl ingest %s END ========", sys.argv[1])


def put_dir(directory_pth: str, bucket_choice: str) -> list[str]:
    """
    Add the directory to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    """
    try:
        job_list = bp.put_directory(directory_pth, bucket_choice)
        print(f"bp.put_directory: {job_list}")
    except Exception as err:
        logger.error("Exception: %s", err)
        print("Exception: %s", err)
    logger.info("PUT COMPLETE - JOB ID retrieved: %s", job_list)

    if job_list is None:
        job_list = []

    for job_id in job_list:
        confirmation = bp.put_notification(job_id)
        print(f"bp.put_notification: {confirmation}")
        logger.info(
            "Job %s registered for completion notification at %s", job_id, confirmation
        )

    return job_list


def pth_rename(folderpth: str, job_list: list[str]) -> Optional[str]:
    """
    Take folder path and change name for job_list
    """
    pth = os.path.split(folderpth)[0]
    if len(job_list) > 1:
        logger.warning("More than one job id returned for folder: %s", folderpth)
        foldername = "_".join(job_list)
    elif len(job_list) == 1:
        foldername = job_list[0]
    else:
        return None

    new_folderpth = os.path.join(pth, foldername)
    os.rename(folderpth, new_folderpth)
    return new_folderpth


if __name__ == "__main__":
    main()
