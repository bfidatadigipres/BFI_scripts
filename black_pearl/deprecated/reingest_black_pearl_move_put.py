#! /usr/bin/env python3

"""
** MUST BE RUN WITH AUTOINGEST PATH AS SYS.ARGV[1] TO TARGET CORRECT PATH **
Script to manage retrieval of Ingest jobs from bp_reingest folders creating
a folder within in this folder called something like ingest_YYYY_MM_DD_HH_MM_SS/

Adds items found top level in bp_reingest to this new ingest folder until
the total size of the folder exceeds upload size, using while loop to control
movement into that folder. When exceeding upload size, the next script action is to take
ingest folder and PUT it using ds3 client and 'put_all_objects_in_directory', and
once complete you iterate returned job ids, and request that a notification is issued
after the PUT is successfully complete using
'put_job_completed_notification_registration_spectra_s3'.

Retrieve job_id for folder once ingest has started (should only ever be one) and use
this to rename the ingesting folder.

Joanna White / Stephen McConnachie
2022
"""

import json
import logging
# Imports
import os
import shutil
import sys
from datetime import datetime, timedelta

import pytz
import yaml
from ds3 import ds3, ds3Helpers

# Setup logging
logger = logging.getLogger("black_pearl_move_reingest")
HDLR = logging.FileHandler(
    os.path.join(os.environ["LOG_PATH"], "black_pearl_move_put_reingest.log")
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)

CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
BUCKET = "imagen"
CONTROL_JSON = os.environ["CONTROL_JSON"]
INGEST_CONFIG = os.environ["INGEST_SIZE"]

# Specific target path
PTH = sys.argv[1]
AUTOINGEST = os.path.join(PTH, "autoingest/bp_reingest/")
JSON_END = os.environ["JSON_END_POINT"]


def check_control():
    """
    Check control json for downtime requests
    """
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j["black_pearl"]:
            logger.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")


def load_yaml(file):
    """Open yaml with safe_load"""
    with open(file) as config_file:
        return yaml.safe_load(config_file)


def get_size(fpath):
    """
    Check the size of given folder path
    return size in kb
    """
    try:
        byte_size = sum(
            os.path.getsize(os.path.join(fpath, f))
            for f in os.listdir(fpath)
            if os.path.isfile(os.path.join(fpath, f))
        )
    except OSError as err:
        logger.warning(
            "get_size(): Cannot reach folderpath for size check: %s\n%s", fpath, err
        )
        byte_size = None

    return byte_size


def check_bp_status(fname):
    """
    Look up filename in BP to avoide
    multiple ingest of files
    """
    query = ds3.HeadObjectRequest(BUCKET, fname)
    result = CLIENT.head_object(query)

    if "DOESNTEXIST" in str(result.result):
        return True


def move_to_ingest_folder(folderpth, file_list, upload_size):
    """
    Runs while loop and moves upto 2TB folder size
    End when 2TB reached or files run out
    """
    remove_list = []
    logger.info(
        "move_to_ingest_folder(): Moving QNAP IMAGEN STORAGE files to %s", folderpth
    )

    for file in file_list:
        status = check_bp_status(file)
        if not status:
            logger.warning(
                "move_to_ingest_folder(): Skipping. File already found in Black Pearl: %s",
                file,
            )
            continue
        fpath = os.path.join(AUTOINGEST, file)
        folder_size = get_size(folderpth)
        if folder_size < upload_size:
            shutil.move(fpath, os.path.join(folderpth, file))
            logger.info(
                "move_to_ingest_folder(): Moved QNAP IMAGEN STORAGE file into new Ingest folder: %s",
                file,
            )
            remove_list.append(file)
        else:
            break

    for f in remove_list:
        if f in file_list:
            file_list.remove(f)
    logger.info(
        "move_to_ingest_folder(): Revised file list in QNAP IMAGEN STORAGE black_pearl_ingest folder: %s",
        file_list,
    )

    return file_list


def create_folderpth():
    """
    Create new folderpth for ingest
    """

    fname = format_dt()
    folderpth = os.path.join(AUTOINGEST, f"ingest_{fname}")
    try:
        os.mkdir(folderpth)
    except OSError as err:
        logger.warning(
            "create_folderpth(): OS error when making directory: %s\n%s", folderpth, err
        )
        folderpth = ""

    return folderpth


def format_dt():
    """
    Return date correctly formatted
    """
    now = datetime.now(pytz.timezone("Europe/London"))
    return now.strftime("%Y-%m-%d_%H-%M-%S")


def check_folder_age(fname):
    """
    Retrieve date time stamp from folder
    Return number of days old
    """
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    dt_str = fname[7:].split("_")
    dt_time = dt_str[1].replace("-", ":")
    new_name = f"{dt_str[0]} {dt_time}.000000"
    dt = datetime.strptime(new_name, fmt)
    now = datetime.strptime(str(datetime.now()), fmt)
    difference = now - dt

    return difference.days


def main():
    """
    Access black_pearl_ingest folder, move items into subfolder
    If subfolder size exceeds 'upload_size', trigger put_dir to send
    the contents to BP in one block. If subfolder doesn't exceed
    'upload_size' (specified by INGEST_CONFIG) leave for next pass.
    """
    # Retrieve an upload size limit in bytes
    upload_size = ""
    data_sizes = load_yaml(INGEST_CONFIG)
    hosts = data_sizes["Host_size"]
    for host in hosts:
        for key, val in host.items():
            if key in AUTOINGEST:
                upload_size = int(val)
    print(f"Upload size: {upload_size} bytes")

    if not upload_size:
        logger.warning("Error retrieving upload size from DPI INGEST yaml")
        sys.exit()

    # Get initial filenames / foldernames
    files = [
        f for f in os.listdir(AUTOINGEST) if os.path.isfile(os.path.join(AUTOINGEST, f))
    ]
    folders = [
        d for d in os.listdir(AUTOINGEST) if os.path.isdir(os.path.join(AUTOINGEST, d))
    ]

    logs = []
    logger.info("======== START Black Pearl ingest QNAP IMAGEN STORAGE START ========")

    # If no files, check for part filled folder first then exit
    if not files:
        for folder in folders:
            check_control()
            folderpth = os.path.join(AUTOINGEST, folder)
            if not folder.startswith("ingest_"):
                continue

            logs.append(f"** Ingest folder found (and no files present): {folderpth}")
            job_list = []
            # Check how old ingest folder is, if over 1 day push anyway
            fname = os.path.split(folderpth)[1]
            days_old = check_folder_age(fname)
            logs.append(f"Folder {folder} is {days_old} days old")
            if days_old >= 0:
                logs.append(
                    f"Ingest folder over {days_old} days old - moving to Black Pearl ingest."
                )
                job_list = put_dir(folderpth)
            else:
                logs.append(
                    "Ingest folder not over 24 hours old. Leaving for more files to be added."
                )
                logger_write(logs)
                continue
            # Rename folder path with job_list so it is bypassed
            if job_list:
                logs.append(
                    f"Job list retrieved for Black Pearl PUT, renaming folder: {job_list}"
                )
                success = pth_rename(folderpth, job_list)
                if not success:
                    logs.append("WARNING! Renaming of folderpath to job id failed.")
                    logs.append(
                        f"WARNING! Please ensure this folder {folderpth} is renamed manually to {job_list}."
                    )
        logs.append("No files or folders remaining to be processed. Script exiting.")
        logs.append("======== END Black Pearl ingest END ========")
        sys.exit()

    while files:
        check_control()
        folderpth = ""
        # Autoingest check for ingest_ path under 2TB
        folders = [
            d
            for d in os.listdir(AUTOINGEST)
            if os.path.isdir(os.path.join(AUTOINGEST, d))
        ]
        for folder in folders:
            if folder.startswith("ingest_"):
                folderpth = os.path.join(AUTOINGEST, folder)
                logs.append(f"** Ingest folder found (and files present): {folderpth}")
                # Only should be needed if renaming fails - may remove
                fsize = get_size(folderpth)
                if fsize < upload_size:
                    logs.append(
                        "Folder will have more files added to reach maximum upload size."
                    )
                else:
                    # Any folders reaching here need a check against BP ingest to see if they've been uploaded and rename failed
                    logs.append(
                        f"WARNING: Skipping ingest folder already over maximum upload size and not renamed: {folderpth}."
                    )
                    logs.append(
                        "WARNING: Please check the contents of this folder are uploaded to BP."
                    )
                    folderpth = ""

        # Create if ingest_ doesn't yet exist
        if not folderpth:
            logs.append("No suitable ingest folder exists, creating new one...")
            folderpth = create_folderpth()

        # Start move to folderpth now identifed
        logs.append(f"Ingest folder selected: {folderpth}")
        files_remaining = move_to_ingest_folder(folderpth, files, upload_size)
        if files_remaining is None:
            logs.append("Problem with folder size extraction in get_size().")
            logger_write(logs)
            continue

        job_list = []
        fsize = get_size(folderpth)
        if fsize > upload_size:
            # Ensure ingest folder is now pushed to black pearl
            logs.append("Starting move of folder path to Black Pearl ingest")
            job_list = put_dir(folderpth)
        else:
            # Check how old ingest folder is, if over 1 day push anyway
            fname = os.path.split(folderpth)[1]
            days_old = check_folder_age(fname)
            logs.append(
                "Folder under min ingest size, checking how long since creation..."
            )
            if days_old >= 1:
                logs.append("Over one day old, moving to Black Pearl ingest")
                job_list = put_dir(folderpth)
            else:
                logs.append("Skipping: Folder not over 1 day old.")
                logger_write(logs)
                continue

        # Rename folder path with job_list so it is bypassed
        if job_list:
            success = pth_rename(folderpth, job_list)
            if not success:
                logs.append("WARNING. Renaming of folderpath to job id failed.")
                logs.append(
                    f"WARNING. Please ensure this folder {folderpth} is renamed manually to {job_list}"
                )

        logs.append(f"Successfully written data to BP. Job list for folder: {job_list}")

        if not files_remaining:
            logs.append(
                "No files remaining in black_pearl_ingest folder, script exiting."
            )

        logs.append("More files to process, restarting move sequence.\n")
        files = files_remaining
        logger_write(logs)

    logs.append("======== END Black Pearl ingest QNAP IMAGEN STORAGE END ========")


def logger_write(logs):
    """
    Output all log messages in a block
    """
    for line in logs:
        if "WARNING" in line:
            logger.warning(line)
        else:
            logger.info(line)


def put_dir(directory_pth):
    """
    Add the directory to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    """
    try:
        put_job_ids = HELPER.put_all_objects_in_directory(
            source_dir=directory_pth,
            bucket=BUCKET,
            objects_per_bp_job=5000,
            max_threads=3,
        )
    except Exception as err:
        logger.error("Exception: %s", err)
        print("Exception: %s", err)
    logger.info("PUT COMPLETE - JOB ID retrieved: %s", put_job_ids)
    job_list = []
    for job_id in put_job_ids:
        job_completed_registration = (
            CLIENT.put_job_completed_notification_registration_spectra_s3(
                ds3.PutJobCompletedNotificationRegistrationSpectraS3Request(
                    notification_end_point=JSON_END, format="JSON", job_id=job_id
                )
            )
        )
        logger.info(
            "Job %s registered for completion notification at %s",
            job_id,
            job_completed_registration.result["NotificationEndPoint"],
        )
        job_list.append(job_id)

    return job_list


def pth_rename(folderpth, job_list):
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
