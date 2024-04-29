#! /usr/bin/env python3

'''
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

Threads hardcoded to 3 per script run / 5000 objects per job

Joanna White / Stephen McConnachie
2022
'''

import os
import sys
import json
import shutil
import logging
from datetime import datetime
import pytz
import yaml
from ds3 import ds3, ds3Helpers

# Global vars
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.environ['CONTROL_JSON']
INGEST_CONFIG = os.environ['INGEST_SIZE']
JSON_END = os.environ['JSON_END_POINT']
DPI_BUCKETS = os.environ.get('DPI_BUCKET')

# Setup logging
log_name = sys.argv[1].replace("/", '_')
logger = logging.getLogger(f'black_pearl_move_put_{sys.argv[1]}')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, f'black_pearl_move_put_{log_name}.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['black_pearl']:
            logger.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def load_yaml(file):
    ''' Open yaml with safe_load '''
    with open(file) as config_file:
        return yaml.safe_load(config_file)


def get_buckets(bucket_collection):
    '''
    Read JSON list return
    key_value and list of others
    '''
    bucket_list = []
    key_bucket = ''

    with open(DPI_BUCKETS) as data:
        bucket_data = json.load(data)
    if bucket_collection == 'bfi':
        for key, value in bucket_data.items():
            if 'preservationbucket' in str(key):
                pass
            elif 'preservation0' in str(key):
                if value is True:
                    key_bucket = key
                bucket_list.append(key)
            elif 'imagen' in str(key):
                bucket_list.append(key)
    else:
        for key, value in bucket_data.items():
            if f"{bucket_collection}0" in str(key):
                if value is True:
                    key_bucket = key
                bucket_list.append(key)

    return key_bucket, bucket_list


def get_size(fpath):
    '''
    Check the size of given folder path
    return size in kb
    '''
    if os.path.isfile(fpath):
        return os.path.getsize(fpath)

    try:
        byte_size = sum(os.path.getsize(os.path.join(fpath, f)) for f in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, f)))
    except OSError as err:
        logger.warning("get_size(): Cannot reach folderpath for size check: %s\n%s", fpath, err)
        byte_size = None

    return byte_size


def check_bp_status(fname, bucket_list):
    '''
    Look up filename in BP buckets
    to avoid multiple ingest of files
    '''

    for bucket in bucket_list:
        query = ds3.HeadObjectRequest(bucket, fname)
        result = CLIENT.head_object(query)
        # Only return false if DOESNTEXIST is missing, eg file found
        if 'DOESNTEXIST' not in str(result.result):
            logger.info("File %s found in Black Pearl bucket %s", fname, bucket)
            return False

    return True


def move_to_ingest_folder(folderpth, upload_size, autoingest, file_list, bucket_list):
    '''
    Runs while loop and moves upto 2TB folder size
    End when 2TB reached or files run out
    '''
    remove_list = []
    print("Move to ingest folder found....")
    logger.info("move_to_ingest_folder(): Moving files to %s", folderpth)

    folder_size = get_size(folderpth)
    max_fill_size = upload_size - folder_size
    for file in file_list:
        if not max_fill_size >= 0:
            logger.info("move_to_ingest_folder(): Folder at capacity. Breaking move to ingest folder.")
            break
        status = check_bp_status(file, bucket_list)
        if not status:
            logger.warning("move_to_ingest_folder(): Skipping. File already found in Black Pearl: %s", file)
            continue
        fpath = os.path.join(autoingest, file)
        file_size = get_size(fpath)
        max_fill_size -= file_size
        shutil.move(fpath, os.path.join(folderpth, file))
        logger.info("move_to_ingest_folder(): Moved file into new Ingest folder: %s", file)
        remove_list.append(file)

    for remove_file in remove_list:
        if remove_file in file_list:
            file_list.remove(remove_file)
    logger.info("move_to_ingest_folder(): Revised file list in Black Pearl ingest folder: %s", file_list)

    return file_list


def create_folderpth(autoingest):
    '''
    Create new folderpth for ingest
    '''

    fname = format_dt()
    folderpth = os.path.join(autoingest, f"ingest_{fname}")
    try:
        os.mkdir(folderpth, mode=0o777)
    except OSError as err:
        logger.warning('create_folderpth(): OS error when making directory: %s\n%s', folderpth, err)
        folderpth = ''

    return folderpth


def format_dt():
    '''
    Return date correctly formatted
    '''
    now = datetime.now(pytz.timezone('Europe/London'))
    return now.strftime('%Y-%m-%d_%H-%M-%S')


def check_folder_age(fname):
    '''
    Retrieve date time stamp from folder
    Return number of days old
    '''
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    dt_str = fname[7:].split('_')
    dt_time = dt_str[1].replace('-', ':')
    new_name = f"{dt_str[0]} {dt_time}.000000"
    date_time = datetime.strptime(new_name, fmt)
    now = datetime.strptime(str(datetime.now()), fmt)
    difference = now - date_time

    return difference.days  # Returns days in integer using timedelta days


def main():
    '''
    Access Black Pearl ingest folders, move items into subfolder
    If subfolder size exceeds 'upload_size', trigger put_dir to send
    the contents to BP bucket in one block. If subfolder doesn't exceed
    'upload_size' (specified by INGEST_CONFIG) leave for next pass.
    '''
    if not sys.argv[1]:
        sys.exit("Missing launch path, script exiting")

    upload_size = fullpath = autoingest = bucket_collection = ''
    if 'netflix' in str(sys.argv[1]):
        fullpath = os.environ['PLATFORM_INGEST_PTH']
        upload_size = 559511627776
        autoingest = os.path.join(fullpath, os.environ['BP_INGEST_NETFLIX'])
        bucket_collection = 'netflix'
    elif 'amazon' in str(sys.argv[1]):
        fullpath = os.environ['PLATFORM_INGEST_PTH']
        upload_size = 559511627776
        autoingest = os.path.join(fullpath, os.environ['BP_INGEST_AMAZON'])
        bucket_collection = 'amazon'
    else:
        # Retrieve an upload size limit in bytes
        data_sizes = load_yaml(INGEST_CONFIG)
        hosts = data_sizes['Host_size']
        for host in hosts:
            for key, val in host.items():
                if str(sys.argv[1]) in key:
                    fullpath = key
                    upload_size = int(val)
        autoingest = os.path.join(fullpath, os.environ['BP_INGEST'])
        bucket_collection = 'bfi'
    print(f"*** Bucket collection: {bucket_collection}")
    print(f"Upload size: {upload_size} bytes")
    print(f"Fullpath: {fullpath} {autoingest}")

    if not os.path.exists(autoingest):
        logger.warning("Complication with autoingest path: %s", autoingest)
        sys.exit('Supplied argument did not match path')
    if not upload_size:
        logger.warning("Error retrieving upload size from DPI INGEST yaml")
        sys.exit()

    # Get current bucket name for bucket_collection type
    bucket, bucket_list = get_buckets(bucket_collection)
    logger.info("Key bucket selected %s, bucket list %s", bucket, bucket_list)
    if 'blobbing' in str(bucket):
        logger.warning("Blobbing bucket selected. Aborting PUT")
        sys.exit()

    # Get initial filenames / foldernames
    files = [f for f in os.listdir(autoingest) if os.path.isfile(os.path.join(autoingest, f))]
    folders = [d for d in os.listdir(autoingest) if os.path.isdir(os.path.join(autoingest, d))]
    if len(files) == 0 and len(folders) >= 1:
        print(f"Files found: {len(files)} - Folders found: {len(folders)}")
        sys.exit()

    logger.info("======== START Black Pearl ingest %s START ========", sys.argv[1])

    # If no files, check for part filled folder first then exit
    if not files:
        for folder in folders:
            check_control()
            folderpth = os.path.join(autoingest, folder)
            if not folder.startswith('ingest_'):
                continue

            logger.info("** Ingest folder found (and no files present): %s", folderpth)
            job_list = []
            # Check how old ingest folder is, if over 1 day push anyway
            fname = os.path.split(folderpth)[1]
            days_old = check_folder_age(fname)
            logger.info("Folder %s is %s days old", folder, days_old)
            if days_old >= 1:
                logger.info("Ingest folder over %s days old - moving to Black Pearl ingest bucket %s.", days_old, bucket)
                job_list = put_dir(folderpth, bucket)
            else:
                logger.info("Ingest folder not over 24 hours old. Leaving for more files to be added.")
                continue
            # Rename folder path with job_list so it is bypassed
            if job_list:
                logger.info("Job list retrieved for Black Pearl PUT, renaming folder: %s", job_list)
                success = pth_rename(folderpth, job_list)
                if not success:
                    logger.warning("Renaming of folderpath to job id failed.")
                    logger.warning("Please ensure this folder %s is renamed manually to %s", folderpth, job_list)
        logger.info("No files or folders remaining to be processed. Script exiting.")
        logger.info("======== END Black Pearl ingest %s END ========", sys.argv[1])
        sys.exit()

    while files:
        check_control()
        folderpth = ''
        # Autoingest check for ingest_ path under 2TB
        folders = [d for d in os.listdir(autoingest) if os.path.isdir(os.path.join(autoingest, d)) and d.startswith("ingest_")]
        if len(folders) >= 1:
            logger.info("One or more ingest folders found. Checking size of each")
            for folder in folders:
                folder_check_pth = os.path.join(autoingest, folder)
                logger.info("** Ingest folder found (and files present): %s", folder_check_pth)
                fsize = get_size(folder_check_pth)
                if fsize < upload_size:
                    logger.info("Folder will have more files added to reach maximum upload size.")
                    folderpth = folder_check_pth
                else:
                    logger.info("Already over maximum upload size, will not add more files: %s", folder_check_pth)

        # If found ingest_ paths not selected for further ingest
        if folderpth == '':
            logger.info("No suitable ingest folder exists, creating new one...")
            folderpth = create_folderpth(autoingest)

        # Start move to folderpth now identified
        logger.info("Ingest folder selected: %s", folderpth)
        print(f"move_to_ingest_folder: {folderpth}, {autoingest}, {files}, {bucket_list}")
        files_remaining = move_to_ingest_folder(folderpth, upload_size, autoingest, files, bucket_list)
        if files_remaining is None:
            logger.info("Problem with folder size extraction in get_size().")
            continue

        job_list = []
        fsize = get_size(folderpth)
        print(f"Folder identified is {fsize} bytes, and upload size limit is {upload_size} bytes")
        if len(os.listdir(folderpth)) == 0:
            logger.info("Script exiting: Folderpath still remains empty after move_to_ingest function: %s", folderpth)
            sys.exit()
        if fsize > upload_size:
            # Ensure ingest folder is now pushed to black pearl
            logger.info("Starting move of folder path to Black Pearl ingest bucket %s", bucket)
            job_list = put_dir(folderpth, bucket)
        else:
            # Check how old ingest folder is, if over 1 day push anyway
            fname = os.path.split(folderpth)[1]
            days_old = check_folder_age(fname)
            logger.info("Folder %s is %s days old.", fname, days_old)
            logger.info("Folder under min ingest size, checking how long since creation...")
            if days_old >= 1:
                logger.info("Over one day old, moving to Black Pearl ingest bucket %s", bucket)
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
                logger.warning("Please ensure this folder %s is renamed manually to %s", folderpth, job_list)

        logger.info("Successfully written data to BP. Job list for folder: %s", job_list)

        if not files_remaining:
            logger.info("No files remaining in Black Pearl ingest folder, script exiting.")

        logger.info("More files to process, restarting move sequence.\n")
        files = files_remaining

    logger.info(f"======== END Black Pearl ingest %s END ========", sys.argv[1])


def put_dir(directory_pth, bucket_choice):
    '''
    Add the directory to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    '''
    try:
        put_job_ids = HELPER.put_all_objects_in_directory(source_dir=directory_pth, bucket=bucket_choice, objects_per_bp_job=5000, max_threads=3)
    except Exception as err:
        logger.error('Exception: %s', err)
        print('Exception: %s', err)
    logger.info("PUT COMPLETE - JOB ID retrieved: %s", put_job_ids)
    job_list = []
    for job_id in put_job_ids:
        # Should always be one, but leaving incase of variation
        job_completed_registration = CLIENT.put_job_completed_notification_registration_spectra_s3(
                ds3.PutJobCompletedNotificationRegistrationSpectraS3Request(notification_end_point=JSON_END, format='JSON', job_id=job_id))
        logger.info('Job %s registered for completion notification at %s', job_id, job_completed_registration.result['NotificationEndPoint'])
        job_list.append(job_id)

    return job_list


def pth_rename(folderpth, job_list):
    '''
    Take folder path and change name for job_list
    '''
    pth = os.path.split(folderpth)[0]
    if len(job_list) > 1:
        logger.warning("More than one job id returned for folder: %s", folderpth)
        foldername = '_'.join(job_list)
    elif len(job_list) == 1:
        foldername = job_list[0]
    else:
        return None

    new_folderpth = os.path.join(pth, foldername)
    os.rename(folderpth, new_folderpth)
    return new_folderpth


if __name__ == "__main__":
    main()
