#!/usr/bin/env python3

'''
RETRIEVE PATH NAME SYS.ARGV[1] FROM CRON LAUNCH

Script to manage ingest of items over 1TB in size

Script PUT actions:
1. Identify supply path and collection for blobbing bucket selection
2. Adds item found second level in black_pearl_(netflix_)ingest/blobbing
3. The script takes subfolder contents and PUT to Black Pearl using ds3Helper
   client, and using the blobbing command for items over 1TB
4. Once complete request that a notification JSON is issued to validate PUT success.
5. Use receieved job_id to rename the PUT subfolder.

Script VALIDATE actions:
1. Download items again from Black Pearl into download check folder (to be identified)
2. Checksum generated for downloaded file
3. Checksums compared to ensure that the PUT item is a perfect match
4. Write output to persistence_queue.csv
    'Ready for persistence checking'
5. Create CID media record and link to Item record
    If this fails, the script updates the folder with 'record_failed_' but continues with the rest
    duration 'HH:MM:SS' of media asset -> unknown field
    byte size of media asset -> unknown field
    Move finished filename to autoingest/transcode folder
6. Once completed above move JSON to Logs/black_pearl/completed folder.
   The empty job id folder is deleted if empty, if not prepended 'error_'

2024
'''

import os
import sys
import csv
import json
import glob
import time
import shutil
import logging
from datetime import datetime
from typing import Optional

# Local import
import bp_utils as bp
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Global vars
LOG_PATH = os.environ['LOG_PATH']
CHECKSUM_PATH = os.path.join(LOG_PATH, 'checksum_md5')
CONTROL_JSON = os.environ['CONTROL_JSON']
INGEST_CONFIG = os.path.join(os.environ['CODE'], 'black_pearl/dpi_ingests.yaml')
JSON_END = os.environ['JSON_END_POINT']
DPI_BUCKETS = os.environ.get('DPI_BUCKET')
MEDIA_REC_CSV = os.path.join(LOG_PATH, 'duration_size_media_records.csv')
MEDIAINFO_PATH = os.path.join(LOG_PATH, 'cid_mediainfo')
PERSISTENCE_LOG = os.path.join(LOG_PATH, 'autoingest', 'persistence_queue.csv')
CID_API = os.environ['CID_API4']
TODAY = str(datetime.today())
CODE = os.environ['CODE']

# Setup logging
LOGGER = logging.getLogger(f'black_pearl_move_put_blobbing_{sys.argv[1].replace("/", "_")}')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, f'black_pearl_move_put_blobbing_{sys.argv[1].replace("/", "_")}.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

LOG_PATHS = {os.environ['QNAP_VID']: os.environ['L_QNAP01'],
             os.environ['QNAP_08']: os.environ['L_QNAP08'],
             os.environ['QNAP_10']: os.environ['L_QNAP10'],
             os.environ['QNAP_H22']: os.environ['L_QNAP02'],
             os.environ['GRACK_H22']: os.environ['L_GRACK02'],
             os.environ['QNAP_06']: os.environ['L_QNAP06'],
             os.environ['QNAP_IMAGEN']: os.environ['L_QNAP04'],
             os.environ['QNAP_FILM']: os.environ['L_QNAP03'],
             os.environ['IS_SC']: os.environ['L_IS_SPEC'],
             os.environ['IS_FILM']: os.environ['L_IS_FILM'],
             os.environ['IS_VID']: os.environ['L_IS_VID'],
             os.environ['IS_ING']: os.environ['L_IS_MED'],
             os.environ['IS_AUD']: os.environ['L_IS_AUD'],
             os.environ['IS_DIG']: os.environ['L_IS_DIGI'],
             os.environ['GRACK_F47']: os.environ['L_IS_VID'],
             os.environ['GRACK_FILM']: os.environ['L_GRACK01'],
             os.environ['QNAP_07']: os.environ['L_QNAP07'],
             os.environ['QNAP_09']: os.environ['L_QNAP09'],
             os.environ['QNAP_11']: os.environ['L_QNAP11']
}


def get_buckets(bucket_collection: str) -> str:
    '''
    Read JSON list return
    key_value and list of others
    '''
    key_bucket = ''

    with open(DPI_BUCKETS) as data:
        bucket_data = json.load(data)
    if bucket_collection == 'netflix':
        for key, value in bucket_data.items():
            if 'netflixblobbing' in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == 'amazon':
        for key, value in bucket_data.items():
            if 'amazonblobbing' in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == 'bfi':
        for key, value in bucket_data.items():
            if 'preservationblobbing' in key.lower():
                if value is True:
                    key_bucket = key

    return key_bucket


def make_check_md5(fpath: str, dpath: str, fname: str) -> Optional[tuple[Optional[str], Optional[str]]]:
    '''
    Generate MD5/metadata docs for fpath
    Locate matching file in CID/checksum_md5 folder
    and see if checksums match. If not, write to log
    '''
    download_checksum = ''
    local_checksum = get_md5(fname)
    print(f"Local checksum found: {local_checksum}")
    checksum_path = os.path.join(CHECKSUM_PATH, f"{fname}.md5")
    if not local_checksum:
        try:
            local_checksum = utils.create_md5_65536(fpath)
            print(f"Local checksum created: {local_checksum}")
            utils.checksum_write(checksum_path, local_checksum, fpath, fname)
            make_metadata(fpath, fname, MEDIAINFO_PATH)
        except Exception as err:
            print(err)
    try:
        download_checksum = utils.create_md5_65536(dpath)
        print(f"Downloaded checksum {download_checksum}")
    except Exception as err:
        print(err)

    if len(local_checksum) > 10 and len(download_checksum) > 10:
        print(f"Created from download: {download_checksum} | Original file checksum: {local_checksum}")
        return download_checksum, local_checksum
    return None, None


def make_metadata(fpath: str, fname: str, mediainfo_path: str) -> None:
    '''
    Create mediainfo files
    '''
    # Run script from media files local directory
    path1 = utils.mediainfo_create('-f', 'TEXT', fpath, mediainfo_path)
    path2 = utils.mediainfo_create('', 'TEXT', fpath, mediainfo_path)
    path3 = utils.mediainfo_create('', 'EBUCore', fpath, mediainfo_path)
    path4 = utils.mediainfo_create('', 'PBCore2', fpath, mediainfo_path)
    path5 = utils.mediainfo_create('', 'XML', fpath, mediainfo_path)
    path6 = utils.mediainfo_create('-f', 'JSON', fpath, mediainfo_path)

    LOGGER.info("Written %s metadata to paths:\n%s\n%s\n%s\n%s\n%s\n%s", fname, path1, path2, path3, path4, path5, path6)


def get_md5(filename: str) -> Optional[str]:
    '''
    Retrieve the local_md5 from checksum_md5 folder
    '''
    file_match = [fn for fn in glob.glob(os.path.join(LOG_PATH, 'checksum_md5/*')) if filename in str(fn)]
    if not file_match:
        return None
    filepath = os.path.join(LOG_PATH, 'checksum_md5', f'{filename}.md5')
    print(f"Found matching MD5: {filepath}")

    try:
        with open(filepath) as text:
            contents = text.readline()
            split = contents.split(" - ")
            local_md5 = split[0]
            local_md5 = str(local_md5)
            text.close()
    except (IOError, IndexError, TypeError) as err:
        print(f"FILE NOT FOUND: {filepath}")
        print(err)

    if local_md5.startswith('None'):
        return None
    else:
        return local_md5


def check_for_media_record(fname: str) -> tuple[Optional[str], Optional[str]]:
    '''
    Check if media record already exists
    In which case the file may be a duplicate
    '''

    search = f"imagen.media.original_filename='{fname}'"
    record = adlib.retrieve_record(CID_API, 'media', search, '0', ['priref', 'access_rendition.mp4'])[1]
    if not record:
        LOGGER.exception('CID check for media record failed')
        return None, None
    try:
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    except (KeyError, IndexError):
        priref = ''
    try:
        access_mp4 = adlib.retrieve_field_name(record[0], 'access_rendition.mp4')[0]
    except (KeyError, IndexError):
        access_mp4 = ''

    return priref, access_mp4


def main():
    '''
    Access Black Pearl ingest folders, move items into subfolder
    If subfolder size exceeds 'upload_size', trigger put_dir to send
    the contents to BP bucket in one block. If subfolder doesn't exceed
    'upload_size' (specified by INGEST_CONFIG) leave for next pass.
    '''
    if not sys.argv[1]:
        sys.exit("Missing launch path, script exiting")

    if 'netflix' in str(sys.argv[1]):
        fullpath = os.environ['PLATFORM_INGEST_PTH']
        autoingest = os.path.join(fullpath, f"{os.environ['BP_INGEST_NETFLIX']}/blobbing/")
        download_folder = os.path.join(autoingest, 'download_check/')
        bucket_collection = 'netflix'
    elif 'amazon' in str(sys.argv[1]):
        fullpath = os.environ['PLATFORM_INGEST_PTH']
        autoingest = os.path.join(fullpath, f"{os.environ['BP_INGEST_AMAZON']}/blobbing/")
        download_folder = os.path.join(autoingest, 'download_check/')
        bucket_collection = 'amazon'
    else:
        # Just configuring for BFI ingests >1TB at this time
        data_sizes = utils.read_yaml(INGEST_CONFIG)
        hosts = data_sizes['Host_size']
        for host in hosts:
            for key, val in host.items():
                if str(sys.argv[1]) in key:
                    fullpath = key
        autoingest = os.path.join(fullpath, f"{os.environ['BP_INGEST']}/blobbing")
        download_folder = os.path.join(autoingest, 'download_check/')
        bucket_collection = 'bfi'
        print(f"*** Bucket collection: {bucket_collection}")
        print(f"Fullpath: {fullpath} {autoingest}")

    for key, val in LOG_PATHS.items():
        if key in autoingest:
            wpath = val
    if not os.path.exists(autoingest):
        LOGGER.warning("Complication with autoingest path: %s", autoingest)
        sys.exit('Supplied argument did not match path')

    # Get current bucket name for bucket_collection type
    bucket = get_buckets(bucket_collection)

    # Get initial files as list, exit if none
    files = [f for f in os.listdir(autoingest) if os.path.isfile(os.path.join(autoingest, f))]
    if not files:
        sys.exit()

    LOGGER.info("======== START Black Pearl blob ingest and validation %s START ========", sys.argv[1])

    for fname in files:
        if not utils.check_control('black_pearl') or not utils.check_control('pause_scripts'):
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')
        if '.DS_Store' in fname:
            continue
        if fname.startswith('.'):
            continue
        if fname.endswith(('.txt', '.md5', '.log', '.mhl', '.ini', '.json')):
            continue
        fpath = os.path.join(autoingest, fname)

        # Begin blobbed PUT (bool argument for checksum validation off/on in ds3Helpers)
        tic = time.perf_counter()
        LOGGER.info("Beginning PUT of blobbing file %s", fname)
        put_job_id = bp.put_single_file(fpath, fname, bucket, check=True)
        toc = time.perf_counter()
        checksum_put_time = (toc - tic) // 60
        LOGGER.info("** Total time in minutes for PUT WITH BP hash validation: %s", checksum_put_time)

        # Confirm job list exists
        if not put_job_id:
            LOGGER.warning("JOB list retrieved for file is not correct. %s: %s", fname, put_job_id)
            LOGGER.warning("Skipping further verification stages. Please investigate error.")
            continue
        LOGGER.info("Successfully written data to BP. Job ID for file: %s", put_job_id)

        # Begin retrieval
        delivery_path = os.path.join(download_folder, fname)
        get_job_id = bp.download_bp_object(fname, download_folder, bucket)
        print(f"File downloaded: {delivery_path}")
        if not os.path.exists(delivery_path):
            LOGGER.warning("Skipping: Failed to download file from Black Pearl: %s", delivery_path)
            continue
        LOGGER.info("Retrieved asset again. GET job ID: %s", get_job_id)
        toc2 = time.perf_counter()
        checksum_put_time2 = (toc2 - toc) // 60
        LOGGER.info("** Total time in minutes for retrieval of BP item: %s", checksum_put_time2)

        # Checksum validation
        print("Obtaining checksum for local file and creating one for downloaded file...")
        LOGGER.info("Generating checksum for downloaded file and comparing to existing local MD5.")
        local_checksum, remote_checksum = make_check_md5(fpath, delivery_path, fname)
        print(local_checksum, remote_checksum)
        if local_checksum is None or local_checksum != remote_checksum:
            LOGGER.warning("Checksums absent / do not match: \n%s\n%s", local_checksum, remote_checksum)
            LOGGER.warning("Skipping further actions with this file. Upload failed.")
            LOGGER.warning("Deleting downloaded file to save space: %s", delivery_path)
            os.remove(delivery_path)
            error_folder = os.path.join(autoingest, 'error/')
            os.makedirs(error_folder, mode=0o777, exist_ok=True)
            LOGGER.warning("Moving file to error folder for human assessment")
            shutil.move(fpath, error_folder)
            persistence_log_message("Failed fixity check: checksums do not match", fpath, wpath, fname)
            continue
        LOGGER.info("Checksums match for file >1TB local and stored on Black Pearl:\n%s\n%s", local_checksum, remote_checksum)
        toc3 = time.perf_counter()
        checksum_put_time3 = (toc3 - toc2) // 60
        LOGGER.info("Total time in minutes for checksum creation and comparison: %s", checksum_put_time3)

        # Delete downloaded file and move to further validation checks
        LOGGER.info("Deleting downloaded file: %s", delivery_path)
        os.remove(delivery_path)

        # App size, duration data to CSV
        byte_size = utils.get_size(fpath)
        object_number = utils.get_object_number(fname)
        duration = utils.get_duration(fpath)
        duration_ms = utils.get_ms(fpath)
        if duration or duration_ms:
            LOGGER.info("Duration: %s MS: %s", duration, duration_ms)

        # Handle string returns - back up to CSV
        if not duration:
            duration = ''
        elif 'N/A' in str(duration):
            duration = ''
        if not duration_ms:
            duration_ms = ''
        elif "N/A" in str(duration_ms):
            duration_ms = ''
        if not byte_size:
            byte_size = ''
        if object_number is None:
            object_number = ''

        duration_size_log(fname, object_number, duration, byte_size, duration_ms)

        # Make global log message
        LOGGER.info("Writing persistence checking message to persistence_queue.csv.")
        persistence_log_message("Ready for persistence checking", fpath, wpath, fname)

        # Prepare move path to not include XML/MXF for transcoding
        ingest_path = os.path.split(autoingest)[0]
        root_path = os.path.split(ingest_path)[0]
        if 'black_pearl_netflix_ingest' in autoingest and not fname.endswith(('.mov', '.MOV')):
            move_path = os.path.join(root_path, 'completed', fname)
        elif 'black_pearl_amazon_ingest' in autoingest and fname.endswith(('.mov', '.MOV')):
            move_path = os.path.join(root_path, 'completed', fname)
        else:
            move_path = os.path.join(root_path, 'transcode', fname)

        # Check for Media Record first and clean up file if found
        LOGGER.info("Checking if Media record already exists for file: %s", fname)
        media_priref, access_mp4 = check_for_media_record(fname)
        if media_priref:
            LOGGER.info("Media record %s already exists for file: %s", media_priref, fpath)
            # Check for already deleted message in global.log
            deletion_confirm = utils.check_global_log(fname, 'Successfully deleted file')
            if deletion_confirm:
                LOGGER.info("DELETING DUPLICATE: File has Media record, and deletion confirmation in global.log \n%s", deletion_confirm)
                try:
                    os.remove(fpath)
                    LOGGER.info("Deleted file: %s", fpath)
                except Exception as err:
                    LOGGER.warning("Unable to delete asset: %s %s", fpath, err)
                    LOGGER.warning("Manual inspection of asset required")
            elif not access_mp4:
                persistence_log_message("Persistence checks passed: delete file", fpath, wpath, fname)
                LOGGER.info("File has media record but has no Access MP4. Moving to transcode folder and updating global.log for deletion.")
                # Move to next folder for autoingest deletion - may not be duplicate
                try:
                    shutil.move(fpath, move_path)
                except Exception:
                    LOGGER.warning("MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s", fpath, move_path)
            else:
                LOGGER.warning("Problem with file %s: Has media record but no deletion message in global.log", fpath)
            continue

        # Create CID media record only if all BP checks pass and no CID Media record already exists
        LOGGER.info("No Media record found for file: %s", fname)
        LOGGER.info("Creating media record and linking via object_number: %s", object_number)
        media_priref = create_media_record(object_number, duration, byte_size, fname, bucket)
        print(media_priref)

        if media_priref:
            # Move file to transcode folder
            try:
                shutil.move(fpath, move_path)
            except Exception:
                LOGGER.warning("MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s", fpath, move_path)

            # Make global log message
            LOGGER.info("Writing persistence checking message to persistence_queue.csv.")
            persistence_log_message("Persistence checks passed: delete file", fpath, wpath, fname)
        else:
            LOGGER.warning("File %s has no associated CID media record created.", fname)
            LOGGER.warning("File will be left in folder for manual intervention.")
        toc4 = time.perf_counter()
        whole_put_time = (toc4 - tic) // 60
        LOGGER.info("** Total time for whole process for PUT with BP hash validation: %s", whole_put_time)

    LOGGER.info(f"======== END Black Pearl blob ingest & validation {sys.argv[1]} END ========")


def persistence_log_message(message: str, path: str, wpath: str, file: str) -> None:
    '''
    Output confirmation to persistence_queue.csv
    '''
    datestamp = str(datetime.now())[:19]

    with open(PERSISTENCE_LOG, 'a') as of:
        writer = csv.writer(of)
        writer.writerow([path, message, datestamp])

    if file:
        with open(os.path.join(LOG_PATH, 'persistence_confirmation.log'), 'a') as of:
            of.write(f"{datestamp} INFO\t{path}\t{wpath}\t{file}\t{message}\n")


def duration_size_log(filename: str, ob_num: str, duration: str, size: int, ms: str) -> None:
    '''
    Save outcome message to duration_size_media_records.csv
    '''
    datestamp = str(datetime.now())[:-7]
    written = False

    with open(MEDIA_REC_CSV, 'r') as doc:
        readme = csv.reader(doc)
        for row in readme:
            if filename in str(row):
                written = True

    if not written:
        with open(MEDIA_REC_CSV, 'a') as doc:
            writer = csv.writer(doc)
            writer.writerow([filename, ob_num, str(duration), str(size), datestamp, str(ms)])

##
def create_media_record(ob_num: str, duration: str, byte_size: str, filename: str, bucket: str) -> Optional[str]:
    '''
    Media record creation for BP ingested file
    duration, and byte_size waiting for new fields
    '''

    part, whole = utils.check_part_whole(filename)
    record_data = ([{'input.name': 'datadigipres'},
                    {'input.date': str(datetime.now())[:10]},
                    {'input.time': str(datetime.now())[11:19]},
                    {'input.notes': 'Digital preservation ingest - automated bulk documentation.'},
                    {'reference_number': filename},
                    {'imagen.media.original_filename': filename},
                    {'object.object_number': ob_num},
                    {'imagen.media.part': part},
                    {'imagen.media.total': whole},
                    {'preservation_bucket': bucket}])

    record_data_xml = adlib.create_record_data(CID_API, 'media', '', record_data)
    record = adlib.post(CID_API, record_data_xml, 'media', 'insertrecord')
    if record:
        try:
            media_priref: Optional[str] = adlib.retrieve_field_name(record, 'priref')[0]
            print(f'** CID media record created with Priref {media_priref}')
            LOGGER.info('CID media record created with priref %s', media_priref)
            return media_priref
        except Exception:
            print(f"\nUnable to create CID media record for {ob_num}")
            LOGGER.exception("Unable to create CID media record!")
            return None


if __name__ == "__main__":
    main()
