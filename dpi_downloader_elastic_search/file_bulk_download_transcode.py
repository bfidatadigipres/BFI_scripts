#!/usr/bin/env python3

'''
Looks to database.db for downloadable files
Retrieves items with status 'Requested'
Stores username, email, download path, folder
filename and status.

Checks if the download type is single or bulk:
Before any actions checks that status has not been
updated to 'Cancelled' since first downloaded.

Bulk:
1. Checks Saved Search number is correctly formatted
2. Counts if prirefs listed in saved search exceed
   50, if so exits with warning
3. Iterates priref list get CID media records
   associated with the saved search, saving original
   filename and reference number for each
4. Iterates all reference numbers downloading items
   from Black Pearl
5. Checks each download has MD5 wholefile checksum
   which matches BP ETag
6. Renames file name from reference number to
   original filename if not same, and updates db
   status with 'Downloaded'

Single:
1. Checks filename valid and has CID media record
2. Checks if reference_number matches CID media record
   if yes extracts filename from imagen.media.original_filename
3. Initialises download of item to supplied path
   and updates status to 'Downloading'
4. When download completed, creates checksum and
   checks for match with ETag.
5. Checks if original_fname matches fname, if not renames
   UMID to filename extracted from CID media record
6. If download completes updates item's status field
   in db with 'Downloaded'

7. Checks for transcode status, if None moves to step 9
   If ProRes initiates ProRes transcode and reports back
   if successful.
   If MP4 initiates MP4 transcode and reports back success
8. If transcode completes updates item's status field
   in database.db with:
   - Transcoded
9. Sends notification email to user who requested download
   with unique transcode message when complete.

Joanna White
2023
'''

# Python packages
import os
import sys
import json
import hashlib
import logging
import itertools
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError, NotFoundError, RequestError, TransportError
from ds3 import ds3, ds3Helpers

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
from downloaded_transcode_prores import transcode_mov
from downloaded_transcode_mp4 import transcode_mp4
from downloaded_transcode_mp4_watermark import transcode_mp4_access

# GLOBAL VARIABLES
CID_API = os.environ['CID_API4']
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.environ['CONTROL_JSON']
CODEPTH = os.environ['CODE']
ES_SEARCH = os.environ['ES_SEARCH_PATH']
EMAIL_SENDER=os.environ['EMAIL_SEND']
EMAIL_PSWD=os.environ['EMAIL_PASS']
FMT = '%Y-%m-%d %H:%M:%s'

# CONNECT TO ES
ES = Elasticsearch([ES_SEARCH])

# Set up logging
LOGGER = logging.getLogger('schedule_database_downloader_transcode')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'scheduled_elasticsearch_downloader_transcode.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_elasticsearch():
    '''
    Check ES index connected
    '''
    if ES.ping():
        return True
    else:
        LOGGER.info('Connection to Elasticsearch not found. Script exiting.')
        sys.exit('Connection to Elasticsearch not found. Script exiting.')


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['black_pearl']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')
        if not j['pause_scripts']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def get_media_original_filename(fname):
    '''
    Retrieve the reference_number from CID media record
    '''
    search = f'reference_number="{fname}"'
    fields = [
        'priref',
        'imagen.media.original_filename',
        'preservation_bucket'
    ]
    record = adlib.retrieve_record(CID_API, 'media', search, '0', fields)[1]
    if record is None:
        LOGGER.exception("get_media_original_filename: Unable to match filename to CID media record: %s", fname)
        return None, None, None
    if 'priref' in str(record):
        media_priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    else:
        print(record)
        media_priref = ''
    if 'imagen.media.original_filename' in str(record):
        orig_fname = adlib.retrieve_field_name(record[0], 'imagen.media.original_filename')[0]
    else:
        print(record)
        orig_fname = ''
    if 'preservation_bucket' in str(record):
        bucket = adlib.retrieve_field_name(record[0], 'preservation_bucket')[0]
    else:
        print(record)
        bucket = ''
    if bucket == '':
        bucket = 'imagen'

    return media_priref, orig_fname, bucket


def get_prirefs(pointer):
    '''
    User pointer number and look up
    for list of prirefs in CID
    '''
    query = {'command': 'getpointerfile',
             'database': 'items',
             'number': pointer,
             'output': 'jsonv1'}

    try:
        result = adlib.get(CID_API, query)
    except Exception as exc:
        LOGGER.exception('get_prirefs(): Unable to get pointer file %s\n%s', pointer, exc)
        result = None

    if not result['adlibJSON']['recordList']['record'][0]['hitlist']:
        return None
    prirefs = result['adlibJSON']['recordList']['record'][0]['hitlist']
    LOGGER.info("Prirefs retrieved: %s", prirefs)
    return prirefs


def get_dictionary(priref_list):
    '''
    Iterate list of prirefs and
    collate data
    '''
    print("Launching get_dictionary()")
    data_dict = {}
    for priref in priref_list:
        data = get_media_record_data(priref)
        data_dict[priref] = data
    print(data_dict)
    return data_dict


def get_media_record_data(priref):
    '''
    Get CID media record details
    '''
    print(f"** Launching get_media_record_data() with priref: {priref}")
    search = f'object.object_number.lref="{priref}"'
    fields = [
        'imagen.media.original_filename',
        'reference_number',
        'preservation_bucket'
    ]

    hits, records = adlib.retrieve_record(CID_API, 'media', search, '0', fields)
    if hits is None:
        LOGGER.exception("get_media_record_data: AdlibV3 unable to retrieve data from API with search: %s", search)
        return []
    if records is None:
        LOGGER.exception("get_media_record_data: Unable to match filename to CID media record: %s", priref)
        return []

    print(hits)
    all_files = []
    for num in range(0, hits):
        if 'reference_number' in str(records[num]):
            ref_num = adlib.retrieve_field_name(records[num], 'reference_number')[0]
            print(ref_num)
        else:
            print(records[num])
            ref_num = ''
        if 'imagen.media.original_filename' in str(records[num]):
            orig_fname = adlib.retrieve_field_name(records[num], 'imagen.media.original_filename')[0]
            print(orig_fname)
        else:
            print(records[num])
            orig_fname = ''
        if 'preservation_bucket' in str(records[num]):
            bucket = adlib.retrieve_field_name(records[num], 'preservation_bucket')[0]
            print(orig_fname)
        else:
            print(records[num])
            bucket = ''
        if bucket == '':
            bucket = 'imagen'
        all_files.append({ref_num: [orig_fname, bucket]})

    return all_files


def get_bp_md5(fname, bucket):
    '''
    Fetch BP checksum to compare
    to new local MD5
    '''
    md5 = ''
    query = ds3.HeadObjectRequest(bucket, fname)
    result = CLIENT.head_object(query)
    try:
        md5 = result.response.msg['ETag']
    except Exception as err:
        print(err)
    if md5:
        return md5.replace('"', '')


def make_check_md5(fpath, fname, bucket):
    '''
    Generate MD5 for fpath
    Locate matching file in CID/checksum_md5 folder
    and see if checksums match. If not, write to log
    '''
    download_checksum = ''

    try:
        hash_md5 = hashlib.md5()
        with open(fpath, "rb") as file:
            for chunk in iter(lambda: file.read(65536), b""):
                hash_md5.update(chunk)
        download_checksum = hash_md5.hexdigest()
    except Exception as err:
        print(err)

    bp_checksum = get_bp_md5(fname, bucket)
    print(f"Created from download: {download_checksum} | Retrieved from BP: {bp_checksum}")
    return str(download_checksum).strip(), str(bp_checksum).strip()


def retrieve_requested():
    '''
    Pull data from ES index, Requested status only
    Remove duplicates and sort for oldest first
    '''
    requested_data = []
    search_results = ES.search(index='dpi_downloads', query={'term': {'status': {'value': 'Requested'}}}, size=200)
    for row in search_results['hits']['hits']:
        get_id = [row['_id']]
        record = [(value) for key, value in row['_source'].items()]
        all_items = tuple(record) + tuple(get_id)
        requested_data.append(all_items)
    return remove_duplicates(requested_data)


def check_for_cancellation(user_id):
    '''
    Pull data from ES index for user ID being processed
    Return status update
    '''
    search_results = ES.search(index='dpi_downloads', query={'term': {'_id': {'value': f'{user_id}'}}}, size=200)
    if len(search_results['hits']['hits']) != 1:
        return None

    return search_results['hits']['hits'][0]['_source']['status']


def remove_duplicates(list_data):
    '''
    Sort and remove duplicates
    using itertools
    '''
    list_data.sort()
    print(f"List data: {list_data}")
    grouped = itertools.groupby(list_data)
    unique = [key for key,_ in grouped]
    print(unique)
    return unique


def update_table(user_id, new_status):
    '''
    Update specific ES index with new
    data, for fname match
    '''
    update_request = {
        "doc": {
            "status": f"{new_status}"
        }
    }

    print(update_request)
    print(user_id)
    try:
        index = ES.update(index='dpi_downloads', id=f"{user_id}", body=update_request)
        if index['result'] == 'updated':
            print(f"Record updated with new status {new_status}")
            return True
    except (TransportError, RequestError, NotFoundError, ConflictError) as err:
        print(f"Error code raised: {err}")
        return False


def check_download_exists(download_fpath, orig_fname, fname, transcode):
    '''
    Check if download already exists
    in path, return new filepath and bool
    for download existance
    '''
    skip_download = False
    if str(orig_fname).strip() != str(fname).strip():
        check_pth = os.path.join(download_fpath, orig_fname)
    else:
        check_pth = os.path.join(download_fpath, fname)

    if os.path.isfile(check_pth) and transcode == 'none':
        return None, None
    elif os.path.isfile(check_pth):
        skip_download = True

    if str(orig_fname).strip() != str(fname).strip():
        new_fpath = os.path.join(download_fpath, orig_fname)
    else:
        new_fpath = os.path.join(download_fpath, fname)

    return new_fpath, skip_download


def main():
    '''
    Retrieve 'Requested' rows from database.db as list of
    tuples and process one at a time.
    '''
    data = retrieve_requested()
    print(data)
    print(type(data))
    if len(data) == 0:
        sys.exit('No data found in Elastic Search request')

    LOGGER.info("================ DPI DOWNLOAD REQUESTS RETRIEVED: %s. Date: %s =================", len(data), datetime.now().strftime(FMT)[:19])
    for row in data:
        check_control()
        check_elasticsearch()
        username = row[0].strip()
        email = row[1].strip()
        fname = row[3].strip()
        dtype = row[2].strip()
        dpath = row[4].strip()
        dfolder = row[5].strip()
        transcode = row[6].strip()
        user_id = row[9]
        LOGGER.info("** New data for download:\n\t- ID %s, User %s, email %s, file %s,\n\t- Downloading to %s in %s folder. Transcode? %s", user_id, username, email, fname, dpath, dfolder, transcode)

        # Check if path supplied valid
        if not os.path.exists(dpath):
            LOGGER.warning("Skipping download. Supplied download filepath error: %s", dpath)
            update_table(user_id, 'Download path invalid')
            continue

        download_fpath = os.path.join(dpath, dfolder)
        print(download_fpath)
        if not os.path.exists(download_fpath):
            os.makedirs(download_fpath, 0o777, exist_ok=True)
            LOGGER.info("Download file path created: %s", download_fpath)
        try:
            os.chmod(download_fpath, 0o777)
        except PermissionError as err:
            print(err)

        priref_list = []
        # Single download
        if dtype == 'single':
            # Try to locate CID media record for file
            media_priref, orig_fname, bucket = get_media_original_filename(fname)
            if not media_priref:
                LOGGER.warning("Filename is not recognised, no matching CID Media record")
                update_table(user_id, 'Filename not in CID')
                continue
            if 'netflix' in bucket:
                LOGGER.warning("Filename is a Netflix item and will not be downloaded")
                update_table(user_id, 'Filename not accessible')
                continue
            if 'amazon' in bucket:
                LOGGER.warning("Filename is an Amazon item and will not be downloaded")
                update_table(user_id, 'Filename not accessible')
                continue
            LOGGER.info("Download file request matched to CID file %s media record %s", orig_fname, media_priref)

            # Check if download already exists
            new_fpath, skip_download = check_download_exists(download_fpath, orig_fname, fname, transcode)
            if not new_fpath:
                update_table(user_id, 'Download complete, no transcode required')
                LOGGER.warning("Downloaded file (no transcode) in location already. Skipping further processing.")
                continue
            status_check = check_for_cancellation(user_id)
            LOGGER.info("Checking status remains 'Requested': %s", status_check)
            if status_check == 'Cancelled':
                LOGGER.warning("File download has been cancelled. Skipping further processing.")
                continue

            if not skip_download:
                # Download from BP
                LOGGER.info("Beginning download of file %s to download path", fname)
                update_table(user_id, 'Downloading')
                download_job_id = download_bp_object(fname, download_fpath, bucket)
                if download_job_id == '404':
                    LOGGER.warning("Download of file %s failed. File not found in Black Pearl tape library.", fname)
                    update_table(user_id, 'Filename not found in Black Pearl')
                    continue
                if not download_job_id:
                    LOGGER.warning("Download of file %s failed. Resetting download status and script exiting.", fname)
                    update_table(user_id, 'Requested')
                    continue
                LOGGER.info("Downloaded file retrieved successfully. Job ID: %s", download_job_id)
                if str(orig_fname).strip() != str(fname).strip():
                    LOGGER.info("Updating download UMID filename with item filename: %s", orig_fname)
                    umid_fpath = os.path.join(download_fpath, fname)
                    os.rename(umid_fpath, new_fpath)

                # Apply CHMOD to download
                os.chmod(new_fpath, 0o777)

                # MD5 Verification
                local_md5, bp_md5 = make_check_md5(new_fpath, fname, bucket)
                LOGGER.info("MD5 checksum validation check:\n\t%s - Downloaded file MD5\n\t%s - Black Pearl retrieved MD5", local_md5, bp_md5)
                if local_md5 == bp_md5:
                    LOGGER.info("MD5 checksums match. Updating Download status to Download database")
                else:
                    LOGGER.warning("MD5 checksums DO NOT match. Updating Download status to Download database")
                update_table(user_id, 'Download complete')

            # Transcode
            trans, failed_trans = create_transcode(new_fpath, transcode, fname, user_id)

            # Delete source download from DPI if not failed transcode/already found in path
            if trans == 'no_transcode':
                LOGGER.info("No transcode requested for this asset.")
            elif not skip_download or not failed_trans:
                LOGGER.info("Deleting downloaded asset: %s", new_fpath)
                os.remove(new_fpath)
            # Send notification email
            send_email_update(email, fname, new_fpath, trans)
            continue

        # dtype is DPI browser collection
        elif dtype == 'dpi_browser_collection':
            priref_list = fname.split(',')
            if not isinstance(priref_list, list):
                update_table(user_id, 'Error with DPI collection details')
                LOGGER.warning("Problem seems to have occurred with retrieval of DPI Browser collection details. Skipping.")
                continue
            status_check = check_for_cancellation(user_id)
            LOGGER.info("Checking status remains 'Requested': %s", status_check)
            if status_check == 'Cancelled':
                LOGGER.warning("DPI browser file download has been cancelled. Skipping further processing.")
                continue
        # dtype is bulk
        elif dtype == 'bulk':
            print(f"Finding prirefs from Pointer file with number: {fname}")
            if not fname.isnumeric():
                update_table(user_id, 'Error with pointer file number')
                LOGGER.warning("Bulk download request. Error with pointer file number: %s.", fname)
                continue
            status_check = check_for_cancellation(user_id)
            LOGGER.info("Checking status remains 'Requested': %s", status_check)
            if status_check == 'Cancelled':
                LOGGER.warning("Bulk download has been cancelled. Skipping further processing.")
                continue
            priref_list = get_prirefs(fname)
            if not isinstance(priref_list, list):
                update_table(user_id, 'Pointer file number not recognised')
                LOGGER.warning("Pointer file not recognised.")
                continue
        else:
            LOGGER.info("Download type not recognised, should be 'bulk', 'single' or 'dpi_browser_collection'.")
            continue

        # Process remaining 'bulk' or 'dpi_browser_collection' items as list of CID item prirefs
        files_processed = {}
        if len(priref_list) > 50:
            update_table(user_id, 'Pointer file over 50 CID items')
            LOGGER.warning("Bulk download request. Too many pointer file entries for download maximum of 50: %s.", len(priref_list))
            continue
        LOGGER.info("Bulk download requested with %s item prirefs to process.", len(priref_list))
        pointer_dct = get_dictionary(priref_list)
        if not any(pointer_dct.values()):
            update_table(user_id, 'Pointer file found no digital media records')
            LOGGER.warning("CID item number supplied in pointer file have no associated CID digital media records: %s", pointer_dct)
            continue
        download_failures = []
        for key, value in pointer_dct.items():
            media_priref = key
            print(key)
            LOGGER.info("** Downloading digital items for CID item record %s", media_priref)
            download_dct = value
            print(value)
            for file in download_dct:
                for k, v in file.items():
                    filename = k
                    orig_fname = v[0]
                    bucket = v[1]
                    print(f"Media priref {media_priref} Filename {filename} Original name {orig_fname} in bucket {bucket}")
                    if not len(filename) > 0:
                        LOGGER.warning("Filename is not recognised, no matching CID Media record")
                        continue
                    if 'netflix' in bucket:
                        LOGGER.warning("Filename is a Netflix item and will not be downloaded")
                        update_table(user_id, 'Filename not accessible')
                        continue
                    LOGGER.info("Download file request matched to CID file %s media record %s", orig_fname, media_priref)

                    # Check if download already exists
                    new_fpath, skip_download = check_download_exists(download_fpath, orig_fname, filename, transcode)
                    if not new_fpath:
                        LOGGER.warning("Download path exists and no transcode required. Skipping.")
                        continue
                    if not skip_download:
                        # Download from BP
                        LOGGER.info("Beginning download of file %s to download path", filename)
                        update_table(user_id, f'Downloading {orig_fname}')
                        download_job_id = download_bp_object(filename, download_fpath, bucket)
                        if download_job_id == '404':
                            LOGGER.warning("Download of file %s failed. File not found in Black Pearl tape library.", filename)
                            update_table(user_id, f'Unable to download {filename} in batch')
                            download_failures.append(f"CID media priref: {media_priref} - Filename: {filename}")
                            continue
                        elif not download_job_id:
                            LOGGER.warning("Download of file %s failed. Attempting to download next item in queue", filename)
                            update_table(user_id, f'Unable to download {filename} in batch')
                            download_failures.append(f"CID media priref: {media_priref} - Filename: {filename}")
                            continue
                        LOGGER.info("Downloaded file retrieved successfully. Job ID: %s", download_job_id)
                        if str(orig_fname).strip() != str(filename).strip():
                            LOGGER.info("Updating download UMID filename with item filename: %s", orig_fname)
                            umid_fpath = os.path.join(download_fpath, filename)
                            os.rename(umid_fpath, new_fpath)

                        # Apply CHMOD to download
                        os.chmod(new_fpath, 0o777)

                        # MD5 Verification
                        local_md5, bp_md5 = make_check_md5(new_fpath, filename, bucket)
                        LOGGER.info("MD5 checksum validation check:\n\t%s - Downloaded file MD5\n\t%s - Black Pearl retrieved MD5", local_md5, bp_md5)
                        if local_md5 == bp_md5:
                            LOGGER.info("MD5 checksums match. Updating Download status to Download database")
                        else:
                            LOGGER.warning("MD5 checksums DO NOT match. Updating Download status to Download database")

                    # Transcode
                    trans, failed_trans = create_transcode(new_fpath, transcode, fname, user_id)

                    # Delete source download from DPI if not failed transcode/already found in path
                    if trans == 'no_transcode':
                        LOGGER.info("No transcode requested for this asset.")
                    elif not skip_download or not failed_trans:
                        LOGGER.info("Deleting downloaded asset: %s", new_fpath)
                        os.remove(new_fpath)
                    files_processed[orig_fname] = f"{trans}"
        # Send notification email
        if len(files_processed) == 0 and len(download_failures) > 0:
            LOGGER.warning("Files failed to download: %s", download_failures)
            send_email_failures_bulk(email, download_fpath, download_failures)
            update_table(user_id, "All downloads failed. Please see email for details")
            continue
        LOGGER.info("Files processed: %s", files_processed)
        send_email_update_bulk(email, download_fpath, files_processed, download_failures)
        if len(download_failures) > 0:
            LOGGER.warning("Files failed to process: %s", download_failures)
            update_table(user_id, "Some items failed to download. See email for successful downloads")
        else:
            update_table(user_id, "Bulk download complete. See email for details")

    LOGGER.info("================ DPI DOWNLOAD REQUESTS COMPLETED. Date: %s =================\n", datetime.now().strftime(FMT)[:19])


def download_bp_object(fname, outpath, bucket):
    '''
    Download the BP object from SpectraLogic
    tape library and save to outpath
    '''
    file_path = os.path.join(outpath, fname)
    get_objects = [ds3Helpers.HelperGetObject(fname, file_path)]
    try:
        get_job_id = HELPER.get_objects(get_objects, bucket)
        print(f"BP get job ID: {get_job_id}")
    except Exception as err:
        LOGGER.warning("Unable to retrieve file %s from Black Pearl", fname)
        if 'NotFound[404]: Could not find requested blobs' in str(err):
            get_job_id = '404'
        else:
            get_job_id = None
    return get_job_id


def create_transcode(new_fpath, transcode, fname, user_id):
    '''
    Transcode files depending on supplied
    transcode preference. Output result of attempt
    for email notifications.
    '''
    trans = None
    failed_trans = False
    if transcode == 'prores':
        LOGGER.info("Transcode to ProRes requested, launching ProRes transcode script...")
        update_table(user_id, f'Transcoding {fname} to ProRes')
        success = transcode_mov(new_fpath)
        if success == 'True':
            trans = 'prores'
            update_table(user_id, 'Download and transcode complete')
        else:
            failed_trans = True
            trans = 'Failed prores'
            update_table(user_id, 'Download and transcode failed')
            LOGGER.warning("Failed to complete transcode. Reason: %s", success)
    elif transcode == 'mp4_proxy':
        LOGGER.info("Transcode to MP4 access copy requested, launching MP4 transcode script...")
        update_table(user_id, f'Transcoding {fname} to MP4 proxy')
        success = transcode_mp4(new_fpath)
        if success == 'True':
            trans = 'mp4'
            update_table(user_id, 'Download and transcode complete')
        elif success in ['audio', 'document']:
            trans = 'wrong file'
            update_table(user_id, 'Download and transcode failed')
        elif success == 'exists':
            trans = 'exists'
            update_table(user_id, 'Download and transcode already exist')
        else:
            failed_trans = True
            trans = 'Failed mp4'
            update_table(user_id, 'Download and transcode failed')
            LOGGER.warning("Failed to complete transcode. Reason: %s", success)
    elif 'mp4_access' in transcode:
        if '_watermark' in transcode:
            LOGGER.info("Transcode to MP4 with watermark request. Launching transcode...")
            update_table(user_id, f'Transcoding {fname} to watermark MP4 access')
            watermark = True
        else:
            LOGGER.info("Transcode to MP4 access copy (no watermark). Launching transcode...")
            update_table(user_id, f'Transcoding {fname} to MP4 access')
            watermark = False
        success = transcode_mp4_access(new_fpath, watermark)
        if success == 'not video':
            print("*** NOT VIDEO")
            failed_trans = True
            trans = 'Failed mp4 access'
            update_table(user_id, 'Download and transcode failed (not video)')
            LOGGER.warning("Failed to complete transcode. Reason: Mimetype is not video file: %s", success)
        elif success == 'exists':
            failed_trans = True
            trans = 'Failed mp4 access'
            update_table(user_id, 'Downloaded, MP4 access file exists in path')
            LOGGER.warning("Failed to complete transcode. Reason: MP4 exists in paths: %s", success)
        elif success == 'True' and watermark is True:
            trans = 'mp4_watermark'
            update_table(user_id, 'Download and transcode complete')
        elif success == 'True':
            trans = 'mp4_access'
            update_table(user_id, 'Download and transcode complete')
        else:
            failed_trans = True
            trans = 'Failed mp4 access'
            update_table(user_id, 'Download and transcode failed')
            LOGGER.warning("Failed to complete transcode. Reason: %s", success)
    else:
        trans = 'no_transcode'

    return trans, failed_trans


def send_email_update(email, fname, download_fpath, tran_status):
    '''
    Update user that their item has been
    downloaded, with path, folder and
    filename of downloaded file
    '''
    from email.message import EmailMessage
    import ssl
    import smtplib

    if tran_status == 'prores':
        mssg = "Your transcode to ProRes has completed and replaces your DPI downloaded file above, appended '_prores.mov'."
    elif tran_status == 'mp4':
        mssg = "Your MP4 access copy and image files have been created for the DPI browser, no file will be found in the DPI location stated above."
    elif tran_status == 'Failed prores':
        mssg = "Your transcode to ProRes has failed. Please request this file has a ProRes MOV creating manually."
    elif tran_status == 'Failed mp4':
        mssg = "Your MP4 proxy file request failed. Please request this file has an MP4 proxy and images creating manually."
    elif tran_status == 'Failed mp4 access':
        mssg = "Your MP4 access request failed. Please request this file has an MP4 access copy (with/without watermark) creating manually."
    elif tran_status == 'wrong file':
        mssg = "Your MP4 access copy request failed because the file supplied was not video or image file."
    elif tran_status == 'exists':
        mssg = "Your transcode file already exists in the correct location."
    elif tran_status == 'mp4_watermark':
        mssg = "Your MP4 access copy with watermark has been created and replaces the download file above, appended '_watermark.mp4'."
    elif tran_status == 'mp4_access':
        mssg = "Your MP4 access copy has been created and replaces the download file above, appended '_access.mp4'."
    elif tran_status == 'no_transcode':
        mssg = "No transcode was requested for this download."

    name_extracted = email.split('.')[0]
    subject = 'DPI file download request completed'
    body = f'''
Hello {name_extracted.title()},

Your DPI download request has completed for file:
{fname}

The file was downloaded to the DPI location that you specified:
{download_fpath}

If you selected a download and transcode option for a new file then the downloaded item will have been deleted and only the transcoded file will remain. {mssg}

If there are problems with the file(s), please raise an issue in the BFI Collections Systems Service Desk:
https://bficollectionssystems.atlassian.net/servicedesk/customer/portal/1

This is an automated notification, please do not reply to this email.

Thank you,
Digital Preservation team'''

    send_mail = EmailMessage()
    send_mail['From'] = EMAIL_SENDER
    send_mail['To'] = email
    send_mail['Subject'] = subject
    send_mail.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        try:
            smtp.login(EMAIL_SENDER, EMAIL_PSWD)
            smtp.sendmail(EMAIL_SENDER, email, send_mail.as_string())
            LOGGER.info("Email notification sent to %s", email)
        except Exception as exc:
            LOGGER.warning("Email notification failed in sending: %s\n%s", email, exc)


def send_email_update_bulk(email, download_fpath, files_processed, failure_list):
    '''
    Update user that their item has been
    downloaded, with path, folder and
    filename of downloaded file
    '''
    from email.message import EmailMessage
    import ssl
    import smtplib

    file_list = []
    for key, value in files_processed.items():
        if value == 'prores':
            file_list.append(f"{key}. ProRes transcode completed and is appended '_prores.mov'.")
        elif value == 'mp4':
            file_list.append(f"{key}. MP4 proxy video and images files created. DPI download has been deleted.")
        elif value == 'Failed prores':
            file_list.append(f"{key}. ProRes transcode failed. DPI download left in path for manual transcode.")
        elif value == 'Failed mp4':
            file_list.append(f"{key}. MP4 proxy video and image creation failed. DPI download left in path for manual transcode.")
        elif value == 'Failed mp4 access':
            file_list.append(f"{key}. MP4 video transcode failed. DPI download left in path for manual transcode..")
        elif value == 'wrong file':
            file_list.append(f"{key}. MP4 proxy video and image creation failed. DPI download is not video or image file.")
        elif value == 'exists':
            file_list.append(f"{key}. MP4 proxy video or image file already exists in the correct location.")
        elif value == 'mp4_watermark':
            file_list.append(f"{key}. MP4 watermark transcode completed and appended '_watermark.mp4'.")
        elif value == 'mp4_access':
            file_list.append(f"{key}. MP4 transcode completed and appended '_access.mp4'.")
        elif value == 'no_transcode':
            file_list.append(f"{key}. No transcode was requested for this download.")

    num_failed = len(failure_list)
    name_extracted = email.split('.')[0]
    subject = 'DPI bulk file download request completed'
    data = '\n'.join(file_list)
    body = f'''
Hello {name_extracted.title()},

Your DPI bulk download request has completed for your files. If you selected a download and transcode option for a new file then the downloaded item will have been deleted and only the transcoded file will remain.

There were {num_failed} failed downloads in your bulk request.

The files were downloaded to the DPI location that you specified:
{download_fpath}

{data}

If there are problems with the files, please raise an issue in the BFI Collections Systems Service Desk:
https://bficollectionssystems.atlassian.net/servicedesk/customer/portal/1

This is an automated notification, please do not reply to this email.

Thank you,
Digital Preservation team'''

    send_mail = EmailMessage()
    send_mail['From'] = EMAIL_SENDER
    send_mail['To'] = email
    send_mail['Subject'] = subject
    send_mail.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        try:
            smtp.login(EMAIL_SENDER, EMAIL_PSWD)
            smtp.sendmail(EMAIL_SENDER, email, send_mail.as_string())
            LOGGER.info("Email notification sent to %s", email)
        except Exception as exc:
            LOGGER.warning("Email notification failed in sending: %s\n%s", email, exc)


def send_email_failures_bulk(email, download_fpath, failed_downloads):
    '''
    Update user that their item has failed
    to download, with path, folder and
    filenames of failed items
    '''
    from email.message import EmailMessage
    import ssl
    import smtplib

    name_extracted = email.split('.')[0]
    subject = 'FAILED: DPI bulk file download request'
    data = '\n'.join(failed_downloads)
    body = f'''
Hello {name_extracted.title()},

I'm afraid your DPI bulk download request has failed. None of your items could be downloaded to your specified path:
{download_fpath}

Failed file details:
{data}

Please raise an issue in the BFI Collections Systems Service Desk:
https://bficollectionssystems.atlassian.net/servicedesk/customer/portal/1

This is an automated notification, please do not reply to this email.

Thank you,
Digital Preservation team'''

    send_mail = EmailMessage()
    send_mail['From'] = EMAIL_SENDER
    send_mail['To'] = email
    send_mail['Subject'] = subject
    send_mail.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        try:
            smtp.login(EMAIL_SENDER, EMAIL_PSWD)
            smtp.sendmail(EMAIL_SENDER, email, send_mail.as_string())
            LOGGER.info("Email notification sent to %s", email)
        except Exception as exc:
            LOGGER.warning("Email notification failed in sending: %s\n%s", email, exc)


if __name__ == '__main__':
    main()
