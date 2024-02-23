#! /usr/bin/env python3

'''
RETRIEVE PATH NAME SYS.ARGV[1] FROM CRON LAUNCH

Script to manage ingest of items over 1TB in size

Script PUT actions:
1. Identify supply path and collection for blobbing bucket selection
2. Adds item found top level in black_pearl_(netflix_)ingest_blobbing to dated ingest
   subfolder - just one per folder.
3. The script takes subfolder contents and PUT to Black Pearl using ds3 client, and
   using the blobbing command for items over 1TB
4. Once complete request that a notification JSON is issued to validate PUT success.
5. Use receieved job_id to rename the PUT subfolder.

Script VALIDATE actions:
1. Download items again from Black Pearl into download check folder (to be identified)
2. Checksum generated for downloaded file
3. Checksums compared to ensure that the PUT item is a perfect match
4. Write output to persistence_queue.csv
    'Ready for persistence checking'
5. Complete a series of BP validation checks including
    ObjectList present, 'AssignedToStorageDomain: true' check, Length match, MD5 checksum match
    Write output to persistence_queue.csv using terms that trigger autoingest deletion
    'Persistence checks passed: delete file'
6. Create CID media record and link to Item record
    If this fails, the script updates the folder with 'record_failed_' but continues with the rest
    duration 'HH:MM:SS' of media asset -> unknown field
    byte size of media asset -> unknown field
    Move finished filename to autoingest/transcode folder
7. Once completed above move JSON to Logs/black_pearl/completed folder.
   The empty job id folder is deleted if empty, if not prepended 'error_'

WIP: Validate functions imported but main() needs extending & duration/size write to CSV need adding

Joanna White
2024
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
JSON_END = os.environ['JSON_END_POINT']
DPI_BUCKETS = os.environ.get('DPI_BUCKET_BLOB')
MEDIA_REC_CSV = os.path.join(LOG_PATH, 'duration_size_media_records.csv')
PERSISTENCE_LOG = os.path.join(LOG_PATH, 'autoingest', 'persistence_queue.csv')
GLOBAL_LOG = os.path.join(LOG_PATH, 'autoingest', 'global.log')
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Setup logging
log_name = sys.argv[1].replace("/", '_')
logger = logging.getLogger(f'black_pearl_move_put_blobbing_{sys.argv[1]}')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, f'black_pearl_move_put_blobbing_{log_name}.log'))
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
    key_bucket = ''

    with open(DPI_BUCKETS_BLOB) as data:
        bucket_data = json.load(data)
    if bucket_collection == 'netflix':
        for key, value in bucket_data.items():
            if bucket_collection in key:
                if value is True:
                    key_bucket = key
    elif bucket_collection == 'bfi':
        for key, value in bucket_data.items():
            if 'preservationblobbing' in key.lower():
                if value is True:
                    key_bucket = key

    return key_bucket


def retrieve_json_data(foldername):
    '''
    Look for matching JSON file
    '''
    json_file = [x for x in os.listdir(JSON_PATH) if str(foldername) in str(x)]
    if json_file:
        return os.path.join(JSON_PATH, json_file[0])


def make_check_md5(fpath, fname):
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

    local_checksum = get_md5(fname)
    print(f"Created from download: {download_checksum} | Retrieved from BP: {local_checksum}")
    return str(download_checksum), str(local_checksum)


def json_check(json_pth):
    '''
    Open json and return value for ObjectsNotPersisted
    Has to be a neater way than this!
    '''
    with open(json_pth) as file:
        dct = json.load(file)
        for k, v in dct.items():
            if k == 'Notification':
                for ky, vl in v.items():
                    if ky == 'Event':
                        for key, val in vl.items():
                            if key == 'ObjectsNotPersisted':
                                return val


def get_file_size(filepath):
    '''
    Retrieve size of path item in bytes
    '''
    return os.path.getsize(filepath)


def check_bp_status(fname, bucket):
    '''
    Look up filename in BP buckets
    to avoid multiple ingest of files
    '''

    query = ds3.HeadObjectRequest(bucket, fname)
    result = CLIENT.head_object(query)
    # Only return false if DOESNTEXIST is missing, eg file found
    if 'DOESNTEXIST' not in str(result.result):
        logger.info("File %s found in Black Pearl bucket %s", fname, bucket)
        return False

    return True


def make_object_num(fname):
    '''
    Receive a filename remove ext,
    find part whole and return as object number
    '''
    name = os.path.splitext(fname)[0]
    name_split = name.split('_')

    if len(name_split) == 3:
        return f"{name_split[0]}-{name_split[1]}"
    elif len(name_split) == 4:
        return f"{name_split[0]}-{name_split[1]}-{name_split[2]}"
    else:
        return None


def get_part_whole(fname):
    '''
    Receive a filename extract part whole from end
    Return items split up
    '''
    name = os.path.splitext(fname)[0]
    name_split = name.split('_')
    if len(name_split) == 3:
        part_whole = name_split[2]
    elif len(name_split) == 4:
        part_whole = name_split[3]
    else:
        part_whole = ''
        return None

    part, whole = part_whole.split('of')
    if part[0] == '0':
        part = part[1]
    if whole[0] == '0':
        whole = whole[1]

    return (part, whole)


def get_ms(filepath):
    '''
    Retrieve duration as milliseconds if possible
    '''
    duration = ''
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        filepath
    ]

    try:
        duration = subprocess.check_output(cmd)
        duration = duration.decode('utf-8')
    except Exception as err:
        logger.info("Unable to extract duration: %s", err)
    if duration:
        return duration.rstrip('\n')
    else:
        return None


def get_duration(filepath):
    '''
    Retrieve duration field if possible
    '''
    duration = ''
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        '-sexagesimal',
        filepath
    ]
    try:
        duration = subprocess.check_output(cmd)
        duration = duration.decode('utf-8')
    except Exception as err:
        logger.info("Unable to extract duration: %s", err)
    if duration:
        return duration.rstrip('\n')
    else:
        return None


def move_to_ingest_folder(folderpth, upload_size, autoingest, file_list, bucket):
    '''
    POSSIBLY NOT NEEDED IF ONE FILE PER INGEST FOLDER
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
        status = check_bp_status(file, bucket)
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


def get_md5(filename):
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
    

def check_for_media_record(fname):
    '''
    Check if media record already exists
    In which case the file may be a duplicate
    '''

    search = f"imagen.media.original_filename='{fname}'"

    query = {'database': 'media',
             'search': search,
             'limit': '0',
             'output': 'json',
             'fields': 'access_rendition.mp4, imagen.media.largeimage_umid'}

    try:
        result = CID.get(query)
    except Exception as err:
        logger.exception('CID check for media record failed: %s', err)
        result = None
    try:
        priref = result.records[0]['priref'][0]
    except (KeyError, IndexError):
        priref = ''
    try:
        access_mp4 = result.records[0]['access_rendition.mp4'][0]
    except (KeyError, IndexError):
        access_mp4 = ''
    try:
        image = result.records[0]['imagen.media.largeimage_umid'][0]
    except (KeyError, IndexError):
        image = ''

    return (priref, access_mp4, image)


def check_global_log(fname):
    '''
    Read global log lines and look for a
    confirmation of deletion from autoingest
    '''
    with open(GLOBAL_LOG, 'r') as data:
        rows = csv.reader(data, delimiter='\t')
        for row in rows:
            if fname in str(row) and 'Successfully deleted file' in str(row):
                print(row)
                return row


def check_global_log_again(fname):
    '''
    Read global log lines and look for a
    confirmation of reingest of file
    '''
    with open(GLOBAL_LOG, 'r') as data:
        rows = csv.reader(data, delimiter='\t')
        for row in rows:
            if fname in str(row) and 'Renewed ingest of file will be attempted' in str(row):
                print(row)
                return row


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
    
    # Just configuring for BFI ingests >1TB at this time
    data_sizes = load_yaml(INGEST_CONFIG)
    hosts = data_sizes['Host_size']
    for host in hosts:
        for key, val in host.items():
            if str(sys.argv[1]) in key:
                fullpath = key
    autoingest = os.path.join(fullpath, os.environ['BP_INGEST_BLOBS'])
    download_folder = os.path.join(fullpath, os.environ['BLOB_DOWNLOAD'])
    bucket_collection = 'bfi'
    print(f"*** Bucket collection: {bucket_collection}")
    print(f"Fullpath: {fullpath} {autoingest}")

    if not os.path.exists(autoingest):
        logger.warning("Complication with autoingest path: %s", autoingest)
        sys.exit('Supplied argument did not match path')

    # Get current bucket name for bucket_collection type
    bucket = get_buckets(bucket_collection)

    # Get initial files as list, exit if none
    files = [f for f in os.listdir(autoingest) if os.path.isfile(os.path.join(autoingest, f))]
    if not files:
        sys.exit()

    logs = []
    logger.info("======== START Black Pearl blob ingest and validation %s START ========", sys.argv[1])

    for fname in files:
        check_control()
        fpath = os.path.join(autoingest, fname)

        # Begin blobbed PUT one item at a time
        job_id = put_file(fname, fpath, bucket)
        
        # Confirm job list exists
        if not job_id:
            logs.append(f"WARNING. JOB list retrieved for file is not correct. {fname}: {job_id}.")
            logs.append("Skipping further verification stages. Please investigate error.")
            logger_write(logs)
            continue
        logs.append(f"Successfully written data to BP. Job list for folder: {job_list}")

        # Begin retrieval
        get_job_id = get_file(fname, download_folder, bucket)
        delivery_path = os.path.join(download_folder, fname)
        if not get_job_id or os.path.exists(delivery_path):
            logs.append(f"Skipping: Failed to download file from Black Pearl: {fname}")
            logger_write(logs)
            continue

        # Checksum validation
        local_checksum, remote_checksum = make_check_md5(fpath, fname)
        if local_checksum != remote_checksum:
            logs.append(f"WARNING: Checksums do not match: \n{local_checksum}\n{remote_checksum}")
            logs.append("Skipping further actions with this file")
            logger_write(logs)
            continue
        logs.append("Checksums match for file >1TB local and stored on Black Pearl:\n{local_checksum}\n{remote_checksum}")

        # Delete downloaded file and move to further validation checks
        logs.append(f"Deleting downloaded file: {delivery_path}")
        os.remove(delivery_path)



    logs.append(f"======== END Black Pearl blob ingest & validation {sys.argv[1]} END ========")


def logger_write(logs):
    '''
    Output all log messages in a block
    '''
    for line in logs:
        if 'WARNING' in line:
            logger.warning(line)
        else:
            logger.info(line)


def put_file(fname, fpath, bucket_choice):
    '''
    Add the directory to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    '''
    file_size = get_file_size(fpath)
    put_objects = [ds3Helpers.HelperPutObject(object_name=f"{fname}", file_path=fpath, size=file_size)]
    put_job_id = HELPER.put_objects(put_objects=put_objects, bucket=bucket_choice, calculate_checksum=True)
    logger.info("PUT COMPLETE - JOB ID retrieved: %s", put_job_id)
    if len(put_job_id) == 36:
        return put_job_id
    return None


def get_file(fname, delivery_path, bucket_choice):
    '''
    Retrieve the file again for checksum
    validation against original
    '''
    get_objects = [ds3Helpers.HelperGetObject(fname, delivery_path)]
    get_job_id = HELPER.get_objects(get_objects, bucket_choice)
    if len(get_job_id) == 36:
        return get_job_id
    return None


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


def get_job_status(job_id):
    '''
    Fetch job status for specific ID
    '''
    size = cached = status = ''

    job_status = CLIENT.get_job_spectra_s3(
                   ds3.GetJobSpectraS3Request(job_id.strip()))

    if job_status.result['CompletedSizeInBytes']:
        size = job_status.result['CompletedSizeInBytes']
    if job_status.result['CachedSizeInBytes']:
        cached = job_status.result['CachedSizeInBytes']
    if job_status.result['Status']:
        status = job_status.result['Status']
    return (status, size, cached)


def get_object_list(fname, bucket):
    '''
    Get all details to check file persisted
    '''

    print(f"Bucket list here in case needed: {bucket}")
    confirmed, md5, length = '', '', ''
    request = ds3.GetObjectsWithFullDetailsSpectraS3Request(name=f"{fname}", include_physical_placement=True)
    try:
        result = CLIENT.get_objects_with_full_details_spectra_s3(request)
        data = result.result
    except Exception as err:
        return None

    if not data['ObjectList']:
        return 'No object list', None, None
    if "'TapeList': [{'AssignedToStorageDomain': 'true'" in str(data):
        confirmed = 'True'
    elif "'TapeList': [{'AssignedToStorageDomain': 'false'" in str(data):
        confirmed = 'False'
    try:
        md5 = data['ObjectList'][0]['ETag']
    except (TypeError, IndexError):
        pass
    try:
        length = data['ObjectList'][0]['Blobs']['ObjectList'][0]['Length']
    except (TypeError, IndexError):
        pass

    return confirmed, md5, length


def persistence_log_message(message, path, wpath, file):
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


def duration_size_log(filename, ob_num, duration, size, ms):
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


def create_media_record(ob_num, duration, byte_size, filename, bucket):
    '''
    Media record creation for BP ingested file
    '''
    record_data = []
    part, whole = get_part_whole(filename)

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

    media_priref = ""

    try:
        i = CUR.create_record(database='media',
                              data=record_data,
                              output='json',
                              write=True)
        if i.records:
            try:
                media_priref = i.records[0]['priref'][0]
                print(f'** CID media record created with Priref {media_priref}')
                logger.info('CID media record created with priref %s', media_priref)
            except Exception:
                logger.exception("CID media record failed to retrieve priref")
    except Exception:
        print(f"\nUnable to create CID media record for {ob_num}")
        logger.exception("Unable to create CID media record!")

    return media_priref


if __name__ == "__main__":
    main()
