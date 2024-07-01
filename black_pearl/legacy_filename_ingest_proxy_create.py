#!/usr/bin/env python3

'''
Launched from shell script to multiple jobs for parallel processing

1. Receive file path and validate is a file/exists
2. Take filename and look up in CSV_PATH, if not there/no priref skip.
3. If priref associated with filename, check CID for item record
   and extract item_type, file_type, code_type, imagen.media.original_filename:
   a. If item_type = Digital, file_type = MP4, code_type matches file, and i.m.o_f is empty:
    - Ingest MP4 to DPI, create new CID media record
    - Create JPEG images thumbnail/largeimage
    - Copy MP4 with correct filename to proxy path if codec compatible (if not encode to H.264?)
    - Write access rendition values to new CID media record
    - Move MP4 to completed folder for deletion
   b. If i_t = Digital, f_t = non MP4, i.m.o_f is populated:
    - Check CID media record for access rendition values
    - If present, no further actions move MP4 to 'already_ingested' folder
    - If not present then create JPEG images and move MP4 to proxy path if codec compatible (if not encode to H.264?)
    - Update existing CID digital media record with access rendition vals
   c. If i_t is not Digital
    - Skip with note in logs, as CID item record data is inaccruate
   d. If i_t = Digital, f_t is empty:
    - Skip with note in logs, that CID item record not sufficient

This script needs to:
- BP Put of individual items
- BP validation of said individual item
- Create CID Digital Media record
- Transcode old MP4 codec types to H.264? (JMW to ask)
  Answer: if codec != 'avc' then push to folder for 'review' 

- Create JPEG file/thumbnail/largeimage from blackdetected MP4 scan
- Append data to CID media records

NOTE: File naming convention shifts with source file list, ingested items and MP4 access copies. 
The script should be agnostic to name types and possibly match N 123456 01of01 and possibly force
'N' and extensions to ext.UPPER() for matching, etc.

Files not to be handled via the regular autoingest/black_pearl scripting
and therefore not to use the autoingest folder structures.

Joanna White
2024
'''

# Global imports
import os
import sys
import json
import pandas
import shutil
import hashlib
import logging
import requests
import datetime
import subprocess
from ds3 import ds3, ds3Helpers

# Private imports
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global paths
QNAP = os.environ['QNAP_REND1']
PROXY_QNAP = os.environ['MP4_ACCESS2']
FILE_PATH = os.path.join(QNAP, 'filename_updater/')
COMPLETED = os.path.join(FILE_PATH, 'completed/')
INGEST = os.path.join(FILE_PATH, 'for_ingest/')
PROXY_CREATE = os.path.join(FILE_PATH, 'proxy_create/')
CSV_PATH = os.path.join(os.environ['ADMIN'], 'legacy_MP4_file_list.csv')
LOG_PATH = os.environ['LOG_PATH']
CID_API = os.environ['CID_API4']
DPI_BUCKETS = os.environ.get('DPI_BUCKET')
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
JSON_END = os.environ['JSON_END_POINT']

# Setup logging
LOGGER = logging.getLogger('legacy_filename_updater')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'legacy_filename_updater.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check control_json isn't False
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['autoingest']:
            print('* Exit requested by downtime_control.json. Script exiting')
            sys.exit('Exit requested by downtime_control.json. Script exiting')


def read_csv_match_file(file):
    '''
    Make set of all entries
    with title as key, and value
    to contain all other entries
    as a list (use pandas)
    '''

    data = pandas.read_csv(CSV_PATH)
    data_dct = data.to_dict(orient='list')
    length = len(data_dct['fname'])

    for num in range(1, length):
        if file in data_dct['fname'][num]:
            return data_dct['priref'][num], data_dct['ob_num'][num]


def check_cid_record(priref, file):
    '''
    Search for ob_num of file name
    and check MP4 in file_type for
    returned record
    '''

    search = f"priref='{priref}'"
    fields = [
        'item_type',
        'file_type',
        'imagen.media.original_filename',
        'reference_number',
        'code_type'
    ]
    record = adlib.retrieve_record(CID_API, 'items', search, '0', fields)[1]
    if not record:
        print(f"Unable to retrieve CID Item record {priref}")

    item_type = file_type = original_fname = ref_num = code_type = ''
    if 'item_type' in str(record[0]):
        item_type = adlib.retrieve_field_name(record[0], 'item_type')[0]
    if 'file_type' in str(record[0]):
        file_type = adlib.retrieve_field_name(record[0], 'file_type')[0]
    if 'imagen.media.original_filename' in str(record[0]):
        original_fname = adlib.retrieve_field_name(record[0], 'imagen.media.original_filename')[0]
    if 'reference_number' in str(record[0]):
        ref_num = adlib.retrieve_field_name(record[0], 'reference_number')[0]
    if 'code_type' in str(record[0]):
        code_type = adlib.retrieve_field_name(record[0], 'code_type')[0]

    return item_type, file_type, original_fname, ref_num, code_type


def check_media_record(fname):
    '''
    Check if CID media record
    already created for filename
    '''
    search = f"imagen.media.original_filename='{fname}'"
    fields = [
        'imagen.media.hls_umid',
        'access_rendition.mp4',
        'input.date'
    ]
    record = adlib.retrieve_record(CID_API, 'media', search, '0', fields)[1]
    if not record: 
        print(f"Unable to retrieve CID Media record for item {fname}")

    priref = media_hls = access_mp4 = input_date = ''
    if 'priref' in str(record[0]):
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    if 'imagen.media.hls_umid' in str(record[0]):
        media_hls = adlib.retrieve_field_name(record[0], 'imagen.media.hls_umid')[0]
    if 'access_rendition.mp4' in str(record[0]):
        access_mp4 = adlib.retrieve_field_name(record[0], 'access_rendition.mp4')[0]
    if 'input.date' in str(record[0]):
        input_date = adlib.retrieve_field_name(record[0], 'input.date')[0]

    return priref, media_hls, access_mp4, input_date


def check_bp_status(fname, bucket_list, local_md5):
    '''
    Look up filename in BP to avoid
    multiple ingests of files
    '''

    for bucket in bucket_list:
        query = ds3.HeadObjectRequest(bucket, fname)
        result = CLIENT.head_object(query)

        if 'DOESNTEXIST' in str(result.result):
            continue

        try:
            md5 = result.response.msg['ETag']
            length = result.response.msg['Content-Length']
            if int(length) > 1 and md5.strip() == local_md5:
                return True
        except (IndexError, TypeError, KeyError) as err:
            print(err)


def get_buckets(bucket_collection):
    '''
    Read JSON list return
    key_value and list of others
    '''
    bucket_list = []
    key_bucket = ''

    with open(DPI_BUCKETS) as data:
        bucket_data = json.load(data)
    if bucket_collection == 'netflix':
        for key, value in bucket_data.items():
            if bucket_collection in key and 'bucket' not in key:
                if value is True:
                    key_bucket = key
                bucket_list.append(key)
    elif bucket_collection == 'bfi':
        for key, value in bucket_data.items():
            if 'preservation' in key.lower() and 'bucket' not in key.lower():
                if value is True:
                    key_bucket = key
                bucket_list.append(key)
            # Imagen path read only now
            if 'imagen' in key:
                bucket_list.append(key)

    return key_bucket, bucket_list


def get_media_ingests(object_number):
    '''
    Use object_number to retrieve all media records
    '''
    search = f'object.object_number="{object_number}"'
    record = adlib.retrieve_record(CID_API, 'media', search, '0', ['imagen.media.original_filename'])[1]
    if not record:
        return None

    original_filenames = []
    for rec in record:
        if 'imagen.media.original_filename' in str(rec):
            filename = adlib.retrieve_field_name(rec, 'imagen.media.original_filename')[0]
            print(f"File found with CID record: {filename}")
            original_filenames.append(filename)

    return original_filenames


def check_codec(fpath):
    '''
    Check MP4 file codec is supported
    otherwise initiate transcode
    '''
    cmd = [
        'mediainfo', '--Language=raw',
        '--Output=Video;%CodecID%',
        fpath
    ]
    codec_id = subprocess.check_output(cmd)
    codec_id = codec_id.decode('utf-8')

    return codec_id


def correct_filename(fname):
    '''
    Correct any strange filename anomalies
    '''
    name_data = fname.split('_')

    if len(name_data) == 1 and 'of' not in fname:
        part_whole = '01of01'
        new_fname = f'{name_data[0].replace('-', '_')}_{part_whole}'
        return new_fname

    if len(name_data) == 2:
        filenum, part_whole = name_data
        if '-' in filenum:
            filenum.replace('-', '_')
        if 'of' in part_whole:
            new_fname = f'{filenum}_{part_whole}'
            return new_fname

    if len(name_data) == 3:
        filenum, additional_data1, additional_data2 = name_data
        if '-' in filenum:
            filenum.replace('-', '_')
        if 'of' in additional_data2 and additional_data1.isnumeric():
            new_fname = f'{filenum}_{additional_data1}_{additional_data2}'
            return new_fname

    LOGGER.warning("Skipping: File name has anomalies: %s", fname)
    return None


def md5_65536(fpath):
    '''
    Hashlib md5 generation, return as 32 character hexdigest
    '''
    try:
        hash_md5 = hashlib.md5()
        with open(fpath, "rb") as fname:
            for chunk in iter(lambda: fname.read(65536), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    except Exception:
        LOGGER.exception("%s - Unable to generate MD5 checksum", fpath)
        return None


def main():
    '''
    Find all files in filename_updater/ folder
    and call up CID to identify whether assets
    need ingesting or access copy work
    '''

    files = [ x for x in os.listdir(FILE_PATH) if os.path.isfile(os.path.join(FILE_PATH, x)) ]
    if not files:
        sys.exit()

    LOGGER.info("============== Legacy filename updater START ==================")
    LOGGER.info("Files located in filename_updated/ folder: %s", ', '.join(files))

    for file in files:
        LOGGER.info("Processing file: %s", file)
        fpath = os.path.join(FILE_PATH, file)
        fname = file.split('.')[0]

        # Find match in CSV
        match_dict = read_csv_match_file(file)
        if match_dict is None:
            LOGGER.warning("File not found in CSV: %s", file)
            continue
        if '#VALUE!' in str(match_dict):
            LOGGER.warning("Skipping: Priref or object_number value missing for file %s", file)
            continue

        object_number, priref = match_dict

        # Look up CID item record for file_type = MP4
        if len(object_number) == 0:
            LOGGER.warning("Skipping. Object number couldn't be created from file renaming.")
            continue
        item_type, file_type, original_fname, ref_num, code_type = check_cid_record(object_number, file)
        if not item_type or file_type:
            LOGGER.warning("Skipping. No CID item record found for object number %s", object_number)
            continue
        if item_type != 'DIGITAL':
            LOGGER.warning("Skipping. Incorrect CID item record attached to MP4 file, not DIGITAL item_type")
            continue
        LOGGER.info("CID item record found with MP4 file-type for %s - codec type %s", object_number, code_type)

        # Begin assessment of actions
        ingest = proxy = False
        media_priref, access_hls, access_mp4, input_date = check_media_record(original_fname)
        if media_priref:
            LOGGER.warning('CID Media record already exists - no ingest needed for file %s', file)
            if len(access_mp4) == 0:
                proxy = True
            else:
                LOGGER.info("MP4 has CID media record with access_mp4 populated. No action necessary.")
                # JMW to ask: Move files to a 'ingest_check_review' folder
                continue
        elif file_type == 'MP4' and original_fname == '':
            ingest = True
            proxy = True
        elif file_type == 'MP4' and len(original_fname) > 3:
            if len(access_hls) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_hls)
            elif len(access_mp4) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_mp4)
            else:
                LOGGER.info("No access copy found for file. Moving MP4 for proxy")
                proxy = True
        elif file_type != 'MP4' and len(original_fname) > 3:
            LOGGER.info("File type for CID item record is not MP4: %s", file_type)
            if len(access_hls) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_hls)
            elif len(access_mp4) > 3:
                LOGGER.info("File has ingested to DPI and has MP4 HLS file: %s", access_mp4)
            else:
                LOGGER.info("No access copy found for file. Moving MP4 for proxy")
                proxy = True
        elif file_type != 'MP4' and original_fname == '':
            # JMW to ask: Move to a folder for 'cid_documentation_review' folder
            shutil.move(here)
            continue
        else:
            continue

        # JMW to ask: We will always force underscore at start N-123456_01of01.mp4
        # Prepare new filename and path formatting (N_123456_01of01, from N-123456)
        new_file = correct_filename(fname)
        if not new_file:
            LOGGER.warning("Could not parse file name. Skipping this item %", fpath)
            continue
        local_checksum = md5_65536(fpath)

        # Start ingest
        if ingest:
            LOGGER.info("MP4 file selected for ingest. Checking if already in Black Pearl: %s", file)
            bucket, bucket_list = get_buckets('bfi')
            check_for_ingest = check_bp_status(ref_num, bucket_list)
            if check_for_ingest is True:
                LOGGER.warning("Reference number found in Black Pearl library. Skipping ingest: %s %s", ref_num, priref)
                continue
            LOGGER.warning("Beginning PUT of file to Black Pearl tape library bucket: %s", bucket)
            job_id = put_file(fpath, ref_num, bucket)
            if not job_id:
                LOGGER.warning("PUT for file %s has failed. Skip all further stages", file)
                continue

            LOGGER.info("Check file has persisted using reference_number: %s", ref_num)
            confirm_persisted = check_bp_status(ref_num, bucket_list, local_checksum)
            if not confirm_persisted:
                LOGGER.warning("PUT for file %s has failed. Skip all further stages", file)
                continue
            LOGGER.info("Confirmation of successful PUT to Black Pearl")

            # Create CID media record
            byte_size = os.path.getsize(fpath)
            media_priref = create_media_record(object_number, duration, byte_size, file, bucket)
            if not media_priref:
                continue

        # Start MP4 check/JPEG creation
        if proxy:
            LOGGER.info("Proxy files required for MP4 asset: %s", file)
            LOGGER.info("MP4 file has MP4 codec: %s", check_codec(fpath))

            # Build path to proxies and new file names
            date_path = input_date[:8].replace('-', '')
            proxy_path = os.path.join(PROXY_QNAP, f'{date_path}/')
            mp4_proxy = os.path.join(proxy_path, f"{new_file}")
            jpeg_path = os.path.join(proxy_path, f"{new_file}.jpg")
            if not os.path.exists(os.path.join(proxy_path, mp4_proxy)):
                LOGGER.info("Cannot find proxy MP4 file in correct path: %s", os.path.join(proxy_path, mp4_proxy))
            
            duration = get_duration(fpath)
            blackdetect_data = get_blackdetect(fpath)
            seconds_for_jpeg = adjust_seconds(duration, blackdetect_data)

            success = get_jpeg(seconds_for_jpeg, fpath, jpeg_path)
            if not success:
                LOGGER.warning("Exiting: JPEG not created from MP4 file")
                continue

            # Generate Full size 600x600, thumbnail 300x300
            full_jpeg = make_jpg(jpeg_path, 'full', None, None)
            thumb_jpeg = make_jpg(jpeg_path, 'thumb', None, None)
            LOGGER.info("New images created at {seconds_for_jpeg} seconds into video:\n - %s\n - %s", full_jpeg, thumb_jpeg)
            if os.path.isfile(full_jpeg) and os.path.isfile(thumb_jpeg):
                os.remove(jpeg_path)
            else:
                LOGGER.warning("One of the JPEG images hasn't created, please check outpath: %s", jpeg_path)

            # Move MP4 file to final location
            shutil.move(fpath, mp4_proxy)

            # Post MPEG/JPEG creation updates to Media record
            media_data = []
            if os.path.isfile(full_jpeg):
                full_jpeg_file = os.path.splitext(full_jpeg)[0]
                print(full_jpeg, full_jpeg_file)
                os.replace(full_jpeg, full_jpeg_file)
                os.chmod(full_jpeg_file, 0o777)
                media_data.append(f"<access_rendition.largeimage>{os.path.split(full_jpeg_file)[1]}</access_rendition.largeimage>")
            if os.path.isfile(thumb_jpeg):
                thumb_jpeg_file = os.path.splitext(thumb_jpeg)[0]
                os.replace(thumb_jpeg, thumb_jpeg_file)
                os.chmod(thumb_jpeg_file, 0o777)
                media_data.append(f"<access_rendition.thumbnail>{os.path.split(thumb_jpeg_file)[1]}</access_rendition.thumbnail>")
            if mp4_proxy:
                media_data.append(f"<access_rendition.mp4>{mp4_proxy}</access_rendition.mp4>")
                os.chmod(mp4_proxy, 0o777)
            LOGGER.info("Writing UMID data to CID Media record: {media_priref}")

            success = cid_media_append(file, media_priref, media_data)
            if success:
                LOGGER.info("JPEG/HLS filename data updated to CID media record")
            else:
                LOGGER.warning("Problem writing UMID data to CID media record: {media_priref}")
                continue

        if os.path.isfile(fpath):
            # Move to a completed location to avoid repeat processing - path to be confirmed
            pass

    LOGGER.info("============== Legacy filename updater END ====================")


def get_duration(fullpath):
    '''
    Retrieves duration information via mediainfo
    where more than two returned, file longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    '''

    cmd = [
        'mediainfo', '--Language=raw',
        '--Full', '--Inform="Video;%Duration%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    duration = subprocess.check_output(cmd)
    if not duration:
        return ''

    duration = duration.decode('utf-8').rstrip('\n')
    print(f"Mediainfo seconds: {duration}")

    if '.' in duration:
        duration = duration.split('.')

    if isinstance(duration, str):
        second_duration = int(duration) // 1000
        return second_duration
    elif len(duration) == 2:
        print("Just one duration returned")
        num = duration[0]
        second_duration = int(num) // 1000
        return second_duration
    elif len(duration) > 2:
        print("More than one duration returned")
        dur1 = f"{duration[0]}"
        dur2 = f"{duration[1][6:]}"
        print(dur1, dur2)
        if int(dur1) > int(dur2):
            second_duration = int(dur1) // 1000
            return (second_duration, '0')
        elif int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return (second_duration, '1')


def get_blackdetect(fpath):
    '''
    Capture black sections of MP4 file
    to dictionary and avoid in JPEG creation
    '''
    ffmpeg_cmd = [
        'ffmpeg', '-i',
        fpath, '-vf', 
        'blackdetect=d=0.05:pix_th=0.10',
        '-an', '-f', 'null', '-'
    ]

    try:
        data = subprocess.run(ffmpeg_cmd, shell=False, check=True, universal_newlines=True, stderr=subprocess.PIPE).stderr
        return data
    except Exception as err:
        LOGGER.warning("FFmpeg command failed: {ffmpeg_call_neat}")
        print(err)


def adjust_seconds(duration, data):
    '''
    Adjust second durations within
    FFmpeg detected blackspace
    '''
    blist = retrieve_blackspaces(data)
    print(f"*** BLACK GAPS: {blist}")
    if not blist:
        return duration // 2

    secs = duration // 4
    clash = check_seconds(blist, secs)
    if not clash:
        return secs

    for num in range(2, 5):
        frame_secs = duration // num
        clash = check_seconds(blist, frame_secs)
        if not clash:
            return frame_secs

    if len(blist) > 2:
        first = blist[1].split(' - ')[1]
        second = blist[2].split(' - ')[0]
        frame_secs = int(first) + (int(second) - int(first)) // 2
        if int(first) < frame_secs < int(second):
            return frame_secs

    return duration // 2


def check_seconds(blackspace, seconds):
    '''
    Create range and check for second within
    '''
    clash = []
    for item in blackspace:
        start, end = item.split(" - ")
        st = int(start) - 1
        ed = int(end) + 1
        if seconds in range(st, ed):
            clash.append(seconds)

    if len(clash) > 0:
        return True


def retrieve_blackspaces(data):
    '''
    Retrieve black detect log and check if
    second variable falls in blocks of blackdetected
    '''
    data_list = data.splitlines()
    time_range = []
    for line in data_list:
        if 'black_start' in line:
            split_line = line.split(":")
            split_start = split_line[1].split('.')[0]
            start = re.sub("[^0-9]", "", split_start)
            split_end = split_line[2].split('.')[0]
            end = re.sub("[^0-9]", "", split_end)
            # Round up to next second for cover
            end = str(int(end) + 1)
            time_range.append(f"{start} - {end}")
    return time_range


def put_file(fpath, ref_num, bucket_name):
    '''
    Add the file to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    Untested: do we to bulk PUT or individually?
    '''
    file_size = os.path.getsize(fpath)
    put_obj = [ds3Helpers.HelperPutObject(object_name=ref_num, file_path=fpath, size=file_size)]
    try:
        put_job_id = HELPER.put_objects(put_objects=put_obj, bucket=bucket_name)
        print(put_job_id)
        LOGGER.info("PUT COMPLETE - JOB ID retrieved: %s", put_job_id)
        return put_job_id
    except Exception as err:
        LOGGER.error('Exception: %s', err)
        print('Exception: %s', err)
        return None


def get_jpeg(seconds, fullpath, outpath):
    '''
    Retrieve JPEG from MP4
    Seconds accepted as float
    '''
    cmd = [
        "ffmpeg",
        "-ss", str(seconds),
        "-i", fullpath,
        "-frames:v", "1",
        "-q:v", "2",
        outpath
    ]

    command = " ".join(cmd)
    print("***********************")
    print(command)
    print("***********************")
    try:
        subprocess.call(cmd)
        return True
    except Exception as err:
        LOGGER.warning("get_jpeg(): failed to extract JPEG\n%s\n%s", command, err)
        return False


def make_jpg(filepath, arg, transcode_pth, percent):
    '''
    Create GM JPEG using command based on argument
    These command work. For full size don't use resize.
    '''
    start_reduce = [
        "gm", "convert",
        "-density", "300x300",
        filepath, "-strip"
    ]

    start = [
        "gm", "convert",
        "-density", "600x600",
        filepath, "-strip"
    ]

    thumb = [
        "-resize", "x180",
    ]

    oversize = [
        "-resize", f"{percent}%x{percent}%",
    ]

    if not transcode_pth:
        out = os.path.splitext(filepath)[0]
    else:
        fname = os.path.split(filepath)[1]
        file = os.path.splitext(fname)[0]
        out = os.path.join(transcode_pth, file)

    if 'thumb' in arg:
        outfile = f"{out}_thumbnail.jpg"
        cmd = start_reduce + thumb + [f"{outfile}"]
    elif 'oversize' in arg:
        outfile = f"{out}_largeimage.jpg"
        cmd = start + oversize + [f"{outfile}"]
    else:
        outfile = f"{out}_largeimage.jpg"
        cmd = start + [f"{outfile}"]

    try:
        subprocess.call(cmd)
    except Exception as err:
        LOGGER.error("JPEG creation failed for filepath: %s\n%s", filepath, err)

    if os.path.exists(outfile):
        return outfile


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

    print(f"Using CUR create_record: database='media', data, output='json', write=True")
    record_data_xml = adlib.retrieve_record_data('', record_data)
    record = adlib.post(CID_API, record_data_xml, 'media', 'insertrecord')
    if not record:
        print(f"\nUnable to create CID media record for {ob_num}")
        LOGGER.exception("Unable to create CID media record!")

    try:
        media_priref = adlib.retrieve_field_name(record, 'priref')[0]
        print(f'** CID media record created with Priref {media_priref}')
        LOGGER.info('CID media record created with priref %s', media_priref)
    except Exception:
        LOGGER.exception("CID media record failed to retrieve priref")
        media_priref = ""

    return media_priref


def cid_media_append(fname, priref, data):
    '''
    Receive data and priref and append to CID media record
    '''
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_mid = ''.join(data)
    payload_end = f"</record></recordList></adlibXML>"
    payload = payload_head + payload_mid + payload_end
    date_supplied = datetime.datetime.now().strftime('%Y-%m-%d')

    rec = adlib.post(CID_API, payload, 'media', 'updaterecord')
    if not rec:
        LOGGER.warning("cid_media_append(): Post of data failed for file %s: %s - %s", fname, priref, post_response.text)
        return False
    elif f'"modification":"{date_supplied}' in str(rec):
        LOGGER.info("cid_media_append(): Write of access_rendition data confirmed successful for %s - Priref %s", fname, priref)
        return True
    else:
        LOGGER.info("cid_media_append(): Write of access_rendition data appear successful for %s - Priref %s", fname, priref)
        return True


if __name__ == '__main__':
    main()
