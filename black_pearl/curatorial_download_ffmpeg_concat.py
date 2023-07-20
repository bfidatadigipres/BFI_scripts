#!/usr/bin/python3

'''
Receive CSV from curatorial formatted so:

Field names:
new_file      <- name for concatenated file, eg N_123456_01of01.mkv
object_number <- for source part file, eg N-123456
part_file     <- name of file, eg N_123456_01of02.mkv
in            <- start time code for beginning of cut, eg 00:01:10.000
out           <- end time code for finishing cut, eg 01:12:10.000

Imports all rows in CSV, where 'new_file' match place all contents into
on single dictionary:
{new_file: {object_number: [part_file, in, out]}, {object_number: [part_file, in, out]},
 new_file: {object_numner: [part_file, in, out]}}

When dictionary compliled, run through following steps:
1. Look up reference_number for each part_file and
   download from Black Pearl, placing into transcode location
2. Build concatenation list from supplied in/out fields and
   filenames, and ensure that the TC is formatted correctly
3. Run the FFmpeg command
4. Check duration of new file matches TC durations (add together)
5. Delete parts, and output notes to log and possibly move completed
   concat file to path for curatorial access.

Joanna White
2023
'''

# Python packages
import os
import sys
import csv
import json
import hashlib
import logging
from datetime import datetime, timedelta
import requests
import subprocess
from ds3 import ds3, ds3Helpers

# Local package
CODE = os.environ['CODE']
sys.path.append(CODE)
import adlib

# GLOBAL VARS
QNAP04 = os.environ['QNAP_IMAGEN']
DESTINATION = os.path.join(QNAP04, 'curatorial_concatenation')
LOG_PATH = os.environ['LOG_PATH']

# API VARIABLES
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)
BUCKET1 = os.environ['BUCKET_OLD']
BUCKET2 = os.environ['BUCKET_NEW']
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)

# Set up logging
LOGGER = logging.getLogger('curatorial_ffmpeg_concat')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'curatorial_ffmpeg_concat.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def find_media_original_filename(fname):
    '''
    Retrieve the object number return all
    attached media record fnames
    '''
    query = {
        'database': 'media',
        'search': f'imagen.media.original_filename={fname}',
        'limit': '0',
        'output': 'json',
        'fields': 'reference_number'
    }

    try:
        query_result = requests.get(CID_API, params=query)
        results = query_result.json()
    except Exception as err:
        LOGGER.exception("get_media_original_filename: Unable to match filename to CID media record: %s\n%s", priref, err)
        print(err)

    try:
        priref = results['adlibJSON']['recordList']['record'][0]['priref'][0]
    except (IndexError, TypeError, KeyError) as exc:
        print(exc)
        priref = ''
    try:
        ref_num = results['adlibJSON']['recordList']['record'][0]['reference_number'][0]
    except (IndexError, TypeError, KeyError) as exc:
        print(exc)
        ref_num = ''

    return priref, ref_num


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


def get_bp_md5(fname):
    '''
    Fetch BP checksum to compare
    to new local MD5
    '''
    md5 = ''
    query = ds3.HeadObjectRequest(BUCKET, fname)
    result = CLIENT.head_object(query)
    try:
        md5 = result.response.msg['ETag']
    except Exception as err:
        print(err)
    if md5:
        return md5.replace('"', '')



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

    bp_checksum = get_bp_md5(fname)
    print(f"Created from download: {download_checksum} | Retrieved from BP: {bp_checksum}")
    return str(download_checksum).strip(), str(bp_checksum).strip()


def download_bp_object(fname, outpath):
    '''
    Download the BP object from SpectraLogic
    tape library and save to outpath
    '''
    file_path = os.path.join(outpath, fname)
    get_objects = [ds3Helpers.HelperGetObject(fname, file_path)]
    try:
        get_job_id = HELPER.get_objects(get_objects, BUCKET)
        print(f"BP get job ID: {get_job_id}")
    except Exception as err:
        LOGGER.warning("Unable to retrieve file %s from Black Pearl", fname)
        get_job_id = None

    return get_job_id


def read_csv(csv_path):
    '''
    Yield contents line by line
    '''
    with open(csv_path, 'r') as file:
        for line in file:
            yield line.split(',')


def create_concat_note(data):
    '''
    Receive source file path
    tc in/tc out for each part
    and add into one list, {concat.txt}
    '''
    pass


def main():
    '''
    Receive CSV path, read lines and group into
    dictionary to process files in groups
    '''
    if len(sys.argv) < 2:
        sys.exit("SYS ARGV missing CSV path")
    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        sys.exit("CSV path is not legitimate")

    check_cid()
    check_control()
    LOGGER.info("Curatorial download FFmpeg concat START =================")

    download_list = []
    files = []
    parts = []
    for item, fname, part, tcin, tcout in read_csv(CSV_PATH):
        download_list.append(part)
        files.append(fname)
        parts.append({fname: [part, tcin, tcout]})

    LOGGER.info("Dictionary of parts extracted from CSV:\n%s", parts)

    # Start download of all CSV parts to folder
    batch_download_imagen = []
    batch_download_pres1 = []
    for part in parts:
        for k, v in part.items():
            priref, ref_num = find_media_original_filename(k)
            LOGGER.info("Matched CID media record %s: ref %s", priref, ref_num)
            if ref_num and bucket == BUCKET1:
                batch_download_imagen.append(ref_num)
            elif ref_num and bucket == BUCKET2:
                batch_download_pres1.append(ref_num)

    for ref in batch_download_imagen:
        job_id = download_bp_object(ref, outpath)
        LOGGER.info("Downloaded file: %s to %s", ref, outpath)
    for ref in batch_download_pres1:
        job_id = download_bp_object(ref, outpath)
        LOGGER.info("Downloaded file: %s to %s", ref, outpath)

    # Create new files from tcin/tcout and downloads
    for part in parts:
        outfile, fname = create_edited_file(part, outpath)
        if not outfile:
            LOGGER.warning("Part missing from concat edit: %s", part)
        LOGGER.info("New edited file created: %s", outfile)
        make_concat_list(fname, f"file {outfile}\n")

    # JMW UPTO HERE Iterate new {file}_concat.txt files creating new items
    # See DR-452 for concat command
s

def create_edited_file(part, outpath):
    '''
    Receive dct containing supplied part
    tc in and tc out, plus outpath. Check
    for previous parts and append new number
    '''
    for key, value in part.items():
        fname = key
        source_file = value[0]
        tcin = value[1]
        tcout = value[2]

    infile = os.path.join(outpath, fname)
    fname = fname.split('.')[0]
    check_file = os.path.join(outpath, fname)
    matches = [ x for x in os.walk(outpath) if check_file in str(x) ]

    if not matches:
        outfile = f"{check_file}_1.mkv"
    else:
        matches.sort()
        last_match = matches[-1].split('.')
        num = int(last_match[-1]) + 1
        outfile = f"{check_file}_{num}.mkv"

    ffmpeg_cmd = [
        'ffmpeg',
        '-ss', tcin.strip(),
        '-to', tcout.strip(),
        '-i', infile,
        '-c', 'copy',
        '-map', '0',
        outfile
    ]

    try:
        code = subprocess.call(ffmpeg_cmd)
        if code != 0:
            LOGGER.warning("FFmpeg command failed: %s", ' '.join(ffmpeg_cmd))
            return '', ''
        else:
            LOGGER.info("FFmpeg command called: %s", ' '.join(ffmpeg_cmd))
            return outfile, fname
    except Exception as err:
        LOGGER.warning(err)
        return '', ''


def make_concat_list(fname, outpath, message):
    '''
    Look for existing concat file if not present, create and write lines
    '''
    file = os.path.splitext(fname)[0]
    concat_txt = os.path.join(outpath, f'{file}_concat.txt'

    if not os.path.isfile(concat_txt):
        with open(concat_txt, 'w+') as out_file:
            out_file.close()

    with open(concat_txt, 'a') as out_file:
        out_file.write(f"{message}\n")



if __name__ == '__main__':
    main()

