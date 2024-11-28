#!/usr/bin/env python3

'''
DPX TAR wrapping script to replace Shell script.
USES SYS.ARGV[] to receive path to item for TAR.
Complete TAR wrapping using Python3 tarfile
on folder or file supplied in tar watch folder.
Compare TAR contents to original using MD5 hash.

Steps:
1. Assess if item supplied is folder/file
2. Generate MD5 dict for original folder/file
3. Initiate TAR wrapping with zero compression
4. Generate MD5 dict for internals of TAR
5. Compare to ensure identical:
   Yes. Output MD5 to manifest and add into TAR file
        Move original folder to 'to_delete' folder
        Move completed closed() TAR to autoingest.
        Update details to local log.
   No. Delete faulty TAR.
       Output warning to Local log and leave file
       for retry at later date. Script exits.
6. Check TAR file is under 1TB size (bytes)
7. Make whole file checksum for TAR file
8. Output file size and whole file checksum
   to script log and local TAR log

TO DO:  Change autoingest path away from STORE
        as and when autoingest paths update

2022
'''

import os
import sys
import json
import shutil
import tarfile
import logging
import hashlib
import datetime

sys.path.append(os.environ['CODE'])
import utils

# Global paths
LOCAL_PATH = os.environ['QNAP_FILM']
AUTOINGEST = os.path.join(LOCAL_PATH, os.environ['AUTOINGEST_STORE'])
LOG_PATH = os.path.join(LOCAL_PATH, os.environ['DPX_SCRIPT_LOG'])

# Logging config
LOGGER = logging.getLogger('tar_wrap_check')
hdlr = logging.FileHandler(os.path.join(LOG_PATH, 'dpx_tar_wrapping_checksum.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def tar_file(fpath):
    '''
    Make tar path from supplied filepath
    Use tarfile to create TAR. Use add()
    with arcname=, reduces tar file names
    to folder level only, and doesn't
    include whole path to folder
    '''
    split_path = os.path.split(fpath)
    tfile = f"{split_path[1]}.tar"
    tar_path = os.path.join(split_path[0], tfile)
    if os.path.exists(tar_path):
        LOGGER.warning("tar_file(): FILE ALREADY EXISTS %s", tar_path)
        return None

    try:
        tarring = tarfile.open(tar_path, 'w:')
        tarring.add(fpath, arcname=f"{split_path[1]}")
        tarring.close()
        return tar_path

    except Exception as exc:
        LOGGER.warning("tar_file(): ERROR WITH TAR WRAP %s", exc)
        tarring.close()
        return None


def get_tar_checksums(tar_path, folder):
    '''
    Open tar file and read/generate MD5 sums
    and return dct {filename: hex}
    '''
    data = {}
    tar = tarfile.open(tar_path, "r|")

    for item in tar:
        if item.isdir():
            continue
        pth, file = os.path.split(item.name)
        if fname in ['ASSETMAP','VOLINDEX']:
            folder_prefix = os.path.basename(pth)
            file = f'{folder_prefix}_{file}'
        try:
            f = tar.extractfile(item)
        except Exception as exc:
            LOGGER.warning("get_tar_checksums(): Unable to extract from tar file\n%s", exc)
            continue

        fname = f"{folder}/{os.path.split(item.name)[1]}" if folder else file

        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: f.read(65536), b""):
            hash_md5.update(chunk)
        data[fname] = hash_md5.hexdigest()

    return data


def get_checksum(fpath, source):
    '''
    Using file path, generate file checksum
    return as list with filename
    '''
    data = {}
    pth, file = os.path.split(fpath)
    if file in ['ASSETMAP','VOLINDEX']:
        folder_prefix = os.path.basename(pth)
        file = f'{folder_prefix}_{file}'
    dct_name = f"{source}/{fname}" if source != '' else fname

    try:
        hash_md5 = hashlib.md5()
        with open(fpath, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_md5.update(chunk)
        data[dct_name] = hash_md5.hexdigest()

    except Exception as exc:
        LOGGER.warning("get_checksum(): FAILED TO GET CHECKSUM %s", exc)

    return data


def make_manifest(tar_path, md5_dct):
    '''
    Output md5 to JSON file format and add to TAR file
    using sorted keys to maintain DPX order
    '''
    md5_path = f"{tar_path}_manifest.md5"
    sorted_dct = sorted(md5_dct)

    try:
        with open(md5_path, 'w+') as json_file:
            json_file.write("TAR content MD5 sum manifest:\n")
            for key in sorted_dct:
                json_file.write(f"  {key}  -  {md5_dct[key]}\n")
            json_file.close()
    except Exception as exc:
        print(exc)

    if os.path.exists(md5_path):
        return md5_path


def main():
    '''
    Receive SYS.ARGV and check path exists or is file/folder
    Generate checksums for all folder contents/single file
    TAR Wrap, then make checksum for inside of TAR contents
    Compare checksum manifests, if match add into TAR and close.
    Delete original file, move TAR to autoingest path.
    '''

    if not utils.check_control('power_off_all'):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')


    if len(sys.argv) != 2:
        LOGGER.warning("SCRIPT EXIT: Error with shell script input:\n %s", sys.argv)
        sys.exit("No argument supplied. Script exiting.")

    fullpath = sys.argv[1]
    print(fullpath)

    if not os.path.exists(fullpath):
        sys.exit("Supplied path does not exists. Please try again.")

    log = []
    log.append(f"\n==== New path for TAR wrap: {fullpath} ====")
    LOGGER.info("==== TAR Wrapping Check script start ===============================")
    LOGGER.info("Path received for TAR wrap using Python3 tarfile: %s", fullpath)
    split_path = os.path.split(fullpath)
    print(split_path)
    tar_source = split_path[1]
    base_path = os.path.split(split_path[0])[0]
    print(base_path, tar_source, split_path)

    # Make paths for moving later
    failures_path = os.path.join(base_path, 'failures/')
    delete_path = os.path.join(base_path, 'to_delete/')
    oversize_path = os.path.join(base_path, 'oversize/')

    # Calculate checksum manifest for supplied fullpath
    local_md5 = {}
    directory = False
    if os.path.isdir(fullpath):
        directory = True

    if directory:
        log.append(f"Path is directory. Building checksum MD5 list.")
        for root, _, files in os.walk(fullpath):
            for file in files:
                dct = get_checksum(os.path.join(root, file), tar_source)
                LOGGER.info(dct)
                local_md5.update(dct)

    else:
        local_md5 = get_checksum(fullpath, '')
        log.append("Path is not a directory and will be wrapped alone")

    log.append("Checksums for local files:")
    for key, val in local_md5.items():
        if key.lower().endswith('.dpx'):
            continue
        data = f"File {key} -- MD5 Checksum {val}"
        log.append(data)

    # Tar folder
    log.append("Beginning TAR wrap now...")
    tar_path = tar_file(fullpath)
    if not tar_path:
        log.append("TAR WRAP FAILED. SCRIPT EXITING!")
        LOGGER.warning("TAR wrap failed for file: %s", fullpath)
        sys.exit(f"EXIT: TAR wrap failed for {fullpath}")

    # Calculate checksum manifest for TAR folder
    if directory:
        tar_content_md5 = get_tar_checksums(tar_path, tar_source)
    else:
        tar_content_md5 = get_tar_checksums(tar_path, '')

    log.append("Checksums from TAR wrapped contents:")
    for key, val in tar_content_md5.items():
        data = f"TAR File {key} -- MD5 Checksum {val}"
        log.append(data)

    # Compare manifests
    if local_md5 == tar_content_md5:
        log.append("MD5 Manifests match, adding manifest to TAR file and moving to autoingest.")
        LOGGER.info("MD5 manifests match.\nLocal path manifest:\n%s\nTAR file manifest:\n%s", local_md5, tar_content_md5)
        md5_manifest = make_manifest(tar_path, tar_content_md5)
        if not md5_manifest:
            LOGGER.warning("Failed to write TAR checksum manifest to JSON file.")
            shutil.move(tar_path, os.path.join(failures_path, f'{tar_source}.tar'))
            sys.exit("Script exit: TAR file MD5 Manifest failed to create")

        LOGGER.info("TAR checksum manifest created. Adding to TAR file %s", tar_path)
        try:
            tar = tarfile.open(tar_path, 'a:')
            tar.add(md5_manifest, arcname=f'{tar_source}.tar_manifest.md5')
            tar.close()
        except Exception as exc:
            LOGGER.warning("Unable to add MD5 manifest to TAR file. Moving TAR file to errors folder.\n%s", exc)
            shutil.move(tar_path, os.path.join(failures_path, f'{tar_source}.tar'))
            sys.exit()

        LOGGER.info("TAR MD5 manifest added to TAR file. Getting wholefile TAR checksum for logs")

        # Get complete TAR wholefile Checksums for logs
        tar_md5 = get_checksum(tar_path, '')
        log.append(f"TAR checksum: {tar_md5}")

        # Get complete size of file following TAR wrap
        file_stats = os.stat(tar_path)
        log.append(f"File size is {file_stats.st_size} bytes")
        if file_stats.st_size > 1099511627770:
            log.append("FILE IS TOO LARGE FOR INGEST TO BLACK PEARL. Moving to oversized folder path")
            LOGGER.warning("MOVING TO OVERSIZE PATH: Filesize too large for ingest to DPI: %s", file_stats.st_size)
            shutil.move(tar_path, os.path.join(oversize_path, f'{tar_source}.tar'))

        log.append("Moving TAR file to Autoingest, and moving source file to deletions path.")
        shutil.move(tar_path, AUTOINGEST)
        LOGGER.info("Moving original file/folder to deletions folder: %s", fullpath)
        shutil.move(fullpath, os.path.join(delete_path, tar_source))

    else:
        set1 = set(local_md5.items())
        set2 = set(tar_content_md5.items())
        md5_diff = set2 ^ set1
        LOGGER.warning("Manifests do not match. Difference:\n%s", md5_diff)
        LOGGER.warning("Moving TAR file to failures, leaving file/folder for retry.")
        log.append("MD5 manifests do not match. Moving TAR file to failures folder for retry")
        shutil.move(tar_path, os.path.join(failures_path, f'{tar_source}.tar'))

    log.append(f"==== Log actions complete: {fullpath} ====")
    # Write all log items in block
    for item in log:
        local_log(base_path, item)

    LOGGER.info("==== TAR Wrapping Check script END =================================")


def local_log(fullpath, data):
    '''
    Output local log data for team
    to monitor TAR wrap process
    '''
    local_log = os.path.join(fullpath, 'python_tar_wrapping_checksum.log')
    timestamp = str(datetime.datetime.now())

    if not os.path.isfile(local_log):
        with open(local_log, 'x') as log:
            log.close()

    with open(local_log, 'a') as log:
        log.write(f"{data} - {timestamp[0:19]}\n")
        log.close()


if __name__ == '__main__':
    main()
