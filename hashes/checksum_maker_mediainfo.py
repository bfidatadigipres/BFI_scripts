#!/usr/bin/env /usr/local/bin/python3

'''
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

Joanna White
2021
'''

import os
import sys
import hashlib
import logging
import subprocess
import datetime
import tenacity

# Global variables
LOG_PATH = os.environ['LOG_PATH']
CODE_PTH = os.environ['CODE_DDP']
CODE = os.environ['CODE']
TODAY = str(datetime.date.today())
CHECKSUM_PATH = os.path.join(LOG_PATH, 'checksum_md5')
CHECKSUM_PATH2 = os.path.join(CODE_PTH, 'Logs', 'checksum_md5')
MEDIAINFO_PATH = os.path.join(LOG_PATH, 'cid_mediainfo')
MEDIAINFO_PATH2 = os.path.join(CODE_PTH, 'Logs', 'cid_mediainfo')

# Setup logging
LOGGER = logging.getLogger('checksum_maker_mediainfo')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'checksum_maker_mediainfo.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def md5_65536(file):
    '''
    Hashlib md5 generation, return as 32 character hexdigest
    '''
    try:
        hash_md5 = hashlib.md5()
        with open(file, "rb") as fname:
            for chunk in iter(lambda: fname.read(65536), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    except Exception:
        LOGGER.exception("%s - Unable to generate MD5 checksum", file)
        return None


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def checksum_write(filename, checksum, filepath):
    '''
    Create a new Checksum file and write MD5_checksum
    Return checksum path where successfully written
    '''
    checksum_path = os.path.join(CHECKSUM_PATH, f"{filename}.md5")
    if os.path.isfile(checksum_path):
        try:
            with open(checksum_path, 'w') as fname:
                fname.write(f"{checksum} - {filepath} - {TODAY}")
                fname.close()
            return checksum_path
        except Exception:
            LOGGER.exception("%s - Unable to write checksum: %s", filename, checksum_path)
    else:
        try:
            with open(checksum_path, 'x') as fnm:
                fnm.close()
            with open(checksum_path, 'w') as fname:
                fname.write(f"{checksum} - {filepath} - {TODAY}")
                fname.close()
            return checksum_path
        except Exception:
            LOGGER.exception("%s Unable to write checksum to path: %s", filename, checksum_path)


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_output_md5(filepath, filename):
    '''
    Runs checksum generation/output to file as separate function allowing for easier retries
    '''
    try:
        md5_checksum = md5_65536(filepath)
        LOGGER.info("%s - MD5 sum generated: %s", filename, md5_checksum)
        return md5_checksum
    except Exception:
        LOGGER.exception("%s - Failed to make MD5 checksum for %s", filename, filepath)
        return None


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def mediainfo_create(arg, output_type, filepath):
    '''
    Output mediainfo data to text files
    '''
    filename = os.path.basename(filepath)

    if len(arg) > 0:
        out_path = os.path.join(MEDIAINFO_PATH, f"{filename}_{output_type}_FULL.txt")

        command = [
            'mediainfo',
            arg,
            '--Details=0',
            f'--Output={output_type}',
            f'--LogFile={out_path}',
            filepath
        ]

    else:
        if 'XML' in output_type:
            ext = 'xml'
            outp = 'XML'
        elif 'EBUCore' in output_type:
            ext = 'xml'
            outp = 'EBUCore'
        elif 'PBCore' in output_type:
            ext = 'xml'
            outp = 'PBCore2'
        elif 'JSON' in output_type:
            ext = 'json'
            outp = 'JSON'
        else:
            ext = 'txt'
            outp = 'TEXT'

        out_path = os.path.join(MEDIAINFO_PATH, f"{filename}_{output_type}.{ext}")
        command = [
            'mediainfo',
            '--Details=0',
            f'--Output={outp}',
            f'--LogFile={out_path}',
            filepath
        ]

    try:
        subprocess.call(command)
        return out_path
    except Exception:
        return False


def checksum_test(check):
    '''
    Check for 'None' where checksum should be
    '''
    if os.path.exists(os.path.join(CHECKSUM_PATH, check)):
        checksum_pth = os.path.join(CHECKSUM_PATH, check)

    with open(checksum_pth, 'r') as file:
        line = file.readline()
        if line.startswith('None'):
            LOGGER.info("None entry found: %s", check)
            return True


def main():
    '''
    Argument passed from shell launch script to GNU parallel bash with Flock lock
    Decorator for two functions ensures retries if Exceptions raised
    '''
    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit('Shell script failed to pass argument to Python script')

    filepath = sys.argv[1]
    path_split = os.path.split(filepath)
    filename = path_split[1]
    path = path_split[0]

    LOGGER.info("============ Python3 %s START =============", filepath)

    # Check if MD5 already generated for file using list comprehension
    check = [f for f in os.listdir(CHECKSUM_PATH) if f.startswith(filename) and not f.endswith(('.ini', '.DS_Store', '.mhl', '.json', '.tmp', '.dpx', '.DPX'))]
    if len(check) > 1:
        sys.exit(f'More than one checksum found with {filename}')

    # Check if existing MD5 starts with 'None'
    if len(check) == 1:
        checksum_present = checksum_test(check[0])
        if checksum_present:
            sys.exit('Checksum already exists for this file, exiting.')

    md5_checksum = make_output_md5(filepath, filename)
    if 'None' in str(md5_checksum):
        md5_checksum = make_output_md5(filepath, filename)

    # Make metadata then write to checksum path as filename.ext.md5
    if 'None' not in str(md5_checksum):
        make_metadata(path, filename)
        success = checksum_write(filename, md5_checksum, filepath)
        LOGGER.info("%s Checksum written to: %s", filename, success)

    LOGGER.info("=============== Python3 %s END ==============", filename)


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_metadata(path, fname):
    '''
    Create mediainfo files
    '''
    # Run script from media files local directory
    os.chdir(path)
    path1 = mediainfo_create('-f', 'TEXT', fname)
    path2 = mediainfo_create('', 'TEXT', fname)
    path3 = mediainfo_create('', 'EBUCore', fname)
    path4 = mediainfo_create('', 'PBCore2', fname)
    path5 = mediainfo_create('', 'XML', fname)
    path6 = mediainfo_create('', 'JSON', fname)

    # Return path back to script directory
    os.chdir(os.path.join(CODE, 'hashes'))
    LOGGER.info("Written metadata to paths:\n%s\n%s\n%s\n%s\n%s\n%s", path1, path2, path3, path4, path5, path6)


if __name__ == '__main__':
    main()
