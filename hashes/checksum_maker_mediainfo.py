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

2021
'''

# External Libraries
import os
import sys
import logging
import subprocess
import datetime
import tenacity

# Custom Libraries
sys.path.append(os.environ['CODE'])
import utils

# Global variables
LOG_PATH = os.environ['LOG_PATH']
CODE_PTH = os.environ['CODE_DDP']
CODE = os.environ['CODE']
TODAY = str(datetime.date.today())
CONTROL_JSON = os.environ['CONTROL_JSON']
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


def checksum_write(checksum_path, checksum, filepath, filename):
    '''
    This function writes the checksum into a file and returns the paths

    Parameters:
    -----------
        checksum_path: string
            the file directory to the checksum file

        checksum: string
            the checksum value generated from the video/film

        filepath: string
            the full file path from absolute to the file(film)

    Returns:
    --------
        checksum_path: string
            the file where the checksum is stored
    '''
    try:
        with open(checksum_path, 'w') as fname:
            fname.write(f"{checksum} - {filepath} - {TODAY}")
            fname.close()
        return checksum_path
    except Exception as e:
        LOGGER.exception(f"{filename} - Unable to write checksum: {checksum_path}")


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def checksum_exist(checksum_path_env, filename, checksum, filepath):
    '''
    Create a new Checksum file and write MD5_checksum
    Return checksum path where successfully written
    '''
    checksum_path = os.path.join(checksum_path_env, f"{filename}.md5")
    if os.path.isfile(checksum_path):
        checksum_path = checksum_write(checksum_path, checksum, filepath, filename)
        return checksum_path
    else:
        with open(checksum_path, 'x') as fnm:
            fnm.close()
        checksum_path = checksum_write(checksum_path, checksum, filepath, filename)
        return checksum_path


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_output_md5(filepath, filename):
    '''
    Runs checksum generation/output to file as separate function allowing for easier retries
    '''
    try:
        md5_checksum = utils.create_md5_65536(filepath)
        LOGGER.info(f"{filename} - MD5 sum generated: {md5_checksum}")
        return md5_checksum
    except Exception as e:
        LOGGER.exception(f"{filename} - Failed to make MD5 checksum for {filepath}")
        return None


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def mediainfo_create(arg, output_type, filepath, mediainfo_path):
    '''
    Output mediainfo data to text files
    '''
    filename = os.path.basename(filepath)
    if arg == '-f':
        if output_type == 'TEXT':
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}_FULL.txt")
        elif output_type == 'JSON':
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}.json")

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
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}.xml")
        elif 'EBUCore' in output_type:
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}.xml")
        elif 'PBCore' in output_type:
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}.xml")
        else:
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}.txt")

        command = [
            'mediainfo',
            '--Details=0',
            f'--Output={output_type}',
            f'--LogFile={out_path}',
            filepath
        ]

    try:
        subprocess.call(command)
        return out_path
    except Exception as e:
        LOGGER.info(e)
        return False


def checksum_test(CHECKSUM_PATH, check):
    '''
    Check for 'None' where checksum should be
    '''
    try:
        if os.path.exists(os.path.join(CHECKSUM_PATH, check)):
            checksum_pth = os.path.join(CHECKSUM_PATH, check)

        with open(checksum_pth, 'r') as file:
            line = file.readline()
            if line.startswith('None'):
                LOGGER.info(f"None entry found: {check}")
                return True

    except Exception as e:
        LOGGER.info(e)
        return None


def main():
    '''
    Argument passed from shell launch script to GNU parallel bash with Flock lock
    Decorator for two functions ensures retries if Exceptions raised
    '''
    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit('Shell script failed to pass argument to Python script')
    if not utils.check_control('power_off_all'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
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
        checksum_present = checksum_test(CHECKSUM_PATH, check[0])
        if checksum_present:
            sys.exit('Checksum already exists for this file, exiting.')

    md5_checksum = make_output_md5(filepath, filename)
    if 'None' in str(md5_checksum):
        md5_checksum = make_output_md5(filepath, filename)

    # Make metadata then write to checksum path as filename.ext.md5
    if 'None' not in str(md5_checksum):
        make_metadata(path, filename, MEDIAINFO_PATH)
        success = checksum_exist(CHECKSUM_PATH, filename, md5_checksum, filepath)
        LOGGER.info("%s Checksum written to: %s", filename, success)

    LOGGER.info("=============== Python3 %s END ==============", filename)


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_metadata(fpath, fname, mediainfo_path):
    '''
    Create mediainfo files
    '''
    # Run script from media files local directory
    os.chdir(fpath)
    path1 = mediainfo_create('-f', 'TEXT', fname, mediainfo_path)
    path2 = mediainfo_create('', 'TEXT', fname, mediainfo_path)
    path3 = mediainfo_create('', 'EBUCore', fname,mediainfo_path)
    path4 = mediainfo_create('', 'PBCore2', fname, mediainfo_path)
    path5 = mediainfo_create('', 'XML', fname, mediainfo_path)
    path6 = mediainfo_create('-f', 'JSON', fname,mediainfo_path)

    # Return path back to script directory
    os.chdir(os.path.join(CODE, 'hashes'))
    LOGGER.info("Written metadata to paths:\n%s\n%s\n%s\n%s\n%s\n%s", path1, path2, path3, path4, path5, path6)


if __name__ == '__main__':
    main()
