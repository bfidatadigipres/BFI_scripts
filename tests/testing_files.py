import subprocess
import os
import utils
import datetime
import re

LOG_PATH = os.environ['LOG_PATH']
CHECKSUM_PATH = os.path.join(LOG_PATH, 'checksum_md5')
MEDIAINFO_PATH = os.path.join(LOG_PATH, 'cid_mediainfo')

PREFIX = [
    'N',
    'C',
    'PD',
    'SPD',
    'PBS',
    'PBM',
    'PBL',
    'SCR',
    'CA'
]

ACCEPTED_EXT = [
    'mxf',
    'xml',
    'tar',
    'dpx',
    'wav',
    'mpg',
    'mpeg',
    'mp4',
    'm2ts',
    'mov',
    'mkv',
    'wmv',
    'tif',
    'tiff',
    'jpg',
    'jpeg',
    'ts',
    'm2ts',
    'rtf',
    'srt',
    'scc',
    'itt',
    'stl',
    'cap',
    'dfxp',
    'dxfp',
    'csv',
    'pdf'
]

def exif_data(dpath):
    '''
    Retrieve exiftool data
    return match to field if available
    '''

    cmd = [
        'exiftool',
        dpath
    ]
    data = subprocess.check_output(cmd)
    data = data.decode('latin-1')

    return data
def get_metadata(stream, arg, dpath):
    '''
    Retrieve metadata with subprocess
    for supplied stream/field arg
    '''

    cmd = [
        'mediainfo', '--Full',
        '--Language=raw',
        f'--Output={stream};%{arg}%',
        dpath
    ]

    meta = subprocess.check_output(cmd)
    return meta.decode('utf-8').rstrip('\n')

def checksum_test(check):
    '''
    Check 
    for 'None' where checksum should be
    '''
    
    if os.path.exists(os.path.join(CHECKSUM_PATH, check)):
        checksum_pth = os.path.join(CHECKSUM_PATH, check)

    with open(checksum_pth, 'r') as file:
        line = file.readline()
        if line.startswith('None'):
            print("None entry found: %s", check)
            return True
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
        TODAY = str(datetime.date.today())
        with open(checksum_path, 'w') as fname:
            fname.write(f"{checksum} - {filepath} - {TODAY}")
            fname.close()
        return checksum_path
    except Exception as e:
        print(e)
        print("%s - Unable to write checksum: %s", filename, checksum_path)

def checksum_exist(filename, checksum, filepath):
    '''
    Create a new Checksum file and write MD5_checksum
    Return checksum path where successfully written
    '''
    checksum_path = os.path.join(CHECKSUM_PATH, f"{filename}.md5")
    if os.path.isfile(checksum_path):
        print('h')
        checksum_path = checksum_write(checksum_path, checksum, filepath, filename)
        print('e')
        return checksum_path
    else:
        print('l')
        with open(checksum_path, 'x') as fnm:
            fnm.close()
            print('p')
        print('running here, file dont exist in cs')
        checksum_path = checksum_write(checksum_path, checksum, filepath, filename)
        return checksum_path

if  __name__ == "__main__":
    print(checksum_exist('MKV_sample.img', 'a249fba2c4a44a9354d2c3d6d0805dd6', 'tests/MKV_sample.img'))
    