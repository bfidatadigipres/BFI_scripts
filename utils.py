'''
Consolidate all repeat modules
to one utils.py document

Joanna White
2024
'''

import re
import os
import csv
import json
import yaml
import hashlib
import logging
import subprocess
import adlib_v3 as adlib

CONTROL_JSON = os.path.join(os.environ.get('LOG_PATH'), 'downtime_control.json')

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
    'mp4',
    'mov',
    'mkv',
    'tif',
    'tiff',
    'jpg',
    'jpeg',
    'ts',
    'srt',
    'scc',
    'itt',
    'stl',
    'cap',
    'dfxp',
    'dxfp'
]


def check_control(arg):
    '''
    Check control json for downtime requests
    based on passed argument
    '''
    if not isinstance(arg, str):
        arg = str(arg)

    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j[arg]:
            return True


def cid_check(cid_api):
    '''
    Tests if CID API operational before
    all other operations commence
    '''
    try:
        adlib.check(cid_api)
    except KeyError:
        return True


def read_yaml(file):
    '''
    Safe open yaml and return as dict
    '''
    with open(file) as config_file:
        d = yaml.safe_load(config_file)
        return d


def read_csv(csv_path):
    '''
    Check CSV for evidence that fname already
    downloaded. Extract download date and return
    otherwise return None.
    '''
    with open(csv_path, 'r') as csvread:
        readme = csv.DictReader(csvread)
        return readme


def check_filename(fname):
    '''
    Run series of checks against BFI filenames
    check accepted prefixes, and extensions
    '''
    if not any(fname.startswith(px) for px in PREFIX):
        return False
    if not re.search("^[A-Za-z0-9_.]*$", fname):
        return False

    sname = fname.split('_')
    if len(sname) > 4 or len(sname) < 3:
        return False
    if len(sname) == 4 and len(sname[2]) != 1:
        return False

    if '.' in fname:
        if len(fname.split('.')) > 2:
            return False
        ext = fname.split('.')[-1]
        if ext.lower() not in (ACCEPTED_EXT):
            return False

    return True


def check_part_whole(fname):
    '''
    Check part whole well formed
    '''
    match = re.search(r'(?:_)(\d{2,4}of\d{2,4})(?:\.)', fname)
    if not match:
        print('* Part-whole has illegal charcters...')
        return None, None
    part, whole = [int(i) for i in match.group(1).split('of')]
    len_check = fname.split('_')
    len_check = len_check[-1].split('.')[0]
    str_part, str_whole = len_check.split('of')
    if len(str_part) != len(str_whole):
        return None, None
    if part > whole:
        print('* Part is larger than whole...')
        return None, None
    return (part, whole)


def get_object_number(fname):
    '''
    Extract object number from name formatted
    with partWhole, eg N_123456_01of03.ext
    '''
    if not any(fname.startswith(px) for px in PREFIX):
        return False
    try:
        splits = fname.split('_')
        object_number = '-'.join(splits[:-1])
    except Exception:
        object_number = None
    return object_number


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
    return meta.decode('utf-8')


def get_mediaconch(dpath, policy):
    '''
    Check for 'pass! {path}' in mediaconch reponse
    for supplied file path and policy
    '''

    cmd = [
        'mediaconch', '--force',
        '-p', policy,
        dpath
    ]
    
    meta = subprocess.check_output(cmd)
    meta = meta.decode('utf-8')
    if meta.startswith(f'pass! {dpath}'):
        return True, meta
    
    return False, meta


def logger(log_path, level, message):
    '''
    Configure and handle logging
    of file events
    '''
    log = os.path.basename(log_path).split('.')[-1]
    LOGGER = logging.getLogger(log)
    HDLR = logging.FileHandler(log_path)
    FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
    HDLR.setFormatter(FORMATTER)
    LOGGER.addHandler(HDLR)
    LOGGER.setLevel(logging.INFO)

    if level == 'info':
        LOGGER.info(message)
    elif level == 'warning':
        LOGGER.warning(message)
    elif level == 'critical':
        LOGGER.critical(message)
    elif level == 'error':
        LOGGER.error(message)
    elif level == 'exception':
        LOGGER.exception(message)


def get_folder_size(fpath):
    '''
    Check the size of given folder path
    return size in kb
    '''
    if os.path.isfile(fpath):
        return os.path.getsize(fpath)

    try:
        byte_size = sum(os.path.getsize(os.path.join(fpath, f)) for f in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, f)))
        return byte_size
    except OSError as err:
        print(f"get_size(): Cannot reach folderpath for size check: {fpath}\n{err}")
        return None


def create_md5_65536(fpath):
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
        print(f"{fpath} - Unable to generate MD5 checksum")
        return None