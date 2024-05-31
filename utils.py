import re
import os
import json
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
            print('Script run prevented by downtime_control.json. Script exiting.')
            return True


def cid_check(cid_api):
    '''
    Tests if CID API operational before
    all other operations commence
    '''
    try:
        adlib.check(cid_api)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        return True


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


