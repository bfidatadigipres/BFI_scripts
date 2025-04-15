'''
Consolidate all repeat modules
to one utils.py document

2024
'''

import re
import os
import ast
import csv
import json
import ffmpeg
import hashlib
import logging
import datetime
import subprocess
from typing import Final, Optional, Iterator, Any
import yaml
import tenacity
import smtplib
import ssl
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


# BFI library
import adlib_v3 as adlib



LOG_PATH: Final = os.environ['LOG_PATH']
CONTROL_JSON: str = os.path.join(os.environ.get('LOG_PATH'), 'downtime_control.json')
GLOBAL_LOG: Final = os.path.join(LOG_PATH, 'autoingest', 'global.log')
SMTP_SERVER = 'mail.smtp2go.com'
SMTP_PORT = 465
EMAIL =  os.environ.get("EMAIL_ADDRESS")
PASSWORD =  os.environ.get("EMAIL_PASSWORD")
CONTEXT = ssl.create_default_context()

PREFIX: Final = [
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

ACCEPTED_EXT: Final = [
    'avi',
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
    'ttf',
    'srt',
    'scc',
    'itt',
    'stl',
    'cap',
    'dfxp',
    'dxfp',
    'csv',
    'pdf',
    'txt',
    'vtt'
]



# (ext: str) -> Optional[str]:
def accepted_file_type(ext):
    '''
    Receive extension and returnc
    matching accepted file_type
    '''
    ftype = {'avi': 'avi',
             'imp': 'mxf, xml',
             'tar': 'dpx, dcp, dcdm, wav',
             'mxf': 'mxf, 50i, imp',
             'mpg': 'mpeg-1, mpeg-ps',
             'mpeg': 'mpeg-1, mpeg-ps',
             'mp4': 'mp4',
             'mov': 'mov, prores',
             'mkv': 'mkv, dpx, dcdm',
             'wav': 'wav',
             'wmv': 'wmv',
             'tif': 'tif, tiff',
             'tiff': 'tif, tiff',
             'jpg': 'jpg, jpeg',
             'jpeg': 'jpg, jpeg',
             'ts': 'mpeg-ts',
             'm2ts': 'mpeg-ts',
             'srt': 'srt',
             'xml': 'xml, imp',
             'scc': 'scc',
             'itt': 'itt',
             'stl': 'stl',
             'rtf': 'rtf',
             'ttf': 'ttf',
             'vtt': 'vtt',
             'cap': 'cap',
             'dxfp': 'dxfp',
             'dfxp': 'dfxp',
             'csv': 'csv',
             'pdf': 'pdf',
             'txt': 'txt'}

    ext = ext.lower()
    for key, val in ftype.items():
        if key == ext:
            return val

    return None


# (arg: str) -> bool:
def check_control(arg):
    '''
    Check control json for downtime requests
    based on passed argument
    if not utils.check_control['arg']:
        sys.exit(message)
    '''
    if not isinstance(arg, str):
        arg = str(arg)

    with open(CONTROL_JSON) as control:
        j: dict[str, str] = json.load(control)
        if j[arg]:
            return True
        else:
            return False


# (cid_api: str) -> bool:
def cid_check(cid_api):
    '''
    Tests if CID API operational before
    all other operations commence
    if not utils.cid_check[API]:
        sys.exit(message)
    '''
    try:
        dct = adlib.check(cid_api)
        print(dct)
        if isinstance(dct, dict):
            return True
    except KeyError:
        return False


# (file: str) -> dict[str, str]:
def read_yaml(file):
    '''
    Safe open yaml and return as dict
    '''
    with open(file) as config_file:
        d = yaml.safe_load(config_file)
        return d


#(csv_path: str) -> Iterator[dict[Any, Any]]:
def read_csv(csv_path):
    '''
    Check CSV for evidence that fname already
    downloaded. Extract download date and return
    otherwise return None.
    '''
    with open(csv_path, 'r') as csvread:
        readme = csv.DictReader(csvread)
        return readme


# (fpath: str) -> str:
def read_extract(fpath):
    '''
    For reading metadata text files
    and returning as a block
    '''
    with open(fpath, 'r') as data:
        readme: str = data.read()

    return readme


# (fname: str) -> bool:
def check_filename(fname):
    '''
    Run series of checks against BFI filenames
    check accepted prefixes, and extensions
    '''
    if not any(fname.startswith(px) for px in PREFIX):
        return False
    if not re.search("^[A-Za-z0-9_.]*$", fname):
        return False

    sname: list[str] = fname.split('_')
    if len(sname) > 4 or len(sname) < 3:
        return False
    if len(sname) == 4 and len(sname[2]) != 1:
        return False

    if '.' in fname:
        if len(fname.split('.')) != 2:
            return False
        ext = fname.split('.')[-1]
        if ext.lower() not in ACCEPTED_EXT:
            return False

    return True


# (fname: str) -> tuple[Optional[int], Optional[int]]:
def check_part_whole(fname):
    '''
    Check part whole well formed
    '''
    match: Optional[re.Match[str]] = re.search(r'(?:_)(\d{2,4}of\d{2,4})(?:\.)', fname)
    if not match:
        print('* Part-whole has illegal charcters...')
        return None, None
    part, whole = [int(i) for i in match.group(1).split('of')]
    len_check = fname.split('_')[-1].split('.')[0]
    #len_check = len_check[-1].split('.')[0]
    str_part, str_whole = len_check.split('of')
    if len(str_part) != len(str_whole):
        return None, None
    if part > whole:
        print('* Part is larger than whole...')
        return None, None
    return part, whole


# (fname: str) -> str | bool | None:
def get_object_number(fname):
    '''
    Extract object number from name formatted
    with partWhole, eg N_123456_01of03.ext
    '''
    if not any(fname.startswith(px) for px in PREFIX):
        return False
    try:
        splits: list[str] = fname.split('_')
        object_number: Optional[str] = '-'.join(splits[:-1])
    except Exception:
        object_number = None
    return object_number


# (ext: str) -> Optional[str]:
def sort_ext(ext):
    '''
    Decide on file type
    '''
    mime_type = {'video': ['mxf', 'mkv', 'mov', 'wmv', 'mp4', 'mpg', 'avi', 'ts', 'mpeg', 'm2ts'],
                 'image': ['png', 'gif', 'jpeg', 'jpg', 'tif', 'pct', 'tiff'],
                 'audio': ['wav', 'flac', 'mp3'],
                 'document': ['docx', 'pdf', 'vtt', 'doc', 'tar', 'srt', 'scc', 'itt', 'stl', 'cap', 'dxfp', 'xml', 'dfxp', 'txt', 'ttf', 'rtf', 'csv', 'txt']}

    ext = ext.lower()
    for key, val in mime_type.items():
        if str(ext) in str(val):
            return key


# (dpath: str) -> str:
def exif_data(dpath):
    '''
    Retrieve exiftool data
    return match to field if available
    '''

    cmd = [
        'exiftool',
        dpath
    ]
    data = subprocess.check_output(cmd, shell=False, universal_newlines=True)
    print(data)
    return data.split('\n')


def probe_metadata(arg, stream, fpath):
    '''
    Use FFmpeg module to extract
    ffprobe data from file
    '''
    try:
        probe = ffmpeg.probe(fpath)
    except ffmpeg.Error as err:
        print(err)
        return None

    for st in probe['streams']:
        if st['codec_type'] == stream:
            return st[arg]

    return None


# (stream: str, arg: str, dpath: str) -> str:
def get_metadata(stream, arg, dpath):
    '''
    Retrieve metadata with subprocess
    for supplied stream/field arg
    '''

    cmd: list[str] = [
        'mediainfo', '--Full',
        '--Language=raw',
        f'--Output={stream};%{arg}%',
        dpath
    ]

    meta = subprocess.check_output(cmd)
    return meta.decode('utf-8').rstrip('\n')


# (dpath: str, policy: str) -> tuple[bool, str]:
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

    meta = subprocess.check_output(cmd).decode('utf-8')
    if meta.startswith(f'pass! {dpath}'):
        return True, meta

    return False, meta


# (filepath: str) -> Optional[str | bytes]:
def get_ms(filepath):
    '''
    Retrieve duration as milliseconds if possible
    '''
    retry = False
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
    except Exception as err:
        print(f"Unable to extract duration with FFprobe: {err}")
        retry = True

    if retry:
        cmd = [
            'mediainfo',
            '--Language=raw', '-f',
            '--Output=General;%Duration%',
            filepath
        ]

        try:
            duration = subprocess.check_output(cmd)
        except Exception as err:
            print(f"Unable to extract duration with MediaInfo: {err}")
    if duration:
        return duration.decode('utf-8').rstrip('\n')
    return None


# (filepath: str) -> Optional[str | bytes]:
def get_duration(filepath):
    '''
    Retrieve duration field if possible
    '''
    retry = False
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
    except subprocess.CalledProcessError as err:
        print(f"Unable to extract duration with FFprobe: {err}")
        retry = True

    if retry:
        cmd = [
            'mediainfo',
            '--Language=raw', '-f',
            '--Output=General;%Duration/String3%',
            filepath
        ]

        try:
            duration = subprocess.check_output(cmd)
        except Exception as err:
            print(f"Unable to extract duration with MediaInfo: {err}")
    if duration:
        return duration.decode('utf-8').rstrip('\n')
    return None


# (log_path: str, level: str, message: str) -> None:
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


# (fpath: str) -> Optional[int]:
def get_size(fpath):
    '''
    Check the size of given folder path
    return size in kb
    '''
    if os.path.isfile(fpath):
        return os.path.getsize(fpath)

    try:
        byte_size: int= sum(os.path.getsize(os.path.join(fpath, f)) for f in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, f)))
        return byte_size
    except OSError as err:
        print(f"get_size(): Cannot reach folderpath for size check: {fpath}\n{err}")
        return None


# (fpath: str):
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


# (fname: str, check_str: str) -> Optional[list[str]]:
def check_global_log(fname, check_str):
    '''
    Read global log lines and look for a
    confirmation of deletion from autoingest
    '''

    with open(GLOBAL_LOG, 'r') as data:
        rows = csv.reader(data, delimiter='\n')
        for row in rows:
            row = row[0].split('\t')
            if fname in str(row) and check_str in str(row):
                print(row)
                return row


# (checksum_path: str, checksum: str, filepath: str, filename: str) -> str:
def checksum_write(checksum_path, checksum, filepath, filename):
    '''
    This function writes the checksum into a txt file with correct
    formatting and returns the path to that document
    '''
    date_string: str = str(datetime.date.today())
    try:
        with open(checksum_path, 'w') as fname:
            fname.write(f"{checksum} - {filepath} - {date_string}")
            fname.close()
        return checksum_path
    except Exception as e:
        print(f"{filename} - Unable to write checksum: {checksum_path}\n{e}")
        raise Exception


# (arg: str, output_type: str, filepath: str, mediainfo_path: str) -> str | bool:
def mediainfo_create(arg, output_type, filepath, mediainfo_path):
    '''
    Output mediainfo data to text files
    '''
    filename: str = os.path.basename(filepath)
    if arg == '-f':
        if output_type == 'TEXT':
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}_FULL.txt")
        elif output_type == 'JSON':
            out_path = os.path.join(mediainfo_path, f"{filename}_{output_type}.json")

        command: list[str] = [
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
    except Exception as e:
        print(e)
        raise Exception

    # Check file created has contents
    file_stats = os.stat(out_path)
    file_size = file_stats.st_size
    if file_size > 0:
        return out_path
    else:
        raise Exception


def local_file_search(fpath, fname):
    '''
    To assist potential for localised
    file movements within folders -
    checksums/metadata creation
    '''
    for root, _, files in os.walk(fpath):
        for file in files:
            if file == fname:
                return os.path.join(root, file)


def send_email(email: str, subject: str, body: str, files: str) -> None:
    '''
    automate the process of sending out simple emails
    '''
    try:
        msg = MIMEMultipart()
        msg['From'] = 'digitalpreservationsystems@bfi.org.uk'
        msg['To'] = email
        msg['Subject'] = subject


        if os.path.getsize(files) < 500000:
            with open(files, 'rb') as file:
                attachment_package = MIMEBase('application', 'octet-stream')
                attachment_package.set_payload((file).read())
                encoders.encode_base64(attachment_package)
                attachment_package.add_header('Content-Disposition', "attachment; filename= %s" % files)
                msg.attach(attachment_package)
        else:
            body += f"\n \n User has added an attachment: {files} which is above the recommended size, find a different method to send the file.\n"

        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=CONTEXT) as smtp:
            smtp.login(EMAIL, PASSWORD)
            smtp.sendmail('digitalpreservationsystems@bfi.org.uk', email, msg.as_string())

        print("Email sent!")

    except Exception as e:
        print(f'Error sending email: {e}')
