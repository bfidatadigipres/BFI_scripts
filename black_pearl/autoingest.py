#!/usr/bin/env python3

'''
Autoingest script that checks for source file validity
for ingest to Black Pearl data tapes, and then checks
for source file copy persistence before deleting original
source file. Documents all actions/errors to global.log

Launched from autoingest_start.sh shell to capture all
printed statements to autoingest.log for daily review.
Requires autoingest_config.yaml for autoingest paths
and mappings.

main():
1. Downloads all persistence_queue messages to dictionary
2. Loads all the autoingest paths from autoingest_config
3. Iterates each autoingest path.
4. Downloads all files from across autoingest folder map.
   Iterates through each file checking:
 a. Checks if path contains '/completed/'. If yes jump
    to step 5.
 b. Checks if path contains special collections image/archive
    path. If yes skip to 6.
 c. Skips any path that does not contain '/ingest/'.
 d. Passes all other items for processing. Skip to step 7.
5. For '/completed/' path items the script jumps to the
   check_for_deletions() function which checks the filename
   against the persistence messages. If confirmation of
   persistence success found this function deletes the file,
   outputs action to logs and loops around to step 4.
6. For special collections image/archive items, the script
   assesses the filename for correctness, based on the unique
   styling, and extracts the part whole for that file.
   Skips to step 8.
7. For all other ingest worthy files, the script assesses the
   filename based on traditional CID filenaming, and extracts
   the standard part whole data. Skips to next step.
8. Completes the following ingest checks:
 a. Checks MIME type is appropriate for CID item
 b. Extracts the priref from the associated Item record using
    the filename as the object_number
 c. Assesses the extension, checking it's accepted as a format
    and checking it matches file_type in the CID item record
 d. Checks if the ingest path includes '/incomplete_scans/'
    folder name. If so, extracts list of all items in that
    folder with the same object_number and checks this file
    is the first part of the group and passes for ingest. If not
    the first part it is skipped until next pass. Skips to 8f.
 e. If not from an '/incomplete_scans/' folder the file is checked
    for single/multipart. If single part (01of01) it is passed
    straight to step 9. If multipart and not '01' the file's
    digital media records are checked using object number to see
    if the previous part has been ingested. IF yes, the file
    is moved to step 9. Otherwise it is passed over.
9. The file is moved from the autoingest/ingest path into
   the black_pearl_ingest folder where it is ingested to DPI.

Joanna White
2022
'''

# Public packages
import os
import re
import csv
import sys
import json
import shutil
import ntpath
import logging
import datetime
import subprocess
import magic
import yaml
from ds3 import ds3

# Private packages
sys.path.append(os.environ['CODE'])
import adlib

# Global paths
LOGS = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
GLOBAL_LOG = os.path.join(LOGS, 'autoingest/global.log')
PERS_LOG = os.path.join(LOGS, 'persistence_confirmation.log')
PERS_QUEUE = os.path.join(LOGS, 'autoingest/persistence_queue.csv')
PERS_QUEUE2 = os.path.join(LOGS, 'autoingest/persistence_queue2.csv')
CONFIG = os.environ['CONFIG_YAML']
DPI_BUCKETS = os.environ['DPI_BUCKET']

# Setup logging
logger = logging.getLogger('autoingest')
hdlr = logging.FileHandler(GLOBAL_LOG)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Setup CID/Black Pearl variables
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor
CLIENT = ds3.createClientFromEnv()

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


def check_control():
    '''
    Check control_json isn't False
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['autoingest']:
            print('* Exit requested by downtime_control.json. Script exiting')
            sys.exit('Exit requested by downtime_control.json. Script exiting')


def log_delete_message(pth, message, file):
    '''
    Save deletion message to persistence_confirmation.log
    '''
    datestamp = datetime.datetime.now()
    data = f"{datestamp} INFO\t{pth}\t{pth}\t{file}\t{message}\n"
    with open(PERS_LOG, 'a+') as out_file:
        out_file.write(data)


def get_persistence_messages():
    '''
    Collect current persistence messages
    '''
    csv_reader = []
    messages = {}
    with open(PERS_QUEUE, 'r') as pq:
        csv_reader = list(csv.reader(pq))

    csv_reader2 = []
    with open(PERS_QUEUE2, 'r') as pq:
        csv_reader2 = list(csv.reader(pq))
    csv_reader.extend(csv_reader2)

    for i in csv_reader:
        if len(i) != 3:
            continue
        p_path = i[0]
        p_message = i[1]
        print(f'{p_path}:\t{p_message}')
        # Keep latest message in dictionary
        if p_path and p_message:
            messages[p_path] = p_message

    return messages


def check_filename(fname):
    '''
    Run series of checks against filename to see
    that it's well formatted
    '''
    if not any(fname.startswith(px) for px in PREFIX):
        return False
    if not re.search("^[A-Za-z0-9_.]*$", fname):
        return False
    if len(fname.split('.')) > 2:
        return False
    sname = fname.split('_')
    if len(sname) > 4 or len(sname) < 3:
        return False
    if len(sname) == 4 and len(sname[2]) != 1:
        return False
    return True


def check_mime_type(fpath, log_paths):
    '''
    Checks the mime type of the file
    and if stream media checks ffprobe
    '''
    if fpath.endswith(('.mxf', '.ts', '.mpg')):
        mime = 'video'
    elif fpath.endswith(('.srt', '.scc', '.xml', '.itt', '.stl', '.cap', '.dfxp', '.dxfp', '.vtt', '.ttml')):
        mime = 'application'
    else:
        mime = magic.from_file(fpath, mime=True)
    try:
        type_ = mime.split('/')[0]
        print(f'* mime type is {type_}')
    except IOError:
        logger.warning('%s\tCannot open file, resource busy', log_paths)
        return False
    if type_ not in ['application', 'audio', 'image', 'video']:
        print(f'* MIMEtype "{type_}" is not permitted...')
        logger.warning('%s\tMIMEtype "%s" is not permitted', log_paths, type_)
        return False
    if type_ in ['audio', 'video']:
        cmd = ['ffprobe',
               '-i', fpath,
               '-loglevel', '-8']
        try:
            code = subprocess.call(cmd)
            if code != 0:
                logger.warning('%s\tffprobe failed to read file: [%s] status', log_paths, code)
                return False
            print('* ffprobe read file successfully - status 0')
        except Exception as err:
            logger.warning('%s\tffprobe failed to read file', log_paths)
            print(err)
            return False
    return True


def get_object_number(fname):
    '''
    Extract object number and check CID for item record
    '''
    if not any(fname.startswith(px) for px in PREFIX):
        return False
    try:
        splits = fname.split('_')
        object_number = '-'.join(splits[:-1])
    except Exception:
        object_number = None
    return object_number


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


def get_size(fpath):
    '''
    Retrieve size of ingest file in bytes
    '''
    return os.path.getsize(fpath)


def process_image_archive(fname, log_paths):
    '''
    Process special collections image
    archive filename structure
    '''
    if any(fname.startswith(px) for px in PREFIX):
        logger.warning('%s\tSpecial Collections path error. Cannot parse <object_number> from filename', log_paths)
        print('* Special Collections path error. Cannot parse <object_number> from filename...')
        return None, None, None, None
    split_name = fname.split('_')
    object_number, part, whole, ext = '','','',''
    if '-' in fname:
        print('* Cannot parse <object_number> from filename...')
        logger.warning('%s\tCannot parse <object_number> from filename', log_paths)
        return None, None, None, None
    try:
        object_number = '-'.join(split_name[:-1])
    except Exception:
        print('* Cannot parse <object_number> from filename...')
        logger.warning('%s\tCannot parse <object_number> from filename', log_paths)
        return None, None, None, None
    try:
        partwhole, ext = split_name[-1].split('.')
        part, whole = partwhole.split('of')
        if len(part) != len(whole):
            print('* Cannot parse partWhole from filename...')
            logger.warning('%s\tCannot parse partWhole from filename', log_paths)
            return None, None, None, None
        if len(part) > 4:
            print('* Cannot parse partWhole from filename...')
            logger.warning('%s\tCannot parse partWhole from filename', log_paths)
            return None, None, None, None
        if int(part) == 0:
            print('* Cannot parse partWhole from filename...')
            logger.warning('%s\tCannot parse partWhole from filename', log_paths)
            return None, None, None, None
        if int(part) > int(whole):
            print('* Cannot parse partWhole from filename...')
            logger.warning('%s\tCannot parse partWhole from filename', log_paths)
            return None, None, None, None
    except Exception as err:
        print('* Cannot parse partWhole from filename...')
        logger.warning('%s\tCannot parse partWhole from filename', log_paths)
        return None, None, None, None

    return (object_number, int(part), int(whole), ext)


def get_item_priref(ob_num):
    '''
    Retrieve item priref, title from CID
    '''
    ob_num = ob_num.strip()

    data = {'database': 'collect',
            'search': f"object_number='{ob_num}'",
            'fields': 'priref',
            'output': 'json'}

    result = CID.get(data)
    try:
        priref = int(result.records[0]['priref'][0])
    except Exception:
        priref = ''
        pass
    return priref


def check_media_record(fname):
    '''
    Check if CID media record
    already created for filename
    '''
    search = f"imagen.media.original_filename='{fname}'"
    query = {
        'database': 'media',
        'search': search,
        'limit': '0',
        'output': 'json',
    }

    try:
        result = CID.get(query)
        if result.hits:
            return True
    except Exception as err:
        print(f"Unable to retrieve CID Media record {err}")
    return False


def get_buckets(bucket_collection):
    '''
    Read JSON list return
    key_value and list of others
    '''
    bucket_list = []

    with open(DPI_BUCKETS) as data:
        bucket_data = json.load(data)
    if bucket_collection == 'netflix':
        for key, _ in bucket_data.items():
            if bucket_collection in key:
                bucket_list.append(key)
    elif bucket_collection == 'bfi':
        for key, _ in bucket_data.items():
            if 'preservation' in key.lower():
                bucket_list.append(key)
            # Imagen path read only now
            if 'imagen' in key:
                bucket_list.append(key)

    return bucket_list


def check_bp_status(fname, bucket_list):
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
            if int(length) > 1 and len(md5) > 30:
                return True
        except (IndexError, TypeError, KeyError) as err:
            print(err)


def ext_in_file_type(ext, priref, log_paths):
    '''
    Check if ext matches file_type
    '''
    ext = ext.lower()
    dct = {'imp': 'mxf, xml',
           'tar': 'dpx, dcp, dcdm, wav',
           'mxf': 'mxf, 50i, imp',
           'mpg': 'mpeg-1, mpeg-2',
           'mp4': 'mp4',
           'mov': 'mov, prores',
           'mkv': 'mkv, dpx',
           'wav': 'wav',
           'tif': 'tif, tiff',
           'tiff': 'tif, tiff',
           'jpg': 'jpg, jpeg',
           'jpeg': 'jpg, jpeg',
           'ts': 'mpeg-ts',
           'srt': 'srt',
           'xml': 'xml, imp',
           'scc': 'scc',
           'itt': 'itt',
           'stl': 'stl',
           'cap': 'cap',
           'dfxp': 'dfxp'}

    try:
        ftype = dct[ext]
        ftype = ftype.split(', ')
    except Exception:
        print(f"Filetype not matched in dictionary: {ext}")
        logger.warning('%s\tFile type is not recognised in autoingest', log_paths)
        return False

    print(ftype)
    query = {'database': 'collect',
             'search': f'priref={priref}',
             'limit': 1,
             'output': 'json',
             'fields': 'file_type'}

    result = CID.get(query)
    try:
        file_type = result.records[0]['file_type']
    except (IndexError, KeyError):
        file_type = []

    if len(file_type) == 1:
        for ft in ftype:
            ft = ft.strip()
            if ft == file_type[0].lower():
                print(f'* extension matches <file_type> in record... {file_type}')
                return True
            elif ft == 'mxf' and ft in file_type[0].lower():
                print(f'* extension matches <file_type> in record... {file_type}')
                return True
        logger.warning('%s\tExtension does not match <file_type> in record', log_paths)
        print(f'* WARNING extension does not match file_type: {ftype} {file_type}')
        return False
    elif len(file_type) > 1:
        logger.warning('%s\tInvalid <file_type> in Collect record. Just one should be present.', log_paths)
        print(f'* WARNING more than one file_type in CID: {file_type}')
    else:
        logger.warning('%s\tInvalid <file_type> in Collect record', log_paths)
        return False


def get_media_ingests(object_number):
    '''
    Use object_number to retrieve all media records
    '''

    dct = {'database': 'media',
           'search': f'object.object_number="{object_number}"',
           'fields': 'imagen.media.original_filename',
           'limit': 0,
           'output': 'json'}

    original_filenames = []
    try:
        result = CID.get(dct)
        print(f'\t* MEDIA_RECORDS test - {result.hits} media records returned with matching object_number')
        print(result.records)
        for r in result.records:
            filename = r['imagen.media.original_filename']
            print(f"File found with CID record: {filename}")
            original_filenames.append(filename[0])
    except Exception as err:
        print(err)

    return original_filenames


def get_ingests_from_log(fname):
    '''
    Iterate global.log looking for
    filename and message match
    to prove ingest status of parts
    '''
    ingest_files = []
    with open(GLOBAL_LOG, 'r') as data:
        lines = data.readlines()
        target_lines = [ x for x in lines if fname in str(x) ]
        for line in target_lines:
            data_line = line.split('\t')
            filename = data_line[4]
            mssg = data_line[5]
            # Get messages featuring filename
            if 'Moved ingest-ready file to BlackPearl ingest folder' in str(mssg):
                ingest_files.append(filename)

    return ingest_files


def asset_is_next_ingest(fname, previous_fname, black_pearl_folder):
    '''
    New function that checks partWhole ingest order
    and looks for previous part in black_pearl_folder
    '''
    fsplit = fname.split('_')
    file = '_'.join(fsplit[:-1])

    ingest_fnames = [f for _,_,files in os.walk(black_pearl_folder) for f in files if f.startswith(file)]

    if previous_fname in str(ingest_fnames):
        return True
    else:
        return False


def asset_is_next(fname, ext, object_number, part, whole, black_pearl_folder):
    '''
    Check which files have persisted already and
    if this file is next in queue
    '''

    if part == 1:
        return 'True'

    fsplit = fname.split('_')
    file = '_'.join(fsplit[:-1])
    range_whole = whole + 1
    filename_range = []

    # Netflix extensions vary within IMP so shouldn't be included in range check
    if 'netflix_ingest' in black_pearl_folder:
        fname_check = fname.split('.')[0]
        for num in range(1, range_whole):
            filename_range.append(f"{file}_{str(num).zfill(2)}of{str(whole).zfill(2)}")
    else:
        fname_check = fname
        for num in range(1, range_whole):
            filename_range.append(f"{file}_{str(num).zfill(2)}of{str(whole).zfill(2)}.{ext}")

    # Get previous parts index (hence -2)
    previous = part - 2
    ingest_fnames = get_media_ingests(object_number)

    if not ingest_fnames:
        in_bp_ingest_folder = asset_is_next_ingest(fname, filename_range[previous], black_pearl_folder)
        if in_bp_ingest_folder:
            print(f"Is the asset in BP ingest folder: {in_bp_ingest_folder} {type(in_bp_ingest_folder)}")
            return 'True'
        return 'False'

    if fname_check in str(ingest_fnames):
        return 'Ingested already'
    elif filename_range[previous] in str(ingest_fnames):
        print(f"Filename previous in ingest_fnames:{filename_range[previous]} {ingest_fnames}")
        return 'True'
    else:
        in_bp_ingest_folder = asset_is_next_ingest(fname, filename_range[previous], black_pearl_folder)
        if in_bp_ingest_folder:
            print(f"Is the asset in BP ingest folder: {in_bp_ingest_folder} {type(in_bp_ingest_folder)}")
            return 'True'
        return 'False'


def sequence_is_next(fpath, fname, object_number, ext, log_paths):
    '''
    Ingest image sequences that do not have
    complete reels as MKV or TAR
    '''
    if not fname.endswith(('.mkv', '.tar')):
        print(f"* Incorrect sequence file type received: {fname} with extension {ext}")
        logger.warning('%s\tIncorrect film sequence file type in folder incomplete_sequences', log_paths)
        return False

    filepath = os.path.split(fpath)[0]
    filename = fname.split('_')
    filename = '_'.join(filename[:2])

    # Check if fname already ingested or if only item in incomplete_scans/ folder
    ingest_fnames = get_media_ingests(object_number)
    if fname in ingest_fnames:
        return 'Ingested already'
    file_list = [ x for x in os.listdir(filepath) if filename in x ]
    if len(file_list) == 1:
        print('* Suitable for ingest')
        return True

    # Check if fname is first in line for ingest from file_list
    sorted_list = file_list.sort()
    if fname == sorted_list[0]:
        print('* Suitable for ingest')
        return True
    else:
        print('* Currently unsuitable for ingest')
        return False


def load_yaml(file):
    '''
    Safe open yaml and return as dict
    '''
    with open(file) as config_file:
        d = yaml.safe_load(config_file)
        return d


def get_mappings(pth, mappings):
    '''
    Get files within config.yaml mappings
    Path limitations for slow storage
    '''
    if '/mnt/qnap_video/' in pth:
        max_ = 1000
    else:
        max_ = 2000

    mapped = []
    count = 0
    for directory in mappings:
        directory_path = os.path.join(pth, directory)
        for root, _, files in os.walk(directory_path):
            if os.path.split(directory_path)[1] == 'video':
                # Skipping adjust path to avoid double ingest of subfolders
                if 'adjust' in root:
                    continue
            for f in files:
                fpath = os.path.join(root, f)
                mapped.append(fpath)
                count += 1
                if count == max_:
                    return mapped
    return mapped


def main():
    '''
    Iterate config hosts, using autoingest mappings
    navigate all autoingest paths and sort files for ingest
    or deletion.
    '''
    messages = {}
    messages = get_persistence_messages()
    print('* Finished collecting persistence_queue messages...')

    print('* Collecting ingest sources from config.yaml...')
    config_dict = load_yaml(CONFIG)

    for host in config_dict['Hosts']:
        print(host)
        linux_host = list(host.keys())[0]
        tree = list(host.keys())[0]

        # Collect files
        files = get_mappings(tree, config_dict['Mappings'])
        print(files)
        for pth in files:
            check_control()
            fpath = os.path.abspath(pth)
            fname = os.path.split(fpath)[-1]

            # Allow path changes for black_pearl_ingest Netflix
            if 'ingest/netflix' in fpath:
                logger.info('%s\tIngest-ready file is from Netflix ingest path, setting Black Pearl Netflix ingest folder')
                black_pearl_folder = os.path.join(linux_host, 'autoingest/black_pearl_netflix_ingest')
            else:
                black_pearl_folder = os.path.join(linux_host, 'autoingest/black_pearl_ingest')

            if '.DS_Store' in fname:
                continue
            if fname.endswith(('.txt', '.md5', '.log', '.mhl', '.ini', '.json')):
                continue
            ext = fname.split('.')[-1]

            print(f'\n====== CURRENT FILE: {fpath} ===========================')

            # Create paths and join for logs
            relative_nix_path = fpath.replace(tree, host[tree])
            relative_path = ntpath.normpath(relative_nix_path)
            log_paths = '\t'.join([fpath, relative_path, fname])

            if 'autoingest/completed/' in fpath:
                # Push completed/ paths straight to deletions checks
                print('* Item is in completed/ path, moving to persistence checks')
                boole = check_for_deletions(fpath, fname, log_paths, messages)
                print(f'File successfully deleted: {boole}')
                continue

            elif 'special_collections' in fpath and 'proxy/image/archive/' in fpath:
                print('* File is Special Collections archive image')
                # Simplified name check
                if not re.search("^[A-Za-z0-9_.]*$", fname):
                    print(f'* Filename formatted incorrectly {fname}')
                    logger.warning("%s\tFilename formatted incorrectly", log_paths)
                    continue
                object_number, part, whole, ext = process_image_archive(fname, log_paths)
                if not object_number or not part:
                    continue

            elif not '/ingest/' in fpath:
                print('* Filepath is not an ingest path')
                continue

            else:
                # NAME/PART WHOLE VALIDATIONS
                if not check_filename(fname):
                    print(f'* Filename formatted incorrectly {fname}')
                    logger.warning("%s\tFilename formatted incorrectly", log_paths)
                    continue
                part, whole = check_part_whole(fname)
                if not part or not whole:
                    print('* Cannot parse partWhole from filename')
                    logger.warning('%s\tCannot parse partWhole from filename', log_paths)
                    continue
                # Get object_number
                object_number = get_object_number(fname)
                if not object_number:
                    print('* Cannot parse <object_number> from filename')
                    logger.warning('%s\tCannot parse <object_number> from filename', log_paths)
                    continue

            # MIME/TYPE VALIDATIONS
            if not check_mime_type(fpath, log_paths):
                continue

            # CID checks
            priref = get_item_priref(object_number)
            if not priref:
                print(f'* Cannot find record with <object_number>...<{object_number}>')
                logger.warning('%s\tCannot find record with <object_number>... <%s>', log_paths, object_number)
                continue
            print(f"* CID item record found with object number {object_number}: priref {priref}")

            # Ext in file_type and file_type validity in Collect database
            confirmed = ext_in_file_type(ext, priref, log_paths)
            if not confirmed:
                continue

            # CID media record check
            media_check = check_media_record(fname)
            if media_check is True:
                print(f'* Filename {fname} already has a CID Media record. Manual clean up needed.')
                logger.warning('%s\tFilename already has a CID Media record: %s', log_paths, fname)
                continue
            print(f'* File {fname} has no CID Media record.')

            # Get BP buckets
            bucket_list = []
            if 'ingest/netflix' in fpath:
                bucket_list = get_buckets('netflix')
            else:
                bucket_list = get_buckets('bfi')

            # BP ingest check
            ingest_check = check_bp_status(fname, bucket_list)
            if ingest_check is True:
                print(f'* Filename {fname} has already been ingested to DPI. Manual clean up needed.')
                logger.warning('%s\tFilename has aleady been ingested to DPI: %s', log_paths, fname)
                continue
            print(f'* File {fname} has not been ingested to DPI yet.')

            # Begin ingest pass
            do_ingest = False

            # Move first part of incomplete scans
            if '/incomplete_scans/' in fpath:
                print('\n*** File is an incomplete scan. Checking if next for ingest ======')
                confirm = sequence_is_next(fpath, fname, object_number, ext, log_paths)
                if confirm is True:
                    print(f"Ingest approved for file {fname}")
                    do_ingest = True
                elif 'Ingested already' in confirm:
                    print('\t\t* Already ingested! Not to be reingested')
                    logger.warning('%s\tThis file name has already been ingested and has CID Media record', log_paths)
                    continue
                else:
                    print('\t\t*Skipping object as previous part has not yet been ingested')
                    logger.info("%s\tSkip object as previous part not yet ingested or queued for ingest", log_paths)
                    continue
            else:
                # Move items for ingest if they are single parts, first parts, or next in queue
                print('\n*** TEST for ASSET_MULTIPART ======')
                if whole == 1:
                    print('\t* file is not multipart...')
                    print('\t* asset is single part and not yet ingested, preparing for ingest...')
                    do_ingest = True
                elif part == 1:
                    print('\t* file is multipart...')
                    print('\t* asset is first part and not yet ingested, preparing for ingest...')
                    do_ingest = True
                else:
                    print('\t* file is multi-part...')
                    print('\t\t* === AUTOINGEST - TEST for ASSET_IS_NEXT ======')
                    result = asset_is_next(fname, ext, object_number, part, whole, black_pearl_folder)
                    if 'No index' in result:
                        print('\t\t***** Indexing logic broken')
                        continue
                    if 'Ingested already' in result:
                        print('\t\t* Already ingested! Not to be reingested')
                        logger.warning('%s\tThis file name has already been ingested and has CID Media record', log_paths)
                        continue
                    if 'False' in result:
                        print('\t\t* multi-part file, not suitable for ingest at this time...')
                        logger.info('%s\tSkip object as previous part not yet ingested or queued for ingest', log_paths)
                        continue
                    # Prepare multiparter ingest configuration
                    print('\n*** TEST for ASSET_MULTIPART and ASSET_IS_NEXT ======')
                    print('\t* asset is multipart and is next in queue...')
                    print('\t\t* multi-part file, suitable for ingest...')
                    do_ingest = True

            # Check if path / no ingests to take place [PROBABLY CAN BE DEPRECATED]
            with open(CONTROL_JSON) as control_pth:
                cp = json.load(control_pth)
                if not cp['do_ingest']:
                    print('* do_ingest set to false in control json, skipping')
                    do_ingest = False
                if not cp[tree]:
                    print('* Path set to false in control json, turning ingest off')
                    do_ingest = False

            # Perform ingest if under 1TB
            if do_ingest:
                size = get_size(fpath)
                if int(size) > 1099511627776:
                    logger.warning('%s\tFile is larger than 1TB. Leaving in ingest folder', log_paths)
                    continue
                print('\t* file has not been ingested, so moving it into Black Pearl ingest folder...')
                try:
                    shutil.move(fpath, os.path.join(black_pearl_folder, fname))
                    print(f'\t** File moved to {os.path.join(black_pearl_folder, fname)}')
                    logger.info('%s\tMoved ingest-ready file to BlackPearl ingest folder', log_paths)
                except Exception as err:
                    print(f'Failed to move file to black_pearl_ingest: {err}')
                    logger.warning('%s\tFailed to move ingest-ready file to BlackPearl ingest folder', log_paths)
                continue


def check_for_deletions(fpath, fname, log_paths, messages):
    '''
    Process files in completed/ folder by checking for persistence
    message that confirms deletion is allowed.
    '''

    # Check if CID media record exists
    media_check = check_media_record(fname)
    if media_check is False:
        print(f'* Filename {fname} has no CID Media record. Leaving for manual checks.')
        logger.warning('%s\tCompleted file has no CID Media record: %s', log_paths, fname)
        return False

    mssg_pth, message = '', ''
    for key, value in messages.items():
        if fname in key:
            mssg_pth = key
            message = value
            print(f'* Message: {mssg_pth} {message}')

            # Get messages featuring filename
            if message == '':
                continue

            # Persistence validation failure
            if message != 'Persistence checks passed: delete file':
                continue

            # Clear to delete
            elif message == 'Persistence checks passed: delete file':
                print(message)
                logger.info('%s\t%s', log_paths, message)
                if os.path.exists(fpath):
                    print(f'* Deleting now: {fpath}')
                    try:
                        os.remove(fpath)
                        logger.info('%s\tSuccessfully deleted file', log_paths)
                        log_delete_message(fpath, 'Successfully deleted file', fname)
                        print('* successfully deleted file based on persistence result...')
                        return True
                    except Exception as err:
                        print(f'* File deletion failed despite filepath existing:\n{err}')
                        logger.warning('%s\tFailed to delete file', log_paths)
                else:
                    print('* File already absent from path. Check problem with persistence message')
    return False


if __name__ == "__main__":
    main()
