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

# Private packages
import bp_utils as bp
sys.path.append(os.environ['CODE'])
import adlib_v3_sess as adlib
import utils

# Global paths
LOGS = os.environ['LOG_PATH']
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
CID_API = os.environ['CID_API4']

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


def check_accepted_file_type(fpath):
    '''
    Retrieve codec and ensure file is accepted type
    TAR accepted from DMS / ProRes all other paths
    '''
    if any(x in fpath for x in ['qnap_access_renditions', 'qnap_10']):
        if fpath.endswith(('tar', 'TAR')):
            return True
    if any(x in fpath for x in ['qnap_06', 'film_operations', 'qnap_film']):
        if fpath.endswith(('mkv', 'MKV')):
            return True
    formt = utils.get_metadata('Video', 'Format', fpath)
    print(f"utils.get_metadata: {formt}")
    if 'ProRes' in str(formt):
        return True
    return False


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
        print(f'* Cannot parse partWhole from filename... {err}')
        logger.warning('%s\tCannot parse partWhole from filename %s', log_paths, err)
        return None, None, None, None

    return object_number, int(part), int(whole), ext


def get_item_priref(ob_num, session):
    '''
    Retrieve item priref, title from CID
    '''
    ob_num = ob_num.strip()
    search = f"object_number='{ob_num}'"
    record = adlib.retrieve_record(CID_API, 'collect', search, '1', session)[1]
    print(f"get_item_priref(): AdlibV3 record for priref:\n{record}")
    try:
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
        print(f"get_item_priref(): AdlibV3 priref: {priref}")
    except Exception:
        priref = ''

    return priref


def check_media_record(fname, session):
    '''
    Check if CID media record
    already created for filename
    '''
    search = f"imagen.media.original_filename='{fname}'"
    print(f"Search used against CID Media dB: {search}")
    try:
        hits = adlib.retrieve_record(CID_API, 'media', search, '0', session)[0]
        if hits is None:
            logger.exception('"CID API was unreachable for Media search: %s', search)
            raise Exception(f"CID API was unreachable for Media search: {search}")
        print(f"check_media_record(): AdlibV3 record for hits: {hits}")
        if hits == 0:
            return False
        elif hits == 1:
            return True
        elif hits > 1:
            return f'Hits exceed 1: {hits}'
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
    if bucket_collection == 'bfi':
        for key, _ in bucket_data.items():
            if 'preservation' in key.lower():
                bucket_list.append(key)
    else:
        for key, _ in bucket_data.items():
            if bucket_collection in key:
                bucket_list.append(key)
    return bucket_list


def ext_in_file_type(ext, priref, log_paths, session):
    '''
    Check if ext matches file_type
    '''

    ftype = utils.accepted_file_type(ext)
    if not ftype:
        print(f"Filetype not matched in dictionary: {ext}")
        logger.warning('%s\tFile type is not recognised in autoingest', log_paths)
        return False

    ftype = ftype.split(', ')
    print(ftype)
    search = f'priref={priref}'
    record = adlib.retrieve_record(CID_API, 'collect', search, '1', session, ['file_type'])[1]
    if record is None:
        return False

    print(f"ext_in_file_type(): AdlibV3 record returned:\n{record}")
    try:
        file_type = adlib.retrieve_field_name(record[0], 'file_type')
        print(f"ext_in_file_type(): AdlibV3 file type: {file_type}")
    except (IndexError, KeyError):
        logger.warning('%s\tInvalid <file_type> in Collect record', log_paths)
        return False

    if len(file_type) == 1:
        for ft in ftype:
            ft = ft.strip()
            if file_type[0] is None:
                return False
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


def get_media_ingests(object_number, session):
    '''
    Use object_number to retrieve all media records
    '''

    search = f'object.object_number="{object_number}"'
    hits, record = adlib.retrieve_record(CID_API, 'media', search, '0', session, ['imagen.media.original_filename'])
    if hits is None:
        logger.exception('"CID API was unreachable for Media search: %s', search)
        raise Exception(f"CID API was unreachable for Media search: {search}")
    print(f"get_media_ingests(): AdlibV3 record returned:\n{record}")

    original_filenames = []
    try:
        print(f'\t* MEDIA_RECORDS test - {hits} media records returned with matching object_number')
        for r in record:
            filename = adlib.retrieve_field_name(r, 'imagen.media.original_filename')[0]
            print(f"get_media_ingests(): AdlibV3 original file name found: {filename}")
            print(f"File found with CID record: {filename}")
            original_filenames.append(filename)
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


def asset_is_next(fname, ext, object_number, part, whole, black_pearl_folder, session):
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

    # Netflix extensions/DPX encodings can vary within filename range so shouldn't be included in range check
    if 'netflix_ingest' in black_pearl_folder or ext.lower() in ['mkv', 'tar']:
        fname_check = fname.split('.')[0]
        for num in range(1, range_whole):
            filename_range.append(f"{file}_{str(num).zfill(2)}of{str(whole).zfill(2)}")
    else:
        fname_check = fname
        for num in range(1, range_whole):
            filename_range.append(f"{file}_{str(num).zfill(2)}of{str(whole).zfill(2)}.{ext}")

    # Get previous parts index (hence -2)
    previous = part - 2
    ingest_fnames = get_media_ingests(object_number, session)

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
    config_dict = utils.read_yaml(CONFIG)
    print(f"utils.read_yaml: {config_dict}")

    sess = adlib.create_session()
    for host in config_dict['Hosts']:
        print(host)
        linux_host = list(host.keys())[0]
        tree = list(host.keys())[0]

        # Collect files
        files = get_mappings(tree, config_dict['Mappings'])
        print(files)
        for pth in files:
            if not utils.check_control('autoingest'):
                logger.info('Script run prevented by downtime_control.json. Script exiting.')
                sys.exit('Script run prevented by downtime_control.json. Script exiting.')
            fpath = os.path.abspath(pth)
            fname = os.path.split(fpath)[-1]

            # Allow path changes for black_pearl_ingest Netflix
            if 'ingest/netflix' in str(fpath):
                logger.info('%s\tIngest-ready file is from Netflix ingest path, setting Black Pearl Netflix ingest folder')
                black_pearl_folder = os.path.join(linux_host, f"{os.environ['BP_INGEST_NETFLIX']}")
                black_pearl_blobbing = f"{black_pearl_folder}/blobbing"
            elif 'ingest/amazon' in str(fpath):
                logger.info('%s\tIngest-ready file is from Amazon ingest path, setting Black Pearl Amazon ingest folder')
                black_pearl_folder = os.path.join(linux_host, f"{os.environ['BP_INGEST_AMAZON']}")
                black_pearl_blobbing = f"{black_pearl_folder}/blobbing"
            else:
                black_pearl_folder = os.path.join(linux_host, f"{os.environ['BP_INGEST']}")
                black_pearl_blobbing = f"{black_pearl_folder}/blobbing"

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
                boole = check_for_deletions(fpath, fname, log_paths, messages, sess)
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
                if not utils.check_filename(fname):
                    print(f'* Filename formatted incorrectly {fname}')
                    logger.warning("%s\tFilename formatted incorrectly", log_paths)
                    continue
                part, whole = utils.check_part_whole(fname)
                print(f"utils.check_part_whole: {part} {whole}")
                if not part or not whole:
                    print('* Cannot parse partWhole from filename')
                    logger.warning('%s\tCannot parse partWhole from filename', log_paths)
                    continue
                # Get object_number
                object_number = utils.get_object_number(fname)
                print(f"utils.get_object_number: {object_number}")
                if not object_number:
                    print('* Cannot parse <object_number> from filename')
                    logger.warning('%s\tCannot parse <object_number> from filename', log_paths)
                    continue

            # MIME/TYPE VALIDATIONS
            if not check_mime_type(fpath, log_paths):
                continue

            # CID checks
            priref = get_item_priref(object_number, sess)
            if not priref:
                print(f'* Cannot find record with <object_number>...<{object_number}>')
                logger.warning('%s\tCannot find record with <object_number>... <%s>', log_paths, object_number)
                continue
            print(f"* CID item record found with object number {object_number}: priref {priref}")

            # Ext in file_type and file_type validity in Collect database
            confirmed = ext_in_file_type(ext, priref, log_paths, sess)
            if not confirmed:
                continue

            # CID media record check
            media_check = check_media_record(fname, sess)
            if media_check is True:
                print(f'* Filename {fname} already has a CID Media record. Manual clean up needed.')
                logger.warning('%s\tFilename already has a CID Media record: %s', log_paths, fname)
                continue
            elif media_check is False:
                print(f'* File {fname} has no CID Media record.')
            elif 'Hits exceed 1' in media_check:
                print(f'* Filename {fname} has more than one CID Media record. Manual attention needed.')
                logger.warning('%s\tFilename has more than one CID Media record: %s', log_paths, fname)
                continue

            # Get BP buckets
            bucket_list = []
            if 'ingest/netflix' in fpath:
                bucket_list = get_buckets('netflix')
            elif 'ingest/amazon' in fpath:
                bucket_list = get_buckets('amazon')
            else:
                bucket_list = get_buckets('bfi')

            # BP ingest check
            status = bp.check_no_bp_status(fname, bucket_list)
            print(f"bp.check_no_bp_status: {status}")
            if status is False:
                print(f'* Filename {fname} has already been ingested to DPI. Manual clean up needed.')
                logger.warning('%s\tFilename has aleady been ingested to DPI: %s', log_paths, fname)
                continue
            print(f'* File {fname} has not been ingested to DPI yet.')

            # Begin ingest pass
            do_ingest = False

            # Move first part of incomplete scans
            if '/incomplete_scans/' in fpath:
                print('\n*** File is an incomplete scan. Moving for ingest ======')
                do_ingest = True
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
                    result = asset_is_next(fname, ext, object_number, part, whole, black_pearl_folder, sess)
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

            # Check if path / no ingests to take place
            if not utils.check_control('do_ingest'):
                print('* do_ingest set to false in control json, skipping')
                do_ingest = False
            if not utils.check_control(tree):
                print('* Path set to false in control json, turning ingest off')
                do_ingest = False

            # Perform ingest if under 1TB
            if do_ingest:
                size = utils.get_size(fpath)
                if size is None:
                    print("Unable to retrieve file size. Skipping for repeat try later.")
                    continue
                print(f"utils.get_size: {size}")
                print('\t* file has not been ingested, so moving it into Black Pearl ingest folder...')
                if int(size) > 1099511627776:
                    logger.info('%s\tFile is larger than 1TB. Checking file is ProRes', log_paths)
                    accepted_file_type = check_accepted_file_type(fpath)
                    if accepted_file_type is True:
                        try:
                            shutil.move(fpath, os.path.join(black_pearl_blobbing, fname))
                            print(f'\t** File moved to {os.path.join(black_pearl_blobbing, fname)}')
                            logger.info('%s\tMoved ingest-ready file to BlackPearl ingest blobbing folder', log_paths)
                        except Exception as err:
                            print(f'Failed to move file to blobbing folder: {black_pearl_blobbing} {err}')
                            logger.warning('%s\tFailed to move ingest-ready file to blobbing folder', log_paths)
                    else:
                        logger.warning('%s\tFile is larger than 1TB and not ProRes. Leaving in ingest folder', log_paths)
                    continue
                try:
                    shutil.move(fpath, os.path.join(black_pearl_folder, fname))
                    print(f'\t** File moved to {os.path.join(black_pearl_folder, fname)}')
                    logger.info('%s\tMoved ingest-ready file to BlackPearl ingest folder', log_paths)
                except Exception as err:
                    print(f'Failed to move file to black_pearl_ingest: {err}')
                    logger.warning('%s\tFailed to move ingest-ready file to BlackPearl ingest folder', log_paths)
                continue


def check_for_deletions(fpath, fname, log_paths, messages, session):
    '''
    Process files in completed/ folder by checking for persistence
    message that confirms deletion is allowed.
    '''

    # Check if CID media record exists
    media_check = check_media_record(fname, session)
    if media_check is False:
        print(f'*********** Filename {fname} has no CID Media record. Leaving for manual checks. (cid_media_record returned False)')
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
    '''
    # Temporary step to delete completed items whose logging failed early August 2024 (QNAP-01 drive failure)
    if 'qnap_imagen_storage/Public/autoingest/completed' in fpath:
        if media_check is True:
            logger.info("Ingested during QNAP-01 drive failure impacting Logs/ writes (August 2024). No deletion confirmation in global.log but CID Media record present. Deleting.")
            os.remove(fpath)
            logger.info('%s\tSuccessfully deleted file', log_paths)
            log_delete_message(fpath, 'Successfully deleted file', fname)
            print('* successfully deleted QNAP-04 item based on CID Media record...')
            return True
    '''
    return False


if __name__ == "__main__":
    main()
