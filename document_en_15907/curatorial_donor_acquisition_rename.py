#! /usr/bin/env LANG=en_UK.UTF-8 /usr/local/bin/python3

'''
Curatorial Donor Acquisition Rename:

*** MUST BE LAUNCHED FROM SHELL START SCRIPT ***
1. Receive list of ‘workflow ****’ folders at a maximum and minimum depth of 2
   folders from the curatorial isilon share. Each entry is passed to this
   Python script and populates sys.argv[1] with the path name and launching
   a single run of the script.
2. Look through ‘workflow ****’ folder for files or folders, iterating through each.
3. Where a folder is found, the contents are checked for image sequence files
   (TIF, DPX, MXF) and a note is appended to a local log (placed within the
   'workflow ****' folder) recommending certain actions to be taken.
4. Where a file is found that is likely a media file, the following steps occur:
   - Extract basename of file and search in CID's digital.acquired_filename field
     for a whole name match. Extract all list entries of the digital.acquired_filename
     so that the part whole can be calculated when there are more than one.
   - Where found: extract object number from record and create new filename using
     object number and part whole, eg N_123456_01of01.
   - Rename files and update to a log within the 'workflows_****' folder.
     Move the successfully renamed file to a success_workflow_*****_01/ folder.
   - Where not found leave in place and append warning about CID failure to local log.

NOTE: DMS may want to alter accepted filetypes over time.

Joanna White 2022
Python 3.6+
'''

# Public packages
import subprocess
import datetime
import logging
import json
import sys
import os

# Private packages
import adlib

# Global path variables
CURATORIAL_PATH = os.environ['IS_CURATORIAL']
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
DIGIOPS_PATH = os.path.join(os.environ['QNAP_11_DIGIOPS'], 'Acquisitions/Curatorial/')
RSYNC_LOG = os.path.join(DIGIOPS_PATH, 'transfer_logs')
CID_API = os.environ['CID_API3']

# Setup logging
LOGGER = logging.getLogger('curatorial_donor_acquisition_rename.log')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'curatorial_donor_acquisition_rename.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# Global variables
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def cid_check():
    '''
    Tests if CID active before all other operations commence
    '''
    try:
        CUR = adlib.Cursor(CID)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical('Cannot establish CID session, exiting script')
        sys.exit()


def cid_retrieve(itemname, search):
    '''
    Receive filename and search in CID items
    Return object number to main
    '''
    query = {'database': 'items',
             'search': search,
             'limit': '0',
             'output': 'json',
             'fields': 'priref, object_number, title, Acquired_filename, digital.acquired_filename'}
    try:
        query_result = CID.get(query)
    except Exception:
        print("cid_retrieve(): Unable to retrieve data for {}".format(itemname))
        LOGGER.exception("cid_retrieve(): Unable to retrieve data for %s", itemname)
        query_result = None
    try:
        acquired1 = query_result.records[0]['Acquired_filename']
    except (KeyError, IndexError) as err:
        acquired1 = []
        LOGGER.warning("cid_retrieve(): Unable to access acquired filename1 %s", err)
        print(err)
    try:
        priref = query_result.records[0]['priref'][0]
        print(priref)
    except (KeyError, IndexError) as err:
        priref = ""
        LOGGER.warning("cid_retrieve(): Unable to access priref %s", err)
    try:
        ob_num = query_result.records[0]['object_number'][0]
    except (KeyError, IndexError) as err:
        ob_num = ""
        LOGGER.warning("cid_retrieve(): Unable to access object_number: %s", err)
    try:
        title = query_result.records[0]['Title'][0]['title'][0]
    except (KeyError, IndexError) as err:
        title = ""
        LOGGER.warning("cid_retrieve(): Unable to access title: %s", err)

    return (priref, ob_num, title, acquired1)


def main():
    '''
    Retrieve file items only, excepting those with unwanted extensions
    search in CID Item for digital.acquired_filename
    Retrieve object number and use to build new filename
    Rename and move to successful_rename/ folder
    '''
    LOGGER.info("=========== START Curatorial Donor Acquisition rename script START ==========")
    check_control()
    cid_check()
    if len(sys.argv) < 2:
        LOGGER.warning("SCRIPT EXITING: Error with shell script input:\n%s\n", sys.argv)
        sys.exit()

    fullpath = sys.argv[1]
    root, workflow_folder = os.path.split(fullpath)

    if not os.path.exists(fullpath):
        sys.exit(f"Incorrect folderpath supplied. Error in name: {fullpath}")

    # Get files and subfolders in Workflow folder
    files = [x for x in os.listdir(fullpath) if os.path.isfile(os.path.join(fullpath, x)) and not x.startswith('donor_acquisition_data')]
    dirs = [d for d in os.listdir(fullpath) if os.path.isdir(os.path.join(fullpath, d))]
    if not dirs and not files:
        LOGGER.info("SKIPPING: Folder path empty of files and directories: %s", fullpath)
        local_logger(f"Skipping as path is empty {fullpath}", fullpath)
        LOGGER.info("============= END Curatorial Donor Acquisition rename script END ============")
        sys.exit()

    if dirs:
        for directory in dirs:
            if 'success_' in directory:
                continue
            if 'workflow_' in directory.lower():
                continue
            dirpath = os.path.join(fullpath, directory)
            LOGGER.info("SKIPPING: %s Folder found, passing information to local log.\n", directory)
            folder_found(dirpath)

    if not files:
        LOGGER.info("SKIPPING: Folder path empty of files: %s", fullpath)
        local_logger(f"Skipping as path is empty of files {fullpath}", fullpath)
        LOGGER.info("============= END Curatorial Donor Acquisition rename script END ============")
        sys.exit()

    # Make new success folder for file moves
    success_path = os.path.join(root, f'success_{workflow_folder}')
    spath = check_path(root, success_path)
    full_spath = os.path.join(root, spath)
    try:
        os.makedirs(full_spath, mode=0o777, exist_ok=False)
        LOGGER.info("Making new success folder: %s", full_spath)
    except FileExistsError as err:
        LOGGER.warning("Folder already exists... %s", err)
        local_logger(f"Skipping as path is empty {fullpath}", fullpath)
        LOGGER.info("============= END Curatorial Donor Acquisition rename script END ============")
        sys.exit()

    # Begin processing files
    for item in files:
        if item.startswith('.'):
            continue
        itempath = os.path.join(fullpath, item)
        LOGGER.info("Item found checking file validity: %s", itempath)
        cid_data, priref, ob_num, title, acquired1 = '', '', '', '', ''
        if itempath.endswith(('.ini', '.json', '.document', '.edl', '.doc', '.docx', '.txt', '.mhl', '.DS_Store', '.log')):
            continue
        print(f"Item path found to process: {itempath}")
        LOGGER.info("** File okay to process: %s", item)
        LOGGER.info("Looking in CID item records for filename match...")

        # Retrieve CID data
        search = f'digital.acquired_filename="{item}"'
        cid_data = cid_retrieve(item, search)
        priref = cid_data[0]
        ob_num = cid_data[1]
        title = cid_data[2]
        acquired1 = cid_data[3]

        # Make changes to found file
        if (len(priref) > 0 and len(ob_num) > 0):
            LOGGER.info("CID item record match. Priref: %s  Object_number: %s  Title: %s", priref, ob_num, title)
            local_logger(f"\n----- New item found: {item} -----", fullpath)
            local_logger(f"Data retrieved from CID Item:\nItem object number: {ob_num} - Title: {title}", fullpath)
            local_logger(f"** Renumbering file with object number {ob_num}", fullpath)
            filename = make_filename(ob_num, acquired1, item)
            if not filename:
                LOGGER.warning("Problem creating new number for %s", item)
                local_logger(f"ERROR CREATING NEW FILENAME: {itempath}", fullpath)
                local_logger("Please check file has no permissions limitations, script will retry later", fullpath)
                continue
            LOGGER.info("Older filename %s to be replaced with new filename %s", item, filename)
            local_logger(f"Old filename: {item}\nNew filename: {filename}", fullpath)
            new_filepath = rename_pth(itempath, filename)
            LOGGER.info("File renamed to %s", new_filepath)
            local_logger(f"File renumbered and filepath updated to: {new_filepath}", fullpath)

            if not os.path.exists(new_filepath):
                LOGGER.warning("Error creating new filepath %s from Object number %s\n", new_filepath, ob_num)
                local_logger(f"ERROR RENAMING FILE. Please check file permissions: {itempath}", fullpath)
                local_logger(f"ERROR RENAMING FILE. This item will not be moved to: {spath}", fullpath)
                continue
            try:
                success_filepath = os.path.join(full_spath, filename)
                os.rename(new_filepath, success_filepath)
                LOGGER.info("File %s relocated to %s", new_filepath, success_filepath)
                local_logger(f"File {fullpath} relocated to {success_filepath}", fullpath)
            except OSError:
                LOGGER.warning("Unable to rename %s to %s\n", new_filepath, success_filepath)
                local_logger(f"Error relocating {new_filepath} to {success_filepath}", fullpath)

        else:
            LOGGER.info("File information not found in CID. Leaving file in place and updating logs.")
            local_logger(f"\nNO CID MATCH FOUND: File found {item} but no CID data retrieved", fullpath)
            local_logger(f"Please check CID item has digital.acquired_filename field populated with {item}\n", fullpath)
        local_logger("---------------- File process complete ----------------", fullpath)

    # Check new workflow folder has content (renaming failures mean empty)
    new_workflow_folder = os.path.basename(spath)
    if len(os.listdir(spath)) == 0:
        LOGGER.info("Success folder empty, no files renamed. Deleting folder: %s", spath)
        local_logger(f"Folder {new_workflow_folder} is empty. Likely no files successfully renamed. Deleting folder now.", fullpath)
        os.rmdir(spath)
        LOGGER.info("============= END Curatorial Donor Acquisition rename script END ============")
        sys.exit()

    # Rsync completed folder over
    local_logger(f"\nStarting RSYNC copy of {new_workflow_folder} to Digiops QC Curatorial path", fullpath)
    print("Rsync start here")
    rsync(spath, DIGIOPS_PATH, new_workflow_folder)
    # Repeat for checksum pass if first pass fails
    print("Repeat rsync start")
    rsync(spath, DIGIOPS_PATH, new_workflow_folder)
    print("Rsync finished")
    local_logger("RSYNC complete.", fullpath)

    LOGGER.info("============= END Curatorial Donor Acquisition rename script END ============")


def check_path(root, spath):
    '''
    Check in directories for path starts
    with spath. If exists, extract last two digits
    and add 1. If not, return spath with _01 on end.
    '''

    pth = os.path.basename(spath)
    dir_list = []
    dirs = os.listdir(root)
    for directory in dirs:
        if pth in directory:
            dir_list.append(directory)

    if not dir_list:
        return f"{spath}_01"
    else:
        dir_list.sort()
        last_dir = dir_list[-1]
        num = int(last_dir[-2:]) + 1
        number = str(num).zfill(2)
        return f"{spath}_{number}"


def make_filename(ob_num, item_list, item):
    '''
    Take individual elements and calculate part whole
    '''
    extension = False
    file = ob_num.replace('-', '_')
    ext = os.path.splitext(item)
    if len(ext[1]) > 0:
        extension = True
    acquired_list = []
    for dct in item_list:
        for _, val in dct.items():
            for string in val:
                acquired_list.append(string)
    part = acquired_list.index(item)
    whole = len(acquired_list)
    part_ = int(part) + 1
    part_ = str(part_).zfill(2)
    whole_ = str(whole).zfill(2)
    part_whole = f"_{part_}of{whole_}"
    if extension:
        filename = f"{file}{part_whole}{ext[1]}"
    else:
        filename = f"{file}{part_whole}"
    return filename


def rsync(file_path1, file_path2, folder):
    '''
    Move workflow folder from Curatorial/QNAP-11
    '''
    log_path = os.path.join(RSYNC_LOG, f"{folder}.log")
    rsync_cmd = [
        'rsync',
        '--info=FLIST2,COPY2,PROGRESS2,NAME2,BACKUP2,STATS2',
        '-acvvh',
        '--no-o', '--no-g',
        file_path1, file_path2,
        f'--log-file={log_path}'
    ]

    try:
        LOGGER.info("rsync(): Beginning rsync move of file %s to %s", file_path1, file_path2)
        print(f"Start rsync: {rsync_cmd}")
        subprocess.call(rsync_cmd)
    except Exception:
        LOGGER.exception("rsync(): Move command failure: %s to %s", file_path1, file_path2)


def folder_found(fullpath):
    '''
    Possibly check within folder for file types, update
    log with instructions to contact DigiOps team?
    '''
    path = os.path.split(fullpath)[0]
    image_seq = ''
    for root, _, files in os.walk(fullpath):
        for file in files:
            filepath = os.path.join(root, file)
            if file.endswith(('.dpx', '.DPX')):
                image_seq = 'DPX'
                break
            elif file.endswith(('.tif', '.tiff', '.TIF', '.TIFF')):
                image_seq = 'TIF'
                break
            elif file.endswith(('.mxf', '.MXF')):
                image_seq = 'MXF'
                break
            elif os.path.isfile(filepath):
                local_logger(f"\n{fullpath}:\nSub folder found containing files. Please review contents and remove from Workflow folder", path)
                break
            else:
                local_logger(f"\n{fullpath}: Folder found containing no files. Please review and remove from Workflow folder", path)
                break
    if len(image_seq) > 0:
        local_logger(f"\n{fullpath}:\nContains {image_seq} media files. Please contact digitalmediaspecialists@bfi.org.uk who will advise best method for preservation", path)


def rename_pth(filepath, new_filename):
    '''
    Receive original file path and rename filename
    based on object number, return new filepath, filename
    '''
    new_filepath = ''
    path, old_filename = os.path.split(filepath)
    new_filepath = os.path.join(path, new_filename)
    print("Renaming {} to {}".format(old_filename, new_filename))

    try:
        os.rename(filepath, new_filepath)
    except OSError:
        LOGGER.warning("There was an error renaming %s to %s", filepath, new_filepath)

    return new_filepath


def local_logger(data, write_path):
    '''
    Output local log data for team to monitor renaming process
    '''
    timestamp = str(datetime.datetime.now())
    log_path = os.path.join(write_path, 'donor_acquisition_data.log')
    if not os.path.isfile(log_path):
        with open(log_path, 'x') as log:
            log.close()

    with open(log_path, 'a+') as log:
        log.write(f"{data}  -  {timestamp[0:19]}\n")
        log.close()


if __name__ == '__main__':
    main()
