#!/usr/bin/env python3

'''
RC DPX renaming script
** MUST BE RUN FROM SHELL LAUNCH SCRIPT **

1. Receive path to DPX sequence and check if path contains '/graded/' or '/raw/'
2. Extract object number from basename of the folder and convert to source object number
3. Check CID and retrieve derived items list for the source, where file_type is ‘DPX’
4. Where object number is found, allocate to ‘raw_ob_num’ variable
5. Check CID retrieve derived item for raw_ob_num, and allocate to ‘graded_ob_num’.
6. Check CID to retrieve 'graded_ob_num' priref
7. Check CID to retrieve production.notes for graded item using 'graded_priref'.
   If ‘Graded DPX’ in production notes:
   - Yes, proceed with renaming and move with supplied ‘raw_ob_num’ and ‘graded_ob_num’
   - No, reset ‘raw_ob_num' and ‘graded_ob_num’ to None as they are not the item records
     generated in the ‘4K Digitisation Partnership project 2021/22’ project.
8. Renumbering folders if object numbers are retained, and if raw or graded DPX highlighted
   by path. Folders are then moved to dpx_to_assess
9. If no match the DPX sequences are not renamed and moved to ‘error_renumbering’ folder
10. All details of movements are written to Isilon log and human readable log in QNAP-06.

NOTE: Updated for Adlib V3
2022
'''

# Public packages
import datetime
import logging
import shutil
import sys
import os
import re

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Global variables
QNAP_PATH = os.environ['QNAP_FILMOPS']
RENUMBER_PATH = os.path.join(QNAP_PATH, os.environ['SEQ_RENUMBER'])
DPX_TO_ASSESS = os.path.join(QNAP_PATH, os.environ['DPX_ASSESS'])
DPX_TO_ASSESS_FOUR = os.path.join(QNAP_PATH, os.environ['DPX_ASSESS_FOUR'])
DPX_FOR_REVIEW = os.path.join(QNAP_PATH, os.environ['DPX_REVIEW'])
LOG_PATH = os.environ['LOG_PATH']
LOCAL_LOG = os.path.join(RENUMBER_PATH, 'renumbering_log.txt')
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API4']
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]

# Setup logging
LOGGER = logging.getLogger('rc_dpx_rename')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'rc_dpx_rename.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_list_retrieve(database: str, search:str) -> list[str]:
    '''
    Receive search and look in CID manifestations
    for list of object numbers/prirefs for file_type DPX
    '''

    hits, records = adlib.retrieve_record(CID_API, database, search, '0', ['priref', 'object_number', 'file_type'])
    if hits is None:
        raise Exception(f'CID API could not be reached with {database} search:\n{search}')
    if not records:
        LOGGER.exception("cid_list_retrieve(): Unable to retrieve data: %s", search)
        return None

    record_list = []
    LOGGER.info("CID hits: %s", hits)
    for record in records:
        try:
            priref = adlib.retrieve_field_name(record, 'priref')[0]
            record_list.append(priref)
        except (IndexError, TypeError, KeyError):
            pass

    return record_list


def cid_retrieve(database: str, search: str) - > tuple[str, str, str, str, str, list[str]]:
    '''
    Receive filename and search in CID records
    Return all available data to main
    '''
    fields: list[str] = [
        'object_number',
        'priref',
        'title',
        'file_type',
        'production.notes',
        'derived_item'
    ]

    try:
        record = adlib.retrieve_record(CID_API, database, search, '0', fields)[1]
    except Exception:
        LOGGER.exception("cid_retrieve(): Unable to retrieve data: %s", search)
        record = None
    if 'object_number' in str(record):
        object_number = adlib.retrieve_field_name(record[0], 'object_number')[0]
    else:
        object_number = ''
    if 'priref' in str(record):
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    else:
        priref = ''
    if 'title' in str(record):
        priref = adlib.retrieve_field_name(record[0], 'title')[0]
    else:
        title = ''
    if 'file_type' in str(record):
        file_type = adlib.retrieve_field_name(record[0], 'file_type')[0]
    else:
        file_type = ''
    if 'production.notes' in str(record):
        prod_notes = adlib.retrieve_field_name(record[0], 'production.notes')[0]
    else:
        prod_notes = ''

    # UNSURE HOW THIS WILL WORK - NEEDS TESTING
    try:
        derived_items = []
        derived_items = adlib.retrieve_field_name(record[0], 'derived_item')
    except (KeyError, IndexError):
        derived_items = ''

    derived_item_list = []
    for dct in derived_items:
        for key, val in dct.items():
            if key == 'derived_item':
                derived_item_list.append(val[0])

    return (object_number, priref, title, file_type, prod_notes, derived_item_list)


def main():
    '''
    Retrieve file items only, excepting those with unwanted extensions
    search in CID Item for digital.acquired_filename
    Retrieve object number and use to build new filename
    Rename and move to successful_rename/ folder
    '''
    LOGGER.info("======= Python launch RC DPX rename script START ======")
    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()
    if not utils.check_control('pause_scripts'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    if not os.geteuid() == 0:
        sys.exit("\nOnly root can run this script\n")

    if len(sys.argv) < 2:
        LOGGER.warning("SCRIPT EXITING: Error with shell script input:\n%s\n", sys.argv)
        sys.exit()

    # Obtain path and check it exists
    fullpath = sys.argv[1]
    if not os.path.exists(fullpath):
        LOGGER.info("SKIPPING: Folder path missing: %s", fullpath)
        local_logger(f"Skipping as path is missing {fullpath}")
        sys.exit()

    dpx = os.path.basename(fullpath)
    LOGGER.info("** %s - DPX sequence found for CID query and renumbering", dpx)
    local_logger(f"\n=============== DPX sequence found for renumbering: {dpx} ==============")

    # Check part whole formatted correctly
    part_whole = dpx.split('_')[-1]
    match = re.match('\d\dof\d\d', part_whole)
    if not match:
        local_logger(f"WARNING! DPX number {dpx} part whole not formatted correctly")
        LOGGER.warning("Folder name error - part whole not formatted correctly: %s", dpx)
        fail_move(fullpath, dpx)
        sys.exit()

    # Determine DPX type
    raw = graded = False
    if '/raw/' in fullpath:
        LOGGER.info("DPX type is RAW DPX")
        local_logger("DPX type is RAW DPX")
        raw = True
    if '/graded/' in fullpath:
        graded = True
        LOGGER.info("DPX type is Graded DPX")
        local_logger("DPX type is Graded DPX")

    print(dpx)
    source_ob_num = make_object_num(dpx)
    print(source_ob_num)
    if source_ob_num is None:
        local_logger(f"WARNING! DPX number {dpx} not formed correctly and failed conversion to object number")
        LOGGER.warning("Folder name error: Object number creation failed from DPX folder: %s", dpx)
        fail_move(fullpath, dpx)
        sys.exit()

    # Retrieve list of derived items from source
    pdata = []
    search = f'(object_number="{source_ob_num}")'
    pdata = cid_retrieve('items', search)
    derived_items = pdata[5]

    # Retrieve raw and graded object numbers from derived_item searches
    raw_ob_num = graded_ob_num = graded_priref = None
    if len(derived_items) == 0:
        LOGGER.warning("No derived items found for Source object number: %s", source_ob_num)
        LOGGER.info("Moving DPX sequence %s to 'error_renumbering' folder for CID data review", dpx)
        local_logger(f"WARNING! Failed to find derived items for {source_ob_num} - moving {dpx} to 'error_renumbering' folder")
        fail_move(fullpath, dpx)
        sys.exit()
    elif len(derived_items) == 1:
        raw_ob_num = derived_items[0]
        search = f'object_number="{raw_ob_num}"'
        bdata = cid_retrieve('items', search)
        graded_ob_num = bdata[5][0]
        LOGGER.info("RAW and Graded DPX object numbers found (pre-validation): RAW %s GRADED %s", raw_ob_num, graded_ob_num)
        local_logger(f"RAW {raw_ob_num} and Graded {graded_ob_num} DPX item numbers found")
        print(f"RAW object number: {raw_ob_num}  Graded DPX number: {graded_ob_num}")
    else:
        derived_list = []
        for derived_item in derived_items:
            search = f'(object_number="{derived_item}" AND file_type="DPX")'
            cdata = cid_retrieve('items', search)
            if 'DPX' in str(cdata):
                derived_list.append(cdata)
        if (len(derived_list)) == 1:
            raw_data = derived_list[0]
            raw_ob_num = raw_data[0]
            graded_ob_num = raw_data[5][0]
            print(f"RAW object number: {raw_ob_num}  Graded DPX number: {graded_ob_num}")
            LOGGER.info("RAW and Graded DPX object numbers found (pre-validation): RAW %s GRADED %s", raw_ob_num, graded_ob_num)
            local_logger(f"RAW {raw_ob_num} and Graded {graded_ob_num} DPX item numbers found")
        elif (len(derived_list)) > 1:
            LOGGER.warning("More than one derived item has DPX file_type. Cannot identify correct RAW object number - manual help needed")
            local_logger("WARNING! Source has more than one DPX derived item. Cannot isolate new number")
            local_logger(f"Please attend to this DPX sequence {dpx} - Manual renaming may be necessary")
            raw_ob_num = graded_ob_num = None

    # Validate graded_ob_number has production.notes = Graded DPX
    # If not present then not Lucy's creations. Reset object numbers
    if graded_ob_num:
        search = f'(object_number="{graded_ob_num}" AND file_type="DPX")'
        gdata = cid_retrieve('items', search)
        graded_priref = gdata[1]
    if graded_priref:
        search = f'(priref="{graded_priref}")'
        gdata = cid_retrieve('items', search)
        if 'Graded DPX' not in str(gdata):
            graded_ob_num = None
            raw_ob_num = None
            LOGGER.warning("FAIL: Graded object number does not have 'Graded DPX' in production.notes")
            local_logger("FAILED: Graded Item record does not have 'Graded DPX' in production.notes")
        else:
            print(gdata)
            LOGGER.info("PASS: Graded Item record has 'Graded DPX' in production.notes")
            local_logger("PASS: Graded Item record has 'Graded DPX' in production.notes")
    else:
        local_logger("FAILED: Graded object number not found")
        LOGGER.warning("FAIL: Graded object number not found")
        graded_ob_num = None
        raw_ob_num = None

    # Commence renaming / move where object numbers present
    if raw and raw_ob_num:
        dpx_rename = raw_ob_num
    elif graded and graded_ob_num:
        dpx_rename = graded_ob_num
    else:
        LOGGER.warning("DPX source object number not matched to any Item record's source_item")
        LOGGER.info("Moving DPX sequence %s to 'error_renumbering' folder for CID data review", dpx)
        local_logger(f"WARNING! Failed to find DPX Item record for {dpx}")
        success = fail_move(fullpath, dpx)
        dpx_rename = None
        if not success:
            LOGGER.warning("fail_move() function exited without warning")

    if dpx_rename is None:
        sys.exit()

    # Continue with renaming
    LOGGER.info("** %s - new DPX sequence name identified and validated", dpx_rename)
    success = rename(fullpath, dpx_rename)
    if success:
        LOGGER.info("%s renamed to %s", dpx, success[1])
        local_logger(f"DPX sequence renamed from {dpx} to {success[1]}")
    else:
        LOGGER.warning("WARNING! Folder %s was not renamed after RAW DPX Item object number %s", source_ob_num, dpx_rename)
        local_logger(f"WARNING! Folder {source_ob_num} was not renamed after RAW DPX Item object number {dpx_rename}")
        sys.exit()

    # Sort folder lengths and move to three/four depth assessment
    move_path = folder_depth(success[0])
    move_success = False
    if move_path is None:
        # Move to dpx_to_review
        LOGGER.warning("WARNING! Folder contents of %s are not three/four depth. Moving to DPX_FOR_REVIEW", success[1])
        local_logger(f"WARNING! Folder {success[1]} does not have regular folder contents. Moving to DPX_FOR_REVIEW")
        move_success = move(success[0], 'review')
    elif 'dpx_to_assess_fourdepth' in move_path:
        LOGGER.info("%s folder has four folder structure and will be moved to DPX_TO_ASSESS_FOURDEPTH", success[1])
        local_logger(f"{success[1]} has folder depth four and will be moved to DPX_TO_ASSESS_FOURDEPTH")
        move_success = move(success[0], 'encoding4')
    else:
        LOGGER.info("%s folder has folder depth of three and will be moved to DPX_TO_ASSESS", success[1])
        local_logger(f"{success[1]} has folder depth of three and will be moved to DPX_TO_ASSESS")
        move_success = move(success[0], 'encoding3')
    # Check move success
    if move_success:
        LOGGER.info("Folder %s successfully moved", dpx_rename)
        local_logger(f"Folder {success[1]} successfully moved")
    else:
        LOGGER.warning("WARNING! Move for %s failed. Manual move required.", success[1])
        local_logger(f"WARNING! Folder {success[1]} not moved. Manual move required.")
        sys.exit()

    local_logger("=============== Renaming and movement of DPX sequence completed ==============")
    LOGGER.info("============= END RC DPX rename script END ============")


def make_object_num(dpx: str) -> Optional[str]:
    '''
    Receive a filename remove part whole
    and return as object number:
    N-123456 or N-123456-2
    '''
    fname = dpx.rstrip('/')
    name_split = fname.split('_')

    if len(name_split) == 3:
        return f"{name_split[0]}-{name_split[1]}"
    elif len(name_split) == 4:
        return f"{name_split[0]}-{name_split[1]}-{name_split[2]}"
    else:
        return None


def fail_move(fullpath: str, dpx: str) -> bool:
    '''
    Handles repeated failure moves
    '''
    move_success = move(fullpath, 'error')
    if move_success:
        LOGGER.info("Successfully moved to errors folder.")
        local_logger("Successfully moved to errors folder.")
        local_logger("=============== Renaming and movement of DPX sequence completed ===============")
        LOGGER.info("============= END RC DPX rename script END ============")
        return True
    else:
        LOGGER.warning("WARNING! Failed to move %s to 'error_numbering' folder", dpx)
        local_logger(f"WARNING! Failed to move {dpx} to 'error_numbering' folder. Please attend to manually")
        local_logger("=============== Renaming and movement of DPX sequence completed ==============")
        LOGGER.info("============= END RC DPX rename script END ============")
        return False


def folder_depth(fullpath: str) -> Optional[str]:
    '''
    Check if folder is three depth of four depth
    '''
    folders = 0
    for _, dirnames, _ in os.walk(fullpath):
        folders += len(dirnames)
    if folders == 2:
        return DPX_TO_ASSESS
    elif folders == 3:
        return DPX_TO_ASSESS_FOUR
    else:
        return None


def rename(filepath: str, ob_num: str) -> tuple[str, str]:
    '''
    Receive original file path and rename filename
    based on object number and original part whole
    return new filepath and filename
    '''
    new_filepath = new_filename = ''
    path, filename = os.path.split(filepath)
    parts = filename.split('_')
    print(parts)
    new_name = ob_num.replace('-', '_')
    new_filename = f"{new_name}_{parts[-1]}"
    print(new_filename)
    print(f"Renaming {filename} to {new_filename}")
    new_filepath = os.path.join(path, new_filename)

    try:
        os.rename(filepath, new_filepath)
    except OSError:
        LOGGER.warning("rename(): There was an error renaming %s to %s", filename, new_filename)

    return (new_filepath, new_filename)


def move(filepath: str, arg: str) -> bool:
    '''
    Move existing filepath to new filepath
    '''
    print(filepath, arg)
    move_path = ''
    if arg == 'error':
        path, filename = os.path.split(filepath)
        if '/raw' in str(path):
            move_path = os.path.join(RENUMBER_PATH, 'error_renumbering/raw/', filename)
        elif '/graded' in str(path):
            move_path = os.path.join(RENUMBER_PATH, 'error_renumbering/graded/', filename)
    if arg == 'encoding3':
        path, filename = os.path.split(filepath)
        move_path = os.path.join(DPX_TO_ASSESS, filename)
    if arg == 'encoding4':
        path, filename = os.path.split(filepath)
        move_path = os.path.join(DPX_TO_ASSESS_FOUR, filename)
    if arg == 'review':
        path, filename = os.path.split(filepath)
        move_path = os.path.join(DPX_FOR_REVIEW, filename)

    try:
        shutil.move(filepath, move_path)
        return True
    except Exception as err:
        LOGGER.warning("Error trying to move file %s to %s.\nError: %s", filename, move_path, err)
        return False


def local_logger(data: str) -> None:
    '''
    Output local log data for team to monitor renaming process
    '''
    timestamp = str(datetime.datetime.now())
    if not os.path.isfile(LOCAL_LOG):
        with open(LOCAL_LOG, 'x') as log:
            log.close()

    with open(LOCAL_LOG, 'a+') as log:
        log.write(f"{data}  -  {timestamp[0:19]}\n")
        log.close()


if __name__ == '__main__':
    main()
