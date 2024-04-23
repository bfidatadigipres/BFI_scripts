#!/usr/bin/ python3

'''
Script to retrieve folders of
Platform timed text named after
CID Item record object_number.

1. Looks for subfolders in STORAGE path
2. Extract object number from folder name
   and makes list of all files within folder
3. Iterates the enclosed files completing stages:
   a/ Build dictionary for new Item record
   b/ Convert to XML using adlib_v3
   c/ Push data to CID to create item record
   d/ If successful rename file after new CID
      item object_number (forced 01of01) and move
      to autoingest path
4. When all files in a folder processed the
   folder is checked as empty and deleted

NOTES: Updated to work with adlib_v3

Joanna White
2024
'''

# Public packages
import os
import sys
import json
import shutil
import logging
import datetime

# Local packages
sys.path.append(os.environ['CODE'])
from adlib_v3 import adlib

# Global variables
LOGS = os.environ.get('LOG_PATH')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
PLATFORM_STORAGE = os.environ.get('PLATFORM_INGEST_PTH')
CID_API = os.environ.get('CID_API4')

# Setup logging
LOGGER = logging.getLogger('document_augmented_platform_timed_text')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_platform_timed_text.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

STORAGE = {
    'Netflix': f"{os.path.join(PLATFORM_STORAGE, os.environ.get('NETFLIX_INGEST'))}, {os.path.join(PLATFORM_STORAGE, 'svod/netflix/timed_text/')}",
    'Amazon': f"{os.path.join(PLATFORM_STORAGE, os.environ.get('AMAZON_INGEST'))}, {os.path.join(PLATFORM_STORAGE, 'svod/amazon/timed_text/')}"
}


def check_control():
    '''
    Check for downtime control
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            LOGGER.info("Script run prevented by downtime_control.json. Script exiting")
            sys.exit("Script run prevented by downtime_control.json. Script exiting")


def cid_check():
    '''
    Test if CID API online
    '''
    try:
        test = adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def cid_check_ob_num(object_number):
    '''
    Looks up object_number and retrieves title
    and other data for new timed text record
    '''
    search = f"object_number='{object_number}'"
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '1', fields=None)
    if hits == 0:
        return None
    return record


def walk_folders(storage):
    '''
    Collect list of folderpaths
    for files named rename_<platform>
    '''
    print(storage)
    timed_text_folders = []
    for root, dirs, _ in os.walk(storage):
        for directory in dirs:
            timed_text_folders.append(os.path.join(root, directory))
    print(f"{len(timed_text_folders)} rename folder(s) found")

    return timed_text_folders


def main():
    '''
    Search for folders named after CID item records
    Check for contents and create new CID item record
    for each timed text within. Rename and move for ingest.
    '''

    LOGGER.info("== Document augmented streaming platform timed text start ===================")
    for key, value in STORAGE.items():
        check_control()
        cid_check()

        platform = key
        autoingest, storage = value.split(', ')

        folder_list = walk_folders(storage)
        if len(folder_list) == 0:
            LOGGER.info("%s timed text record creation script. No folders found.", platform)
            continue

        for fpath in folder_list:
            if not os.path.exists(fpath):
                LOGGER.warning("Folder path is not valid: %s", fpath)
                continue
            object_number = os.path.basename(fpath)
            file_list = os.listdir(fpath)
            if not file_list:
                LOGGER.warning("Skipping. No files found in folderpath: %s", fpath)
                continue
            LOGGER.info("Files found in target folder %s: %s", object_number, ', '.join(file_list))

            # Check object number valid
            record = cid_check_ob_num(object_number)
            if record is None:
                LOGGER.warning("Skipping: Record could not be matched with object_number")
                continue

            priref = adlib.retrieve_field_name(record[0], 'priref')[0]
            print(f"Priref matched with retrieved folder name: {priref}")
            LOGGER.info("Priref matched with folder name: %s", priref)

            # Create CID item record for each timed text in folder
            for file in file_list:
                ext = file.split('.')[-1]
                item_record = create_new_item_record(priref, file, record)
                if item_record is None:
                    continue

                tt_priref = adlib.retrieve_field_name(item_record, 'priref')[0]
                tt_ob_num = adlib.retrieve_field_name(item_record, 'object_number')[0]
                LOGGER.info("** CID Item record created: %s", tt_priref)
                print(f"CID Item record created: {tt_priref}, {tt_ob_num}")

                # Rename file to new filename from object-number
                new_fname = f"{tt_ob_num.replace('-', '_')}_01of01.{ext}"
                new_fpath = os.path.join(fpath, new_fname)
                LOGGER.info("%s to be renamed %s", file, new_fname)
                rename_success = rename_or_move('rename', os.path.join(fpath, file), new_fpath)
                if rename_success is False:
                    LOGGER.warning("Unable to rename file: %s", os.path.join(fpath, file))
                elif rename_success is True:
                    LOGGER.info("File successfully renamed. Moving to %s ingest path", platform)
                elif rename_success == 'Path error':
                    LOGGER.warning("Path error: %s", os.path.join(fpath, file))

                # Move file to new autoingest path
                move_success = rename_or_move('move', new_fpath, os.path.join(autoingest, new_fname))
                if move_success is False:
                    LOGGER.warning("Error with file move to autoingest, leaving in place for manual assistance")
                elif move_success is True:
                    LOGGER.info("File successfully moved to %s ingest path: %s\n", platform, autoingest)
                elif move_success == 'Path error':
                    LOGGER.warning("Path error: %s", new_fpath)

            # Check fpath is empty and delete
            if len(os.listdir(fpath)) == 0:
                LOGGER.info("All files processed in folder: %s", object_number)
                LOGGER.info("Deleting empty folder: %s", fpath)
                os.rmdir(fpath)
            else:
                LOGGER.warning("Leaving folder %s in place as files still remaining in folder %s", object_number, os.listdir(fpath))

    LOGGER.info("== Document augmented streaming platform timed text end =====================\n")


def build_record_defaults(platform):
    '''
    Return all record defaults
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': f'{platform} metadata integration - automated bulk documentation for timed text'},
               {'record_access.user': 'BFIiispublic'},
               {'record_access.rights': '0'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'System Management'},
               {'record_access.rights': '3'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Information Specialist'},
               {'record_access.rights': '3'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Digital Operations'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Documentation'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Curator'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Special Collections'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': 'Librarian'},
               {'record_access.rights': '2'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'record_access.user': '$REST'},
               {'record_access.rights': '1'},
               {'record_access.reason': 'SENSITIVE_LEGAL'}])

    return record


def rename_or_move(arg, file_a, file_b):
    '''
    Use shutil or os to move/rename
    from file a to file b. Verify change
    before confirming success/failure
    '''

    if not os.path.isfile(file_a):
        return 'Path error'

    if arg == 'move':
        try:
            shutil.move(file_a, file_b)
        except Exception as err:
            LOGGER.warning("rename_or_move(): Failed to %s file to new destination: \n%s\n%s", arg, file_a, file_b)
            print(err)
            return False

    if arg == 'rename':
        try:
            os.rename(file_a, file_b)
        except Exception as err:
            LOGGER.warning("rename_or_move(): Failed to %s file to new destination: \n%s\n%s", arg, file_a, file_b)
            print(err)
            return False

    if os.path.isfile(file_b):
        return True
    return False


def make_item_record_dict(priref, file, record):
    '''
    Get CID item record for source and borrow data
    for creation of new CID item record
    '''
    ext = file.split('.')[-1]
    if 'Acquisition_source' in str(record):
        platform = adlib.retrieve_field_name(record[0], 'acquisition.source')[0]
        record_default = build_record_defaults(platform)
    else:
        record_default = build_record_defaults('Streaming platform')

    item = []
    item.extend(record_default)
    item.append({'record_type': 'ITEM'})
    item.append({'item_type': 'DIGITAL'})
    item.append({'copy_status': 'M'})
    item.append({'copy_usage.lref': '131560'})
    item.append({'accession_date': str(datetime.datetime.now())[:10]})

    if 'Title' in str(record):
        imp_title = adlib.retrieve_field_name(record[0], 'title')[0]
        item.append({'title': f"{imp_title} (Timed Text)"})
        if adlib.retrieve_field_name(record[0], 'title_article')[0]:
            item.append({'title.article': adlib.retrieve_field_name(record[0], 'title_article')[0]})
        item.append({'title.language': 'English'})
        item.append({'title.type': '05_MAIN'})
    else:
        LOGGER.warning("No title data retrieved. Aborting record creation")
        return None
    if 'Part_of' in str(record):
        parent_priref = adlib.retrieve_field_name(record[0]['Part_of'][0]['part_of_reference'][0], 'priref')[0]
        item.append({'part_of_reference.lref': parent_priref})
    else:
        LOGGER.warning("No part_of_reference data retrieved. Aborting record creation")
        return None
    item.append({'related_object.reference.lref': priref})
    item.append({'related_object.notes': 'Timed text for'})
    if len(ext) > 1:
        item.append({'file_type': ext.upper()})
    if 'acquisition.date' in str(record):
        item.append({'acquisition.date': adlib.retrieve_field_name(record[0], 'acquisition.date')[0]})
    if 'acquisition.method' in str(record):
        item.append({'acquisition.method': adlib.retrieve_field_name(record[0], 'acquisition.method')[0]})
    if 'Acquisition_source' in str(record):
        if 'Netflix' in adlib.retrieve_field_name(record[0], 'acquisition.source')[0]:
            item.append({'acquisition.source.lref': '143463'})
            item.append({'acquisition.source.type': 'DONOR'})
        elif 'Amazon' in adlib.retrieve_field_name(record[0], 'acquisition.source')[0]:
            item.append({'acquisition.source.lref': '999923912'})
            item.append({'acquisition.source.type': 'DONOR'})
    item.append({'access_conditions': 'Access requests for this collection are subject to an approval process. '\
                                      'Please raise a request via the Collections Systems Service Desk, describing your specific use.'})
    item.append({'access_conditions.date': str(datetime.datetime.now())[:10]})
    if 'grouping' in str(record):
        item.append({'grouping': adlib.retrieve_field_name(record[0], 'grouping')[0]})
    if 'language' in str(record):
        item.append({'language': adlib.retrieve_field_name(record[0], 'language')[0]})
        item.append({'language.type': adlib.retrieve_field_name(record[0], 'language.type')[0]})
    if len(file) > 1:
        item.append({'digital.acquired_filename': file})
        item.append({'digital.acquired_filename.type': 'FILE'})

    return item


def create_new_item_record(priref, fname, record):
    '''
    Build new CID item record from existing data and make CID item record
    '''
    item_dct = make_item_record_dict(priref, fname, record)
    LOGGER.info(item_dct)
    item_xml = adlib.create_record_data('', item_dct)
    new_record = adlib.post(CID_API, item_xml, 'items', 'insertrecord', '')
    if new_record is None:
        LOGGER.warning("Skipping: CID item record creation failed: %s", item_xml)
        return None
    LOGGER.info("New CID item record created: %s", new_record)
    return new_record


if __name__ == '__main__':
    main()
