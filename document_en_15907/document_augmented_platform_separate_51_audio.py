#!/usr/bin/ python3

'''
Script to retrieve folders of
Platform separate 5.1 audio files named after
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

NOTES: Integrated with adlib_v3 for test

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
import requests

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global variables
LOGS = os.environ.get('LOG_PATH')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API4')
PLATFORM_STORAGE = os.environ.get('PLATFORM_INGEST_PTH')

# Setup logging
LOGGER = logging.getLogger('document_augmented_platform_separate_51_audio')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_platform_separate_51_audio.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

STORAGE = {
    'Netflix': f"{os.path.join(PLATFORM_STORAGE, os.environ.get('NETFLIX_INGEST'))}, {os.path.join(PLATFORM_STORAGE, 'svod/netflix/separate5_1/')}",
    'Amazon': f"{os.path.join(PLATFORM_STORAGE, os.environ.get('AMAZON_INGEST'))}, {os.path.join(PLATFORM_STORAGE, 'svod/amazon/separate5_1/')}"
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


def cid_check(object_number):
    '''
    Looks up object_number and retrieves title
    and other data for new separate 5.1 audio record
    '''
    search = f"object_number='{object_number}'"
    hits, record = adlib.retrieve_record('items', search, '0', fields=None)
    if hits == 0:
        return None
    return record


def walk_folders(storage):
    '''
    Collect list of folderpaths
    for files named rename_<platform>
    '''
    print(storage)
    folders = []
    for root, dirs, _ in os.walk(storage):
        for directory in dirs:
            folders.append(os.path.join(root, directory))
    print(f"{len(folders)} rename folder(s) found")

    return folders


def main():
    '''
    Search for folders named after CID item records
    Check for contents and create new CID item record
    for each audio file within. Rename and move for ingest.
    '''

    LOGGER.info("== Document augmented streaming platform separate audio start ===================")
    for key, value in STORAGE.items():
        check_control()
        platform = key
        autoingest, storage = value.split(', ')

        folder_list = walk_folders(storage)
        if len(folder_list) == 0:
            LOGGER.info("%s Separate 5.1 audio record creation script. No folders found.", platform)
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
            record = cid_check(object_number)
            if record is None:
                LOGGER.warning("Skipping: Record could not be matched with object_number")
                continue

            priref = adlib.retrieve_field_name(record[0], 'priref')
            if not priref:
                continue
            print(f"Priref matched with retrieved folder name: {priref}")
            LOGGER.info("Priref matched with folder name: %s", priref)

            # Create CID item record for each audio file in folder
            for file in file_list:
                if not file.endswith(('.WAV', '.wav')):
                    LOGGER.warning("File contained in separate5_1 audio folder that is not WAV: %s", file)
                ext = file.split('.')[-1]
                item_data = create_new_item_record(priref, file, record)
                if item_data is None:
                    continue
                LOGGER.info("** CID Item record created: %s - %s", item_data[0], item_data[1])

                # Append quality comments to new CID item record
                qual_comm = "5.1 audio supplied separately as IMP contains Dolby Atmos IAB"
                success = adlib.add_quality_comments(priref, qual_comm)
                if not success:
                    LOGGER.warning("Quality comments were not written to record: %s", priref)
                LOGGER.info("Quality comments added to CID item record %s", priref)

                # Rename file to new filename from object-number
                tt_priref = item_data[0]
                tt_ob_num = item_data[1]
                print(f"CID Item record created: {tt_priref}, {tt_ob_num}")
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

    LOGGER.info("== Document augmented streaming platform separate audio end =====================\n")


def build_record_defaults(platform):
    '''
    Return all record defaults
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': f'{platform} metadata integration - automated bulk documentation for separate 5.1 audio'},
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


def make_item_record_dict(priref, fname, record):
    '''
    Get CID item record for source and borrow data
    for creation of new CID item record
    '''

    if 'Acquisition_source' in str(record):
        platform = record[0]['Acquisition_source'][0]['acquisition.source'][0]
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
        imp_title = record[0]['Title'][0]['title'][0]
        item.append({'title': f"{imp_title} (5.1 audio)"})
        if 'title.article' in str(record):
            item.append({'title.article': record[0]['Title'][0]['title.article'][0]})
        item.append({'title.language': 'English'})
        item.append({'title.type': '05_MAIN'})
    else:
        LOGGER.warning("No title data retrieved. Aborting record creation")
        return None
    if 'Part_of' in str(record):
        item.append({'part_of_reference.lref': record[0]['Part_of'][0]['part_of_reference'][0]['priref'][0]})
    else:
        LOGGER.warning("No part_of_reference data retrieved. Aborting record creation")
        return None
    item.append({'related_object.reference.lref': priref})
    item.append({'related_object.notes': '5.1 audio for'})
    item.append({'file_type': 'WAV'})
    item.append({'code_type': 'WAV'})
    if 'acquisition.date' in str(record):
        item.append({'acquisition.date': record[0]['acquisition.date'][0]})
    if 'acquisition.method' in str(record):
        item.append({'acquisition.method': record[0]['acquisition.method'][0]})
    if 'Acquisition_source' in str(record):
        item.append({'acquisition.source': record[0]['Acquisition_source'][0]['acquisition.source'][0]})
        item.append({'acquisition.source.type': record[0]['Acquisition_source'][0]['acquisition.source.type'][0]['value'][0]})
    item.append({'access_conditions': 'Access requests for this collection are subject to an approval process. '\
                                      'Please raise a request via the Collections Systems Service Desk, describing your specific use.'})
    item.append({'access_conditions.date': str(datetime.datetime.now())[:10]})
    if 'grouping' in str(record):
        item.append({'grouping': record[0]['grouping'][0]})
    if 'language' in str(record):
        item.append({'language': record[0]['language'][0]['language'][0]})
        item.append({'language.type': record[0]['language'][0]['language.type'][0]['value'][0]})
    if len(fname) > 1:
        item.append({'digital.acquired_filename': fname})

    return item


def create_new_item_record(priref, fname, record):
    '''
    Build new CID item record from existing data and make CID item record
    '''
    item_dct = make_item_record_dict(priref, fname, record[0])
    LOGGER.info(item_dct)
    item_xml = adlib.create_record_data('', item_dct)
    new_record = adlib.post(item_xml, 'items', 'insertrecord', '')
    if new_record is None:
        LOGGER.warning("Skipping: CID item record creation failed: %s", item_xml)
        return None
    LOGGER.info("New CID item record created: %s", new_record)
    return new_record


if __name__ == '__main__':
    main()
