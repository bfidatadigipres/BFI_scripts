#!/usr/bin/ python3

'''
DEPRECATED?
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
   d/ Build dct for all contained WAV files for
      5.1 audio, and create dict of old filenames
      and new filenames ordered for 5.1 L,R,C,LFE,Ls,Rs
   e/ Rename the files one by one with partwhole 0*of06
      and move to autoingest
   f/ Update all digital.acquired_filenames to CID item
      record and append quality_comments also
4. When all files in a folder processed the
   folder is checked as empty and deleted

NOTES: Integrated with adlib_v3 for test

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
from typing import Final, Optional, Any

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Global variables
LOGS: Final = os.environ.get('LOG_PATH')
CONTROL_JSON: Final = os.path.join(LOGS, 'downtime_control.json')
PLATFORM_STORAGE: Final = os.environ.get('PLATFORM_INGEST_PTH')
CID_API: Final = os.environ.get('CID_API4')

# Setup logging
LOGGER = logging.getLogger('document_augmented_platform_separate_51_audio')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_platform_separate_51_audio.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

STORAGE: Final = {
    'Netflix': f"{os.path.join(PLATFORM_STORAGE, os.environ.get('NETFLIX_INGEST'))}, {os.path.join(PLATFORM_STORAGE, 'svod/netflix/separate5_1/')}",
    'Amazon': f"{os.path.join(PLATFORM_STORAGE, os.environ.get('AMAZON_INGEST'))}, {os.path.join(PLATFORM_STORAGE, 'svod/amazon/separate5_1/')}"
}

ORDER: Final = {
    'L': '01',
    'R': '02',
    'C': '03',
    'LFE': '04',
    'Ls': '05',
    'Rs': '06'
}


def cid_check_ob_num(object_number: str) -> Optional[list[dict[str, Any]]]:
    '''
    Looks up object_number and retrieves title
    and other data for new separate 5.1 audio record
    '''
    search = f"object_number='{object_number}'"
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '0')
    if hits == 0:
        return None
    return record


def walk_folders(storage: str) ->list[str]:
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


def main() -> None:
    '''
    Search for folders named after CID item records
    Check for contents and create new CID item record
    for each audio file within. Rename and move for ingest.
    '''

    LOGGER.info("== Document augmented streaming platform separate audio start ===================")
    for key, value in STORAGE.items():
        if not utils.check_control('pause_scripts'):
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')
        if not utils.cid_check(CID_API):
            LOGGER.critical("* Cannot establish CID session, exiting script")
            sys.exit("* Cannot establish CID session, exiting script")

        platform = key
        autoingest, storage = value.split(', ')

        folder_list = walk_folders(storage)
        if len(folder_list) == 0:
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
            if len(file_list) != 6:
                LOGGER.warning("Skipping. Incorrect amount of files found in path: %s", fpath)
                continue
            LOGGER.info("Files found in target folder %s: %s", object_number, ', '.join(file_list))

            # Check object number valid
            record = cid_check_ob_num(object_number)
            if record is None:
                LOGGER.warning("Skipping: Record could not be matched with object_number")
                continue

            source_priref = adlib.retrieve_field_name(record[0], 'priref')[0]
            if not source_priref:
                continue
            print(f"Priref matched with retrieved folder name: {source_priref}")
            LOGGER.info("Priref matched with folder name: %s", source_priref)

            # Create CID item record for batch of six audio files in folder
            item_record = create_new_item_record(source_priref, record)
            if item_record is None:
                continue
            print(item_record)
            new_priref = adlib.retrieve_field_name(item_record, 'priref')[0]
            new_ob_num = adlib.retrieve_field_name(item_record, 'object_number')[0]
            LOGGER.info("** CID Item record created: %s - %s", new_priref, new_ob_num)
            print(f"CID Item record created: {new_priref}, {new_ob_num}")

            file_names = build_fname_dct(file_list, new_ob_num)
            print(file_names)

            filename_dct = {}
            for key, value in file_names.items():
                new_fname = key
                old_fname = value
                filename_dct[old_fname] = new_fname

                if not old_fname.endswith(('.WAV', '.wav')):
                    LOGGER.warning("File contained in separate5_1 audio folder that is not WAV: %s", old_fname)

                new_fpath = os.path.join(fpath, new_fname)
                LOGGER.info("%s to be renamed %s", old_fname, new_fname)
                rename_success = rename_or_move('rename', os.path.join(fpath, old_fname), new_fpath)
                if rename_success is False:
                    LOGGER.warning("Unable to rename file: %s", os.path.join(fpath, old_fname))
                elif rename_success is True:
                    LOGGER.info("File successfully renamed. Moving to %s ingest path", platform)
                elif rename_success == 'Path error':
                    LOGGER.warning("Path error: %s", os.path.join(fpath, old_fname))

                # Move file to new autoingest path
                move_success = rename_or_move('move', new_fpath, os.path.join(autoingest, new_fname))
                if move_success is False:
                    LOGGER.warning("Error with file move to autoingest, leaving in place for manual assistance")
                elif move_success is True:
                    LOGGER.info("File successfully moved to %s ingest path: %s\n", platform, autoingest)
                elif move_success == 'Path error':
                    LOGGER.warning("Path error: %s", new_fpath)

            # Write all dict names to digital.acquired_filename in CID item record
            success = create_digital_original_filenames(new_priref, filename_dct)
            if not success:
                LOGGER.warning("Skipping further actions. Digital acquired filenames not written to CID item record: %s", new_priref)
                continue
            LOGGER.info("CID item record <%s> filenames appended to digital.acquired_filenamed field", new_priref)
            LOGGER.info("Digital Acquired Filename data added to CID item record %s", new_priref)
            qual_comm = "5.1 audio supplied separately as IMP contains Dolby Atmos IAB."
            success = adlib.add_quality_comments(CID_API, new_priref, qual_comm)
            if not success:
                LOGGER.warning("Quality comments were not written to record: %s", new_priref)
            LOGGER.info("Quality comments added to CID item record %s", new_priref)

            # Check fpath is empty and delete
            if len(os.listdir(fpath)) == 0:
                LOGGER.info("All files processed in folder: %s", object_number)
                LOGGER.info("Deleting empty folder: %s", fpath)
                os.rmdir(fpath)
            else:
                LOGGER.warning("Leaving folder %s in place as files still remaining in folder %s", object_number, os.listdir(fpath))

    LOGGER.info("== Document augmented streaming platform separate audio end =====================\n")


def build_fname_dct(file_list: list[str], ob_num: str) -> dict[str, str]:
    '''
    Take file list and build dict of names
    '''
    fallback_num = 1
    alt_numbering = False
    file_names = {}
    for file in file_list:
        # Build file name/new filename dict
        channel, ext = file.split('.')[-2:]
        if alt_numbering:
            part = str(fallback_num).zfill(2)
            fallback_num += 1
        else:
            for key, val in ORDER.items():
                if channel == key:
                    part = val
        if not part:
            part = str(fallback_num).zfill(2)
            fallback_num += 1
            alt_numbering = True
        new_fname = f"{ob_num.replace('-', '_')}_{part}of06.{ext}"
        file_names[new_fname] = file

    return dict(sorted(file_names.items()))


def build_record_defaults(platform: str) -> list[dict[str, str]]:
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
               {'record_access.reason': 'SENSITIVE_LEGAL'}])
               #{'record_access.user': '$REST'},
               #{'record_access.rights': '1'},
               #{'record_access.reason': 'SENSITIVE_LEGAL'}])

    return record


def rename_or_move(arg: str, file_a: str, file_b: str) -> str | bool:
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


def make_item_record_dict(priref: str, record: list[dict[str, Any]]) -> Optional[list[dict[str, str]]]:
    '''
    Get CID item record for source and borrow data
    for creation of new CID item record
    '''

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
        item.append({'title': f"{imp_title} (5.1 audio)"})
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
    item.append({'related_object.notes': '5.1 audio for'})
    item.append({'file_type': 'WAV'})
    item.append({'code_type': 'WAV'})
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

    return item


def create_digital_original_filenames(priref: str, asset_list_dct: dict[str, str]) -> bool:
    '''
    Create entries for digital.acquired_filename
    and append to the CID item record.
    '''
    payload = f"<adlibXML><recordList><record priref='{priref}'>"
    for key, val in asset_list_dct.items():
        filename = f'{key} - Renamed to: {val}'
        LOGGER.info("Writing to digital.acquired_filename: %s", filename)
        pay_mid = f"<Acquired_filename><digital.acquired_filename>{filename}</digital.acquired_filename><digital.acquired_filename.type>FILE</digital.acquired_filename.type></Acquired_filename>"
        payload = payload + pay_mid

    pay_edit = f"<Edit><edit.name>datadigipres</edit.name><edit.date>{str(datetime.datetime.now())[:10]}</edit.date><edit.time>{str(datetime.datetime.now())[11:19]}</edit.time><edit.notes>Netflix automated digital acquired filename update</edit.notes></Edit>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload + pay_edit + payload_end

    LOGGER.info("** Appending digital.acquired_filename data to item record now...")
    LOGGER.info(payload)

    try:
        result = adlib.post(CID_API, payload, 'items', 'updaterecord')
        print(f"Item appended successful! {priref}\n{result}")
        LOGGER.info("Successfully appended digital.acquired_filenames to Item record %s", priref)
        print(result)
        return True
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to append digital.acquired_filenames to Item record %s", priref)
        print(f"CID item record append FAILED!! {priref}")
        return False


def create_new_item_record(priref: str, record: list[dict[str, Any]]) -> Optional[dict[Any, Any]]:
    '''
    Build new CID item record from existing data and make CID item record
    '''
    item_dct = make_item_record_dict(priref, record)
    LOGGER.info(item_dct)
    item_xml = adlib.create_record_data(CID_API, 'items', '', item_dct)
    new_record = adlib.post(CID_API, item_xml, 'items', 'insertrecord')
    if new_record is None:
        LOGGER.warning("Skipping: CID item record creation failed: %s", item_xml)
        return None
    LOGGER.info("New CID item record created: %s", new_record)
    return new_record


if __name__ == '__main__':
    main()
