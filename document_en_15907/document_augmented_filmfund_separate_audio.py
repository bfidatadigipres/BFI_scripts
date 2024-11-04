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

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3_sess as adlib
import utils

# Global variables
LOGS = os.environ.get('LOG_PATH')
STORAGE = os.path.join(os.environ.get('QNAP_11'), 'separate_audio/')
AUTOINGEST = os.path.join(os.environ.get('AUTOINEGST_QNAP11'), 'ingest/autodetect/')
CID_API = os.environ.get('CID_API4')

# Setup logging
LOGGER = logging.getLogger('document_augmented_platform_separate_51_audio')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_platform_separate_51_audio.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

ORDER = {
    'L': '01',
    'R': '02',
    'C': '03',
    'LFE': '04',
    'Ls': '05',
    'Rs': '06'
}


def cid_check_ob_num(object_number, session):
    '''
    Looks up object_number and retrieves title
    and other data for new separate 5.1 audio record
    '''
    search = f"object_number='{object_number}'"
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '0', session)
    if hits is None:
        raise Exception(f"CID API was unreachable for Items search: {search}")
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

    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    folder_list = walk_folders(STORAGE)
    if len(folder_list) == 0:
        sys.exit("No folders found at this time.")

    LOGGER.info("== Document augmented Film Fund separate audio start ===================")
    session = adlib.create_session()
    for fpath in folder_list:
        if not utils.check_control('pause_scripts'):
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')
        if not os.path.exists(fpath):
            LOGGER.warning("Folder path is not valid: %s", fpath)
            continue
        object_number = os.path.basename(fpath)
        file_list = os.listdir(fpath)
        if not file_list:
            LOGGER.warning("Skipping. No files found in folderpath: %s", fpath)
            continue
        if len(file_list) == 6:
            qual_comm = f"5.1 audio supplied separately for Film Fund item {object_number}."
            ftype = '5.1'
        elif len(file_list) == 1:
            qual_comm = f"Dolby Atmos supplied separately for Film Fund item {object_number}."
            ftype = 'atmos'
        else:
            LOGGER.warning("Skipping. Incorrect amount of files found in separate_audio path: %s", fpath)
            continue
        LOGGER.info("File(s) found in target Film Fund folder: %s", ', '.join(file_list))

        # Check object number valid
        record = cid_check_ob_num(object_number, session)
        if record is None:
            LOGGER.warning("Skipping: Record could not be matched with object_number")
            continue
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
        if not priref:
            continue
        print(f"Priref matched with retrieved folder name: {priref}")
        LOGGER.info("Priref matched with Film Fund folder name: %s", priref)

        # Create CID item record for batch of six audio files in folder
        item_record = create_new_item_record(priref, record, ftype, session)
        if item_record is None:
            continue
        print(item_record)
        new_priref = adlib.retrieve_field_name(item_record, 'priref')[0]
        new_ob_num = adlib.retrieve_field_name(item_record, 'object_number')[0]
        LOGGER.info("** CID Item record created: %s - %s", new_priref, new_ob_num)
        print(f"CID Item record created: {new_priref}, {new_ob_num}")

        file_names = build_fname_dct(file_list, new_ob_num)
        print(file_names)

        filename_dct = []
        for key, value in file_names.items():
            new_fname = key
            old_fname = value
            if not old_fname.endswith(('.WAV', '.wav')):
                LOGGER.warning("File contained in separate audio folder that is not WAV/MOV: %s", old_fname)

            new_fpath = os.path.join(fpath, new_fname)
            LOGGER.info("%s to be renamed %s", old_fname, new_fname)
            rename_success = rename_or_move('rename', os.path.join(fpath, old_fname), new_fpath)
            if rename_success is False:
                LOGGER.warning("Unable to rename file: %s", os.path.join(fpath, old_fname))
            elif rename_success is True:
                LOGGER.info("File successfully renamed. Moving to Film Fund autoingest path")
            elif rename_success == 'Path error':
                LOGGER.warning("Path error: %s", os.path.join(fpath, old_fname))

            # Move file to new autoingest path
            move_success = rename_or_move('move', new_fpath, os.path.join(AUTOINGEST, new_fname))
            if move_success is False:
                LOGGER.warning("Error with file move to autoingest, leaving in place for manual assistance")
            elif move_success is True:
                LOGGER.info("File successfully moved to Film Fund autoingest path: %s\n", autoingest)
            elif move_success == 'Path error':
                LOGGER.warning("Path error: %s", new_fpath)
            filename_dct.append({"digital.acquired_filename": f"{old_fname} - Renamed to: {new_fname}"})
            filename_dct.append({"digital.acquired_filename.type": "FILE"})

        # Append digital.acquired_filename and quality_comments to new CID item record
        payload = adlib.create_record_data(CID_API, 'items', new_priref, filename_dct)
        record = adlib.post(CID_API, payload, 'items', 'updaterecord')
        if not record:
            LOGGER.warning("Filename changes were not updated to digital.acquired_filename fields: %s", filename_dct)
        LOGGER.info("Digital Acquired Filename data added to CID item record %s", new_priref)
        success = adlib.add_quality_comments(CID_API, new_priref, qual_comm, session)
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

    LOGGER.info("== Document augmented Film Fund separate audio end =====================\n")


def build_fname_dct(file_list, ob_num, platform):
    '''
    Take file list and build dict of names
    '''
    file_names = {}
    if platform == 'Netflix':
        fallback_num = 1
        alt_numbering = False
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

    if platform == 'Amazon':
        for file in file_list:
            ext = file.split('.')[-1]
            new_fname = f"{ob_num.replace('-', '_')}_01of01.{ext}"
            file_names[new_fname] = file

    return dict(sorted(file_names.items()))


def build_record_defaults(platform):
    '''
    Return all record defaults
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': f'{platform} metadata integration - automated bulk documentation for separate audio'}])

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


def make_item_record_dict(priref, ftype, record):
    '''
    Get CID item record for source and borrow data
    for creation of new CID item record
    '''

    if 'Acquisition_source' in str(record):
        platform = adlib.retrieve_field_name(record[0], 'acquisition.source')[0]
        record_default = build_record_defaults(platform)
    else:
        record_default = build_record_defaults('Film Fund')

    item = []
    item.extend(record_default)
    item.append({'record_type': 'ITEM'})
    item.append({'item_type': 'DIGITAL'})
    item.append({'copy_status': 'M'})
    item.append({'copy_usage.lref': '131560'})
    item.append({'accession_date': str(datetime.datetime.now())[:10]})

    if 'Title' in str(record):
        title = adlib.retrieve_field_name(record[0], 'title')[0]
        if ftype == '5.1':
            item.append({'title': f"{title} (5.1 audio)"})
            item.append({'related_object.notes': '5.1 audio for'})
        elif ftype == 'atmos':
            item.append({'title': f"{title} (Dolby Atmos)"})
            item.append({'related_object.notes': 'Dolby Atmos for'})
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
    if ftype == "5.1":
        item.append({'related_object.notes': '5.1 audio for'})
    elif ftype == 'atmos':
        item.append({'related_object.notes': 'Dolby Atmos for'})
    item.append({'file_type': 'WAV'})
    item.append({'code_type': 'WAV'})
    if 'acquisition.date' in str(record):
        item.append({'acquisition.date': adlib.retrieve_field_name(record[0], 'acquisition.date')[0]})
    if 'acquisition.method' in str(record):
        item.append({'acquisition.method': adlib.retrieve_field_name(record[0], 'acquisition.method')[0]})
    if 'Acquisition_source' in str(record):
        item.append({'acquisition.source': platform})
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


def create_new_item_record(priref, record, ftype, session):
    '''
    Build new CID item record from existing data and make CID item record
    '''
    item_dct = make_item_record_dict(priref, record, ftype)
    LOGGER.info(item_dct)
    item_xml = adlib.create_record_data(CID_API, 'items', '', item_dct)
    new_record = adlib.post(CID_API, item_xml, 'items', 'insertrecord', session)
    if new_record is None:
        LOGGER.warning("Skipping: CID item record creation failed: %s", item_xml)
        return None
    LOGGER.info("New CID item record created: %s", new_record)
    return new_record


if __name__ == '__main__':
    main()
