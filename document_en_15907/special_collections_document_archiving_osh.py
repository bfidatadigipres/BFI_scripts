#!/usr/bin/env python3

'''
Special Collections Document Archiving script for OSH

Script stages:
1. Iterate through supplied sys.argv[1] folder path
2. For each subfolder split folder name: ob_num / ISAD(G) level / Title
3. Create CID record for each folder following level from folder name
   - Only when subfolder starts with object number of parent folder
   - Only creating Series, Sub Series and Sub Sub Series (awaiting record_type for last)
   - For items (any digital document) create an Archive Item record (df='ITEM_ARCH')
4. Join to the parent/children records through the ob_num part/part_of
5. Once at bottom of folders in sub or sub sub series, order files by creation date (if possble)
6. Check for filename already in digital.acquired_filename in CID already (report where found)
7. CID archive item records are to be made for each, and linked to parent folder:
      Named GUR-2-1-1-1-1, GUR-2-1-1-1-2 etc based on parent's object number
      Original filename is to be captured into the Item record digital.acquired_filename
      Rename the file and move to autoingest.

2025
'''

# Public packages
import os
import sys
import shutil
import requests
import logging
import datetime
from typing import Optional, Final, List, Dict, Any

# Private packages
sys.path.append(os.environ.get('CODE'))
import adlib_v3_sess as adlib
import utils

# Global path variables
AUTOINGEST = os.path.join(os.environ.get('AUTOINGEST_BP_SC'), 'ingest/autodetect/')
LOG = os.path.join(os.environ.get('LOG_PATH'), 'special_collections_document_archiving_osh.log')
MEDIAINFO_PATH = os.path.join(os.environ.get('LOG_PATH'), 'cid_mediainfo/')
CID_API = os.environ.get('CID_API4')

LOGGER = logging.getLogger('sc_document_archiving_osh')
HDLR = logging.FileHandler(LOG)
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_retrieve(fname: str, record_type: str, session) -> Optional[tuple[str, str, str]]:
    '''
    Receive filename and search in CID works dB
    Return selected data to main()
    '''
    search: str = f'object_number="{fname}" and Df="{record_type}"'
    fields: list[str] = [
        'priref',
        'title',
        'title.article'
    ]

    record = adlib.retrieve_record(CID_API, 'archivescatalogue', search, '1', session, fields)[1]
    LOGGER.info("cid_retrieve(): Making CID query request with:\n%s", search)
    if not record:
        print(f"cid_retrieve(): Unable to retrieve data for {fname}")
        utils.logger(LOG, 'exception', f"cid_retrieve(): Unable to retrieve data for {fname}")
        return None

    if 'priref' in str(record):
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    else:
        priref = ""
    if 'Title' in str(record):
        title = adlib.retrieve_field_name(record[0], 'title')[0]
    else:
        title = ""
    if 'title.article' in str(record):
        title_article = adlib.retrieve_field_name(record[0], 'title.article')[0]
    else:
        title_article = ""

    return priref, title, title_article


def record_hits(fname: str, record_type: str, session) -> Optional[Any]:
    '''
    Count hits and return bool / NoneType
    '''
    search: str = f'object_number="{fname}" and Df="{record_type}"'
    print(search)
    hits = adlib.retrieve_record(CID_API, 'archivescatalogue', search, 1, session)[0]
    print(hits)
    if hits is None:
        return None
    if int(hits) == 0:
        return False
    if int(hits) > 0:
        return True


def sort_dates(file_list: List[str]) -> List[str]:
    '''
    Get modification date of files, and sort into newest first
    return with enumeration number
    '''
    time_list = []
    for file_path in file_list:
        time = os.path.getmtime(file_path)
        time_list.append(f"{time} - {file_path}")

    time_list.sort()

    enum_list = []
    for i, name in enumerate(time_list):
        enum_list.append(f"{name.split(' - ', 1)[-1]}, {i + 1}")

    return enum_list


def folder_split(fname):
    '''
    Split folder name into parts
    '''
    fsplit = fname.split('_', 2)
    print(fsplit)
    if len(fsplit) != 3:
        LOGGER.warning("Folder has not split as anticipated: %s", fsplit)
        return None, None, None
    ob_num, record_type, title = fsplit
    if not ob_num.startswith(('GUR', '')):
        LOGGER.warning("Object number is not formatted as anticipated: %s", ob_num)
        return None, None, None

    return ob_num, record_type, title


def main():
    '''
    Iterate supplied folder, find image files in folders
    named after work and create analogue/digital item records
    for every photo. Clean up empty folders.
    '''
    if not utils.check_control('power_off_all'):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        sys.exit("* Cannot establish CID session, exiting script")

    LOGGER.info("=========== Special Collections rename - Document Archiving OSH START ============")

    base_dir = sys.argv[1]  # sub_fond level path
    sub_fond = os.path.basename(base_dir)
    print(base_dir)
    sf_ob_num, sf_record_type, sf_title = folder_split(sub_fond)
    print(f"Sub fond data found: {sf_ob_num}, {sf_record_type}, {sf_title}")
    LOGGER.info("Sub fond data: %s, %s, %s", sf_ob_num, sf_record_type, sf_title)

    if not os.path.isdir(base_dir):
        sys.exit("Folder path is not a valid path")

    series = []
    sub_series = []
    sub_sub_series = []
    sub_sub_sub_series = []
    file = []
    for root, dirs, _ in os.walk(base_dir):
        for directory in dirs:
            if not str(directory).startswith(sf_ob_num):
                continue
            dpath = os.path.join(root, directory)
            if '_series_' in str(directory):
                series.append(dpath)
            elif '_sub-series_' in str(directory):
                sub_series.append(dpath)
            elif '_sub-sub-series_' in str(directory):
                sub_sub_series.append(dpath)
            elif '_sub-sub-sub-series_' in str(directory):
                sub_sub_sub_series.append(dpath)
            elif '_file_' in str(directory):
                file.append(dpath)

    session = adlib.create_session()
    defaults_all, defaults_items = build_defaults()

    # Process series filepaths first
    if not series:
        sys.exit("No series data found, exiting as not possible to iterate lower than series.")
    series.sort()
    LOGGER.info("Series found %s: %s", len(series), ', '.join(series))

    # Create records
    priref_list = create_record(series, session, defaults_all)

    # Check for item_archive files within folders
    for key, val in priref_list.items():
        print(f"Folder path: {key} - priref {val}")
        file_list = [x for x in os.listdir(key) if os.path.isfile(os.path.join(key, x))]
        if len(file_list) == 0:
            continue
        LOGGER.info("Files found to create Item Archive records: %s", ', '.join(file_list))
        # Create ITEM_ARCH records and rename files / move to new subfolders? TBC

    '''
    print("**** SUB SERIES PROCESSING ****")
    sub_series_ob_num = []
    sub_series_structure = {}
    folders = []
    files = []
    for fpath in series_structure_all:
        print(fpath)
        sub_path, folder = os.path.split(fpath)
        parent_ob_num = os.path.basename(sub_path).split('_', 1)[0]
        ob_num, record_type, title = folder_split(folder)
        if ob_num is None:
            continue
        sub_series_ob_num.append(f"New object number: {ob_num} / Parent: {parent_ob_num} / Record type: {record_type} / {title}")
        print(f"New sub_series object number: {ob_num} / Parent: {parent_ob_num} / Record type: {record_type} / {title}")
        folders = [f"{os.path.join(fpath, x)}" for x in os.listdir(fpath) if os.path.isdir(os.path.join(fpath, x)) and x.startswith(ob_num)]
        if folders:
            folders.sort()
            sub_series_structure[f"{fpath} folders"] = folders
        files = [f"{os.path.join(fpath, x)}" for x in os.listdir(fpath) if os.path.isfile(f"{os.path.join(fpath, x)}")]
        if files:
            enum_files = sort_dates(files)
            sub_series_structure[f"{fpath} files"] = enum_files

    print("Archive items found in creation date order:")
    for file in sub_series_structure[f"{fpath} files"]:
        print(f"{fpath} files")

    sub_sub_series_structure = {}
    print("**** SUB SUB SERIES PROCESSING ****")
    folders = []
    files = []
    for key, value in sub_series_structure.items():
        if 'sub-sub-series' not in str(key):
            continue
        if ' folders' in key:
            if not value:
                continue
            for fpath in value:
                print(fpath)
                sub_path, folder = os.path.split(fpath)
                parent_ob_num = os.path.basename(sub_path).split('_', 1)[0]
                ob_num, record_type, title = folder_split(folder)
                if ob_num is None:
                    continue
                sub_series_ob_num.append(f"New object number: {ob_num} / Parent: {parent_ob_num} / Record type: {record_type} / {title}")
                print(f"New sub_sub_series object number: {ob_num} / Parent: {parent_ob_num} / Record type: {record_type} / {title}")
                files = [f"{os.path.join(fpath, x)}" for x in os.listdir(fpath) if os.path.isfile(f"{os.path.join(fpath, x)}")]
                if files:
                    enum_files = sort_dates(files)
                    sub_sub_series_structure[f"{fpath}"] = enum_files

    file_structure = {}
    print("**** FILES PROCESSING ****")
    files = []
    for key, value in sub_series_structure.items():
        if ' folders' in key:
            if not value:
                continue
            if 'sub-sub-series' in str(key):
                continue
            for fpath in value:
                print(fpath)
                sub_path, folder = os.path.split(fpath)
                parent_ob_num = os.path.basename(sub_path).split('_', 1)[0]
                ob_num, record_type, title = folder_split(folder)
                if ob_num is None:
                    continue
                sub_series_ob_num.append(f"New object number: {ob_num} / Parent: {parent_ob_num} / Record type: {record_type} / {title}")
                print(f"New sub_sub_series object number: {ob_num} / Parent: {parent_ob_num} / Record type: {record_type} / {title}")
                files = [f"{os.path.join(fpath, x)}" for x in os.listdir(fpath) if os.path.isfile(f"{os.path.join(fpath, x)}")]
                if files:
                    enum_files = sort_dates(files)
                    file_structure[f"{fpath}"] = enum_files
    LOGGER.info("File data retrieved:\n%s", file_structure)


    print("Archive items found in creation date order")
    for k, v in sub_sub_series_structure.items():
        for file in v:
            print(f"\t{file}")
    print("=--------------------------------=")
    print("Archive File items found in creation date order")
    for k, v in file_structure.items():
        for file in v:
            print(f"\t{file}")
    '''
    LOGGER.info("=========== Special Collections - Document Archiving OSH END ==============")


def create_record(folder_list: List[str], session: requests.Session, defaults: List[Dict[str]]) -> Dict[str, str]:
    '''
    Accept list of folder paths and create a record
    where none already exist, linking to the parent
    record
    '''
    record_types = [
        'sub_fonds'
        'series',
        'sub-series',
        'sub-sub-series',
        'sub-sub-sub-series',
        'file'
    ]

    priref_dct = {}
    for fpath in folder_list:
        root, folder = os.path.split(fpath)
        p_ob_num, p_record_type, _ = folder_split(os.path.basename(root))

        print(f"Folder to be processed {folder}")
        ob_num, record_type, local_title = folder_split(folder)
        idx = record_types.index(record_type)
        if isinstance(idx, int):
            print(f"Record type match: {record_types[idx]} - checking parent record_type is correct.")
            pidx = idx - 1
            if record_types[pidx] != p_record_type:
                LOGGER.warning("Problem with supplied record types in folder name, skipping")
                continue
        if ob_num is None:
            continue

        # Check if parent already created to allow for repeat runs against folders
        p_exist = record_hits(p_ob_num, p_record_type.upper(), session)
        if p_exist is None:
            LOGGER.warning("API may not be available. Skipping for safety.")
            continue
        elif p_exist is False:
            LOGGER.info("Skipping creation of child record to %s, record not matched in CID", p_ob_num)
            continue
        LOGGER.info("Parent record matched in CID: %s", p_ob_num)
        p_priref, title, title_art = cid_retrieve(p_ob_num, p_record_type.upper(), session)
        LOGGER.info("Parent priref %s, Title %s %s", p_priref, title_art, title)

        # Check if record already exists before creating new record
        exist = record_hits(ob_num, record_type.upper(), session)
        if exist is None:
            LOGGER.warning("API may not be available. Skipping for safety %s", folder)
            continue
        elif exist is True:
            priref, title, title_art = cid_retrieve(ob_num, record_type.upper(), session)
            LOGGER.info("Skipping creation. Record for %s already exists", ob_num)
            priref_dct[fpath] = priref
            continue

        LOGGER.info("No record found. Proceeding.")
        # Create record here
        new_priref = create_record(ob_num, record_type.upper(), p_priref, local_title, session, defaults)
        if new_priref is None:
            LOGGER.warning("Record failed to create using data: %s, %s, %s, %s,\n%s", ob_num, record_type.upper(), p_priref, local_title, defaults)
            continue

        LOGGER.info("New %s record_type created: %s", record_type.upper(), priref)
        print(f"New series record created: {ob_num} - {priref} / Parent: {p_ob_num} / Record type: {record_type} / {local_title}")
        priref_dct[fpath] = new_priref

    return priref_dct


# Waiting to see about defaults, it's possible on function can make all records
def create_record(object_number, record_type, parent_priref, title, session, defaults=None) -> Optional[Any]:
    '''
    Receive dict of series data
    and create records for each
    and create CID records
    '''
    if not defaults:
        return None

    series = [
        {'Df': record_type.upper()},
        {'description_level_object': 'ARCHIVE'},
        {'object_number': object_number},
        {'part_of_reference.lref': parent_priref},
        {'archive_title.type': '07_arch'},
        {'title': title}
    ]
    series.extend(defaults)

    # Convert to XML
    print(series)
    series_xml = adlib.create_record_data(CID_API, 'archivescatalogue', session, '', series)
    print(series_xml)
    rec = adlib.post(CID_API, series_xml, 'archivescatalogue', 'insertrecord', session)
    if not rec:
        LOGGER.warning("Failed to create new Series record")
        return None


def create_sub_series(object_number, record_type, parent_priref, parent_ob_num, title):
    '''
    Receive dict of series data
    and create records for each
    and create CID records
    '''
    dct = [
        {'Df': 'SUB_SERIES'},
        {'object_number': object_number},
        {'record_type': record_type},
        {'part_of_reference.lref': parent_priref},
        {'archive_title.type': '07_arch'},
        {'title': title}
    ]
    dct.append({'Df': 'SUB_SERIES'})
    pass


def create_sub_sub_series(sub_sub_series_dct, parent_priref, parent_ob_num, title_art, title):
    '''
    Receive dict of series data
    and create records for each
    and create CID records
    This record_type is not yet created!
    '''
    dct = {}
    dct.append({'Df': 'SUB_SUB_SERIES'})
    pass


def create_files(files_dct, parent_priref, parent_ob_num, title_art, title):
    '''
    Receive dict of series data
    and create records for each
    and create CID records
    '''
    dct = {}
    dct.append({'Df': 'FILES'})
    pass


def create_archive_item(ipath, num, parent_priref, parent_ob_num, title):
    '''
    Get data needed for creation of item archive record
    Receive item fpath, enumeration, parent priref/ob num and title
    '''
    root, iname = os.path.split(ipath)
    ext = os.path.splitext(iname)
    ob_num = f"{parent_ob_num}-{num}"
    new_name = f"{ob_num}.{ext}"
    record_dct = [(
        {'Df': 'ITEM_ARCH'},
        {'archive_title.type': '07_arch'},
        {'title': title},
        {'digital.acquired_filename': iname},
        {'object_number': ob_num}
    )]
    pass



def rename_files(fpath, parent_ob_num, index):
    '''
    Using index and parent object number
    create a new filename and rename fpath
    '''
    pass


def build_defaults():
    '''
    Use this function to just build standard defaults for all GUR records
    Discuss what specific record data they want in every record / some records
    '''
    text = '''
The arrangement of the collection maintains its original order, \
as stored on a shared Google Drive for Gurinder Chadha and her team. \
Consequently, the collection is archived in alphabetical order, \
including productions, rather than by date; please refer to the \
material’s date for further details. \
The collection has been organised into five series, each relating \
to a different area of activity.
'''
    text2 = '''
The working digital documents and images related to the films and \
television programmes, directed, produced, and/or written by \
Gurinder Chadha.
'''

    records_all = [
        # {'record_access.owner': 'Special Collections'}, Most probably $REST here
        {'record_access.user': 'BFIiispublic'},
        {'record_access.rights': '0'},
        {'content.person.name.lref': '378012'},
        {'content.person.name.type': 'PERSON'},
        {'system_of_arrangement': text},
        {'production.date.notes': '2013-2024'},
        {'content.description': text2},
        {'institution.name.lref': '999570701'}, # BFI National Archive
        {'input.name': 'datadigipres'},
        {'input.date': str(datetime.datetime.now())[:10]},
        {'input.time': str(datetime.datetime.now())[11:19]},
        {'input.notes': 'Automated record creation for Our Screen Heritage OSH strand 3, to facilitate ingest to Archivematica.'}
    ]

    records_items = []

    return records_all, records_items


def update_acquired_filename(priref, acquired_filename, session):
    '''
    Update the file's original filename
    to digital.acquired_filename field
    using new item record priref
    '''
    payload_head: str = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_mid: str = f"<digital.acquired_filename>'{acquired_filename}</digital.acquired_filename>"
    payload_end: str = "</record></recordList></adlibXML>"
    payload: str = payload_head + payload_mid + payload_end

    record = adlib.post(CID_API, payload, 'archivescatalogue', 'updaterecord', session)
    if record is None:
        return False
    elif 'error' in str(record):
        return False
    else:
        return True


def rename(filepath: str, ob_num: str) -> tuple[str, str]:
    '''
    Receive original file path and rename filename - TBU
    based on object number, return new filepath, filename
    '''
    new_filepath, new_filename = '', ''
    ipath, filename = os.path.split(filepath)
    ext = os.path.splitext(filename)[1]
    new_name = ob_num.replace('-', '_')
    new_filename = f"{new_name}_01of01{ext}"
    print(f"Renaming {filename} to {new_filename}")
    new_filepath = os.path.join(ipath, new_filename)

    try:
        os.rename(filepath, new_filepath)
    except OSError:
        LOGGER.warning("There was an error renaming %s to %s", filename, new_filename)

    return (new_filepath, new_filename)


def move(filepath: str, arg: str) -> bool:
    '''
    Move existing filepaths to Autoingest - TBU
    '''
    if os.path.exists(filepath) and 'fail' in arg:
        pth: str = os.path.split(filepath)[0]
        failures: str = os.path.join(pth, 'failures/')
        os.makedirs(failures, mode=0o777, exist_ok=True)
        print(f"move(): Moving {filepath} to {failures}")
        try:
            shutil.move(filepath, failures)
            return True
        except Exception as err:
            LOGGER.warning("Error trying to move file %s to %s. Error %s", filepath, failures, err)
            return False
    elif os.path.exists(filepath) and 'ingest' in arg:
        print(f"move(): Moving {filepath} to {AUTOINGEST}")
        try:
            shutil.move(filepath, AUTOINGEST)
            return True
        except Exception:
            LOGGER.warning("Error trying to move file %s to %s", filepath, AUTOINGEST)
            return False
    else:
        return False


if __name__ == '__main__':
    main()
