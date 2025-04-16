#!/usr/bin/env python3

'''
Special Collections Document Archiving script for OSH

Script stages:
MUST BE SUPPLIED WITH SYS.ARGV[1] AT SUB-FOND LEVEL PATH
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
import magic
import requests
import logging
import datetime
from typing import Optional, List, Dict, Any

# Private packages
sys.path.append(os.environ.get('CODE'))
import adlib_v3_sess as adlib
import utils

# Global path variables
AUTOINGEST = os.path.join(os.environ.get('AUTOINGEST_BP_SC'), 'ingest/autodetect/')
LOG = os.path.join(os.environ.get('LOG_PATH'), 'special_collections_document_archiving_osh.log')
MEDIAINFO_PATH = os.path.join(os.environ.get('LOG_PATH'), 'cid_mediainfo/')
CID_API = os.environ.get('CID_API3')

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
    search: str = f'(object_number="{fname}" and Df="{record_type}")'
    print(search)
    fields: list[str] = [
        'priref',
        'title',
        'title.article'
    ]

    record = adlib.retrieve_record(CID_API, 'archivescatalogue', search, '1', session, fields)[1]
    print(record)
    LOGGER.info("cid_retrieve(): Making CID query request with:\n%s", search)
    if not record:
        search: str = f'object_number="{fname}"'
        record = adlib.retrieve_record(CID_API, 'archivescatalogue', search, '1', session, fields)[1]
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


def record_hits(fname: str, session) -> Optional[Any]:
    '''
    Count hits and return bool / NoneType
    '''
    search: str = f'object_number="{fname}"'
    print(search)
    hits = adlib.retrieve_record(CID_API, 'archivescatalogue', search, 1, session)[0]
    print(hits)
    if hits is None:
        return None
    if int(hits) == 0:
        return False
    if int(hits) > 0:
        return True


def get_children_items(ppriref: str, session) -> Optional[List[str]]:
    '''
    Get all children of a given priref
    '''
    search: str = f'part_of_reference="{ppriref}" and Df="ITEM_ARCH"'
    print(search)
    fields: list[str] = [
        'priref',
        'object_number'
    ]

    records = adlib.retrieve_record(CID_API, 'archivescatalogue', search, '0', session, fields)
    print(records)
    if not records:
        return None

    item_list = []
    for r in records:
        item_list.append(adlib.retrieve_field_name(r, 'object_number')[0])

    return item_list


def sort_dates(file_list: List[str], last_child_num: str) -> List[str]:
    '''
    Get modification date of files, and sort into newest first
    return with enumeration number
    JMW this must handle adding new files into a list that are found
    at a later date by sensing existing ITEM_ARCH childen and getting last 'num'
    '''
    time_list = []
    for file_path in file_list:
        time = os.path.getmtime(file_path)
        time_list.append(f"{time} - {file_path}")

    time_list.sort()
    enum_list = []
    for i, name in enumerate(time_list):
        i += last_child_num
        enum_list.append(f"{name.split(' - ', 1)[-1]}, {i + 1}")
    print(f"Enumerated list: {enum_list}")
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


def get_image_data(ipath: str) -> list[dict[str, str]]:
    '''
    Create dictionary for Image
    metadata from Exif data source
    '''
    ext = os.path.splitext(ipath)
    exif_metadata = utils.exif_data(ipath)
    if 'Corrupt data' in str(exif_metadata):
        LOGGER.info("Exif cannot read metadata for file: %s", ipath)
        metadata_dct = [
            {'file_size', os.path.getsize(ipath)},
            {'file_size.type': 'Bytes'},
            {'file_type': ext.upper()}
        ]
        return metadata_dct

    print(type(exif_metadata))
    print(exif_metadata)
    if not isinstance(exif_metadata, list):
        return None

    data = [
        'File Modification Date/Time, production.date.notes',
        'File Type, file_type',
        'MIME Type, media_type',
        'Software, source_software'
    ]

    image_dict = []
    for mdata in exif_metadata:
        if ':' not in str(mdata):
            continue
        field, value = mdata.split(':', 1)
        for d in data:
            exif_field, cid_field = d.split(', ')
            if 'File Type   ' in exif_field:
                try:
                    ft, ft_type = value.split(' ')
                    if len(ft) > 1 and len(ft_type) > 1:
                        image_dict.append({f'{cid_field}': ft.strip()})
                        image_dict.append({f'{cid_field}.type': ft_type.strip()})
                    else:
                        image_dict.append({f'{cid_field}': value.strip()})
                except ValueError as err:
                    image_dict.append({f'{cid_field}': value.strip()})
                    print(err)
            elif exif_field == field.strip():
                image_dict.append({f'{cid_field}': value.strip()})

    image_dict.append({'file_size', os.path.getsize(ipath)})
    image_dict.append({'file_size.type': 'Bytes'})
    return image_dict


def build_defaults():
    '''
    Use this function to just build standard defaults for all GUR records
    Discuss what specific record data they want in every record / some records
    '''
    text = ''' The arrangement of the collection maintains its original order, \
as stored on a shared Google Drive for Gurinder Chadha and her team. \
Consequently, the collection is archived in alphabetical order, \
including productions, rather than by date; please refer to the \
materials date for further details.\n\n \
The collection has been organised into five series, each relating \
to a different area of activity.'''
    text2 = ''' The working digital documents and images related to the films and \
television programmes, directed, produced, and/or written by \
Gurinder Chadha.'''

    records_all = [
        {'record_access.user': 'BFIiispublic'},
        {'record_access.rights': '0'},
        {'content.person.name.lref': '378012'},
        {'content.person.name.type': 'PERSON'},
        {'system_of_arrangement': text},
        {'content.description': text2},
        {'institution.name.lref': '999570701'},
        {'analogue_or_digital': 'DIGITAL'},
        {'digital.born_or_derived': 'BORN_DIGITAL'},
        {'input.name': 'datadigipres'},
        {'input.date': str(datetime.datetime.now())[:10]},
        {'input.time': str(datetime.datetime.now())[11:19]},
        {'input.notes': 'Automated record creation for Our Screen Heritage OSH strand 3, to facilitate ingest to Archivematica.'}
    ]

    return records_all


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

    base_dir = sys.argv[1]  # Always sub_fond level path
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
    defaults_all = build_defaults()

    # Process series filepaths first
    if len(series) == 0:
        sys.exit("No series data found, exiting as not possible to iterate lower than series.")
    series.sort()
    LOGGER.info("Series found %s: %s", len(series), ', '.join(series))

    # Create record for folder
    s_priref_list = create_folder_record(series, session, defaults_all)
    LOGGER.info("New records created for series beneath %s - %s:\n%s", sf_ob_num, sf_record_type.upper().replace('-', '_'), ', '.join(s_priref_list))

    if not sub_series:
        sys.exit("No sub-series data found, exiting as not possible to iterate lower than sub-series")
    LOGGER.info("Sub-series found %s: %s", len(sub_series), ', '.join(sub_series))

    # Create records for folders
    if series:
        print(series, defaults_all)
        series_dcts, series_items = handle_repeat_folder_data(series, session, defaults_all)
        LOGGER.info("Processed the following Series and Series items:")
        for s in series_dcts:
            LOGGER.info(s)
        for i in series_items:
            LOGGER.info(i)
    if sub_series:
        s_series_dcts, s_series_items = handle_repeat_folder_data(sub_series, session, defaults_all)
        LOGGER.info("Processed the following Sub series and Sub series items:")
        for s in s_series_dcts:
            LOGGER.info(s)
        for i in s_series_items:
            LOGGER.info(i)
    if sub_sub_series:
        ss_series_dcts, ss_series_items = handle_repeat_folder_data(sub_sub_series, session, defaults_all)
        LOGGER.info("Processed the following Sub-sub series and Sub-sub series items:")
        for s in ss_series_dcts:
            LOGGER.info(s)
        for i in ss_series_items:
            LOGGER.info(i)

    if sub_sub_sub_series:
        sss_series_dcts, sss_series_items = handle_repeat_folder_data(sub_sub_sub_series, session, defaults_all)
        LOGGER.info("Processed the following Sub-sub-sub series and Sub-sub-sub series items:")
        for s in sss_series_dcts:
            LOGGER.info(s)
        for i in sss_series_items:
            LOGGER.info(i)

    if file:
        file_dcts, file_items = handle_repeat_folder_data(file, session, defaults_all)
        LOGGER.info("Processed the following File and File items:")
        for s in file_dcts:
            LOGGER.info(s)
        for i in file_items:
            LOGGER.info(i)

    LOGGER.info("=========== Special Collections - Document Archiving OSH END ==============")


def handle_repeat_folder_data(record_type_list, session, defaults_all):
    '''
    Create record at folder level irrespective
    of record_type, inherited from folder name.
    Get back dict of fpaths and prirefs, then
    look within each for documents that need recs.
    '''
    print(f"Received {record_type_list}, {defaults_all}")
    priref_dct = create_folder_record(record_type_list, session, defaults_all)
    LOGGER.info("Records created/identified:\n%s", priref_dct)

    # Check for item_archive files within folders
    file_order = {}
    item_prirefs = []
    for key, val in priref_dct.items():
        p_priref, p_ob_num = val.split(' - ')

        print(f"Folder path: {key} - priref {p_priref} - object number {p_ob_num}")
        file_list = [os.path.join(key, x) for x in os.listdir(key) if os.path.isfile(os.path.join(key, x))]
        if len(file_list) == 0:
            LOGGER.info("No files found in path: %s", key)
            continue

        # Sort into numerical order based on mod times
        # Get object numbers of items already linked to parent priref
        child_list = get_children_items(p_priref, session)
        if child_list:
            child_list.sort()
            last_child_num = child_list[-1].split('-')[-1]
            print(f"Last child number: {last_child_num}")
            LOGGER.info("Children of record found. Passing last number to enumartion: %s", last_child_num)
        else:
            last_child_num = '0'
        print(file_list)
        enum_files = sort_dates(file_list, int(last_child_num))
        file_order[f"{key}"] = enum_files
        LOGGER.info("%s files found to create Item Archive records: %s", len(file_order), ', '.join(file_order))

        # Create ITEM_ARCH records and rename files / move to new subfolders?
        item_priref_group = create_archive_item_record(file_order, key, p_priref, session, defaults_all)
        item_prirefs.append(item_priref_group)

    return priref_dct, item_prirefs


def create_folder_record(folder_list: List[str], session: requests.Session, defaults: List[Dict[str, str]]) -> Dict[str, str]:
    '''
    Accept list of folder paths and create a record
    where none already exist, linking to the parent
    record
    '''
    record_types = [
        'sub-fonds',
        'series',
        'sub-series',
        'sub-sub-series',
        'sub-sub-sub-series',
        'file'
    ]
    print(f"Received {folder_list}, {defaults}")
    priref_dct = {}
    for fpath in folder_list:
        root, folder = os.path.split(fpath)
        p_ob_num, p_record_type, _ = folder_split(os.path.basename(root))
        print(f"Parent folder: {root} - {p_ob_num} - {p_record_type}")
        print(f"Folder to be processed {folder}")
        ob_num, record_type, local_title = folder_split(folder)
        print(ob_num)
        print(record_type)
        print(local_title)

        if ob_num is None:
            continue
        # Skip file, it can sit any level in types
        if record_type != 'file':
            idx = record_types.index(record_type)
            if isinstance(idx, int):
                print(f"Record type match: {record_types[idx]} - checking parent record_type is correct.")
                print(idx)
                print(idx - 1)
                print(record_types[idx - 1])
                print(p_record_type)
                pidx = idx - 1
                if record_types[pidx] != p_record_type:
                    LOGGER.warning("Problem with supplied record types in folder name, skipping")
                    continue

        # Check if parent already created to allow for repeat runs against folders
        p_exist = record_hits(p_ob_num, session)
        if p_exist is None:
            LOGGER.warning("API may not be available. Skipping for safety.")
            continue
        elif p_exist is False:
            LOGGER.info("Skipping creation of child record to %s, record not matched in CID", p_ob_num)
            continue
        LOGGER.info("Parent record matched in CID: %s", p_ob_num)
        p_priref, title, title_art = cid_retrieve(p_ob_num, p_record_type.upper().replace('-', '_'), session)
        LOGGER.info("Parent priref %s, Title %s %s", p_priref, title_art, title)

        # Check if record already exists before creating new record
        exist = record_hits(ob_num, session)
        if exist is None:
            LOGGER.warning("API may not be available. Skipping for safety %s", folder)
            continue
        elif exist is True:
            priref, title, title_art = cid_retrieve(ob_num, record_type.upper().replace('-', '_'), session)
            LOGGER.info("Skipping creation. Record for %s already exists", ob_num)
            priref_dct[fpath] = f"{priref} - {ob_num}"
            continue
        LOGGER.info("No record found. Proceeding.")

        # Create record here
        cid_record_type = record_type.upper().replace('-', '_')
        data = [
            {'Df': cid_record_type},
            {'description_level_object': 'ARCHIVE'},
            {'object_number': ob_num},
            {'part_of_reference.lref': p_priref},
            {'archive_title.type': '07_arch'},
            {'title': local_title}
        ]
        data.extend(defaults)
        new_priref = post_record(session, data)
        if new_priref is None:
            LOGGER.warning("Record failed to create using data: %s, %s, %s, %s,\n%s", ob_num, cid_record_type, p_priref, local_title, data)
            continue

        LOGGER.info("New %s record_type created: %s", cid_record_type, new_priref)
        print(f"New series record created: {ob_num} - {new_priref} / Parent: {p_ob_num} / Record type: {cid_record_type} / {local_title}")
        priref_dct[fpath] = f"{new_priref} - {ob_num}"

    return priref_dct


def post_record(session, record_data=None) -> Optional[Any]:
    '''
    Receive dict of series data
    and create records for each
    and create CID records
    '''
    if record_data is None:
        return None

    # Convert to XML
    print(record_data)
    record_xml = adlib.create_record_data(CID_API, 'archivescatalogue', session, '', record_data)
    print(record_xml)
    try:
        rec = adlib.post(CID_API, record_xml, 'archivescatalogue', 'insertrecord', session)
        if rec is None:
            LOGGER.warning("Failed to create new record:\n%s", record_xml)
            return None
        elif 'priref' not in str(rec):
            LOGGER.warning("Failed to create new record:\n%s", record_xml)
            return None
        priref = adlib.retrieve_field_name(rec, 'priref')[0]
        return priref
    except Exception as err:
        raise err


def create_archive_item_record(file_order, parent_path, parent_priref, session, defaults_all):
    '''
    Get data needed for creation of item archive record
    Receive item fpath, enumeration, parent priref/ob num and title
    '''
    print('Create archive item record!')
    parent_ob_num, _, title = folder_split(os.path.basename(parent_path))
    LOGGER.info("Processing files for parent %s in path: %s", parent_priref, parent_path)
    print(file_order)

    all_item_prirefs = {}
    for _, value in file_order.items():
        for ip in value:
            ipath, num = ip.split(', ', 1)
            print(ipath, num)
            if not os.path.isfile(ipath):
                LOGGER.warning("Corrupt file path supplied: %s", ipath)
                continue

            # Get particulars
            mime = magic.Magic(mime=True)
            mime_type = mime.from_file(ipath)
            iname = os.path.basename(ipath)
            LOGGER.info("------ File: %s --- number %s --- mime %s ------", iname, num, mime_type)
            ext = os.path.splitext(iname)
            ob_num = f"{parent_ob_num}-{num.strip()}"
            new_name = f"{ob_num}.{ext}"

            # Create exif metadata / checksum
            if 'image' in mime_type or 'application' in mime_type:
                metadata_dct = get_image_data(ipath)
            else:
                LOGGER.warning("File type not recognised: %s", mime_type)
            checksum = utils.create_md5_65536(ipath)

            record_dct = [
                {'Df': 'ITEM_ARCH'},
                {'part_of_reference.lref': parent_priref},
                {'archive_title.type': '07_arch'},
                {'title': title}, # Inheriting from the parent folder?
                {'digital.acquired_filename': iname},
                {'object_number': ob_num},
                {'received_checksum.type': 'MD5'},
                {'received_checksum.data': str(datetime.datetime.now())[:19]}, # Should we be generating received checksum data?
                {'received_checksum.value': checksum}
            ]

            if metadata_dct:
                record_dct.extend(metadata_dct)
            record_dct.extend(defaults_all)

            # Check record not already existing - then create record and receive priref
            exist = record_hits(ob_num, session)
            if exist is None:
                LOGGER.warning("API may not be available. Skipping record creation for safety %s", iname)
                continue
            elif exist is True:
                priref, title, _ = cid_retrieve(ob_num, 'ITEM_ARCH', session)
                LOGGER.warning("Skipping creation. Record %s / %s already exists: <%s>", title, ob_num, priref)
                continue

            # Create
            LOGGER.info("Data collated for record creation: %s", record_dct)
            new_priref = post_record(session, record_dct)
            if new_priref is None:
                LOGGER.warning("Record creation failed: %s", record_dct)
                return None

            all_item_prirefs[new_priref] = f"{iname} - {new_name}"
            LOGGER.info("New record created for Item Archive: %s", new_priref)

            # Do we need to new folder in new location here?
            new_fpath = os.path.join(parent_path, new_name)
            try:
                LOGGER.info("File renaming:\n - %s\n - %s", ipath, new_fpath)
                os.rename(ipath, new_fpath)
                if os.path.isfile(new_fpath):
                    LOGGER.info("File renaming was successful.")
                else:
                    LOGGER.warning("File renaming failed.")
            except OSError as err:
                LOGGER.warning("File renaming error: %s", err)

    print(f"Item prirefs: {all_item_prirefs}")
    sys.exit("One run only for test to preserve enumeration")
    return all_item_prirefs


if __name__ == '__main__':
    main()
