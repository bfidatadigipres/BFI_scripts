#!/usr/bin/ spython3

'''
Script to receive MOV
Amazon files named with UID.
UID placed into Item record
digital.acquired_filename field
of CID item record.

Receives folder of MOVs complete with
video and audio desc also MOV wrapped.

1. Searches STORAGE for 'rename_amazon'
   folders and extracts CID item record
   number from enclosed folders.
2. Iterates folders, finds match for folder name
   with CID item record, ie 'N-123456'. Creates
   N_123456_filename prefix
3. Retrieves metadata for each MOV (or possibly
   other file wrapper) in folder
4. Uses CID item record filename prefix to name
   the AV MOV file with UHD HDR content (colour
   primaries denote HDR with BT.2020)
5. New CID item records are made for the remaining
   files (anticipated a UHD SDR colourspace BT.709,
   and mov with audio description only, no video stream)
6. These files are renamed with the CID item record
   object_number, likely all with 01of01.
7. Adds original filename and to existing and new
   CID item records in 'digital.acquired_filename'
   field. Formatting for this:
   "<Original Filename> - Renamed to: N_123456_01of06.mxf"
8. Renamed files are moved to autoingest new
   black_pearl_amazon_ingest path where new put
   scripts ensure file is moved to amazon01 bucket.

Joanna White
2024
'''

# Public packages
import os
import sys
import json
import shutil
import logging
import requests
import datetime
import subprocess

# Local packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
STORAGE_PTH = os.environ.get('PLATFORM_INGEST_PTH')
AMZ_PTH = os.environ.get('AMAZON_PATH')
AMZ_INGEST = os.environ.get('AMAZON_INGEST')
AUTOINGEST = os.path.join(STORAGE_PTH, AMZ_INGEST)
STORAGE = os.path.join(STORAGE_PTH, AMZ_PTH)
ADMIN = os.environ.get('ADMIN')
LOGS = os.path.join(ADMIN, 'Logs')
CODE = os.environ.get('CODE_PATH')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Setup logging
LOGGER = logging.getLogger('document_augmented_amazon_renaming')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_amazon_renaming.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


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
    and other data for new timed text record
    '''
    query = {'database': 'items',
             'search': f'object_number="{object_number}"',
             'limit': '1',
             'output': 'json'}
    try:
        query_result = CID.get(query)
        return query_result.records
    except Exception as err:
        print(f"cid_check(): Unable to match supplied name with CID Item record: {object_number} {err}")

    return None


def walk_folders():
    '''
    Collect list of folderpaths
    for files named rename_amazon
    '''
    print(STORAGE)
    rename_folders = []
    for root, dirs, _ in os.walk(STORAGE):
        for dir in dirs:
            if 'rename_amazon' == dir:
                rename_folders.append(os.path.join(root, dir))
    print(f"{len(rename_folders)} rename folder(s) found")
    folder_list = []
    for rename_folder in rename_folders:
        print(rename_folder)
        folders = os.listdir(rename_folder)
        if not folders:
            LOGGER.info("Amazon file renaming script. Skipping as rename folder empty: %s", rename_folder)
            continue
        for folder in folders:
            print(folder)
            fpath = os.path.join(rename_folder, folder)
            if os.path.isdir(fpath):
                folder_list.append(os.path.join(rename_folder, folder))
            else:
                LOGGER.warning("Amazon file renaming script. Non-folder item found in rename_amazon path: %s", fpath)

    return folder_list


def retrieve_metadata(fpath, mfile):
    '''
    Retrieve metadata for each file
    '''
    cmd = [
        'mediainfo', '-f',
        '--Language=raw',
        '--Output=Video;%colour_primaries%',
        os.path.join(fpath, mfile)
    ]

    cmd2 = [
        'ffprobe', '-v',
        'error', '-select_streams',
        'a', '-show_entries',
        'stream=index:stream_tags=language',
        '-of', 'compact=p=0:nk=1',
        os.path.join(fpath, mfile)
    ]

    colour_prim = subprocess.check_output(cmd)
    colour_prim = colour_prim.decode('utf-8')
    audio_spec = subprocess.check_output(cmd2)
    audio_spec = audio_spec.decode('utf-8')
    print(colour_prim, audio_spec)

    if 'BT.2020' in str(colour_prim):
        return 'HDR'
    if 'BT.709' in str(colour_prim):
        return 'SDR'
    if '0|eng' in str(audio_spec):
        return 'Audio Description'
    return None
    

def main():
    '''
    Check watch folder for folder containing
    MOV files. Look to match to folder name with
    CID item record.
    Where matched, identify HDR and process as
    digital asset for matched CID item record
    then create additional CID item records for
    remaining video/audio files (wrapped .mov)
    '''
    check_control()

    folder_list = walk_folders()
    if len(folder_list) == 0:
        LOGGER.info("Amazon file renaming script. No folders found.")
        sys.exit()

    LOGGER.info("== Document augmented Amazon renaming start =================")
    for fpath in folder_list:
        folder = os.path.split(fpath)[1].strip()
        LOGGER.info("Folder path found: %s", fpath)
        record = cid_check(folder)
        if record is None:
            LOGGER.warning("Skipping: Record could not be matched with object_number")
            continue
        priref = record[0]['priref'][0]
        ob_num = record[0]['object_number'][0]

        LOGGER.info("Folder matched to CID Item record: %s | %s | %s", folder, priref, ob_num)
        mov_list = [x for x in os.listdir(fpath) if x.endswith(('.mov', '.MOV'))]
        all_items = [x for x in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, x))]
        if len(mov_list) != len(all_items):
            LOGGER.warning("Folder contains files that are not MOV: %s", fpath)
            continue

        # Retrieving metadata for all MOV file
        for mov_file in mov_list:
            ext = mov_file.split('.')[1]
            item_xml = ''
            if ext.lower != 'mov':
                LOGGER.warning("Extension of file found that is not MOV. Skipping: %s", mov_file)
                continue
            metadata = retrieve_metadata(fpath, mov_file)
            if 'HDR' in metadata:
                LOGGER.info("UHD HDR file found: %s", mov_file)
                new_filename = f"{ob_num.replace('-', '_')}_01of01.{ext}"
                new_fpath = os.path.join(fpath, new_filename)
                digital_note = f'{mov_file}. Renamed to {new_filename}'

                success = create_digital_original_filenames(priref, folder.strip(), digital_note)
                if not success:
                    LOGGER.warning("Skipping further actions. Acquired filename not written to CID item record: %s", priref)
                    break
                LOGGER.info("CID item record <%s> filenames appended to digital.acquired_filenamed field", priref)
                LOGGER.info("Renaming file %s to %s", mov_file, new_filename)
                os.rename(os.path.join(fpath, mov_file), new_fpath)
                if os.path.exists(new_fpath):
                    LOGGER.info("File renamed successfully. Moving to autoingest/ingest/amazon")
                    shutil.move(os.path.join(fpath, mov_file), os.path.join(AUTOINGEST, new_filename))
                    continue
                else:
                    LOGGER.warning("Failed to rename file. Leaving in folder for manual intervention.")
                    continue
            if 'SDR' in metadata:
                LOGGER.info("UHD SDR file found: %s", mov_file)
                # Build dictionary from CID item record
                item_data = make_item_record_dict(priref, mov_file, ext, record, 'UHD SDR version')
                item_xml = CUR.create_record_data('', item_data)
                print(item_xml)
            elif 'Audio Description' in metadata:
                LOGGER.info("Audio Description file found: %s", mov_file)
                # Build dictionary from CID item record
                item_data = make_item_record_dict(priref, mov_file, ext, record, 'Audio Description')
                item_xml = CUR.create_record_data('', item_data)
                print(item_xml)
            else:
                LOGGER.warning("File found with metadata not recognised. Skipping this item.")
                continue

            # Make new item record
            new_priref, new_ob_num = push_record_create(item_xml, 'items', 'insertrecord')
            if new_priref is None:
                LOGGER.warning("Creation of new CID item record failed with XML: \n%s", item_xml)
                continue
            LOGGER.info("** CID Item record created: %s - %s", new_priref, new_ob_num)

            new_filename = f"{new_ob_num.replace('-', '_')}_01of01.{ext}"
            new_fpath = os.path.join(fpath, new_filename)
            digital_note = f'{mov_file}. Renamed to {new_filename}'
            success = create_digital_original_filenames(new_priref, folder.strip(), digital_note)
            if not success:
                LOGGER.warning("Skipping further actions. Asset item list not written to CID item record: %s", new_priref)
                continue
            LOGGER.info("CID item record <%s> filenames appended to digital.acquired_filenamed field", new_priref)
            LOGGER.info("Renaming file %s to %s", mov_file, new_filename)
            os.rename(os.path.join(fpath, mov_file), new_fpath)
            if os.path.exists(new_fpath):
                LOGGER.info("File renamed successfully. Moving to autoingest/ingest/amazon")
                shutil.move(os.path.join(fpath, mov_file), os.path.join(AUTOINGEST, new_filename))
                continue
            else:
                LOGGER.warning("Failed to rename file. Leaving in folder for manual intervention.")
                continue

        # Check folder is empty and delete
        contents = list(os.listdir(fpath))
        if len(contents) == 0:
            os.rmdir(fpath)
            LOGGER.info("Amazon folder empty, deleting %s", fpath)
        else:
            LOGGER.warning("Amazon folder not empty, leaving in place for checks: %s", fpath)

    LOGGER.info("== Document augmented Amazon renaming end ===================\n")


def make_item_record_dict(priref, file, ext, record, arg):
    '''
    Get CID item record for source and borrow data
    for creation of new CID item record
    '''
    item = []
    record_default = defaults()
    item.extend(record_default)
    item.append({'record_type': 'ITEM'})
    item.append({'item_type': 'DIGITAL'})
    item.append({'copy_status': 'M'})
    item.append({'copy_usage.lref': '131560'})
    item.append({'accession_date': str(datetime.datetime.now())[:10]})

    if 'Title' in str(record):
        mov_title = record[0]['Title'][0]['title'][0]
        item.append({'title': f"{mov_title} ({arg})"})
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
    item.append({'related_object.notes': f'{arg} for'})
    if 'SDR' in arg:
        item.append({'file_type.lref': '397457'}) # Unsure how to set file type here also, MOV?
    elif 'Audio Description' in arg:
        item.append({file_type: 'MOV'}) # Unsure how to set file type here - MOV?
    if 'acquisition.date' in str(record):
        item.append({'acquisition.date': record[0]['acquisition.date'][0]})
    if 'acquisition.method' in str(record):
        item.append({'acquisition.method.lref': record[0]['acquisition.method.lref'][0]})
    if 'Acquisition_source' in str(record):
        item.append({'acquisition.source.lref': record[0]['Acquisition_source'][0]['acquisition.source.lref'][0]})
        item.append({'acquisition.source.type': record[0]['Acquisition_source'][0]['acquisition.source.type'][0]['value'][0]})
    item.append({'access_conditions': 'Access requests for this collection are subject to an approval process. '\
                                      'Please raise a request via the Collections Systems Service Desk, describing your specific use.'})
    item.append({'access_conditions.date': str(datetime.datetime.now())[:10]})
    if 'grouping' in str(record):
        item.append({'grouping': record[0]['grouping'][0]})
    if 'language' in str(record):
        item.append({'language': record[0]['language'][0]['language'][0]})
        item.append({'language.type': record[0]['language'][0]['language.type'][0]['value'][0]})
    if len(file) > 1:
        item.append({'digital.acquired_filename': file})

    return item


def create_digital_original_filenames(priref, digital_note):
    '''
    Create entries for digital.acquired_filename
    and append to the CID item record.
    '''

    item_append_dct = []
    item_append_dct.append({'digital.acquired_filename': digital_note})
    item_append_dct.append({'digital.acquired_filename.type': 'File'})

    item_edit_data = ([{'edit.name': 'datadigipres'},
                       {'edit.date': str(datetime.datetime.now())[:10]},
                       {'edit.time': str(datetime.datetime.now())[11:19]},
                       {'edit.notes': 'Amazon automated digital acquired filename update'}])

    item_append_dct.extend(item_edit_data)
    LOGGER.info("** Appending data to work record now...")

    result = item_append(priref, item_append_dct)
    if result:
        print(f"Item appended successful! {priref}")
        LOGGER.info("Successfully appended MOV digital.acquired_filenames to Item record %s", priref)
        return True
    else:
        LOGGER.warning("Failed to append MOV digital.acquired_filenames to Item record %s", priref)
        print(f"CID item record append FAILED!! {priref}")
        return False


def item_append(priref, item_append_dct):
    '''
    Items passed in item_dct for amending to CID item record
    '''

    try:
        result = CUR.update_record(priref=priref,
                                   database='items',
                                   data=item_append_dct,
                                   output='json',
                                   write=True)
        print("*** CID item record append result:")
        print(result)
        return True
    except Exception as err:
        LOGGER.warning("item_append(): Unable to append work data to CID item record %s", err)
        print(err)
        return False


def defaults():
    '''
    Build defaults for new CID item records
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': 'Amazon metadata integration - automated bulk documentation'},
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
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'grouping.lref': '400947'}, # JMW Will need replacing when new grouping made for Amazon
               {'language.lref': '74129'},
               {'language.type': 'DIALORIG'},
               {'record_type': 'ITEM'},
               {'item_type': 'DIGITAL'},
               {'copy_status': 'M'},
               {'copy_usage.lref': '131560'},
               {'file_type.lref': '397457'}, # ProRes 422 HQ Interlaced (can't find progressive)
               {'accession_date': str(datetime.datetime.now())[:10]}])

    return record


def push_record_create(payload, database, method):
    '''
    Use requests.request to push data to the
    CID API as grouped XML
    '''
    hdrs = {'Content-Type': 'text/xml'}
    prms = {
        'command': method,
        'database': database,
        'xmltype': 'grouped',
        'output': 'json'
    }

    try:
        response = requests.request('POST', CID_API, headers=hdrs, params=prms, data=payload, timeout=1200)
        print(response.text)
    except Exception as err:
        LOGGER.critical("push_record_create(): Unable to create %s record with %s and payload: \n%s", database, method, payload)
        print(err)
        return None, None

    if 'recordList' in response.text:
        records = json.loads(response.text)
        priref = records['adlibJSON']['recordList']['record'][0]['priref'][0]
        object_number = records['adlibJSON']['recordList']['record'][0]['object_number'][0]
        return priref, object_number
    return None, None


if __name__ == '__main__':
    main()
