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
import datetime
import xmltodict

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


def cid_check(imp_fname):
    '''
    Sends CID request for series_id data
    '''
    query = {'database': 'items',
             'search': f'digital.acquired_filename="{imp_fname}"',
             'limit': '1',
             'output': 'json',
             'fields': 'priref, object_number, digital.acquired_filename.type'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        print(f"cid_check(): Unable to match IMP with Item record: {imp_fname} {err}")
        query_result = None
    try:
        priref = query_result.records[0]['priref'][0]
        print(f"cid_check(): Priref: {priref}")
    except (IndexError, KeyError, TypeError):
        priref = ''
    try:
        ob_num = query_result.records[0]['object_number'][0]
        print(f"cid_check(): Object number: {ob_num}")
    except (IndexError, KeyError, TypeError):
        ob_num = ''
    try:
        file_type = query_result.records[0]['Acquired_filename'][0]['digital.acquired_filename.type'][0]['value'][0]
        print(f"cid_check(): File type: {file_type}")
    except (IndexError, KeyError, TypeError):
        file_type = ''

    return priref, ob_num, file_type.title()


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


def main():
    '''
    Check watch folder for IMP folder
    look to match IMP folder name with
    CID item record.
    Where matched, process contents
    read PKL XML for part whole order
    and check contents match Asset list.
    '''
    check_control()

    folder_list = walk_folders()
    if len(folder_list) == 0:
        LOGGER.info("Amazon file renaming script. No folders found.")
        sys.exit()

    LOGGER.info("== Document augmented Amazon renaming start =================")
    for fpath in folder_list:
        folder = os.path.split(fpath)[1]
        LOGGER.info("Folder path found: %s", fpath)
        priref, ob_num, file_type = cid_check(folder.strip())
        print(f"CID item record found: {priref} with matching {file_type.title()}")

        if not priref:
            LOGGER.warning("Cannot find CID Item record for this folder: %s", fpath)
            continue
        if file_type != 'Folder':
            LOGGER.warning("Incorrect filename type retrieved in CID. Skipping.")
            continue

        LOGGER.info("Folder matched to CID Item record: %s | %s | %s", folder, priref, ob_num)
        mov_list = [x for x in os.listdir(fpath) if x.endswith(('.mov', '.MOV'))]
        all_items = [x for x in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, x))]
        if len(mov_list) != len(all_items):
            LOGGER.warning("Folder contains files that are not MOV: %s", fpath)
            continue
        packing_list = ''

        # Retrieving metadata for all MOV files
        for mov_file in mov_list:
            metadata = retrieve_metadata(fpath, mov_file)
            if 'HDR' in metadata:
                ext = mov_file.split('.')[1]
                new_hdr_filename = f"{ob_num.replace('-', '_')}_01of01.{ext}"
                digital_note = f'{mov_file}. Renamed to {new_hdr_filename}'


        # JMW UPTO HERE

        # Write all dict names to digital.acquired_filename in CID item record, re-write folder name
        success = create_digital_original_filenames(priref, folder.strip(), asset_items)
        if not success:
            LOGGER.warning("Skipping further actions. Asset item list not written to CID item record: %s", priref)
            continue
        LOGGER.info("CID item record <%s> filenames appended to digital.acquired_filenamed field", priref)

        # Rename all files in IMP folder
        LOGGER.info("Beginning renaming of IMP folder assets:")
        success_rename = True
        for key, value in asset_items.items():
            filepath = os.path.join(fpath, key)
            new_filepath = os.path.join(fpath, value)
            if os.path.isfile(filepath):
                LOGGER.info("\t- Renaming %s to new filename %s", key, value)
                os.rename(filepath, new_filepath)
                if not os.path.isfile(new_filepath):
                    LOGGER.warning("\t-  Error renaming file %s!", key)
                    success_rename = False
                    break
        if not success_rename:
            LOGGER.warning("SKIPPING: Failure to rename files in IMP %s", fpath)
            continue

        # Move to local autoingest black_pearl_amazon_ingest (subfolder for amazon01 bucket put)
        LOGGER.info("ALL IMP %s FILES RENAMED SUCCESSFULLY", folder)
        LOGGER.info("Moving to autoingest:")
        for file in asset_items.values():
            moving_asset = os.path.join(fpath, file)
            LOGGER.info("\t- %s", moving_asset)
            shutil.move(moving_asset, AUTOINGEST)
            if os.path.isfile(moving_asset):
                LOGGER.warning("Movement of file %s to autoingest failed!", moving_asset)
                LOGGER.warning(" - Please move manually")

        # Check IMP folder is empty and delete - Is this stage wanted? Waiting to hear from Andy
        contents = list(os.listdir(fpath))
        if len(contents) == 0:
            os.rmdir(fpath)
            LOGGER.info("IMP folder empty, deleting %s", fpath)
        else:
            LOGGER.warning("IMP not empty, leaving in place for checks: %s", fpath)

    LOGGER.info("== Document augmented Amazon renaming end ===================\n")


def build_defaults():
    '''
    Build record and item defaults
    Not active, may not be needed
    Record contents may need review!
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': 'Amazon metadata integration - automated bulk documentation'},
               {'record_access.user': 'BFIiispublic'},
               {'record_access.rights': '0'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'grouping.lref': '400947'},
               {'language.lref': '71429'},
               {'language.type': 'DIALORIG'}])

    item = ([{'record_type': 'ITEM'},
             {'item_type': 'DIGITAL'},
             {'copy_status': 'M'},
             {'copy_usage.lref': '131560'},
             {'file_type.lref': '401103'}, # IMP
             {'code_type.lref': '400945'}, # Mixed
             {'accession_date': str(datetime.datetime.now())[:10]},
             {'acquisition.method.lref': '132853'}, # Donation - with written agreement ACQMETH
             {'acquisition.source.lref': '999823516'}, # Amazon Prime Video
             {'acquisition.source.type': 'DONOR'}])

    return record, item


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
    LOGGER.info(name_updates)

    result = item_append(priref, item_append_dct)
    if result:
        print(f"Item appended successful! {priref}")
        LOGGER.info("Successfully appended IMP digital.acquired_filenames to Item record %s", priref)
        return True
    else:
        LOGGER.warning("Failed to append IMP digital.acquired_filenames to Item record %s", priref)
        print(f"CID item record append FAILED!! {priref}")
        return False


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

    colour_prim = subprocess.check_output(cmd)
    colour_prim = colour_prim.decode('utf-8')
    if '2020' in colour_prim:
        return {f'{fname}': 'UHD HDR'}
    elif '709' in colour_prim:
        return {f'{fname}': 'UHD SDR'}
    elif colour_prim == '':
        return {f'{fname}': 'No video'}
    else:
        return None


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


if __name__ == '__main__':
    main()
