#!/usr/bin/ python3

'''
Script to receive IMP
Netflix folder named with UID.
UID placed into Item record
digital.acquired_filename field
of CID item record.

Receives IMP folder complete with
MXF video and audio files, and XML
metadata files.

Steps:
1. Finds match for IMP folder name
   within watch folder, from CID item
   record, ie 'N-123456'
2. Creates N_123456_ filename prefix
3. Opens each XML in folder looking
   for string match to '<PackingList'
4. Iterates <PackingList><AssetList><Asset>
   blocks extracting <OriginalFilmName language='en'>
   into list of items
5. Numbers each following order retrieved,
   ie, if 6 assets 'N_123456_01of06' for first item
   and 'N_123456_06of06' for last item, adds to dict.
   Checks same amount of items in PKL as in folder.
6. Iterates dictionary adding original filename and
   new name to CID item record 'digital.acquired_filename'
   field, which allows repeated entries. Formatting:
   "<Original Filename> - Renamed to: N_123456_01of06.mxf"
7. Open each XML and write content to the label.text
   and label.type field (possibly new field)
8. XML and MXF contents of IMP folder are renamed
   as per dictionary and moved to autoingest new
   black_pearl_ingest_netflix path (to be confirmed)
   where new put scripts ensure file is moved to
   the netflix01 bucket.

NOTE: The CID item record digital.acquired_filename
      entry is essential to let the CID media record
      have the digital.acquired_filename populated
      for the XML/MXF original name. So step 6 should
      be robust with good reporting if failures occur.
      Notes of original filename/new name must be logged
      as a back up to this.

FUTURE: This script will need to make new CID item
      item records in the future for ProRes and subtitle
      data, and link to the parent manifestation.

Joanna White
2023
'''

# Public packages
import os
import sys
import shutil
import logging
import datetime
import xmltodict

# Local packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
STORAGE_PTH = os.environ.get('TRANSCODING')
NETFLIX_PTH = os.environ.get('NETFLIX_PATH')
NET_INGEST = os.environ.get('NETFLIX_AUTOINGEST')
AUTOINGEST = os.path.join(STORAGE_PTH, NET_INGEST)
STORAGE = os.path.join(STORAGE_PTH, NETFLIX_PTH)
ADMIN = os.environ.get('ADMIN')
LOGS = os.path.join(ADMIN, 'Logs')
CODE = os.environ.get('CODE_PATH')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Date variables
TODAY = datetime.date.today()
TWO_WEEKS = TODAY - datetime.timedelta(days=14)
START = f"{TWO_WEEKS.strftime('%Y-%m-%d')}T00:00:00"
END = f"{TODAY.strftime('%Y-%m-%d')}T23:59:00"
TITLE_DATA = ''
UPDATE_AFTER = '2022-07-01T00:00:00'

# Setup logging
LOGGER = logging.getLogger('document_augmented_netflix_renaming')
HDLR = logging.FileHandler(os.path.join(LOGS, 'document_augmented_netflix_renaming.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


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


def main():
    '''
    Check watch folder for IMP folder
    look to match IMP folder name with
    CID item record.
    Where matched, process contents
    read PKL XML for part whole order
    and check contents match Asset list.
    '''
    folder_list = [x for x in os.listdir(STORAGE) if os.path.isdir(os.path.join(STORAGE, x))]
    if len(folder_list) == 0:
        LOGGER.info("Netflix IMP renaming script. No folders found.")
        sys.exit()

    LOGGER.info("== Document augmented Netflix renaming start =================")
    for folder in folder_list:
        fpath = os.path.join(STORAGE, folder)
        priref, ob_num, file_type = cid_check(folder.strip())
        print(f"CID item record found: {priref} with matching {file_type.title()}")

        if not priref:
            LOGGER.warning("Cannot find CID Item record for this folder: %s", fpath)
            continue
        if file_type != 'Folder':
            LOGGER.warning("Incorrect filename type retrieved in CID. Skipping.")
            continue

        LOGGER.info("Folder matched to CID Item record: %s | %s | %s", folder, priref, ob_num)
        xml_list = [x for x in os.listdir(fpath) if x.endswith(('.xml', '.XML'))]
        mxf_list = [x for x in os.listdir(fpath) if x.endswith(('.mxf', '.MXF'))]
        all_items = [x for x in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, x))]
        total_items = len(mxf_list) + len(xml_list)
        if total_items != len(all_items):
            LOGGER.warning("Folder contains files that are not XML or MXF: %s", fpath)
            continue
        packing_list = ''
        xml_content_all = []
        # Read/write to CID item record, and identify the PackingList
        for xml in xml_list:
            with open(os.path.join(fpath, xml), 'r') as xml_text:
                xml_content = xml_text.readlines()
            if '<PackingList' in xml_content[1]:
                packing_list = os.path.join(fpath, xml)
            lines = ''.join(xml_content)
            xml_content_all.append(lines)
        print(xml_content_all)
        print(packing_list)
        success = xml_item_append(priref, xml_content_all)

        if not packing_list:
            LOGGER.warning("No PackingList found in folder: %s", fpath)
            continue

        # Extracting PackingList content to dict and count
        asset_dct = {}
        with open(packing_list, 'r') as readfile:
            asset_text = readfile.read()
            asset_dct = xmltodict.parse(f"""{asset_text}""")
        asset_dct_list = asset_dct['PackingList']['AssetList']['Asset']

        # If XML asset not in PKL, add here:
        asset_whole = len(asset_dct_list)
        if asset_whole != (total_items - 2):
            LOGGER.warning("Folder contents does not match length of packing list: %s", fpath)
            LOGGER.warning("PKL length %s -- Total MXF + CPL file in folder %s", asset_whole, total_files)
            continue

        # Build asset_list, PKL order first, followed by remaining XML
        LOGGER.info("PackingList returned %s items, matching MXF content + CPL XML.", asset_whole)
        asset_items = {}
        object_num = 1
        new_filenum_prefix = ob_num.replace('-', '_')
        for asset in asset_dct_list:
            filename = asset['OriginalFileName']['#text']
            ext = os.path.splitext(filename)[1]
            if not filename:
                LOGGER.warning("Exiting processing this asset - Could not retrieve original filename: %s", asset)
                continue
            print(f"Filename found {filename}")
            new_filename = f"{new_filenum_prefix}_{str(object_num).zfill(2)}of{str(total_items).zfill(2)}{ext}"
            asset_items[filename] = new_filename
            object_num += 1
        for xml in xml_list:
            if xml not in asset_items.keys():
                new_filename = f"{new_filenum_prefix}_{str(object_num).zfill(2)}of{str(total_items).zfill(2)}.xml"
                asset_items[xml] = new_filename
                object_num += 1

        if len(asset_items) != total_items:
            LOGGER.warning("Failed to retrieve all filenames from PackingList Assets: %s", asset_dct_list)
            continue

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
                LOGGER.info("\t-  Renaming %s to new filename %s", key, value)
                os.rename(filepath, new_filepath)
                if not os.path.isfile(new_filepath):
                    LOGGER.warning("\t-  Error renaming file %s!", key)
                    success_rename = False
                    break
        if not success_rename:
            LOGGER.warning("SKIPPING: Failure to rename files in IMP %s", fpath)
            continue

        # Move to local autoingest black_pearl_ingest_netflix (subfolder for netflix01 bucket put)
        LOGGER.info("ALL IMP %s FILES RENAMED SUCCESSFULLY", folder)
        for file in asset_items.values():
            moving_asset = os.path.join(fpath, file)
            shutil.move(moving_asset, AUTOINGEST)
            if os.path.isfile(moving_asset):
                LOGGER.warning("Movement of file %s to %s failed!", moving_asset, AUTOINGEST)
                LOGGER.warning(" - Please move manually")
            LOGGER.info("%s moved to autoingest path")

        # Check IMP folder is empty and delete - Is this stage wanted?
        contents = list(os.listdir(fpath))
        if len(contents) == 0:
            os.remove(fpath)
            LOGGER.info("IMP folder empty, deleting %s", fpath)
        else:
            LOGGER.warning("IMP not empty, leaving in place for checks: %s", fpath)

        '''
        # Make new item records here (get title, etc from CID item record, parent priref)
        record, item = build_defaults()
        priref_item = create_item(priref_man, data_dct, record, item)
        if len(priref_item) == 0:
             LOGGER.warning("Monograph item record creation failed, skipping all further stages")
             continue
        print(f"PRIREF FOR NEW ITEM: {priref_item}")
        '''
    LOGGER.info("== Document augmented Netflix renaming end ===================")


def build_defaults():
    '''
    Build record and item defaults
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': 'Netflix metadata integration - automated bulk documentation'},
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
             {'acquisition.source.lref': '143463'}, # Netflix
             {'acquisition.source.type': 'DONOR'}])

    return record, item


def create_digital_original_filenames(priref, folder_name, asset_list_dct):
    '''
    Create entries for digital.acquired_filename
    and append to the CID item record.
    '''
    name_updates = []
    for key, val in asset_list_dct.items():
        name_updates.append({'digital.acquired_filename': f'{key} - Renamed to: {val}'})
        name_updates.append({'digital.acquired_filename.type': 'File'})
    name_updates.append({'digital.acquired_filename': folder_name})
    name_updates.append({'digital.acquired_filename.type': 'Folder'})

    # Append cast/credit and edit name blocks to work_append_dct
    item_append_dct = []
    item_append_dct.extend(name_updates)
    item_edit_data = ([{'edit.name': 'datadigipres'},
                       {'edit.date': str(datetime.datetime.now())[:10]},
                       {'edit.time': str(datetime.datetime.now())[11:19]},
                       {'edit.notes': 'Netflix automated digital acquired filename update'}])

    item_append_dct.extend(item_edit_data)
    LOGGER.info("** Appending data to work record now...")
    print("*********************")
    print(item_append_dct)
    print("*********************")

    result = item_append(priref, item_append_dct)
    if result:
        print(f"Item appended successful! {priref}")
        LOGGER.info("Successfully appended IMP digital.acquired_filenames to Item record %s", priref)
        return True
    else:
        LOGGER.warning("Failed to append IMP digital.acquired_filenames to Item record %s", priref)
        print(f"CID item record append FAILED!! {priref}")
        return False


def xml_item_append(priref, xml_data):
    '''
    Write XML data to CID item record
    '''
    num = 1
    label_dct = []
    for xml_block in xml_data:
        label_dct.append({'label.source': f'Netflix XML data {num}'})
        label_dct.append({'label.text': xml_block})
        num += 1

    success = item_append(priref, label_dct)
    if success:
        return True


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
