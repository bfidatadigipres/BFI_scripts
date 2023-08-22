#!/usr/bin/ python3

'''
Script to search in CID item
records for Netflix groupings
where digital.acquired_filename
field is populated with 'File'
entries.

Check in CID media records
for matching assets, and if present
map the original filename to the
digital.acquired_filename field
in CID digital media record

Joanna White
2023
'''

# Public packages
import os
import sys
import logging
import datetime

# Local packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
ADMIN = os.environ.get('ADMIN')
LOGS = os.path.join(ADMIN, 'Logs')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
CID_API = os.environ.get('CID_API')
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)

# Setup logging
LOGGER = logging.getLogger('netflix_original_filename_updater')
HDLR = logging.FileHandler(os.path.join(LOGS, 'netflix_original_filename_updater.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_check_items():
    '''
    Sends CID request for digital.acquired_filename
    block for iteration
    '''
    query = {'database': 'items',
             'search': f'(grouping.lref="400947" and file_type="IMP")',
             'limit': '0',
             'output': 'json',
             'fields': 'object_number, digital.acquired_filename'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.warning(f"cid_check_items(): Unable to retrieve any Netflix groupings from CID item records: {err}")
        query_result = None
    try:
        return_count = query_result.hits
        LOGGER.info(f"{return_count} CID item records found")
    except (IndexError, TypeError, KeyError):
        pass

    return query_result.records


def cid_check_filenames(priref):
    '''
    Sends CID request for object number
    checks if filename already populated
    '''
    query = {'database': 'items',
             'search': f'priref="{priref}"',
             'limit': '0',
             'output': 'json',
             'fields': 'priref, digital.acquired_filename, digital.acquired_filename.type'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.warning(f"cid_check_filenames(): Unable to find CID digital media record match: {priref} {err}")
        query_result = None

    try:
        file_name_block = query_result.records[0]['Acquired_filename']
        LOGGER.info(f"cid_check_filenames(): File names retrieved: {file_name_block}")
    except (IndexError, KeyError, TypeError):
        file_name_block = ''

    return file_name_block


def cid_check_media(priref, original_filename):
    '''
    Check for CID media record linked to Item priref
    and see if digital.acquired_filename field populated
    '''
    query = {'database': 'media',
             'search': f'object.object_number.lref="{priref}"',
             'limit': '0',
             'output': 'json',
             'fields': 'priref, digital.acquired_filename, digital.acquired_filename.type'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.warning(f"cid_check_media(): Unable to find CID digital media record match: {priref} {err}")
        query_result = None
    try:
        mpriref = query_result.records[0]['priref'][0]
        LOGGER.info(f"cid_check_media(): CID media record priref: {mpriref}")
    except (IndexError, KeyError, TypeError):
        mpriref = ''
    try:
        file_name = query_result.records[0]['Acquired_filename'][0]['digital.acquired_filename'][0]['Value'][0]
        LOGGER.info(f"cid_check_media(): File names: {file_name}")
    except (IndexError, KeyError, TypeError):
        file_name = ''
    try:
        file_name_type = query_result.records[0]['Acquired_filename'][0]['digital.acquired_filename.type'][0]['Value'][0]
        LOGGER.info(f"cid_check_media(): File name types: {file_name_type}")
    except (IndexError, KeyError, TypeError):
        file_name_type = ''

    if original_filename in str(file_name):
        return mpriref, True
    if len(priref) > 0:
        return mpriref, False
    return None, None


def main():
    '''
    Look for all Netflix items
    recently created (date period
    needs defining) and check CID
    media record has original filename
    populated for all IMP items.
    '''

    records = cid_check_items()
    priref_list = []
    for record in records:
        priref_list.append(record['priref'][0])

    # Iterate list of prirefs
    for priref in priref_list:
        digital_filenames = cid_check_filenames(priref)
        if digital_filenames:
            print(priref)
            print(digital_filenames)
            print("**********")
        if not 'File' in str(digital_filenames):
            LOGGER.info("No digital acquired filenames yet")
            continue
        LOGGER.info("Digital filenames found for IMP ingested items!")

        for filenames in digital_filenames:
            fname = filenames['digital.acquired_filename'][0]
            ftype = filenames['digital.acquired_filename.type'][0]['value'][0]
            if ' - Renamed to: ' in fname:
                original_fname, ingest_name = fname.split(' - Renamed to: ')
                mpriref, match = cid_check_media(priref, original_fname)
                LOGGER.info("Ingest asset identified: %s", ingest_name)
                if mpriref and match:
                    LOGGER.info("SKIPPING: Digital acquired filename already added to CID digital media record %s - %s", mpriref, original_fname)
                    continue
                if mpriref and not match:
                    LOGGER.info("CID media record found %s - Updating digital.acquired_filename to record %s", mpriref, original_fname)
                    success = update_cid_media_record(mpriref, original_fname)
                    if not success:
                        LOGGER.warning("FAILED: Update of original filename to CID media record %s: %s", mpriref, original_fname)
                        continue
                    LOGGER.info("SUCCESS: CID media record %s updated with original filename: %s", mpriref, original_fname)
                if not mpriref:
                    LOGGER.info(f"SKIPPING: No CID media record created yet for ingesting asset: {ingest_name}")
                    continue
            else:
                LOGGER.info("SKIPPING: Acquired filename, not in scope for update: %s - %s", fname, ftype)


def update_cid_media_record(priref, orig_fname):
    '''
    CID media record found without
    original filename, append here
    '''
    name_updates = []
    name_updates.append({'digital.acquired_filename': orig_fname})
    name_updates.append({'digital.acquired_filename.type': 'File'})

    # Append file name with edit block
    media_append_dct = []
    media_append_dct.extend(name_updates)
    edit_data = ([{'edit.name': 'datadigipres'},
                  {'edit.date': str(datetime.datetime.now())[:10]},
                  {'edit.time': str(datetime.datetime.now())[11:19]},
                  {'edit.notes': 'Netflix automated digital acquired filename update'}])

    media_append_dct.extend(edit_data)
    LOGGER.info("** Appending data to CID media record now...")
    print("*********************")
    print(media_append_dct)
    print("*********************")

    try:
        result = CUR.update_record(priref=priref,
                                   database='media',
                                   data=media_append_dct,
                                   output='json',
                                   write=True)
        LOGGER.info("Successfully appended IMP digital.acquired_filenames to Item record %s", priref)
        return True
    except Exception as err:
        LOGGER.warning("Failed to append IMP digital.acquired_filenames to CID media record %s", priref)
        return False


if __name__ == '__main__':
    main()
