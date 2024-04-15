#!/usr/bin/ python3

'''
Script to search in CID item
records for streaming groupings
where digital.acquired_filename
field is populated with 'File'
entries and with edit date for
digital.acquired_filename update
in last 30 days only

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
import json
import logging
import itertools
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

# Specific date work
FORMAT = '%Y-%m-%d'
TODAY_DATE = datetime.date.today()
TODAY = TODAY_DATE.strftime(FORMAT)

# Setup logging
LOGGER = logging.getLogger('stream_original_filename_updater')
HDLR = logging.FileHandler(os.path.join(LOGS, 'streaming_original_filename_updater.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

GROUPINGS = {
    'Netflix': '400947, IMP',
    'Amazon': '401361, MOV'
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


def cid_check_items(grouping, file_type, platform):
    '''
    Sends CID request for digital.acquired_filename
    block for iteration
    '''
    query = {'database': 'items',
             'search': f'(grouping.lref="{grouping}" and file_type="{file_type}")',
             'limit': '0',
             'output': 'json',
             'fields': 'object_number, digital.acquired_filename'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.warning("cid_check_items(): Unable to retrieve any %s groupings from CID item records: %s", platform, err)
        query_result = None
    try:
        return_count = query_result.hits
        LOGGER.info("%s CID item records found: %s", platform, return_count)
    except (IndexError, TypeError, KeyError):
        pass

    return query_result.records


def cid_check_filenames(priref, platform):
    '''
    Sends CID request for object number
    checks if filename already populated
    '''
    query = {'database': 'items',
             'search': f'priref="{priref}"',
             'limit': '0',
             'output': 'json',
             'fields': 'priref, digital.acquired_filename, digital.acquired_filename.type, edit.notes, edit.date'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.warning("cid_check_filenames(): Unable to find CID digital media record match for platform %s: %s %s", platform, priref, err)
        query_result = None

    try:
        file_name_block = query_result.records[0]['Acquired_filename']
        LOGGER.info("cid_check_filenames(): Total file names retrieved: %s", len(file_name_block))
    except (IndexError, KeyError, TypeError):
        file_name_block = ''

    try:
        edit = query_result.records[0]['Edit']
    except (IndexError, KeyError, TypeError):
        edit = ''

    if 'edit.notes' in str(edit):
        for edits in edit:
            edit_date = edits['edit.date'][0]
            edit_note = edits['edit.notes'][0]
            if f'{platform} automated digital acquired filename' in edit_note:
                return file_name_block, edit_date

    return file_name_block, ''


def cid_check_media(priref, original_filename, ingest_fname):
    '''
    Check for CID media record linked to Item priref
    and see if digital.acquired_filename field populated
    '''
    query = {'database': 'media',
             'search': f'imagen.media.original_filename="{ingest_fname}"',
             'limit': '0',
             'output': 'json',
             'fields': 'priref, digital.acquired_filename, digital.acquired_filename.type'}
    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.warning("cid_check_media(): Unable to find CID digital media record match: %s %s", priref, err)
        query_result = None

    try:
        mpriref = query_result.records[0]['priref'][0]
        LOGGER.info("cid_check_media(): CID media record priref: %s", mpriref)
    except (IndexError, KeyError, TypeError):
        mpriref = None
    try:
        file_name = query_result.records[0]['Acquired_filename'][0]['digital.acquired_filename'][0]
        LOGGER.info("cid_check_media(): File names: %s", file_name)
    except (IndexError, KeyError, TypeError):
        file_name = ''
    try:
        file_name_type = query_result.records[0]['Acquired_filename'][0]['digital.acquired_filename.type'][0]['value'][0]
        LOGGER.info("cid_check_media(): File name types: %s", file_name_type)
    except (IndexError, KeyError, TypeError):
        file_name_type = ''

    if original_filename in str(file_name):
        return mpriref, True
    if mpriref:
        return mpriref, False
    return None, None


def date_gen(date_str):
    '''
    Yield date range back to main. Py3.7+
    '''
    from_date = datetime.date.fromisoformat(date_str)
    while True:
        yield from_date
        from_date = from_date - datetime.timedelta(days=1)


def main():
    '''
    Look for all Platform items with edit.date for automated
    acquired filename work created within 30 days
    and retrieve digital_acquired.filenames for
    files only and match to imagen.media.original_filename
    of CID media record ingested for CID item. Check if
    digital_acquired.filename already populated in CID media
    - if not update the CID digital media record with IMP name.
    '''

    LOGGER.info("=== Streaming Platform original filename updates START ===================")

    for key, value in GROUPINGS.items():
        platform = key
        grouping_lref, file_type = value.split(', ')

        records = cid_check_items(grouping_lref, file_type, platform)
        if not records:
            LOGGER.info("Skipping: No records recovered for %s", platform)
            continue

        # Generate ISO date range for last 30 days for edit.date check
        date_range = []
        period = itertools.islice(date_gen(TODAY), 30)
        for dt in period:
            date_range.append(dt.strftime(FORMAT))
        print(f"Target date range for {platform} check: {', '.join(date_range)}")

        priref_list = []
        for record in records:
            priref_list.append(record['priref'][0])

        # Iterate list of prirefs
        for priref in priref_list:
            check_control()
            digital_filenames, edit_date = cid_check_filenames(priref, platform)
            if edit_date not in date_range:
                LOGGER.info("Skipping priref %s, out of date range: %s", priref, edit_date)
                continue
            LOGGER.info("\n* Record found with edit date in range for processing: %s %s", priref, edit_date)

            if 'File' not in str(digital_filenames):
                continue
            LOGGER.info("Digital filenames found for %s ingested items:", file_type)

            for filenames in digital_filenames:
                fname = filenames['digital.acquired_filename'][0]
                ftype = filenames['digital.acquired_filename.type'][0]['value'][0]
                if ' - Renamed to: ' in fname:
                    original_fname, ingest_name = fname.split(' - Renamed to: ')
                    mpriref, match = cid_check_media(priref, original_fname, ingest_name)
                    if not mpriref:
                        LOGGER.info("\tIngest asset not found in CID media records: %s", ingest_name)
                        continue
                    LOGGER.info("\tIngest asset identified: %s", ingest_name)
                    if mpriref and match:
                        LOGGER.info("\tSKIPPING: Digital acquired filename already added to CID digital media record %s - %s", mpriref, original_fname)
                        continue
                    if mpriref and not match:
                        LOGGER.info("\tCID media record found %s - Updating digital.acquired_filename to record %s", mpriref, original_fname)
                        success = update_cid_media_record(mpriref, original_fname, platform, file_type)
                        if not success:
                            LOGGER.warning("\tFAILED: Update of original filename to CID media record %s: %s", mpriref, original_fname)
                            continue
                        LOGGER.info("\tSUCCESS: CID media record %s updated with original filename: %s", mpriref, original_fname)
                    if not mpriref:
                        LOGGER.info("\tSKIPPING: No CID media record created yet for ingesting asset: %s", ingest_name)
                        continue
                else:
                    LOGGER.info("\tSKIPPING: Acquired filename, not in scope for update: %s - %s", fname, ftype)

    LOGGER.info("=== Streaming Platform original filename updates END =====================")


def update_cid_media_record(priref, orig_fname, platform, file_type):
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
                  {'edit.notes': f'{platform} automated digital acquired filename update'}])

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
        LOGGER.info("Successfully appended %s digital.acquired_filenames to CID media record %s", file_type, priref)
        return True
    except Exception as err:
        LOGGER.warning("Failed to append %s digital.acquired_filenames to CID media record %s %s", file_type, priref, err)
        return False


if __name__ == '__main__':
    main()
