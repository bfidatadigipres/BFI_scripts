#!/usr/bin/env python3

'''
Called by splitting scripts
Refactored to Py3
June 2022
'''

# Public packages
import os
import sys
import datetime
import tenacity

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Configure adlib
CID_API = os.environ['CID_API4']
CODE_PATH = os.environ['CODE']


def log_print(data):
    '''
    Temp func to track failures in
    CID item record creation
    '''
    with open(os.path.join(CODE_PATH, 'splitting_scripts/temp_logs/h22_item_records.log'), 'a') as file:
        file.write(f"{datetime.datetime.now().isoformat()}\n")
        file.write(f"{data}\n")
        file.write("--------------------------------\n")


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def fetch_existing_object_number(source_object_number):
    ''' Retrieve the Object Number for an existing MKV record, for use in renaming
        the existing Matroska (single Item) or naming the segment'''

    search = f'(file_type=MKV and source_item->(object_number="{source_object_number}"))'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '1', ['object_number'])

    if hits > 0:
        derived_object_number = adlib.retrieve_field_name(record[0], 'object_number')[0]
        return derived_object_number
    else:
        raise Exception('Unable to retrieve data from Item record')


def new_or_existing(source_object_number, segments, duration, extension, note=None):
    ''' Create a new item record for multi-reeler if one doesn't already exist,
        otherwise return the ID of the existing record '''

    hits, record = already_exists(source_object_number)
    if hits == 1:
        destination_object = adlib.retrieve_field_name(record, 'object_number')[0]
        log_print(f"new_or_existing(): Found CID item record - {destination_object}")
        return destination_object
    if hits > 1:
        log_print(f"new_or_existing(): Multiple records found {record}")
        return None
        # Append segmentation information
        # Increment total item duration
    if hits == 0:
        # Create new
        log_print(f"new_or_existing(): No record found {source_object_number}, creating new one")
        destination_object = new(source_object_number, segments, duration, extension, note)
        return destination_object


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def already_exists(source_object_number):
    ''' Has an MKV record already been created for source? '''

    search = f'(grouping.lref=398385 and source_item->(object_number="{source_object_number}"))'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '0')
    
    if hits >= 1:
        log_print(f"already_exists(): {record}")
        return hits, record[0]
    else:
        return hits, None


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def new(source_object_number, segments, duration, extension, note=None):
    ''' Create a new item record '''

    # Fetch source item data
    search = f'object_number="{source_object_number}"'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '1', ['priref', 'title', 'part_of_reference.lref'])

    if hits > 0:
        source_lref = int(adlib.retrieve_field_name(record[0], 'priref')[0])
    try:
        title = adlib.retrieve_field_name(record[0], 'title')[0]
    except Exception:
        title = ''
    try:
        parent_priref = adlib.retrieve_field_name(record[0], 'part_of_reference.lref')[0]
    except Exception:
        parent_priref = ''

    # Construct new record
    rec = ([{'record_type': 'ITEM'},
            {'item_type': 'DIGITAL'},
            {'copy_status': 'M'},
            {'copy_usage': 'Restricted access to preserved digital file'},
            {'file_type': extension.upper()},
            {'grouping.lref': '398385'},
            {'input.name': 'datadigipres'},
            {'input.date': datetime.datetime.now().isoformat()[:10]},
            {'input.time': datetime.datetime.now().isoformat()[11:].split('.')[0]},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent_priref)},
            {'title': title},
            {'title.type': '10_ARCHIVE'}])

    # Append duration if given
    if duration:
        # string_duration = time.strftime('%H:%M:%S', time.gmtime(int(duration)))
        rec.append({'video_duration': str(duration)})

    # Append segmentation data
    for t in segments:
        rec.append({'video_part': f'{t[0]}-{t[1]}'})

    # Input note if given
    if note is not None:
        rec.append({'input.notes': note})

    rec_xml = adlib.create_record_data('', rec)
    new_record = adlib.post(CID_API, rec_xml, 'items', 'insertrecord')
    if new_record:
        try:
            new_object = adlib.retrieve_field_name(new_record, 'object_number')[0]
            log_print(f"new(): New record created: {new_object} for source object {source_object_number}")
            return new_object
        except Exception as exc:
            raise Exception('Failed to retrieve new Object Number') from exc

    else:
        raise Exception('Error creating record') from exc
