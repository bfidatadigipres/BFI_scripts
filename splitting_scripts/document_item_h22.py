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
import adlib

# Configure adlib
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)


def log_print(data):
    '''
    Temp func to track failures in
    CID item record creation
    '''
    with open('/home/datadigipres/code/git/BFIscripts/splitting_scripts/h22_item_records.log', 'a') as file:
        file.write(f"{datetime.datetime.now().isoformat()}\n")
        file.write(f"{data}\n")
        file.write("--------------------------------\n")


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def fetch_existing_object_number(source_object_number):
    ''' Retrieve the Object Number for an existing MKV record, for use in renaming
        the existing Matroska (single Item) or naming the segment'''

    q = {'database': 'items',
         'search': f'(file_type=MKV and source_item->(object_number="{source_object_number}"))',
         'fields': 'object_number',
         'limit': '1',
         'output': 'json'}

    try:
        result = CID.get(q)
        if result.hits > 0:
            derived_object_number = result.records[0]['object_number'][0]
            return derived_object_number
        raise Exception('Expected Item record to exist, none found')
    except Exception as exc:
        raise Exception('Unable to retrieve data from Item record') from exc


def new_or_existing(source_object_number, segments, duration, extension, note=None):
    ''' Create a new item record for multi-reeler if one doesn't already exist,
        otherwise return the ID of the existing record '''

    result = already_exists(source_object_number)
    if result:
        log_print(f"already_exists(): {result.records}")
        if result.hits == 1:
            destination_object = result.records[0]['object_number'][0]
            log_print(f"new_or_existing(): Found CID item record - {destination_object}")
            return destination_object
        log_print(f"new_or_existing(): Multiple records found {result.records}")
        return None
        # Append segmentation information
        # Increment total item duration
    else:
        # Create new
        log_print(f"new_or_existing(): No record found {source_object_number}, creating new one")
        destination_object = new(source_object_number, segments, duration, extension, note)
        return destination_object


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def already_exists(source_object_number):
    ''' Has an MKV record already been created for source? '''

    q = {'database': 'items',
         'search': f'(grouping.lref=398385 and source_item->(object_number="{source_object_number}"))',
         'limit': '0',
         'output': 'json'}

    try:
        result = CID.get(q)
        log_print(f"already_exists(): {result.records}")
    except Exception as exc:
        raise Exception from exc

    if result.hits >= 1:
        return result
    else:
        return None


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def new(source_object_number, segments, duration, extension, note=None):
    ''' Create a new item record '''

    now_date = datetime.datetime.now().isoformat()[:10]
    now_time = datetime.datetime.now().isoformat()[11:].split('.')[0]

    # Fetch source item data
    q = {'database': 'items',
         'search': f'object_number="{source_object_number}"',
         'fields': 'priref,title,part_of_reference',
         'limit': '1',
         'output': 'json'}

    source = CID.get(q)
    source_lref = int(source.records[0]['priref'][0])

    try:
        title = source.records[0]['Title'][0]['title'][0]
    except Exception:
        title = ''

    try:
        parent = source.records[0]['Part_of'][0]['part_of_reference'][0]['priref'][0]
    except Exception:
        parent = ''

    # Construct new record
    rec = ([{'record_type': 'ITEM'},
            {'item_type': 'DIGITAL'},
            {'copy_status': 'M'},
            {'copy_usage': 'Restricted access to preserved digital file'},
            {'file_type': extension.upper()},
            {'grouping.lref': '398385'},
            {'input.name': 'datadigipres'},
            {'input.date': now_date},
            {'input.time': now_time},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent)},
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

    try:
        response = CUR.create_record(database='items',
                                     data=rec,
                                     output='json',
                                     write=True)
        if response.records:
            try:
                new_object = response.records[0]['object_number'][0]
                log_print(f"new(): New record created: {new_object} for source object {source_object_number}")
                return new_object
            except Exception as exc:
                raise Exception('Failed to retrieve new Object Number') from exc

    except Exception as exc:
        raise Exception('Error creating record') from exc
