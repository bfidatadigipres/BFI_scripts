#!/usr/bin/env python3

'''
Called by splitting scripts
Refactored to Py3
June 2022
'''

# Private packages
import os
import sys
import datetime
import tenacity

# Public packages
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
    with open('/home/datadigipres/code/git/BFIscripts/splitting_scripts/F47_item_records.log', 'a') as file:
        file.write(f"{datetime.datetime.now().isoformat()}\n")
        file.write(f"{data}\n")
        file.write("--------------------------------\n")


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def fetch_existing_object_number(source_object_number):
    ''' Retrieve the Object Number for an existing MKV record, for use in renaming
        the existing Matroska (single Item) or naming the segment'''

    q = {'database': 'items',
         'search': f'(source_item->(object_number="{source_object_number}")) and grouping.lref=397987',
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
        if result.hits == 1:
            print(result.records)
            destination_object = result.records[0]['object_number'][0]
        else:
            destination_object = None
            print('Expected one item record to exist, multiple found')
        # Append segmentation information
        # Increment total item duration

    else:
        # Create new
        destination_object = new(source_object_number, segments, duration, extension, note)

    return destination_object


def new_or_existing_no_segments(source_object_number, extension, note=None):
    ''' Create a new item record for multi-reeler if one doesn't already exist,
        otherwise return the ID of the existing record '''

    result = already_exists(source_object_number)
    if result:
        if result.hits == 1:
            destination_object = result.records[0]['object_number'][0]
        else:
            destination_object = None
            print('Expected one item record to exist, multiple found')

        # Append segmentation information
        # Increment total item duration

    else:
        # Create new
        destination_object = new_no_segments(source_object_number, extension, note)

    return destination_object


def new_or_existing_no_segments_mopup(source_object_number, extension, grouping, note=None):
    ''' Create a new item record for multi-reeler if one doesn't already exist,
        otherwise return the ID of the existing record '''

    result = already_exists_grouping(source_object_number, grouping)
    if result:
        if result.hits == 1:
            destination_object = result.records[0]['object_number'][0]
        else:
            destination_object = None
            print('Expected one item record to exist, multiple found')

        # Append segmentation information
        # Increment total item duration

    else:
        # Create new
        destination_object = new_no_segments_mopup(source_object_number, extension, grouping, note)

    return destination_object


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def already_exists(source_object_number):
    ''' Has an F47 record already been created for source? '''

    q = {'database': 'items',
         'search': f'(source_item->(object_number="{source_object_number}")) and grouping.lref=397987',
         'limit': '0',
         'output': 'json'}

    try:
        result = CID.get(q)
        log_print(f"already_exists(): {result.records}")
    except Exception as exc:
        log_print(f"already_exists(): {exc}")
        raise Exception from exc

    if result.hits >= 1:
        return result
    else:
        return None


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def already_exists_grouping(source_object_number, grouping_lref):
    ''' Has an F47 record already been created for source? '''

    q = {'database': 'items',
         'search': f'(source_item->(object_number="{source_object_number}")) and grouping.lref={grouping_lref}',
         'limit': '0',
         'output': 'json'}

    try:
        result = CID.get(q)
        log_print(f"already_exists(): {result.records}")
    except Exception as exc:
        log_print(f"already_exists(): {exc}")
        raise Exception from exc

    if result.hits >= 1:
        return result
    else:
        return None


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def new_no_segments_mopup(source_object_number, extension, grouping, note=None):
    '''
    Create a new item record
    Python 3 changes to record creation - unsure of write impact
    '''

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
            {'code_type': 'FFV1 v3'},
            {'grouping.lref': grouping},
            {'input.name': 'datadigipres'},
            {'input.date': now_date},
            {'input.time': now_time},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent)},
            {'title': title},
            {'title.type': '10_ARCHIVE'}])

    # Input note if given
    if note is not None:
        rec.append({'input.notes': note})

    log_print(f"NO SEGMENTS\n{rec}")

    try:
        response = CUR.create_record(database='items',
                                    data=rec,
                                     output='json',
                                     write=True)
        if response.records:
            try:
                new_object = response.records[0]['object_number'][0]
                return new_object
            except Exception as exc:
                raise Exception('Failed to retrieve new Object Number') from exc
    except Exception as exc:
        log_print(f"new_no_segments(): {exc}")
        raise Exception('Unable to create record') from exc


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def new_no_segments(source_object_number, extension, note=None):
    '''
    Create a new item record
    Python 3 changes to record creation - unsure of write impact
    '''

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
            {'code_type': 'FFV1 v3'},
            {'grouping.lref': '397987'},
            {'input.name': 'datadigipres'},
            {'input.date': now_date},
            {'input.time': now_time},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent)},
            {'title': title},
            {'title.type': '10_ARCHIVE'}])

    # Input note if given
    if note is not None:
        rec.append({'input.notes': note})

    log_print(f"NO SEGMENTS\n{rec}")

    try:
        response = CUR.create_record(database='items',
                                     data=rec,
                                     output='json',
                                     write=True)
        if response.records:
            try:
                new_object = response.records[0]['object_number'][0]
                return new_object
            except Exception as exc:
                raise Exception('Failed to retrieve new Object Number') from exc
    except Exception as exc:
        log_print(f"new_no_segments(): {exc}")
        raise Exception('Unable to create record') from exc


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
            {'code_type': 'FFV1 v3'},
            {'grouping.lref': '397987'},
            {'input.name': 'datadigipres'},
            {'input.date': now_date},
            {'input.time': now_time},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent)},
            {'title': title}])

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

    log_print(f"SEGMENTS\n{rec}")

    try:
        response = CUR.create_record(database='items',
                                     data=rec,
                                     output='json',
                                     write=True)
        if response.records:
            try:
                new_object = response.records[0]['object_number'][0]
                return new_object
            except Exception as exc:
                raise Exception('Failed to retrieve new Object Number') from exc

    except Exception as exc:
        log_print(f"new(): {exc}")
        raise Exception('Error creating record') from exc
