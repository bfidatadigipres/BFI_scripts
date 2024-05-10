#!/usr/bin/env python3

'''
Called by splitting scripts
Refactored to Py3
Updated for Adlib V3
June 2022
'''

# Private packages
import os
import sys
import datetime
import tenacity

# Public packages
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
    with open(os.path.join(CODE_PATH, 'splitting_scripts/temp_logs/ofcom_item_records.log'), 'a') as file:
        file.write(f"{datetime.datetime.now().isoformat()}\n")
        file.write(f"{data}\n")
        file.write("--------------------------------\n")


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def fetch_existing_object_number(source_object_number):
    '''
    Retrieve the Object Number for an existing MKV record, for use in renaming
    the existing Matroska (single Item) or naming the segment
    '''

    search = f'(source_item->(object_number="{source_object_number}")) and grouping.lref=397987'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '1', ['object_number'])

    if hits > 0:
        derived_object_number = adlib.retrieve_field_name(record[0], 'object_number')[0]
        return derived_object_number
    else:
        raise Exception('Unable to retrieve data from Item record')


def new_or_existing(source_object_number, segments, duration, extension, note=None):
    '''
    Create a new item record for multi-reeler if one doesn't already exist,
    otherwise return the ID of the existing record
    '''

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


def new_or_existing_no_segments(source_object_number, extension, note=None):
    '''
    Create a new item record for multi-reeler if one doesn't already exist,
    otherwise return the ID of the existing record
    '''

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
        destination_object = new_no_segments(source_object_number, extension, note)
        return destination_object


def new_or_existing_no_segments_mopup(source_object_number, extension, grouping, note=None):
    ''' Create a new item record for multi-reeler if one doesn't already exist,
        otherwise return the ID of the existing record '''

    hits, record = already_exists(source_object_number, grouping)
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
        destination_object = new_no_segments_mopup(source_object_number, extension, grouping, note)
        return destination_object


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def already_exists(source_object_number):
    '''
    Has an F47 record already been created for source?
    '''

    search = f'(source_item->(object_number="{source_object_number}")) and grouping.lref=397987'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '0')

    if hits >= 1:
        log_print(f"already_exists(): {record}")
        return hits, record[0]
    else:
        return hits, None


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def already_exists_grouping(source_object_number, grouping_lref):
    '''
    Has an F47 record already been created for source?
    '''

    search = f'(source_item->(object_number="{source_object_number}")) and grouping.lref={grouping_lref}'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '0')

    if hits >= 1:
        log_print(f"already_exists_grouping(): {record}")
        return hits, record[0]
    else:
        return hits, None


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def new_no_segments_mopup(source_object_number, extension, grouping, note=None):
    '''
    Create a new item record
    Python 3 changes to record creation - unsure of write impact
    '''

    # Fetch source item data
    search = f'object_number="{source_object_number}"'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '1')

    if hits > 0:
        source_lref = int(adlib.retrieve_field_name(record[0], 'priref')[0])
    if 'title' in str(record):
        title = adlib.retrieve_field_name(record[0], 'title')[0]
    if not title:
        return None
    if 'part_of_reference' in str(record):
        parent_priref = adlib.retrieve_field_name(record[0]['Part_of'][0]['part_of_reference'][0], 'priref')[0]
    if not parent_priref:
        return None

    # Construct new record
    rec = ([{'record_type': 'ITEM'},
            {'item_type': 'DIGITAL'},
            {'copy_status': 'M'},
            {'copy_usage': 'Restricted access to preserved digital file'},
            {'file_type': extension.upper()},
            {'code_type': 'FFV1 v3'},
            {'grouping.lref': grouping},
            {'input.name': 'datadigipres'},
            {'input.date': datetime.datetime.now().isoformat()[:10]},
            {'input.time': datetime.datetime.now().isoformat()[11:].split('.')[0]},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent_priref)},
            {'title': title},
            {'title.type': '10_ARCHIVE'}])

    # Input note if given
    if note is not None:
        rec.append({'input.notes': note})

    log_print(f"NO SEGMENTS\n{rec}")

    print(rec)
    rec_xml = adlib.create_record_data('', rec)
    print(rec_xml)
    new_record = adlib.post(CID_API, rec_xml, 'items', 'insertrecord')
    if new_record:
        try:
            new_object = adlib.retrieve_field_name(new_record, 'object_number')[0]
            log_print(f"new(): New record created: {new_object} for source object {source_object_number}")
            return new_object
        except Exception as exc:
            raise Exception('Failed to retrieve new Object Number') from exc

    else:
        log_print(f"new_no_segments(): Failed to create record with:\n{rec_xml}")
        raise Exception('Unable to create record')


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def new_no_segments(source_object_number, extension, note=None):
    '''
    Create a new item record
    Python 3 changes to record creation - unsure of write impact
    '''

    # Fetch source item data
    search = f'object_number="{source_object_number}"'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '1')

    if hits > 0:
        source_lref = int(adlib.retrieve_field_name(record[0], 'priref')[0])
    if 'title' in str(record):
        title = adlib.retrieve_field_name(record[0], 'title')[0]
    if not title:
        return None
    if 'part_of_reference' in str(record):
        parent_priref = adlib.retrieve_field_name(record[0]['Part_of'][0]['part_of_reference'][0], 'priref')[0]
    if not parent_priref:
        return None

    # Construct new record
    rec = ([{'record_type': 'ITEM'},
            {'item_type': 'DIGITAL'},
            {'copy_status': 'M'},
            {'copy_usage': 'Restricted access to preserved digital file'},
            {'file_type': extension.upper()},
            {'code_type': 'FFV1 v3'},
            {'grouping.lref': '397987'},
            {'input.name': 'datadigipres'},
            {'input.date': datetime.datetime.now().isoformat()[:10]},
            {'input.time': datetime.datetime.now().isoformat()[11:].split('.')[0]},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent_priref)},
            {'title': title},
            {'title.type': '10_ARCHIVE'}])

    # Input note if given
    if note is not None:
        rec.append({'input.notes': note})

    log_print(f"NO SEGMENTS\n{rec}")

    rec_xml = adlib.create_record_data('', rec)
    print(rec_xml)
    new_record = adlib.post(CID_API, rec_xml, 'items', 'insertrecord')
    if new_record:
        try:
            new_object = adlib.retrieve_field_name(new_record, 'object_number')[0]
            log_print(f"new(): New record created: {new_object} for source object {source_object_number}")
            return new_object
        except Exception as exc:
            raise Exception('Failed to retrieve new Object Number') from exc

    else:
        log_print(f"new_no_segments(): Failed to create record with:\n{rec_xml}")
        raise Exception('Unable to create record')


@tenacity.retry(stop=(tenacity.stop_after_delay(10) | tenacity.stop_after_attempt(10)))
def new(source_object_number, segments, duration, extension, note=None):
    '''
    Create a new item record
    '''

    # Fetch source item data
    search = f'object_number="{source_object_number}"'
    hits, record = adlib.retrieve_record(CID_API, 'items', search, '1')
    print(record)
    if hits > 0:
        source_lref = int(adlib.retrieve_field_name(record[0], 'priref')[0])
        print(source_lref)
    if 'title' in str(record):
        title = adlib.retrieve_field_name(record[0], 'title')[0]
        print(title)
    if not title:
        return None
    if 'part_of_reference' in str(record):
        parent_priref = adlib.retrieve_field_name(record[0]['Part_of'][0]['part_of_reference'][0], 'priref')[0]
        print(parent_priref)
    if not parent_priref:
        return None

    # Construct new record
    rec = ([{'record_type': 'ITEM'},
            {'item_type': 'DIGITAL'},
            {'copy_status': 'M'},
            {'copy_usage': 'Restricted access to preserved digital file'},
            {'file_type': extension.upper()},
            {'code_type': 'FFV1 v3'},
            {'grouping.lref': '397987'},
            {'input.name': 'datadigipres'},
            {'input.date': datetime.datetime.now().isoformat()[:10]},
            {'input.time': datetime.datetime.now().isoformat()[11:].split('.')[0]},
            {'source_item.lref': str(source_lref)},
            {'source_item.content': 'IMAGE_SOUND'},
            {'part_of_reference.lref': str(parent_priref)},
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
    print(rec)
    rec_xml = adlib.create_record_data('', rec)
    print(rec_xml)
    new_record = adlib.post(CID_API, rec_xml, 'items', 'insertrecord')
    if new_record:
        try:
            new_object = adlib.retrieve_field_name(new_record, 'object_number')[0]
            log_print(f"new(): New record created: {new_object} for source object {source_object_number}")
            return new_object
        except Exception as exc:
            raise Exception('Failed to retrieve new Object Number') from exc

    else:
        log_print(f"new_no_segments(): Failed to create record with:\n{rec_xml}")
        raise Exception('Error creating record')
