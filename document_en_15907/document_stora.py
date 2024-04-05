
#!/usr/bin/env/python3

'''
THIS SCRIPT DEPENDS ON PYTHON ENV PATH

Create CID record hierarchies for Work-Manifestation-Item
using STORA created csv metadata source and traversing filesystem paths to files
    1. Create work-manifestation-item for each csv in the path
    2. Add the WebVTT subtitles to the Item record using requests (to avoid escape
       characters being introduced in Python3 adlib.py
    3. Rename the MPEG transport stream file with the Item object number, into autoingest
    4. Rename the subtitles.vtt file with the Item object number, move to Isilon folder
    5. Identify the folder as completed by renaming the csv with .documented suffix

Stephen McConnachie / Joanna White
Refactored Py3 2023
'''

# Public packages
import os
import sys
import csv
import json
import shutil
import logging
import datetime
import requests
from lxml import etree

# Private packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
STORAGE = os.environ['STORA_PATH']
AUTOINGEST_PATH = os.environ['STORA_AUTOINGEST']
CODE_PATH = os.environ['CODE_DDP']
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
SUBS_PTH = os.environ['SUBS_PATH']
CID_API = os.environ['CID_API3']
cid = adlib.Database(url=CID_API)
cur = adlib.Cursor(cid)

# Setup logging
logger = logging.getLogger('document_stora')
hdlr = logging.FileHandler(os.path.join(LOG_PATH, 'document_stora.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Path date variables
TODAY = datetime.date.today()
YEST = TODAY - datetime.timedelta(days=1)
YEST_CLEAN = YEST.strftime('%Y-%m-%d')
YEAR = YEST_CLEAN[0:4]
#YEAR = '2023'
STORAGE_PATH = os.path.join(STORAGE, YEAR)


def check_control():
    '''
    Check for downtime control
    '''

    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts'] or not j['stora']:
            logger.info("Script run prevented by downtime_control.json. Script exiting.")
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def check_cid():
    '''
    Run a CID check to ensure online
    '''
    try:
        logger.info('* Initialising CID session... Script will exit if CID off line')
        cid = adlib.Database(url=CID_API)
        cur = adlib.Cursor(cid)
        logger.info("* CID online, script will proceed")
    except Exception as err:
        print(f"* Cannot establish CID session, exiting script {err}")
        logger.exception('Cannot establish CID session, exiting script')
        sys.exit()


def csv_retrieve(fullpath):
    '''
    Fall back for missing descriptions, and output all content to utb field
    '''
    data = {}
    print(f"csv_retrieve(): PATH: {fullpath}")
    if not os.path.exists(fullpath):
        logger.warning("No info.csv file found. Skipping CSV retrieve")
        print("No info.csv file found. Skipping CSV retrieve")
        return None

    with open(fullpath, 'r', encoding='utf-8') as inf:
        rows = csv.reader(inf)
        for row in rows:
            print(row)
            data = {'channel': row[0], 'title': row[1], 'description': row[2], \
                    'title_date_start': row[3], 'time': row[4], 'duration': row[5], 'actual_duration': row[6]}
            logger.info('%s\tCSV being processed: %s', fullpath, data['title'])

    return data


def generate_variables(data):
    '''
    Take CSV data and generate variable for CID records
    '''
    channel = data['channel']
    title = data['title']
    if 'programmes start' in title:
        title = f'{channel} {title}'
    description = data['description']
    description = description.replace("\'", "'")
    title_date_start = data['title_date_start']
    time = data['time']

    broadcast_company = code_type = ''
    if 'BBC' in channel or 'CBeebies' in channel:
        code_type = 'MPEG-4 AVC'
        broadcast_company = '454'
    if 'ITV' in channel:
        code_type = 'MPEG-4 AVC'
        broadcast_company = '20425'
    if channel == 'More4' or channel == 'Film4' or channel == 'E4':
        code_type = 'MPEG-2'
        broadcast_company = '73319'
    if channel == 'Channel4':
        code_type = 'MPEG-4 AVC'
        broadcast_company = '73319'
    if '5' in channel or 'Five' in channel:
        code_type = 'MPEG-2'
        broadcast_company = '24404'
    if 'Al Jazeera' in channel:
        code_type = 'MPEG-4 AVC'
        broadcast_company = '125338'
    if 'GB News' in channel:
        code_type = 'MPEG-4 AVC'
        broadcast_company = '999831694'
    if 'Sky News' in channel:
        code_type = 'MPEG-2'
        broadcast_company = '78200'
    if 'Talk TV' in channel:
        code_type = 'MPEG-4 AVC'
        broadcast_company = '999883795'

    duration = data['duration']
    duration_hours, duration_minutes = duration.split(':')[:2]
    duration_hours_integer = int(duration_hours)
    duration_minutes_integer = int(duration_minutes)
    duration_total = (duration_hours_integer * 60) + duration_minutes_integer

    actual_duration = data['actual_duration']
    actual_duration_hours, actual_duration_minutes, actual_duration_seconds = actual_duration.split(':')
    actual_duration_hours_integer = int(actual_duration_hours)
    actual_duration_minutes_integer = int(actual_duration_minutes)
    actual_duration_total = (actual_duration_hours_integer * 60) + actual_duration_minutes_integer
    actual_duration_seconds_integer = int(actual_duration_seconds)

    return (title, description, title_date_start, time, duration_total, actual_duration_total, actual_duration_seconds_integer, channel, broadcast_company, code_type)


def build_defaults(title, description, title_date_start, time, duration_total, actual_duration_total, actual_duration_seconds_integer, channel, broadcast_company, code_type):
    '''
    Get detailed information
    and build record_defaults dict
    '''
    record = ([{'input.name': 'datadigipres'},
               {'input.date': str(datetime.datetime.now())[:10]},
               {'input.time': str(datetime.datetime.now())[11:19]},
               {'input.notes': 'STORA off-air television capture - automated bulk documentation'},
               {'record_access.user': 'BFIiispublic'},
               {'record_access.rights': '0'},
               {'record_access.reason': 'SENSITIVE_LEGAL'},
               {'grouping.lref': '398775'},
               {'title': title},
               {'title.language': 'English'},
               {'title.type': '05_MAIN'}])

    work = ([{'record_type': 'WORK'},
             {'worklevel_type': 'MONOGRAPHIC'},
             {'work_type': 'T'},
             {'description.type.lref': '100298'},
             {'title_date_start': title_date_start},
             {'title_date.type': '04_T'},
             {'description': description},
             {'description.type': 'Synopsis'},
             {'description.date': str(datetime.datetime.now())[:10]}])

    work_restricted = ([{'application_restriction': 'MEDIATHEQUE'},
                        {'application_restriction.date': str(datetime.datetime.now())[:10]},
                        {'application_restriction.reason': 'STRATEGIC'},
                        {'application_restriction.duration': 'PERM'},
                        {'application_restriction.review_date': '2030-01-01'},
                        {'application_restriction.authoriser': 'mcconnachies'},
                        {'application_restriction.notes': 'Automated off-air television capture - pending discussion'}])

    manifestation = ([{'record_type': 'MANIFESTATION'},
                      {'manifestationlevel_type': 'TRANSMISSION'},
                      {'format_high_level': 'Video - Digital'},
                      {'colour_manifestation': 'C'},
                      {'sound_manifestation': 'SOUN'},
                      {'language.lref': '74129'},
                      {'language.type': 'DIALORIG'},
                      {'transmission_date': title_date_start},
                      {'transmission_start_time': time},
                      {'transmission_duration': duration_total},
                      {'runtime': actual_duration_total},
                      {'runtime_seconds': actual_duration_seconds_integer},
                      {'broadcast_channel': channel},
                      {'broadcast_company.lref': broadcast_company},
                      {'transmission_coverage': 'DIT'},
                      {'aspect_ratio': '16:9'},
                      {'country_manifestation': 'United Kingdom'},
                      {'notes': 'Manifestation representing the UK Freeview television broadcast of the Work.'}])

    item = ([{'record_type': 'ITEM'},
             {'item_type': 'DIGITAL'},
             {'copy_status': 'M'},
             {'copy_usage.lref': '131560'},
             {'file_type': 'MPEG-TS'},
             {'code_type': code_type},
             {'source_device': 'STORA'},
             {'acquisition.method': 'Off-Air'}])

    return (record, work, work_restricted, manifestation, item)


def main():
    '''
    Iterate through all info.csv.redux / info.csv.stora
    which have no matching EPG data. Create CID work - manifestation - item records
    '''

    check_control()
    check_cid()
    logger.info('========== STORA documentation script STARTED ===============================================')

    # Iterate through all info.csv.redux/.stora creating CID records
    for root, _, files in os.walk(STORAGE_PATH):
        for file in files:
            # Check control json for STORA false
            check_control()

            if not file.endswith('.stora'):
                continue

            fullpath = os.path.join(root, file)
            data = csv_retrieve(fullpath)
            if len(data) > 0:
                print(f'* CSV found and being processed - {fullpath}')
                logger.info('%s\tCSV being processed: %s', fullpath, data['title'])
                print(f'* Data parsed from csv: {data}')

            # Create variables from csv sources
            var_data = generate_variables(data)
            title = var_data[0]
            description = var_data[1]
            title_date_start = var_data[2]
            time = var_data[3]
            duration_total = var_data[4]
            actual_duration_total = var_data[5]
            actual_duration_seconds_integer = var_data[6]
            channel = var_data[7]
            broadcast_company = var_data[8]
            code_type = var_data[9]
            acquired_filename = os.path.join(root, 'stream.mpeg2.ts')

            # Create defaults for all records in hierarchy
            record, work, work_restricted, manifestation, item = build_defaults(
                                                                     title,
                                                                     description,
                                                                     title_date_start,
                                                                     time,
                                                                     duration_total,
                                                                     actual_duration_total,
                                                                     actual_duration_seconds_integer,
                                                                     channel,
                                                                     broadcast_company,
                                                                     code_type)

            # create a Work-Manifestation CID record hierarchy
            work_id = create_work(fullpath, title, record, work, work_restricted)
            man_id = create_manifestation(work_id, fullpath, title, record, manifestation)

            # Create CID record for Item, first managing subtitles text if present
            old_webvtt = os.path.join(root, "subtitles.vtt")
            webvtt_payload = build_webvtt_dct(old_webvtt)

            item_id, item_ob_num = create_item(man_id, fullpath, title, acquired_filename, record, item)
            if not item_id:
                print('* Item record failed to create. Marking Work and Manifestation with DELETE warning')
                mark_for_deletion(work_id, man_id, fullpath)
                continue

            # Build webvtt payload
            if webvtt_payload:
                success = push_payload(item_id, webvtt_payload)
                if not success:
                    logger.warning("Unable to push webvtt_payload to CID Item %s: %s", item_id, webvtt_payload)

            # Rename csv with .documented
            documented = f'{fullpath}.documented'
            print(f'* Renaming {fullpath} to {documented}')
            try:
                os.rename(fullpath, f"{fullpath}.documented")
            except Exception as err:
                print(f'** PROBLEM: Could not rename {fullpath} to {documented}. {err}')
                logger.critical('%s\tCould not rename to %s', fullpath, documented)

            # Rename transport stream file with Item object number
            item_object_number_underscore = item_ob_num.replace('-', '_')
            new_filename = f'{item_object_number_underscore}_01of01.ts'
            destination = os.path.join(AUTOINGEST_PATH, new_filename)
            print(f'* Renaming {acquired_filename} to {destination}')
            try:
                shutil.move(acquired_filename, destination)
                logger.info('%s\tRenamed %s to %s', fullpath, acquired_filename, destination)
            except Exception as err:
                print(f'** PROBLEM: Could not rename {acquired_filename} to {destination}. {err}')
                logger.critical('%s\tCould not rename %s to %s. Error: %s', fullpath, acquired_filename, destination, err)

            # Rename GOOD subtitle file with Item object number and move to Isilon for use later in MTQ workflow
            if webvtt_payload:
                logger.info('%s\tWebVTT subtitles data included in Item %s', fullpath, item_id)
                old_vtt = fullpath.replace(file, "subtitles.vtt")
                new_vtt_name = f'{item_object_number_underscore}_01of01.vtt'
                new_vtt = f'{SUBS_PTH}{new_vtt_name}'
                print(f'* Renaming {old_vtt} to {new_vtt}')
                try:
                    shutil.move(old_vtt, new_vtt)
                    logger.info('%s\tRenamed %s to %s', fullpath, old_vtt, new_vtt)
                except Exception as err:
                    print(f'** PROBLEM: Could not rename {old_vtt} to {new_vtt}. {err}')
                    logger.critical('%s\tCould not rename %s to %s. Error: %s', fullpath, old_vtt, new_vtt, err)
            else:
                print("Subtitle data is absent. Subtitle.vtt file will not be renamed or moved")

    logger.info('========== STORA documentation script END ===================================================\n')


def create_work(fullpath, title, record_defaults, work_defaults, work_restricted_defaults):
    '''
    Create CID record for Work
    '''
    work_values = []
    work_id = ''
    object_number = ''
    work_values.extend(record_defaults)
    work_values.extend(work_defaults)
    work_values.extend(work_restricted_defaults)

    work_values_xml = cur.create_record_data('', data=work_values)
    if work_values_xml is None:
        return None
    print("***************************")
    print(work_values_xml)

    try:
        logger.info("Attempting to create Work record for item %s", title)
        data = push_record_create(work_values_xml, 'works', 'insertrecord')
        if data:
            work_id = data[0]
            object_number = data[1]
            print(f'* Work record created with Priref {work_id} Object number {object_number}')
            logger.info('%s\tWork record created with priref %s', fullpath, work_id)
        else:
            print(f"Creation of record failed using method Requests: 'works', 'insertrecord'\n{work_values_xml}")
            return None
    except Exception as err:
        print(f"* Unable to create Work record for <{title}>")
        print(err)
        logger.critical('%s\tUnable to create Work record for <%s>', fullpath, title)
        logger.critical(err)
        return None

    return work_id


def create_manifestation(work_id, fullpath, title, record_defaults, manifestation_defaults):

    '''
    Create CID record for Manifestation
    '''
    manifestation_id, object_number = '', ''
    manifestation_values = []
    manifestation_values.extend(record_defaults)
    manifestation_values.extend(manifestation_defaults)
    manifestation_values.append({'part_of_reference.lref': work_id})

    man_values_xml = cur.create_record_data('', data=manifestation_values)
    if man_values_xml is None:
        return None
    print("***************************")
    print(man_values_xml)

    try:
        logger.info("Attempting to create Manifestation record for item %s", title)
        data = push_record_create(man_values_xml, 'manifestations', 'insertrecord')
        if data:
            manifestation_id = data[0]
            object_number = data[1]
            print(f'* Manifestation record created with Priref {manifestation_id} Object number {object_number}')
            logger.info('%s\tManifestation record created with priref %s', fullpath, manifestation_id)

    except Exception as err:
        if 'bool' in str(err):
            logger.critical("Unable to write manifestation record <%s>", manifestation_id)
            print(f"Unable to write manifestation record - error: {err}")
            return None
        print(f"*** Unable to write manifestation record: {err}")
        logger.critical("Unable to write manifestation record <%s> %s", manifestation_id, err)
        raise

    return manifestation_id


def push_record_create(payload, database, method):
    '''
    Receive adlib formed XML but use
    requests to create the CID record
    '''
    params = {
        'command': method,
        'database': database,
        'xmltype': 'grouped',
        'output': 'json'
    }

    headers = {'Content-Type': 'text/xml'}

    try:
        response = requests.request('POST', CID_API, headers=headers, params=params, data=payload, timeout=1200)
    except Exception as err:
        logger.critical("Unable to create <%s> record with <%s> and payload:\n%s", database, method, payload)
        print(err)
        return None
    print(f"Record list: {response.text}")
    if 'recordList' in response.text:
        records = json.loads(response.text)
        priref = records['adlibJSON']['recordList']['record'][0]['priref'][0]
        object_number = records['adlibJSON']['recordList']['record'][0]['object_number'][0]
        return priref, object_number
    return None


def build_webvtt_dct(old_webvtt):
    '''
    Open WEBVTT and if content present
    append to CID item record
    '''

    print("Attempting to open and read subtitles.vtt")
    if not os.path.exists(old_webvtt):
        print(f"subtitles.vtt not found: {old_webvtt}")
        return None

    with open(old_webvtt, encoding='utf-8') as webvtt_file:
        webvtt_payload = webvtt_file.read()
        webvtt_file.close()

    if not webvtt_payload:
        print("subtitles.vtt could not be open")
        logger.warning("Unable to open subtitles.vtt - file absent")
        return None

    if not '-->' in webvtt_payload:
        print("subtitles.vtt has no data present in file")
        logger.warning("subtitles.vtt data is absent")
        return None

    return webvtt_payload.replace("\'", "'")


def create_item(manifestation_id, fullpath, title, acquired_filename, record_defaults, item_defaults):
    '''
    Create item record, and if failure of item record
    creation then add delete warning to work and manifestation records
    '''
    item_id, item_object_number = '',''
    item_values = []
    item_values.extend(record_defaults)
    item_values.extend(item_defaults)
    item_values.append({'part_of_reference.lref': manifestation_id})
    item_values.append({'digital.acquired_filename': acquired_filename})

    item_values_xml = cur.create_record_data('', data=item_values)
    if item_values_xml is None:
        return None
    print("***************************")
    print(item_values_xml)

    try:
        logger.info("Attempting to create CID item record for item %s", title)
        data = push_record_create(item_values_xml, 'items', 'insertrecord')
        if data:
            item_id = data[0]
            item_object_number = data[1]
            print(f'* Item record created with Priref {item_id} Object number {item_object_number}')
            logger.info('%s\tItem record created with priref %s', fullpath, item_id)

    except Exception as err:
        print(f'** PROBLEM: Unable to create Item record for <{title}> {err}')
        logger.critical('%s\tPROBLEM: Unable to create Item record for <%s>, marking Work and Manifestation records for deletion', fullpath, title)

    return item_id, item_object_number


def mark_for_deletion(work_id, manifestation_id, fullpath):
    '''
    Update work and manifestation records with deletion prompt in title
    '''
    work = f'''<record>
               <priref>{work_id}</priref>
               <title>DELETE - STORA record creation problem</title>
               </record>
            '''
    payload = etree.tostring(etree.fromstring(work))

    try:
        r = cur._write('collect', payload)
        if not r.error:
            logger.info('%s\tRenamed Work %s with deletion prompt in title, for bulk deletion', fullpath, work_id)
        else:
            logger.warning('%s\tUnable to rename Work %s with deletion prompt in title, for bulk deletion', fullpath, work_id)
    except Exception as err:
        logger.warning('%s\tUnable to rename Work %s with deletion prompt in title, for bulk deletion. Error: %s', fullpath, work, err)

    manifestation = f'''<record>
                        <priref>{manifestation_id}</priref>
                        <title>DELETE - Redux record creation problem</title>
                        </record>
                     '''
    payload = etree.tostring(etree.fromstring(manifestation))
    try:
        r = cur._write('collect', payload)
        if not r.error:
            logger.info('%s\tRenamed Manifestation %s with deletion prompt in title', fullpath, manifestation_id)
        else:
            logger.warning('%s\tUnable to rename Manifestation %s with deletion prompt in title', fullpath, manifestation_id)
    except Exception as err:
        logger.warning('%s\tUnable to rename Manifestation %s with deletion prompt in title. Error: %s', fullpath, manifestation, err)


def push_payload(item_id, webvtt_payload):
    '''
    Push webvtt payload separately to Item record
    creation, to manage escape character injects
    '''

    label_type = 'SUBWEBVTT'
    label_source = 'Extracted from MPEG-TS created by STORA recording'
    # Make payload
    pay_head = f'<adlibXML><recordList><record priref="{item_id}">'
    label_type_addition = f'<label.type>{label_type}</label.type>'
    label_addition = f'<label.source>{label_source}</label.source><label.text><![CDATA[{webvtt_payload}]]></label.text>'
    pay_end = '</record></recordList></adlibXML>'
    payload = pay_head + label_type_addition + label_addition + pay_end

    lock_success = write_lock(item_id)
    if lock_success:
        post_response = requests.post(
            CID_API,
            params={'database': 'items', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'json'},
            data = {'data': payload})
        if '<error><info>' in str(post_response.text):
            logger.warning('push_payload(): Error returned from requests post: %s %s', item_id, payload)
            print(post_response.text)
            unlock_record(item_id)
            return False
        else:
            logger.info('push_payload(): No error warning in requests post return. Payload written.')
            return True
    else:
        logger.warning('push_payload()): Unable to lock item record %s', item_id)
        return False


def write_lock(item_id):
    '''
    Lock Item record for requests push of XML data
    '''
    try:
        response = requests.post(
            CID_API,
            params={'database': 'items', 'command': 'lockrecord', 'priref': f'{item_id}', 'output': 'json'})
        print(response.text)
        return True
    except Exception as err:
        logger.warning('write_lock(): Failed to lock Item %s \n%s', item_id, err)


def unlock_record(item_id):
    '''
    Manage failed Request push
    Unlock item record again
    '''
    try:
        response = requests.post(
            CID_API,
            params={'database': 'items', 'command': 'unlockrecord', 'priref': f'{item_id}', 'output': 'json'})
        print(response.text)
        return True
    except Exception as err:
        logger.warning('unlock_record(): Failed to unlock Item record %s\n%s', item_id, err)


if __name__ == '__main__':
    main()
