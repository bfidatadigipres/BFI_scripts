#!/usr/bin/env python3

'''
Take feed from nominated CID pointer file and process into Workflow jobs
for original DigiBeta Multi Machine Environment in F47

Dependencies:
1. Pointer file number 1208 where Items are added for processing
2. LOGS/f47_selecta.log
3. f47/selections.csv
4. f47/submitta.py
'''

# Public imports
import os
import sys
import csv
import json
import uuid
import datetime
from tenacity import retry, stop_after_attempt

# Local imports
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
sys.path.append(os.environ['WORKFLOW'])
import tape_model
import selections

LOGS = os.environ['LOG_PATH']
CID_API = os.environ['CID_API3']
NOW = datetime.datetime.now()
DT_STR = NOW.strftime("%d/%m/%Y %H:%M:%S")
SELECTIONS = os.path.join(os.environ['WORKFLOW'], 'f47/selections.csv')


def check_control():
    '''
    Check downtime control and stop script of False
    '''
    with open(os.path.join(LOGS, 'downtime_control.json')) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            write_to_log(f'Script run prevented by downtime_control.json. Script exiting. {DT_STR}\n')
            sys.exit('Exit requested by downtime_control.json')


def cid_check():
    '''
    Test if CID API online
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        write_to_log("* Cannot establish CID session, exiting script\n")
        print("* Cannot establish CID session, exiting script")
        sys.exit()


def get_candidates():
    '''
    Retrieve items from pointer file 1208
    '''
    q = {'command': 'getpointerfile',
         'database': 'items',
         'number': 1208,
         'output': 'jsonv1'}

    try:
        result = adlib.get(CID_API, q)
        candidates = result['adlibJSON']['recordList']['record'][0]['hitlist']
    except Exception as exc:
        print(result['adlibJSON']['diagnostic'])
        raise Exception('Cannot getpointerfile') from exc

    write_to_log(f"Total candidates: {len(candidates)}")
    return candidates


@retry(stop=stop_after_attempt(10))
def get_object_number(priref):
    '''
    Retrieve object number using priref
    '''
    search = f'priref={priref}'
    record = adlib.retrieve_record(CID_API, 'items', search, '1', ['object_number'])[1]
    if record is None:
        raise Exception
    object_number = adlib.retrieve_field_name(record[0], 'object_number')[0]
    return object_number


def main():
    check_control()
    cid_check()

    write_to_log(f'=== Processing Items in F47 Pointer File === {DT_STR}\n')
    write_to_log('Fetching csv data, building selected items list and fetching candidates.\n')
    f47_select_csv = os.path.join(os.environ['WORKFLOW'], 'f47/selections.csv')
    selects = selections.Selections(input_file=f47_select_csv)
    selected_items = selects.list_items()
    candidates = get_candidates()

    # Process candidate selections in pointer
    dupe_check = []
    for priref in candidates:
        write_to_log(f"Candidate number: {candidates.index(priref)} ")
        obj = get_object_number(priref)

        # Ignore already selected items
        matched = False
        for sel in selected_items:
            if obj == sel:
                write_to_log(f'* Item already in f47/selections.csv: {priref} {obj} - Matched to {sel}\n')
                matched = True
                break
        if matched is True:
            continue

        # Model tape carrier
        write_to_log(f'Modelling tape carrier for item {priref} {obj}\n')
        try:
            t = tape_model.Tape(obj)
        except Exception:
            write_to_log(f'Could not model tape from object: {priref} {obj}')
            continue

        # Get data
        fmt = t.format()
        # Process only Digibetas
        if fmt != 'Digital Betacam':
            continue
        d = t.identifiers
        d['format'] = fmt
        d['duration'] = t.duration()
        dates = t.content_dates()
        if dates:
            d['content_dates'] = ','.join([str(i) for i in dates])

        item_ids = [i['object_number'] for i in t.get_identifiers()]
        d['items'] = ','.join(item_ids)
        d['item_count'] = len(item_ids)
        d['location'] = t.location()
        d['uid'] = str(uuid.uuid4())

        write_to_log('This tape will be added to f47/selections.csv:\n')
        write_to_log(f'{str(d)}\n')

        # Add tape to f47/selections.csv if unique
        print(f'add: {str(d)}')
        str_check = str(d).split('uid')[0]
        if str_check in dupe_check:
            write_to_log(f"Skipping write to CSV, exact match already written to CSV: {str_check}")
            continue
        dupe_check.append(str_check)
        # selections.add(**d) DEPRECATED
        result = selections_add(d)
        if result is None:
            write_to_log("Failed to write data to selections.csv")
            sys.exit("Failed to write data to selections.csv")

    write_to_log(f'=== Items in F47 Pointer File completed === {DT_STR}\n')


def selections_add(data):
    '''
    Write list to new row in CSV
    Replacing broken selection.add()
    Temporary, for refactoring
    '''
    data_list = []
    if not isinstance(data, dict):
        return None

    if not 'can_ID' in data:
        data_list.append('')
    else:
        data_list.append(data['can_ID'])
    if not 'package_number' in data:
        data_list.append('')
    else:
        data_list.append(data['package_number'])
    if not 'uid' in data:
        data_list.append('')
    else:
        data_list.append(data['uid'])
    if not 'location' in data:
        data_list.append('')
    else:
        data_list.append(data['location'])
    if not 'duration' in data:
        data_list.append('')
    else:
        data_list.append(data['duration'])
    if not 'format' in data:
        data_list.append('')
    else:
        data_list.append(data['format'])
    if not 'item_count' in data:
        data_list.append('')
    else:
        data_list.append(data['item_count'])
    if not 'content_dates' in data:
        data_list.append('')
    else:
        data_list.append(data['content_dates'])
    if not 'items' in data:
        data_list.append('')
    else:
        data_list.append(data['items'])

    try:
        print(f'Adding amended data list: {data_list}')
        with open(SELECTIONS, 'a') as file:
            writer = csv.writer(file)
            writer.writerow(data_list)
            return True
    except Exception:
        write_to_log("Failed to write data to selections.csv")
        return None


def write_to_log(message):
    '''
    Write to F47 selecta log
    '''
    with open(os.path.join(LOGS, 'f47_selecta.log'), 'a') as file:
        file.write(message)
        file.close()


if __name__ == '__main__':
    main()
