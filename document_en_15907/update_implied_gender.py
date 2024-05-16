#!/usr/bin/env python3

'''
Assign a gender to any uncatalogued people agents if the given name(s)
extracted from the <name> and <used_for> fields appear unambigiously
in the ONS baby names dataset.

Updated fro Py3.11 / Adlib V3
'''

# Public packages
import os
import sys
import csv
import json
import logging

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global variables
LOGS = os.environ['LOG_PATH']
CSV = os.path.join(LOGS, 'gendered_names.csv')
CID_API = os.environ['CID_API4']
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')

# Setup logging
LOGGER = logging.getLogger('update_implied_gender')
HDLR = logging.FileHandler(os.path.join(LOGS, 'update_implied_gender.log'))
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


def cid_check():
    '''
    Test if CID API online
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def load_names():
    '''
    Load ONS dataset to memory
    '''
    gendered_names = {}
    csv_file = open(CSV)
    rows = csv.reader(csv_file)
    for row in rows:
        gendered_names[row[0].lower()] = row[1]

    return gendered_names


def retrieve_people_records():
    '''
    Download all people in scope from 2019-01-01
    '''
    search = '(creation>"2019-01-01" and party.class=PERSON and name=* and not (forename.implied_gender=MALE,FEMALE,UNRESOLVED)) and not use=*'
    fields = [
        'priref',
        'name',
        'used_for'
    ]

    hits, records = adlib.retrieve_record(CID_API, 'people', search, '0', fields)
    if hits == 0:
        return hits, None
    else:
        return hits, records


def main():
    '''
    Download uncatalogued people from cid
    Check for name match, and updated estimated gender
    '''
    check_control()
    cid_check()

    LOGGER.info("------------ Gender script start -------------------------")
    LOGGER.info('* Downloading <people> records in scope...')
    hits, records = retrieve_people_records()
    if hits == 0:
        LOGGER.warning("Exiting: No files found in CID people record search")
        sys.exit()
    gendered_names = load_names()

    count = 0
    # Parse cid people records
    for r in records:
        if count == 50:
            LOGGER.info("Processed 500, breaking here")
            break
        count += 1

        person_priref = adlib.retrieve_field_name(r, 'priref')[0]
        print(person_priref)
        gender_balance = {'M':0, 'F':0, '?':0}

        # Aggregate name variations
        names = []
        try:
            names.append(adlib.retrieve_field_name(r, 'name')[0])
        except (IndexError, ValueError, TypeError) as err:
            LOGGER.warning("Exiting: Unable to append name from record: \n%s", r)
            print(err)
            continue

        if 'Used_for' in str(r):
            names.append(adlib.retrieve_field_name(r, 'used_for')[0])

        # Extract forenames
        print(names)
        forenames = []
        for n in names:
            if ',' in n:
                forenames.extend(n.split(',')[-1].strip().split(' '))

        # Query unique forenames against ONS baby namese
        forenames = set(forenames)
        print(forenames)
        for fn in forenames:
            query_name = fn.lower()

            if query_name in gendered_names:
                gender_balance[gendered_names[query_name]] += 1

        # Weigh gender balance
        female = gender_balance['F']
        male = gender_balance['M']

        if female > male:
            gender = 'FEMALE'
            print('Gender likely FEMALE')
        elif male > female:
            gender = 'MALE'
            print('Gender likely MALE')
        else:
            gender = '?'

        # Write MALE/FEMALE to cid record
        print(f"Offline for test: Updating record here: {person_priref}")
        '''
        success = update_implied_gender(person_priref, gender)
	if 'recordList' in str(success):
            LOGGER.info('* updated record [%s] "%s" to %s', person_priref, names[0], gender)
        else:
            LOGGER.warning('* Failed to update record [%s] "%s" to %s', person_priref, names[0], gender)
        '''
    LOGGER.info("------------ Gender script end ---------------------------")


def update_implied_gender(priref, gender):
    '''
    Update gender data to Person record
    '''
    now = str(datetime.datetime.now())[:10]
    time = str(datetime.datetime.now())[11:19]
    p_head = f'<adlibXML><recordList><record><priref>{priref}</priref>'
    p_mid = f'<forename.implied_gender>{gender}</forename.implied_gender>'
    p_edit1 = f'<Edit><edit.date>{now}</edit.date><edit.notes>Automatic gender determination ({gender}) using ONS baby names</edit.notes>'
    p_edit2 = f'<edit.name>pip:gender</edit.name><edit.time>{time}</edit.time></Edit>'
    p_end = '</record></recordList></adlibXML>'
    payload = p_head + p_mid + p_edit1 + p_edit2 + p_end

    record = adlib.post(CID_API, payload, 'people', 'updaterecord')
    if not record:
        return False
    return record


if __name__ == '__main__':
    main()

