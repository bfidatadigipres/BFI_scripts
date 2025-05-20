#!/usr/bin/env python3

'''
Assign a gender to any uncatalogued people agents if the given name(s)
extracted from the <name> and <used_for> fields appear unambigiously
in the ONS baby names dataset.

Updated fro Py3.11 2024 / Adlib V3
'''

# Public packages
import os
import sys
import csv
import json
import logging
import datetime
from typing import Final, Optional

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Global variables
LOGS: Final = os.environ['LOG_PATH']
CSV: Final  = os.path.join(LOGS, 'gendered_names.csv')
CID_API: Final = utils.get_current_api()
CONTROL_JSON: Final  = os.path.join(LOGS, 'downtime_control.json')

# Setup logging
LOGGER = logging.getLogger('update_implied_gender')
HDLR = logging.FileHandler(os.path.join(LOGS, 'update_implied_gender.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def load_names() -> dict[str, str]:
    '''
    Load ONS dataset to memory
    '''
    gendered_names: dict[str, str] = {}
    with open(CSV, 'r') as csv_file:
        rows = csv.reader(csv_file)
        for row in rows:
            gendered_names[row[0].lower()] = row[1]

    return gendered_names


def retrieve_people_records() -> tuple[int, list[dict[str, Optional[str]]]]:
    '''
    Download 500 people in scope from 2019-01-01
    and update their implied gender to record
    '''
    search: str = '(creation>"2019-01-01" and party.class=PERSON and name=* and not (forename.implied_gender=MALE,FEMALE,UNRESOLVED)) and not use=*'
    fields: list[str] = [
        'priref',
        'name',
        'used_for'
    ]

    hits, records = adlib.retrieve_record(CID_API, 'people', search, '500', fields)
    if hits is None:
        raise Exception(f'CID API could not be reached with People search:\n{search}')
    if hits == 0:
        return hits, None
    else:
        return hits, records


def main():
    '''
    Download uncatalogued people from cid
    Check for name match, and updated estimated gender
    '''
    if not utils.check_control('pause_scripts'):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()

    LOGGER.info("------------ Gender script start -------------------------")
    LOGGER.info('* Downloading <people> records in scope...')
    hits, records = retrieve_people_records()
    if hits == 0:
        LOGGER.warning("Exiting: No files found in CID people record search")
        sys.exit()
    gendered_names: dict[str, str] = load_names()

    # Parse cid people records
    for r in records:

        person_priref = adlib.retrieve_field_name(r, 'priref')[0]
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
        forenames = []
        for n in names:
            if n is None:
                LOGGER.warning("No names found fpr person priref: %s\n%s", person_priref, r)
                continue
            if ',' in n:
                forenames.extend(n.split(',')[-1].strip().split(' '))

        # Query unique forenames against ONS baby namese
        forenames = set(forenames)
        for fn in forenames:
            query_name = fn.lower()

            for key, val in gendered_names.items():
                if key == query_name:
                    print(f"Match {key} and {query_name}: Value is {val}")
                    gender_balance[val] += 1

        # Weigh gender balance
        female = gender_balance['F']
        male = gender_balance['M']
        print(male, female)
        if female > male:
            gender = 'FEMALE'
            print('Gender likely FEMALE')
        elif male > female:
            gender = 'MALE'
            print('Gender likely MALE')
        else:
            gender = 'UNRESOLVED'
            print('Gender is unresolved')

        # Write MALE/FEMALE to cid record
        success = update_implied_gender(person_priref, gender)
        if 'Automatic gender determination' in str(success):
            LOGGER.info('* updated record [%s] "%s" to %s', person_priref, names[0], gender)
        else:
            LOGGER.warning('* Failed to update record [%s] "%s" to %s', person_priref, names[0], gender)

    LOGGER.info("------------ Gender script end ---------------------------")


def update_implied_gender(priref: str, gender: str) -> str:
    '''
    Update gender data to Person record
    '''
    now: str = str(datetime.datetime.now())[:10]
    time: str = str(datetime.datetime.now())[11:19]
    p_head: str = f'<adlibXML><recordList><record><priref>{priref}</priref>'
    p_mid: str  = f'<forename.implied_gender>{gender}</forename.implied_gender>'
    p_edit1: str  = f'<Edit><edit.date>{now}</edit.date><edit.notes>Automatic gender determination ({gender}) using ONS baby names</edit.notes>'
    p_edit2: str  = f'<edit.name>pip:gender</edit.name><edit.time>{time}</edit.time></Edit>'
    p_end: str  = '</record></recordList></adlibXML>'
    payload: str  = p_head + p_mid + p_edit1 + p_edit2 + p_end

    record = adlib.post(CID_API, payload, 'people', 'updaterecord')
    if not record:
        return False
    return record


if __name__ == '__main__':
    main()
