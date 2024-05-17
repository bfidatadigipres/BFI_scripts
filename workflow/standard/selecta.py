#!/usr/bin/env python3

'''
Take feed from nominated CID pointer file and process into Workflow jobs
'pointer 242' was the F47 HLF DigiBetas selection pointer file
then 'pointer 616' was next. As of Jan 2023 the new Off-Air DigiBetas
selection pointer file is 'pointer 1208'
'''

import os
import sys
import json
import uuid
import selections
import datetime
from adlib import adlib
from tqdm import tqdm

sys.path.append(os.environ['WORKFLOW'])
import tape_model

CID_API = os.environ['CID_API4']
LOGS = os.environ['LOG_PATH']
now = datetime.datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

f = open(os.path.join(LOGS, 'f47_selecta.log'), 'w' )
f.write( '=== Processing Items in F47 Pointer File ===' + dt_string + '\n' )
f.close()


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(os.path.join(LOGS, 'downtime_control.json')) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            f = open(os.path.join(LOGS, 'f47_selecta.log'), 'w' )
            f.write('Script run prevented by downtime_control.json. Script exiting.' + dt_string + '\n' )
            f.close()
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def get_candidates():
    q = {'command': 'getpointerfile',
         'database': 'items',
         'number': 1208,
         'output': 'json'}

    candidates = []

    try:
        result = cid.get(q)
        candidates = result.records[0]['hit']
    except Exception:
        raise
        print(result.diagnostic)
        raise Exception('Cannot getpointerfile')

    return candidates


def get_object_number(priref):
    d = {'database': 'items', 'search': 'priref={}'.format(priref), 'output': 'json', 'fields': 'object_number'}

    result = cid.get(d)
    object_number = result.records[0]['object_number'][0]
    return object_number


# Initialisations
check_control()

try:
    cid = adlib.Database(url=CID_API)
except Exception:
    raise Exception('Unable to connect to CID')

selections = selections.Selections(input_file='f47_selections.csv')
selected_items = selections.list_items()
candidates = get_candidates()

# Process candidate selections in pointer
for priref in tqdm(candidates, desc='Selecta', leave=False):
    obj = get_object_number(priref)
    print('Current Item is {} / {}'.format(priref, obj))

    # Ignore already selected items
    if obj in selected_items:
       f = open(os.path.join(LOGS, 'f47_selecta.log'), 'a' )
       f.write( '* Item already in selections.csv: ' + priref + '\n' )
       f.close()
       continue

    # Model tape carrier
    f = open(os.path.join(LOGS, 'f47_selecta.log'), 'a' )
    f.write( 'Attempting to model tape from object: {}'.format(obj))
    f.close()

    try:
        t = tape_model.Tape(obj)
    except Exception:
        f = open(os.path.join(LOGS, 'f47_selecta.log'), 'a' )
        f.write( 'Could not model tape from object: {}'.format(obj))
        f.close()
        tqdm.write('Could not model tape from object: {}'.format(obj))
        continue

    # Process only Digibetas
    fmt = t.format()
    if fmt != 'Digital Betacam':
        continue

    # Get data
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
    d['uid'] = uuid.uuid4()

    f = open(os.path.join(LOGS, 'f47_selecta.log'), 'a' )
    f.write( 'This tape will be added to f47_selections.csv:' + '\n' )
    f.write( str(d) + '\n' )
    f.close()

    # Add tape to f47_selections.csv if unique
    tqdm.write('add: {}'.format(str(d)))
    selections.add(**d)

