#!/usr/bin/env python3

'''
Take feed from nominated pointer file and process into Workflow jobs
for next Multi Machine Environment in F47 (first was DigiBeta)

Dependencies:
1. Pointer file number 454 where Items are added for processing
2. d3/selecta.log
3. d3/selections.csv
4. d3/submitta.py
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

f = open(os.path.join(LOGS, 'd3_selecta.log'), 'w' )
f.write( '=== Processing Items in D3 Pointer File ===' + dt_string + '\n' )
f.close()

with open(os.path.join(LOGS, 'downtime_control.json')) as control:
    j = json.load(control)
    if not j['pause_scripts']:
        f = open(os.path.join(LOGS, 'd3_selecta.log'), 'w' )
        f.write('Script run prevented by downtime_control.json. Script exiting.' + dt_string + '\n' )
        f.close()
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def get_candidates():
    q = {'command': 'getpointerfile',
         'database': 'items',
         'number': 454,
         'output': 'json',
         'limit': '0'}

    candidates = []

    try:
        result = cid.get(q)
        candidates = result.records[0]['hit']
    except Exception:
        raise
        print(result.diagnostic)
        raise Exception('Cannot getpointerfile')
    print("********** SELECTA FOUND ITEMS FROM POINTER 454 **************")
    print(candidates)
    return candidates


def get_object_number(priref):
    d = {'database': 'items', 'search': 'priref={}'.format(priref), 'output': 'json', 'fields': 'object_number'}

    result = cid.get(d)
    object_number = result.records[0]['object_number'][0]
    return object_number


# Initialisations
try:
    cid = adlib.Database(url=CID_API)
except Exception:
    raise Exception('Unable to connect to CID')

selections = selections.Selections(input_file='d3_selections.csv')
selected_items = selections.list_items()
candidates = get_candidates()

# Process candidate selections in pointer
for priref in tqdm(candidates, desc='Selecta', leave=False):
    obj = get_object_number(priref)

    # Ignore already selected items
    if obj in selected_items:
       f = open(os.path.join(LOGS, 'd3_selecta.log'), 'a' )
       f.write('* Item already in d3_selections.csv: ' + priref + '\n' )
       f.close()
       continue

    # Model tape carrier
    try:
        t = tape_model.Tape(obj)
    except Exception:
        tqdm.write('Could not model tape from object: {}'.format(obj))
        continue

    # NOT RELEVANT IN THIS CONTEXT - Process only Digibetas
    fmt = t.format()
    #if fmt != 'Digital Betacam':
    #    continue

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

    f = open(os.path.join(LOGS, 'd3_selecta.log'), 'a' )
    f.write( 'This tape will be added to d3_selections.csv:' + '\n' )
    f.write( str(d) + '\n' )
    f.close()

    # Add tape to f47_selections.csv if unique
    tqdm.write('add: {}'.format(str(d)))
    selections.add(**d)

