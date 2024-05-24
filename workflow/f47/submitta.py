#!/usr/bin/env python3

'''
Aggregate candidate selections from CSV
into optimised batches and submit Workflow jobs

Dependencies:
1. LOGS/f47_sumbitta.log
2. f47/selections.csv
3. f47/errors.csv
'''

import os
import sys
import csv
import json
import yaml
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

sys.path.append(os.environ['WORKFLOW'])
import workflow

LOGS = os.environ['LOG_PATH']
now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

f = open(os.path.join(LOGS, 'f47_submitta.log'), 'w' )
f.write( '=== Processing Items in F47 selections.csv === ' + dt_string + '\n' )
f.close()


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(os.path.join(LOGS, 'downtime_control.json')) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            f = open(os.path.join(LOGS, 'f47_submitta.log'), 'w' )
            f.write('Script run prevented by downtime_control.json. Script exiting.' + dt_string + '\n' )
            f.close()
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


# Check control json
check_control()

# Load configuration variables
configuration = yaml.load(open('config.yaml', 'r'))
batch_size = configuration['Batches']['TapesPerBatch']
batches_per_iteration = configuration['Batches']['BatchesPerIteration']

# Submit [iterations] x [size] tapes
for n in tqdm(range(batches_per_iteration), desc='Submitta', leave=False):

    # Load submissions
    print '* Opening submissions csv...'
    submissions = {}
    with open('submissions.csv', 'r') as f:
        rows = csv.reader(f)
        headers = next(rows)

        for r in rows:
            uid = r[0]
            submissions[uid] = r[1:]

    # Load selections
    print '* Opening selections csv...'
    df = pd.read_csv('f47_selections.csv')

    # Remove submissions from selections data frame
    print '* Removing submissions from selections...'
    unsubmitted_df = df[~df['uid'].isin(submissions)]

    # Replace NaNs with blank strings
    unsubmitted_df = unsubmitted_df.replace(pd.np.nan, '', regex=True)

    # Optimise candidate selections
    print '* Optimising candidate selections...'
    unsubmitted_df.sort_values(by=['location', 'duration', 'item_count', 'content_dates'],
                               ascending=[False, False, False, True],
                               inplace=False)

    f = open(os.path.join(LOGS, 'f47_submitta.log'), 'a' )
    f.write( str(unsubmitted_df) + '\n' )
    f.close()

    batch_items = []
    batch = unsubmitted_df.head(batch_size)

    print '* Check batch_size...'
    if len(batch) != batch_size:
        print '* Batch size check results: len(batch) != batch_size...'
        print '* Therefore quitting...'
        continue

    # Create batch
    print '* Batch size check results: len(batch) = batch_size...'
    print '* Creating batch...'
    for i in tqdm(batch.iterrows(), total=batch_size, desc='Optimising', leave=False):
        index, row = i

        # Wrangle data for output
        data = row.tolist()
        submission = [data[2]] + data[0:1] + data[3:]

        # Get item identifiers
        items = submission[-1].split(',')
        prirefs = [workflow.get_priref(i) for i in items]
        batch_items.extend(prirefs)

        # Track submission
        print '* Writing submissions to submissions csv...'
        with open('f47_submissions.csv', 'a') as of:
            writer = csv.writer(of)
            writer.writerow(submission)

    # Populate topNode fields
    print '* Creating Workflow topnode metadata...'
    today = datetime.today().strftime('%d-%m-%Y')
    job_metadata = dict(configuration['WorkflowMetadata'])

    # Append date and batch number to title
    q = 'topNode="x" and description="*F47 / Ofcom*" and input.name="collectionssystems"'
    lifetime_batches = workflow.count_jobs_submitted(q) + 1
    job_metadata['description'] = '{} / {} / {}'.format(job_metadata['description'],
                                                        today, lifetime_batches)
    # Calculate deadline
    deadline = (datetime.today() + timedelta(days=10)).strftime('%Y-%m-%d')
    job_metadata['completion.date'] = deadline
    job_metadata['negotiatedDeadline'] = deadline

    # Create Workflow records
    print '* Creating Workflow records in CID...'
    batch = workflow.F47Batch(items=batch_items, **job_metadata)
    if not batch.successfully_completed:
        error_row = [str(today), batch.priref, batch.task.job_number, ','.join(batch_items)]

        with open('errors.csv', 'a') as of:
            writer = csv.writer(of)
            writer.writerow(error_row)

        f = open(os.path.join(LOGS, 'f47_submitta.log'), 'a' )
        f.write( str(error_row) + '\n' )
        f.close()

        print('Error creating F47 Workflow job: {}'.format(batch.priref))
