#!/usr/bin/env python3

'''
To be run from a server where Spectra SDK installed
and run from ENV3, so I'd suggest:
BK-CI-DATA11:
`source /home/datadigipres/code/ENV3/bin/activate`
'''

import os
import csv
import sys
from ds3 import ds3

# Setup client/paths
CLIENT = ds3.createClientFromEnv()
CSV_PATH = 'absolute path here Stephen'
NEW_CSV_PATH = 'absolute path for destination CSV'


def read_csv(csv_path):
    '''
    Yield contents line by line
    '''
    with open(csv_path) as file:
        for row in file:
            # Use this if you add headers to columns and don't want them to break code!
            if row.startswith('filename'):
                continue
            yield row.split(',')


def fetch_length(bucket, ref_num):
    '''
    Fetch length from Black Pearl using
    HeadObjectRequest
    '''
    r = ds3.HeadObjectRequest(bucket, ref_num)
    result = CLIENT.head_object(r)
    return result.response.msg['content-length']


def main():
    '''
    Open CSV, yield each line
    retrieve BP data and write
    to new CSV
    '''
    for ref_num, bucket in read_csv(CSV_PATH):
        check = check_complete(ref_num)
        if check:
            # Already processed this item in CSV, renewed pass after script break
            print(f"Skipping: {ref_num} found in {NEW_CSV_PATH}")
            continue
        file_size = fetch_length(bucket, ref_num)
        write_to_new_csv([ref_num, bucket, file_size])


def write_to_new_csv(data):
    '''
    Check if CSV already exists
    '''
    if not os.path.exists(NEW_CSV_PATH):
        sys.exit("Incorrect CSV path supplied for destination.")
    with open(NEW_CSV_PATH, 'a', newline='') as csv_file:
        datawrite = csv.write(csv_file)
        datawrite.writerow(data)
        csv_file.close()


def check_complete(ref_num):
    '''
    Look for filename match in NEW_CSV_PATH
    and return True so it can skip
    '''
    for fname, _ in read_csv(NEW_CSV_PATH):
        if ref_num == fname:
            return True


if __name__ == '__main__':
    main()
