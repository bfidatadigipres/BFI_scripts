#!/usr/bin/env python3

import os
import csv
import sys
from ds3 import ds3
sys.path.append(os.environ['CODE'])
import utils

# Setup client/paths
CLIENT = ds3.createClientFromEnv()
CSV_PATH = '/mnt/qnap_imagen_storage/Public/Admin/code/filesize_extractor/ofcom_dpi_ingest_1_3.csv'
NEW_CSV_PATH = '/mnt/qnap_imagen_storage/Public/Admin/code/filesize_extractor/ofcom_dpi_ingest_filesizes.csv'


def read_csv(csv_path):
    '''
    Yield contents line by line
    '''
    with open(csv_path) as file:
        for row in file:
            row = row.strip()  # Remove whitespace and newlines
            if row:  # Skip empty lines
                yield row.split(',')


def fetch_length(bucket, ref_num):
    '''
    Fetch length from Black Pearl using
    HeadObjectRequest
    '''
    r = ds3.HeadObjectRequest(bucket, ref_num)
    result = CLIENT.head_object(r)
    return result.response.msg['content-length']


def write_to_new_csv(data):
    '''
    Create CSV if it doesn't exist and append data
    '''
    # Create file with headers if it doesn't exist
    if not os.path.exists(NEW_CSV_PATH):
        with open(NEW_CSV_PATH, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['reference_number', 'bucket', 'file_size'])

    with open(NEW_CSV_PATH, 'a', newline='') as csv_file:
        datawrite = csv.writer(csv_file)
        datawrite.writerow(data)


def check_complete(ref_num):
    '''
    Look for filename match in NEW_CSV_PATH
    and return True so it can skip
    '''
    if not os.path.exists(NEW_CSV_PATH):
        return False

    try:
        with open(NEW_CSV_PATH, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader, None)  # Skip header row if it exists
            for row in csv_reader:
                if row and row[0] == ref_num:  # Check if row exists and first column matches
                    return True
    except Exception as e:
        print(f"Error reading completion status: {e}")
        return False

    return False


def main():
    '''
    Open CSV, yield each line
    retrieve BP data and write
    to new CSV
    '''
    if not utils.check_control("power_off_all"):
        print("Script run prevented by downtime_control.json. Script exiting")
        sys.exit('Script run prevented by downtime_control.json. Script exiting')
    for row in read_csv(CSV_PATH):
        if len(row) < 2:
            print(f"Skipping invalid row: {row}")
            continue

        bucket, ref_num = row

        check = check_complete(ref_num)
        if check:
            print(f"Skipping: {ref_num} found in {NEW_CSV_PATH}")
            continue

        try:
            file_size = fetch_length(bucket, ref_num)
            if file_size is None or len(file_size) == 0:
                print(f"Fail: {ref_num} size not retrieved from Black Pearl")
                continue
            write_to_new_csv([ref_num, bucket, file_size])
        except Exception as e:
            print(f"Error processing {ref_num}: {e}")
            continue


if __name__ == '__main__':
    main()
