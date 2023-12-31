#!/usr/bin/env python3

'''
*** THIS SCRIPT MUST RUN FROM SHELL LAUNCH PASSING PATH TO MD5 ***
Script to clean up Checksum  files that belong to filepaths deleted by autoingest,
writes checksums to CID media record priref before deleting files.

1. Receive path to MD5 file
2. Extract filename from title and check file is worth processing
3. Open copy of persistence_queue.csv using csv.DictReader()
4. Iterate through path keys looking for path match to filename and 'delete file'
5. Where there's a match look up work on CID and retrieve media record priref
6. Write checksum to CID media record in notes field
7. Where data written successfully delete checksum
8. Where there's no match leave original checksum and may still be needed

Joanna White 2021
Python3.8+
'''

# Global packages
import os
import sys
import json
import datetime
import logging
import csv

# Local packages
sys.path.append(os.environ['CODE'])
import adlib

# Global variables
LOG_PATH = os.environ['LOG_PATH']
CHECKSUM_PATH = os.path.join(LOG_PATH, 'checksum_md5')
CSV_PATH = os.path.join(LOG_PATH, 'autoingest/global.log')
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API3']

# Setup logging
LOGGER = logging.getLogger('checksum_clean_up')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'checksum_clean_up.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# Local date vars for script comparison/activation
TODAY_TIME = str(datetime.datetime.now())
TIME = TODAY_TIME[11:19]
DATE = datetime.date.today()
TODAY = str(DATE)

# CID URL details
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def check_cid():
    '''
    Test CID online before script progresses
    '''
    try:
        CUR = adlib.Cursor(CID)
    except Exception as e:
        print(e)
        sys.exit()


def name_split(filepath):
    '''
    Splits name of checksum file to filename
    cuts '.md5' from the end to leave whole filename
    '''
    fname = ''
    filename_ext = ''
    filename_ext = os.path.basename(filepath)
    fname = filename_ext[:-4]
    return fname


def checksum_split(data):
    '''
    Splits string and returns
    '''
    md5 = path = date = ''
    try:
        data_split = data.split(" - ")
        md5 = data_split[0]
        path = data_split[1]
        date = data_split[2]
    except Exception as e:
        print(e)

    if len(date) > 0:
        return (md5, path, date)
    else:
        data_split = data.split("  ")
        md5 = data_split[0]
        path = data_split[1]
        date = ''
        return (md5, path, date)


def find_csv_row(fname):
    '''
    Recover checksum filenames from global.log (copy)
    row where object number matched file/directory name
    Checks for delete message in whole, returns message if present
    '''
    message = ''
    with open(CSV_PATH, 'r') as textfile:
        for row in textfile.readlines():
            if str(fname) in str(row):
                if 'Successfully deleted file' in str(row):
                    return row


def cid_retrieve(fname):
    '''
    Retrieve priref for media record from imagen.media.original_filename
    '''
    priref = ''
    search = f"imagen.media.original_filename='{fname}'"
    query = {'database': 'media',
             'search': search,
             'limit': '0',
             'output': 'json',
             'fields': 'checksum.value'
    }
    try:
        query_result = CID.get(query)
    except Exception as e:
        print(e)
        query_result = None
    print(query_result.records)
    try:
        priref = query_result.records[0]['priref'][0]
    except (KeyError, IndexError, AttributeError) as e:
        priref = ""
        print(e)
    try:
        checksum_val = query_result.records[0]['Checksum']['checksum.value'][0]
    except (KeyError, IndexError, AttributeError) as err:
        checksum_val = ''
        print(err)

    return priref, checksum_val


def read_checksum(path):
    '''
    Open text file at path
    Readlins() and store in variable
    '''
    with open(path) as data:
        readme = data.readlines()
        return readme
    data.close()


def main():
    '''
    Clean up scripts for checksum files that have been processed by autoingest
    writing text file dumps to media record in CID
    '''
    check_control()
    check_cid()

    if len(sys.argv) != 2:
        LOGGER.warning("SCRIPT NOT STARTING: MD5 path argument error: %s", sys.argv)
        sys.exit(f'Supplied argument error: {sys.argv}')

    filepath = sys.argv[1]
    fname = name_split(filepath)
    LOGGER.info("===== CHECKSUM CLEAN UP SCRIPT START: %s =====", fname)

    if not os.path.exists(filepath):
        LOGGER.warning("%s -- MD5 path does not exist: %s", fname, filepath)
        sys.exit(f'Supplied path does not exist: {filepath}')

    if fname.endswith((".ini", ".DS_Store", ".mhl", ".json")):
        LOGGER.info("%s -- Skipping as non media file detected.", fname)
        sys.exit(f'Supplied file is not a media file {fname}')

    LOGGER.info("%s -- Processing checksum", fname)
    message = find_csv_row(fname)
    if 'Successfully deleted file' in str(message):
        LOGGER.info("%s -- Associated file deleted in autoingest. Retrieving priref for CID media record", fname)
        priref, checksum_val = cid_retrieve(fname)
        LOGGER.info("Priref <%s> - Checksum value: <%s>", priref, checksum_val)
        if len(checksum_val) > 20:
            LOGGER.info("SKIPPING: %s Checksum already present in media record", checksum_val)
            os.remove(filepath)
            sys.exit("Checksum already exists. Exiting.")
        if len(priref) > 0 and priref.isnumeric():
            LOGGER.info("%s -- Priref retrieved: %s. Writing checksum to record", fname, priref)

            # Get checksum data and write to media record notes
            ck_data = read_checksum(filepath)
            ck_data = str(ck_data[0])
            checksum_data = checksum_split(ck_data)
            md5 = checksum_data[0]
            md5_path = checksum_data[1]
            md5_date = checksum_data[2]
            if 'None' in str(md5):
                LOGGER.warning("%s -- MD5 is 'None', exiting without writing checksum to CID. Deleting MD5.", fname)
                os.remove(filepath) # MD5 file deletion
                LOGGER.info("===== CHECKSUM CLEAN UP SCRIPT COMPLETE %s =====", fname)
                sys.exit()

            # Format as XML
            checksum1 = f"<Checksum><checksum.value>{md5}</checksum.value><checksum.type>MD5</checksum.type>"
            checksum2 = f"<checksum.date>{md5_date}</checksum.date><checksum.path>'{md5_path}'</checksum.path></Checksum>"
            checksum3 = f"<Edit><edit.name>datadigipres</edit.name><edit.date>{str(datetime.datetime.now())[:10]}</edit.date>"
            checksum4 = f"<edit.time>{str(datetime.datetime.now())[11:19]}</edit.time>"
            checksum5 = "<edit.notes>Automated bulk checksum documentation.</edit.notes></Edit>"
            checksum = checksum1 + checksum2 + checksum3 + checksum4 + checksum5
            '''
            # Format as dict
            checksum = [({'checksum.value': md5},
                         {'checksum.type': 'MD5'},
                         {'checksum.date': md5_date},
                         {'checksum.path': md5_path},
                         {'edit.name': 'datadigipres'},
                         {'edit.date': str(datetime.datetime.now())[:10]},
                         {'edit.time': str(datetime.datetime.now())[11:19]},
                         {'edit.notes': 'Automated bulk checksum documentation.'})]
            '''
            try:
                LOGGER.info("%s -- Attempting to write checksum data to Checksum fields", fname)
                status = record_append(priref, checksum, fname)
                if status:
                    LOGGER.info("%s -- Successfully written checksum data to Checksum fields! Deleting checksum file", fname)
                    os.remove(filepath)
                else:
                    LOGGER.warning("%s -- FAIL: Checksum write to media record! Leaving to attempt again later:\n%s\n%s", fname, checksum, status)
            except Exception as e:
                LOGGER.warning("%s -- Unable to append checksum to media record %s", fname, priref, e)

        else:
            LOGGER.info("%s -- No priref retrieved, skipping", fname)
    else:
        LOGGER.info("File name %s not entered as 'Successfully deleted file' in global.log", fname)
    LOGGER.info("===== CHECKSUM CLEAN UP SCRIPT COMPLETE %s =====", fname)


def record_append(priref, checksum_data, fname):
    '''
    Receive checksum data and priref and write to CID media record
    '''
    try:
        result = CUR.update_record(priref=priref,
                                   database='media',
                                   data=checksum_data,
                                   output='json',
                                   write=True)
        if result.hits == 1:
            LOGGER.info("%s -- record_append(): ** Checksum data written to media record %s notes field", fname, priref)
            return True
        elif result.hits == 0:
            return False
    except Exception as e:
        print(f"Error {e}")
        return False


if __name__ == '__main__':
    main()
