#!/usr/bin/env python3

'''
Digital pick script collects files needed for DigiOps processing
by looking in Workflow jobs for files that need to be retrieved
from DPI Black Pearl tape library.

Script actions:
1. Script works through Workflow Jobs querying 'description'
   field looking for trigger statement 'DGO' (long-term phrase to be defined).
   It must only look for items with a completion date within CHECK_RANGE,
   and a Status=InProgress.
2. Captures all instances as a dict and returns to main() workflow_jobs,
   with each workflow job's priref as key
3. Iterates the workflow_jobs:
   a. Extract priref, jobnumber, contact_person, request_details
   b. Uses workflow priref to extract list of 'child' items
   c. Builds output folder path using jobnumber and request.from.name
      (or contact.person if request.from.name is absent)
   d. Iterates child item records:
      i. Extracts object number from each child's 'description' field
      ii. Checks if filename is first part in many part wholes, builds list
      iii. Extact each filename from imagen.media.original_filename field of Media record
      iv. Check digital_pick.csv to see if filename already been downloaded
      v. Launch BP download and receive BP job ID
      vi. Check new download path/file exists.
      vii. If yes, write particular file data to digital_pick.csv
   e. Create XML payload with new DPI download date message, prepended to
      contents of request_details field.
   f. Overwrite request.details data to Workflow record.
4. Exit script with final log message.

Joanna White
2022
'''

# Python packages
import os
import sys
import csv
import json
import hashlib
import logging
from datetime import datetime, timedelta
import requests
from ds3 import ds3, ds3Helpers

# Local package
CODE = os.environ['CODE']
sys.path.append(CODE)
import adlib

# API VARIABLES
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)
BUCKET = "imagen"
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)

# GLOBAL VARIABLES
PICK_FOLDER = os.environ['DIGITAL_PICK']
PICK_CSV = os.path.join(PICK_FOLDER, 'digital_pick.csv')
PICK_TEXT = os.path.join(PICK_FOLDER, 'checksum_failures.txt')
LOG_PATH = os.environ['LOG_PATH']
CHECKSUM_PATH = os.path.join(LOG_PATH, 'checksum_md5')
CHECK_RANGE = 14
SEARCH_TERM = 'DPIDL'
USERNAME = os.environ['USERNAME']
FMT = "%Y-%m-%d"
FORMAT = "%Y-%m-%d %H:%M:%S"
TODAY = datetime.strftime(datetime.now(), FORMAT)
CONTROL_JSON = os.environ['CONTROL_JSON']

# Set up logging
LOGGER = logging.getLogger('bp_get_digital_pick')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'black_pearl_digital_pick.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['black_pearl']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')
        if not j['pause_scripts']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def fetch_workflow_jobs():
    '''
    Search for in target workflow jobs, compile into a
    dictionary and return to main()
    '''
    todayd, endd = get_date_range()
    search = f"request.details='*{SEARCH_TERM}*' and completion.date>'{todayd}' and completion.date<'{endd}' and status=InProgress sort completion.date ascending"
    query = {
        'database': 'workflow',
        'search': search,
        'limit': '0',
        'output': 'json',
        'fields': 'priref, object_number, jobnumber, contact_person, request.details, request.from.name'
    }

    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.exception("fetch_workflow_jobs: Unable to retrieve workflow data upto: %s", d)
        query_result = None

    try:
        total_jobs = len(query_result.records)
    except TypeError:
        return None

    workflow_jobs = {}
    for num in range(total_jobs):
        try:
            priref = query_result.records[num]['priref'][0]
        except (IndexError, TypeError, KeyError):
            priref = ''
        try:
            jobnumber = query_result.records[num]['jobnumber'][0]
        except (IndexError, KeyError, TypeError):
            jobnumber = ''
        try:
            contact_person = query_result.records[num]['contact_person'][0]
        except (IndexError, KeyError, TypeError):
            contact_person = ''
        try:
            request_details = query_result.records[num]['request.details'][0]
        except (IndexError, KeyError, TypeError):
            request_details = ''
        try:
            request_from = query_result.records[num]['request.from.name'][0]
        except (IndexError, KeyError, TypeError):
            request_from = ''

        if request_from:
            request_from = request_from.replace(' ', '_').lower()

        workflow_list = [jobnumber, contact_person, request_details, request_from]
        workflow_jobs[priref] = workflow_list

    return workflow_jobs


def get_date_range():
    '''
    Return CHECK_RANGE day from today's date
    '''
    today = datetime.now()
    dr = today + timedelta(days=CHECK_RANGE)
    todays_date = datetime.strftime(today, FMT)
    end_date = datetime.strftime(dr, FMT)
    return todays_date, end_date


def fetch_item_list(priref):
    '''
    Fetch a workflow job's items list
    '''
    search = f"parent_record={priref} and recordType=ObjectList"
    query = {
        'database': 'workflow',
        'search': search,
        'limit': '0',
        'output': 'json'
    }

    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.exception("fetch_workflow_jobs: Unable to retrieve workflow data upto: %s\n%s", priref, err)
        query_result = None
    print(query_result.records)

    try:
        children = query_result.records[0]['child']
    except (IndexError, TypeError, KeyError):
        children = ''
    print(children, len(children))
    if children:
        return children


def get_child_ob_num(priref):
    '''
    Retrieve the child's object number from workflow
    '''
    search = f"priref={priref}"
    query = {
        'database': 'workflow',
        'search': search,
        'limit': '0',
        'output': 'json',
        'fields': 'description'
    }
    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.exception("get_child_ob_num: Unable to retrieve workflow data upto: %s", err)
        print(err)
        query_result = None
    try:
        ob_num = query_result.records[0]['description'][0]
        print(ob_num)
        return ob_num
    except (IndexError, TypeError, KeyError):
        return None


def get_media_original_filename(search):
    '''
    Retrieve the child's object number from workflow
    '''
    query = {
        'database': 'media',
        'search': search,
        'limit': '0',
        'output': 'json',
        'fields': 'imagen.media.original_filename, reference_number'
    }

    try:
        query_result = CID.get(query)
    except Exception as err:
        LOGGER.exception("get_media_original_filename: Unable to retrieve Media data upto: %s", err)
        print(err)
        query_result = None

    try:
        orig_fname = query_result.records[0]['imagen.media.original_filename'][0]
    except (IndexError, TypeError, KeyError):
        orig_fname = ''
    try:
        ref_num = query_result.records[0]['reference_number'][0]
    except (IndexError, TypeError, KeyError):
        ref_num = ''

    return orig_fname, ref_num


def get_missing_part_names(filename):
    '''
    Extract range from part 01of*
    call up Digital Media record for
    each part retrieve ref number
    return "ref_name:filename"
    '''
    fname_list = []
    filename, ext = os.path.splitext(filename)
    fname_split = filename.split('_')
    part_whole = fname_split[-1]
    fname = '_'.join(fname_split[:-1])
    if 'of' not in part_whole:
        return None
    part, whole = part_whole.split('of')

    if len(part) == 2:
        for count in range(2, int(whole) + 1):
            new_fname = f"{fname}_{str(count).zfill(2)}of{whole}{ext}"
            orig_name, ref_num = get_media_original_filename(f"imagen.media.original_filename={new_fname}")
            if not ref_num:
                print("Skipping No Digital Media record found for file {new_fname}")
                continue
            print(f"CID Digital media record found. Ingest name {orig_name} Reference number {ref_num}")
            fname_list.append(f"{new_fname}:{ref_num}")
    elif len(part) == 3:
        for count in range(2, int(whole) + 1):
            new_fname = f"{fname}_{str(count).zfill(3)}of{whole}{ext}"
            orig_name, ref_num = get_media_original_filename(f"imagen.media.original_filename={new_fname}")
            if not ref_num:
                print("Skipping No Digital Media record found for file {new_fname}")
                continue
            fname_list.append(f"{new_fname}:{ref_num}")
    else:
        print('Unanticpated part whole number length. Script update needed')

    return fname_list


def get_bp_md5(fname):
    '''
    Fetch BP checksum to compare
    to new local MD5
    '''
    md5 = ''
    query = ds3.HeadObjectRequest(BUCKET, fname)
    result = CLIENT.head_object(query)
    try:
        md5 = result.response.msg['ETag']
    except Exception as err:
        print(err)
    if md5:
        return md5.replace('"', '')


def make_check_md5(fpath, fname):
    '''
    Generate MD5 for fpath
    Locate matching file in CID/checksum_md5 folder
    and see if checksums match. If not, write to log
    '''
    download_checksum = ''

    try:
        hash_md5 = hashlib.md5()
        with open(fpath, "rb") as file:
            for chunk in iter(lambda: file.read(65536), b""):
                hash_md5.update(chunk)
        download_checksum = hash_md5.hexdigest()
    except Exception as err:
        print(err)

    local_checksum = get_bp_md5(fname)
    print(f"Created from download: {download_checksum} | Retrieved from BP: {local_checksum}")
    return str(download_checksum), str(local_checksum)


def checksum_log(message):
    '''
    Append checksum message to checksum
    log where no match found
    '''
    datestamp = datetime.now()
    data = f"{datestamp}, {message}\n"

    with open(PICK_TEXT, 'a+') as out_file:
        out_file.write(data)


def check_csv(fname):
    '''
    Check CSV for evidence that fname already
    downloaded. Extract download date and return
    otherwise return None.
    '''
    with open(PICK_CSV, 'r') as csvread:
        readme = csv.DictReader(csvread)
        for row in readme:
            if str(fname) in str(row):
                return row['date']


def main():
    '''
    Start Workflow search, iterate results and build list
    of files for download from DPI. Map in digital_pick.csv
    to avoid repeating unecessary DPI downloads
    '''
    check_control()
    workflow_jobs = fetch_workflow_jobs()
    if not workflow_jobs:
        sys.exit("Script exiting. No workflow jobs found status=InProgress for next two weeks.")
    if len(workflow_jobs) == 0:
        sys.exit("Script exiting. No workflow jobs found status=InProgress for next two weeks.")

    LOGGER.info("=========== Digital Pick script start ===========")
    LOGGER.info("Workflow jobs retrieved for next two weeks:\n%s", workflow_jobs)

    # Iterate all workflow jobs for next 2 weeks
    for wf in workflow_jobs.items():
        priref = wf[0]
        jobnumber = wf[1][0]
        contact_person = wf[1][1]
        request_details = wf[1][2]
        request_from = wf[1][3]
        LOGGER.info("Looking at Workflow job number %s - priref %s", jobnumber, priref)

        # Fetch child items of ObjectList
        children = fetch_item_list(priref)
        if len(children) == 0:
            LOGGER.info("Skipping. No children found for Priref: %s", priref)
            continue

        # Build folder name for BP file retrieval location
        if jobnumber and request_from:
            outpath = os.path.join(PICK_FOLDER, f'{jobnumber}_{request_from}')
        elif jobnumber:
            outpath = os.path.join(PICK_FOLDER, f'{jobnumber}_{contact_person}')
        else:
            LOGGER.info("Skipping. No Workflow jobnumber found for priref: %s", priref)
            continue

        # Iterate children of Workflow job
        downloads = []
        for child_priref in children:
            child_ob_num = get_child_ob_num(child_priref)
            LOGGER.info("Child object number returned from description field: <%s>", child_ob_num)
            filename, ref_num = get_media_original_filename(f"object.object_number='{child_ob_num}'")
            print(child_priref, child_ob_num, filename, ref_num)
            LOGGER.info("Looking at child object number %s - priref %s", child_ob_num, child_priref)
            if not filename:
                LOGGER.info("Skipping. No matching Media record object number / imagen original filename: %s", child_ob_num)
                downloads.append('False')
                continue

            # Check if file is first part of sequence of files
            parts_downloads = []
            filenames = [filename]
            if '01of01' not in filename:
                parts_downloads = get_missing_part_names(filename)
                if not parts_downloads:
                    pass
                else:
                    for parts in parts_downloads:
                        filenames.append(parts.split(':')[0])

            downloaded_fnames = []
            for fname in filenames:
                downloaded = check_csv(fname)
                if downloaded:
                    downloaded_fnames.append(fname)
                    LOGGER.info("DOWNLOADED: File %s already downloaded: %s", filename, downloaded)
                    downloads.append('False')
            if len(filenames) == len(downloaded_fnames):
                LOGGER.info("All parts downloaded: %s", filenames)
                continue

            # Create new jobnumber folder
            if not os.path.exists(outpath):
                os.makedirs(outpath, mode=0o777, exist_ok=True)

            if filename.strip() == ref_num.strip():
                umid = False
                download_fname = filename
            else:
                download_fname = ref_num
                umid = True
                LOGGER.info("File to be retrieved from BP with UMID: %s", ref_num)

            # Call up BP and get the file object
            if filename not in downloaded_fnames:
                download_job_id = download_bp_object(download_fname, outpath)
                if os.path.exists(os.path.join(outpath, download_fname)):
                    # Write successful download to CSV
                    if umid:
                        os.rename(os.path.join(outpath, download_fname), os.path.join(outpath, filename))
                    download_checksum, bp_checksum = make_check_md5(os.path.join(outpath, filename), download_fname)
                    if len(bp_checksum) == 0 or len(download_checksum) == 0:
                        LOGGER.warning("Checksums could not be retrieved %s | %s. Writing warning to checksum_failure.log", download_checksum, bp_checksum)
                        checksum_log(f"Error accessing checksum for {filename} | BP checksum: {bp_checksum} | Downloaded file checksum: {download_checksum}")
                    elif bp_checksum.strip() != download_checksum.strip():
                        LOGGER.warning("Checksums do not match %s | %s. Writing warning to checksum_failure.log", download_checksum, bp_checksum)
                        checksum_log(f"Error accessing checksum for {filename} | BP checksum: {bp_checksum} | Downloaded file checksum: {download_checksum}")
                    else:
                        LOGGER.info("Black Pearl checksum '%s' matches generated checksum for download file '%s'", bp_checksum, download_checksum)
                    data = [filename, outpath, priref, jobnumber, contact_person, child_priref, child_ob_num, download_job_id, datetime.strftime(datetime.now(), FORMAT)]
                    write_to_csv(data)
                    LOGGER.info("File %s downloaded to %s", filename, outpath)
                    LOGGER.info("digital_pick.csv updated: %s", data)
                    downloads.append('True')
                else:
                    LOGGER.warning("Skipping this item: BP download failed for file %s", filename)
                    downloads.append('False')

            # Download other parts if they exist
            if parts_downloads:
                LOGGER.info('Multiple parts need to be downloaded for this job')
                for download_fname_part in parts_downloads:
                    part_fname, part_umid = download_fname_part.split(':')
                    if part_fname in downloaded_fnames:
                        LOGGER.info("Skipping this item as already downloaded: %s", part_fname)
                        continue
                    if part_fname == part_umid:
                        dpart_fname = part_fname
                    else:
                        dpart_fname = part_umid
                    d_job_id = download_bp_object(dpart_fname, outpath)
                    if os.path.exists(os.path.join(outpath, dpart_fname)):
                        # Write successful download to CSV
                        if part_fname != dpart_fname:
                            os.rename(os.path.join(outpath, dpart_fname), os.path.join(outpath, part_fname))
                        # Make checksum test
                        download_checksum, bp_checksum = make_check_md5(os.path.join(outpath, part_fname), dpart_fname)
                        if bp_checksum is None or download_checksum is None:
                            LOGGER.warning("Checksums could not be retrieved %s | %s. Writing warning to checksum_failure.log", download_checksum, bp_checksum)
                            checksum_log(f"Error accessing checksum for {part_fname} | BP checksum: {bp_checksum} | Downloaded file checksum: {download_checksum}")
                        elif bp_checksum.strip() != download_checksum.strip():
                            LOGGER.warning("Checksums do not match %s | %s. Writing warning to checksum_failure.log", download_checksum, bp_checksum)
                            checksum_log(f"Error accessing checksum for {part_fname} | BP checksum: {bp_checksum} | Downloaded file checksum: {download_checksum}")
                        else:
                            LOGGER.info("Black Pearl checksum '%s' matches generated checksum for download file '%s'", bp_checksum, download_checksum)
                        data = [part_fname, outpath, priref, jobnumber, contact_person, child_priref, child_ob_num, d_job_id, datetime.strftime(datetime.now(), FORMAT)]
                        write_to_csv(data)
                        LOGGER.info("File %s downloaded to %s", filename, outpath)
                        LOGGER.info("digital_pick.csv updated: %s", data)
                        downloads.append('True')
                    else:
                        LOGGER.warning("Skipping this item: BP download failed for file %s", fname)
                        downloads.append('False')

        # Check for any successful uploads
        if 'True' not in downloads:
            LOGGER.info("No items downloaded for this Workflow: %s", wf)
            continue

        # Update workflow request.details field when all completed
        payload = build_payload(priref, request_details, datetime.strftime(datetime.now(), FORMAT))
        print(payload)

        success_lock = write_lock(priref)
        if not success_lock:
            print("Request to lock record failed")
        else:
            completed = write_payload(priref, payload)
            if not completed:
                success_unlock = unlock_record(priref)
                if not success_unlock:
                    print("Request to unlock record failed")
                    LOGGER.warning("FAILED: Unlock of Workflow record %s", priref)
                LOGGER.warning("FAILED: Payload write to CID workflow request.details field: %s", priref)
            else:
                LOGGER.info("Workflow %s - DPI download complete and request.details field updated", priref)

    LOGGER.info("=========== Digital Pick script end =============\n")


def download_bp_object(fname, outpath):
    '''
    Download the BP object from SpectraLogic
    tape library and save to outpath
    '''
    file_path = os.path.join(outpath, fname)
    get_objects = [ds3Helpers.HelperGetObject(fname, file_path)]
    try:
        get_job_id = HELPER.get_objects(get_objects, BUCKET)
        print(f"BP get job ID: {get_job_id}")
    except Exception as err:
        LOGGER.warning("Unable to retrieve file %s from Black Pearl", fname)
        get_job_id = None

    return get_job_id


def write_lock(priref):
    '''
    Apply a write lock to record before updating metadata
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': 'workflow', 'command': 'lockrecord', 'priref': f'{priref}', 'output': 'json'}
        )
        print("write_lock() response:")
        print(post_response.text)
        return True
    except Exception as err:
        LOGGER.warning("write_lock: Unable to lock record %s:\n%s", priref, err)


def build_payload(priref, data, today):
    '''
    Build payload info to write to Workflow record
    '''
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_addition = f"<request.details>DPI Download completed {today}. {data}</request.details>"
    payload_edit = f"<edit.name>{USERNAME}</edit.name><edit.date>{today[:10]}</edit.date><edit.time>{today[11:]}</edit.time>"
    payload_end = "</record></recordList></adlibXML>"
    return payload_head + payload_addition + payload_edit + payload_end


def write_payload(priref, payload):
    '''
    Recieve header, payload and priref and write
    to CID workflow record
    '''
    post_response = requests.post(
        CID_API,
        params={'database': 'workflow', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'json'},
        data={'data': payload}
    )
    print("write_payload() response:")
    print(post_response.text)
    if "<error><info>" in str(post_response.text) or 'error' in str(post_response.text):
        LOGGER.warning("write_payload: Error returned for requests.post for %s:\n%s", priref, payload)
        return False
    else:
        LOGGER.info("write_payload: No error returned in post_response.text. Payload successfully written.")
        return True


def unlock_record(priref):
    '''
    Only used if write fails to unlock record
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': 'workflow', 'command': 'unlockrecord', 'priref': f'{priref}', 'output': 'json'}
        )
        print("unlock_record() response:")
        print(post_response.text)
        return True
    except Exception as err:
        LOGGER.warning("unlock_record: Unable to unlock record. Please check record and unlock manually %s:\n%s", priref, err)


def write_to_csv(data):
    '''
    Write all file data to CSV as confirmation
    of successful download.
    '''
    with open(PICK_CSV, 'a', newline='') as csvfile:
        datawriter = csv.writer(csvfile)
        datawriter.writerow(data)
        csvfile.close()


if __name__ == '__main__':
    main()
