#!/usr/bin/env python3

'''
Digital pick script collects files needed for DigiOps processing
by looking in Workflow jobs for files that need to be retrieved
from DPI Black Pearl tape library.

Script actions:
1. Script works through Workflow Jobs querying 'description'
   field looking for trigger statement 'DPIDL'
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
      v. Launch BP download using preservation_bucket entry and receive BP job ID
      vi. Check new download path/file exists.
      vii. If yes, write particular file data to digital_pick.csv
   e. Create XML payload with new DPI download date message, prepended to
      contents of request_details field.
   f. Overwrite request.details data to Workflow record.
4. Exit script with final log message.

NOTES: Updated to work with adlib_v3

2022
'''

# Python packages
import os
import sys
import csv
import json
import hashlib
import logging
from xml.sax.saxutils import escape
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt

# Local package
import bp_utils as bp
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

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
HEADERS = {'Content-Type': 'text/xml'}
CID_API = os.environ['CID_API4']

# Set up logging
LOGGER = logging.getLogger('bp_get_digital_pick')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'black_pearl_digital_pick.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def fetch_workflow_jobs():
    '''
    Search for in target workflow jobs, compile into a
    dictionary and return to main()
    '''
    todayd, endd = get_date_range()
    search = f"request.details='*{SEARCH_TERM}*' and completion.date>'{todayd}' and completion.date<'{endd}' and status=InProgress sort completion.date ascending"
    hits, records = adlib.retrieve_record(CID_API, 'workflow', search, '0', ['priref', 'jobnumber', 'contact_person', 'request.details', 'request.from.name'])
    if hits is None:
        LOGGER.exception('"CID API was unreachable for Workflow search:\n%s', search)
        raise Exception(f"CID API was unreachable for Workflow search:\n{search}")
    if hits == 0:
        LOGGER.info("fetch_workflow_jobs: No matching InProgress jobs found.")
        return None
    if not records:
        LOGGER.exception("fetch_workflow_jobs: No workflow data found")
        return None

    workflow_jobs = {}
    for num in range(0, hits):
        try:
            priref = adlib.retrieve_field_name(records[num], 'priref')[0]
        except (IndexError, TypeError, KeyError):
            priref = ''
        try:
            jobnumber = adlib.retrieve_field_name(records[num], 'jobnumber')[0]
        except (IndexError, KeyError, TypeError):
            jobnumber = ''
        try:
            contact_person = adlib.retrieve_field_name(records[num], 'contact_person')[0]
        except (IndexError, KeyError, TypeError):
            contact_person = ''
        try:
            request_details = adlib.retrieve_field_name(records[num], 'request.details')[0]
        except (IndexError, KeyError, TypeError):
            request_details = ''
        try:
            request_from = adlib.retrieve_field_name(records[num], 'request.from.name')[0]
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
    records = adlib.retrieve_record(CID_API, 'workflow', search, '0')[1]
    if not records:
        LOGGER.exception("fetch_workflow_jobs: Unable to retrieve workflow data upto: %s", priref)
        return None
    print(records)

    try:
        children = adlib.retrieve_field_name(records[0], 'child')
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
    records = adlib.retrieve_record(CID_API, 'workflow', search, '0', ['description'])[1]
    if not records:
        LOGGER.exception("get_child_ob_num: Unable to retrieve workflow data")
        return None
    try:
        ob_num = adlib.retrieve_field_name(records[0], 'description')[0]
        print(ob_num)
        return ob_num
    except (IndexError, TypeError, KeyError):
        return None


@retry(stop=stop_after_attempt(10))
def get_media_original_filename(search):
    '''
    Retrieve the first returned media record
    for a match against object.object_number
    (may return many)
    '''
    records = adlib.retrieve_record(CID_API, 'media', search, '0', ['imagen.media.original_filename', 'reference_number', 'preservation_bucket'])[1]
    if not records:
        LOGGER.exception("get_media_original_filename: Unable to retrieve Media data")
        return None, None, None

    try:
        orig_fname = adlib.retrieve_field_name(records[0], 'imagen.media.original_filename')[0]
    except (IndexError, TypeError, KeyError):
        orig_fname = ''
    try:
        ref_num = adlib.retrieve_field_name(records[0], 'reference_number')[0]
    except (IndexError, TypeError, KeyError):
        ref_num = ''
    try:
        bucket = adlib.retrieve_field_name(records[0], 'preservation_bucket')[0]
    except (IndexError, TypeError, KeyError):
        bucket = ''

    if len(bucket) < 3:
        bucket = 'imagen'

    return orig_fname, ref_num, bucket


def bucket_check(bucket, filename):
    '''
    Check CID media record that bucket
    matches for all parts
    '''

    search = f'reference_number="{filename}"'
    records = adlib.retrieve_record(CID_API, 'media', search, '1', ['preservation_bucket'])[1]
    if not records:
        LOGGER.exception("bucket_check(): Unable to retrieve Media data")
        return None
    try:
        download_bucket = adlib.retrieve_field_name(records[0], 'preservation_bucket')[0]
    except (IndexError, TypeError, KeyError):
        download_bucket = ''

    if len(download_bucket) > 3:
        return download_bucket
    else:
        return bucket


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
            orig_name, ref_num, bucket = get_media_original_filename(f"imagen.media.original_filename={new_fname}")
            if not ref_num:
                print("Skipping No Digital Media record found for file {new_fname}")
                continue
            print(f"CID Digital media record found. Ingest name {orig_name} Reference number {ref_num}")
            fname_list.append(f"{new_fname}:{ref_num}:{bucket}")
    elif len(part) == 3:
        for count in range(2, int(whole) + 1):
            new_fname = f"{fname}_{str(count).zfill(3)}of{whole}{ext}"
            orig_name, ref_num, bucket = get_media_original_filename(f"imagen.media.original_filename={new_fname}")
            if not ref_num:
                print("Skipping No Digital Media record found for file {new_fname}")
                continue
            fname_list.append(f"{new_fname}:{ref_num}:{bucket}")
    else:
        print('Unanticpated part whole number length. Script update needed')

    return fname_list


def make_check_md5(fpath, fname, bucket):
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

    local_checksum = bp.get_bp_md5(fname, bucket)
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
    if not utils.check_control('black_pearl') or not utils.check_control('pause_scripts'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

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
        if children is None or len(children) == 0:
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
            filename, ref_num, bucket = get_media_original_filename(f"object.object_number='{child_ob_num}'")
            if not filename:
                LOGGER.info("Skipping. No matching Media record object number / imagen original filename: %s", child_ob_num)
                downloads.append('False')
                continue
            print(child_priref, child_ob_num, filename, ref_num)
            LOGGER.info("Looking at child object number %s - priref %s", child_ob_num, child_priref)

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
                download_job_id = bp.download_bp_object(download_fname, outpath, bucket)
                if os.path.exists(os.path.join(outpath, download_fname)):
                    # Write successful download to CSV
                    if umid:
                        os.rename(os.path.join(outpath, download_fname), os.path.join(outpath, filename))
                    download_checksum, bp_checksum = make_check_md5(os.path.join(outpath, filename), download_fname, bucket)
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
                    part_fname, part_umid, download_bucket = download_fname_part.split(':')
                    if part_fname in downloaded_fnames:
                        LOGGER.info("Skipping this item as already downloaded: %s", part_fname)
                        continue
                    if part_fname == part_umid:
                        dpart_fname = part_fname
                    else:
                        dpart_fname = part_umid
                    # check bucket for all parts, can't assume they match
                    d_job_id = bp.download_bp_object(dpart_fname, outpath, download_bucket)
                    if os.path.exists(os.path.join(outpath, dpart_fname)):
                        # Write successful download to CSV
                        if part_fname != dpart_fname:
                            os.rename(os.path.join(outpath, dpart_fname), os.path.join(outpath, part_fname))
                        # Make checksum test
                        download_checksum, bp_checksum = make_check_md5(os.path.join(outpath, part_fname), dpart_fname, download_bucket)
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
        record = adlib.post(CID_API, payload, 'workflow', 'updaterecord')
        if not record:
            LOGGER.warning("FAILED: Payload write to CID workflow request.details field: %s", priref)
        else:
            LOGGER.info("Workflow %s - DPI download complete and request.details field updated", priref)

    LOGGER.info("=========== Digital Pick script end =============\n")


def build_payload(priref, data, today):
    '''
    Build payload info to write to Workflow record
    '''
    cleaned_data = escape(data)
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_addition = f"<request.details>DPI Download completed {today}. {cleaned_data}</request.details>"
    payload_edit = f"<edit.name>{USERNAME}</edit.name><edit.date>{today[:10]}</edit.date><edit.time>{today[11:]}</edit.time>"
    payload_end = "</record></recordList></adlibXML>"
    return payload_head + payload_addition + payload_edit + payload_end


def write_payload(priref, payload):
    '''
    Recieve header, payload and priref and write
    to CID workflow record
    '''
    record = adlib.post(CID_API, payload, 'workflow', 'updaterecord')
    if not record:
        LOGGER.warning("write_payload: Error returned for requests.post for %s:\n%s", priref, payload)
        return False
    else:
        print("write_payload() response:")
        print(record)
        LOGGER.info("write_payload: No error returned in post_response.text. Payload successfully written.")
        return True


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
