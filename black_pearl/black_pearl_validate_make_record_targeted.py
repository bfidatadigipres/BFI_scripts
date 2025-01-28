#!/usr/bin/env python3

'''
Script to clean up Black Pearl PUT jobs by retrieving
JSON notifications, matching job id to folder name in
black_pearl_ingest paths.

Iterates all autoingest paths looking for folders that
don't start with 'ingest', 'error' or 'blob'. Extract folder
name and look for JSON file matching. Open matching JSON.

If JSON indicates that some files haven't successfully
written to tape, then those matching items are removed
(using dictionary enclodes in JSON file 'ObjectsNotPersisted')
from folder and placing back into Black Pearl ingest top
level for reattempt to ingest.

Where a JSON matches, and all items have written successfully:
1. Iterate through filenames in folder and complete these steps:
   - Write output to persistence_queue.csv
     'Ready for persistence checking'
   - Complete a series of BP validation checks including
     ObjectList present, 'AssignedToStorageDomain: true' check, Length match, MD5 checksum match
     Write output to persistence_queue.csv using terms that trigger autoingest deletion
     'Persistence checks passed: delete file'
   - Create CID media record and link to Item record
     If this fails, the script updates the folder with 'record_failed_' but continues with the rest
     duration 'HH:MM:SS' of media asset -> unknown field
     byte size of media asset -> unknown field
     Move finished filename to autoingest/transcode folder
2. Once completed above move JSON to Logs/black_pearl/completed folder.
   The empty job id folder is deleted if empty, if not prepended 'error_'

NOTE: Restriction in main() temporarily in place to allow second version of script
      to target specific (slow) paths, allowing the rest to move quickly. Eventually
      this will be set to QNAP-04 STORA full time.

2022
'''

import os
import sys
import csv
import glob
import json
import shutil
import logging
from datetime import datetime

# Local import
import bp_utils as bp
CODE_PATH = os.environ['CODE']
sys.path.append(CODE_PATH)
import adlib_v3_sess as adlib
import utils

# Global variables
BPINGEST = os.environ['BP_INGEST']
BPINGEST_NETFLIX = os.environ['BP_INGEST_NETFLIX']
BPINGEST_AMAZON = os.environ['BP_INGEST_AMAZON']
LOG_PATH = os.environ['LOG_PATH']
JSON_PATH = os.path.join(LOG_PATH, 'black_pearl')
CID_API = os.environ['CID_API4']
INGEST_CONFIG = os.path.join(CODE_PATH, 'black_pearl/dpi_ingests.yaml')
MEDIA_REC_CSV = os.path.join(LOG_PATH, 'duration_size_media_records.csv')
PERSISTENCE_LOG = os.path.join(LOG_PATH, 'autoingest', 'persistence_queue.csv')

# Setup logging
logger = logging.getLogger('black_pearl_validate_make_record_targeted')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'black_pearl_validate_make_record_targeted.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)

LOG_PATHS = {os.environ['QNAP_VID']: os.environ['L_QNAP01'],
             os.environ['QNAP_08']: os.environ['L_QNAP08'],
             os.environ['QNAP_10']: os.environ['L_QNAP10'],
             os.environ['QNAP_H22']: os.environ['L_QNAP02'],
             os.environ['GRACK_H22']: os.environ['L_GRACK02'],
             os.environ['QNAP_06']: os.environ['L_QNAP06'],
             os.environ['QNAP_IMAGEN']: os.environ['L_QNAP04'],
             os.environ['QNAP_FILM']: os.environ['L_QNAP03'],
             os.environ['IS_SC']: os.environ['L_IS_SPEC'],
             os.environ['IS_FILM']: os.environ['L_IS_FILM'],
             os.environ['IS_VID']: os.environ['L_IS_VID'],
             os.environ['IS_ING']: os.environ['L_IS_MED'],
             os.environ['IS_AUD']: os.environ['L_IS_AUD'],
             os.environ['IS_DIG']: os.environ['L_IS_DIGI'],
             os.environ['GRACK_F47']: os.environ['L_IS_VID'],
             os.environ['GRACK_FILM']: os.environ['L_GRACK01'],
             os.environ['QNAP_07']: os.environ['L_QNAP07'],
             os.environ['QNAP_09']: os.environ['L_QNAP09'],
             os.environ['QNAP_11']: os.environ['L_QNAP11'],
             os.environ['QNAP_TEMP']: os.environ['L_QNAP_TEMP'],
             os.environ['EDITSHARE']: os.environ['L_EDITSHARE'],
             os.environ['BP_VIDEO']: os.environ['L_BP_VIDEO'],
             os.environ['BP_AUDIO']: os.environ['L_BP_AUDIO'],
             os.environ['BP_SC']: os.environ['L_BP_SC'],
             os.environ['BP_DIGITAL']: os.environ['L_BP_DIGITAL'],
             os.environ['BP_FILM1']: os.environ['L_BP_FILM1'],
             os.environ['BP_FILM2']: os.environ['L_BP_FILM2'],
             os.environ['BP_FILM3']: os.environ['L_BP_FILm3'],
             os.environ['BP_FILM4']: os.environ['L_BP_FILM4'],
             os.environ['BP_FILM5']: os.environ['L_BP_FILM5'],
             os.environ['BP_FILM6']: os.environ['L_BP_FILM6']
}


def retrieve_json_data(foldername):
    '''
    Look for matching JSON file
    '''
    json_file = [x for x in os.listdir(JSON_PATH) if str(foldername) in str(x)]
    if json_file:
        return os.path.join(JSON_PATH, json_file[0])


def json_check(json_pth):
    '''
    Open json and return value for ObjectsNotPersisted
    Has to be a neater way than this!
    '''
    with open(json_pth) as file:
        dct = json.load(file)
        for k, v in dct.items():
            if k == 'Notification':
                for ky, vl in v.items():
                    if ky == 'Event':
                        for key, val in vl.items():
                            if key == 'ObjectsNotPersisted':
                                return val


def get_md5(filename):
    '''
    Retrieve the local_md5 from checksum_md5 folder
    '''
    file_match = [fn for fn in glob.glob(os.path.join(LOG_PATH, 'checksum_md5/*')) if filename in str(fn)]
    if not file_match:
        return None

    filepath = os.path.join(LOG_PATH, 'checksum_md5', f'{filename}.md5')
    print(f"Found matching MD5: {filepath}")

    try:
        with open(filepath) as text:
            contents = text.readline()
            split = contents.split(" - ")
            local_md5 = split[0]
            local_md5 = str(local_md5)
            text.close()
    except (IOError, IndexError, TypeError) as err:
        print(f"FILE NOT FOUND: {filepath}")
        print(err)

    if local_md5.startswith('None'):
        return None
    else:
        return local_md5


def check_for_media_record(fname, session):
    '''
    Check if media record already exists
    In which case the file may be a duplicate
    '''
    priref = access_mp4 = ''
    search = f"imagen.media.original_filename='{fname}'"

    try:
        result = adlib.retrieve_record(CID_API, 'media', search, '0', session)[1]
    except Exception as err:
        logger.exception('CID check for media record failed: %s', err)

    if result:
        try:
            priref = adlib.retrieve_field_name(result[0], 'priref')[0]
        except (KeyError, IndexError):
            pass
        try:
            access_mp4 = adlib.retrieve_field_name(result[0], 'access_rendition.mp4')[0]
        except (KeyError, IndexError):
            pass

    return priref, access_mp4


def main():
    '''
    Load dpi_ingest.yaml
    Iterate host paths looking in black_pearl_ingest/ for folders
    not starting with 'ingest_'. When found, check in json path for
    matching folder names to json filename
    '''
    if not utils.check_control('black_pearl') or not utils.check_control('pause_scripts'):
        logger.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    if not utils.cid_check(CID_API):
        logger.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")
    ingest_data = utils.read_yaml(INGEST_CONFIG)
    hosts = ingest_data['Host_size']
    sess = adlib.create_session()

    autoingest_list = []
    for host in hosts:
        # This path has own script
        if not '/mnt/qnap_04' in str(host):
            continue
        # Build autoingest list for separate iteration
        for pth in host.keys():
            autoingest_list.append(os.path.join(pth, BPINGEST))

    print(autoingest_list)
    for autoingest in autoingest_list:
        if not os.path.exists(autoingest):
            print(f"**** Path does not exist: {autoingest}")
            continue

        if 'black_pearl_netflix_ingest' in autoingest:
            bucket, bucket_list = bp.get_buckets('netflix')
        elif 'black_pearl_amazon_ingest' in autoingest:
            bucket, bucket_list = bp.get_buckets('amazon')
        else:
            bucket, bucket_list = bp.get_buckets('bfi')

        folders = [x for x in os.listdir(autoingest) if os.path.isdir(os.path.join(autoingest, x))]
        if not folders:
            continue

        for folder in folders:
            if not utils.check_control('black_pearl'):
                logger.info('Script run prevented by downtime_control.json. Script exiting.')
                sys.exit('Script run prevented by downtime_control.json. Script exiting.')
            if folder.startswith(('ingest_', 'error_', 'blob', '.')):
                continue
            logger.info("======== START Black Pearl validate/CID Media record START ========")
            logger.info("Folder found that is not an ingest folder, or has failed or errored files within: %s", folder)
            json_file = success = ''

            failed_folder = None
            if folder.startswith('pending_'):
                fpath = os.path.join(autoingest, folder)
                logger.info("Failed folder found, will pass on for repeat processing. No JSON needed: %s", folder)
                failed_folder = folder.split("_")[-1]

            elif len(folder) > 36:
                logger.info("Too many concatenated job IDs - skipping! %s", folder)
                success = None
                continue

            else:
                fpath = os.path.join(autoingest, folder)
                logger.info("Folder found that is not ingest or errored folder. Checking if JSON exists for %s.", folder)
                json_file = retrieve_json_data(folder)

                if not json_file:
                    logger.info("No matching JSON found for folder.")
                    continue

                logger.info("Matching JSON found for BP Job ID: %s", folder)
                # Check in JSON for failed BP job object
                failed_files = json_check(json_file)
                if failed_files:
                    for ffile in failed_files:
                        for key, value in ffile.items():
                            if key == 'Name':
                                logger.info("FAILED: Moving back into Black Pearl ingest folder:\n%s", value)
                                print(f"shutil.move({os.path.join(fpath, value)}, {os.path.join(autoingest, value)})")
                                try:
                                    shutil.move(os.path.join(fpath, value), os.path.join(autoingest, value))
                                except Exception as exc:
                                    print(exc)
                                    logger.warning("Failed ingest file %s couldn't be moved out of path: %s", value, fpath)
                else:
                    logger.info("No files failed transfer to BP data tape")

            success = process_files(autoingest, folder, bucket, bucket_list, sess)
            if not success:
                continue

            if 'Job complete' in success:
                logger.info("All files in %s have completed processing successfully", folder)
                # Check job folder is empty, if so delete else leave and prepend 'error_'
                if len(os.listdir(fpath)) == 0:
                    logger.info("All files moved to completed. Deleting empty job folder: %s.", folder)
                    os.rmdir(fpath)
                else:
                    logger.warning("Folder %s is not empty as expected. Adding 'error_{}' to folder and leaving.", folder)
                    if folder.startswith('failed_'):
                        efolder = f"error_{failed_folder}"
                    else:
                        efolder = f"error_{folder}"
                    try:
                        os.rename(os.path.join(autoingest, folder), os.path.join(autoingest, efolder))
                    except Exception:
                        logger.warning("Unable to rename folder %s to %s - please handle this manually.", folder, efolder)

            elif 'Not complete' in success:
                logger.warning("BP tape confirmation not yet complete. Leaving until next pass: %s", folder)
                continue

            else:
                if len(success) > 0:
                    # Where CID records not made, files in this list left in job folder and folder renamed
                    logger.warning("List of files returned that didn't get CID media records: %s.", success)
                    logger.warning("Leaving in job folder. Prepending folder with 'pending_{}.")
                    if folder.startswith('pending_'):
                        ffolder = f"pending_{failed_folder}"
                    else:
                        ffolder = f"pending_{folder}"
                    try:
                        os.rename(os.path.join(autoingest, folder), os.path.join(autoingest, ffolder))
                    except Exception:
                        logger.warning("Unable to rename folder %s to %s - please handle this manually", folder, ffolder)

            # Moving JSON to completed folder
            if json_file:
                logger.info("Moving JSON file to completed folder: %s", json_file)
                pth, jsn = os.path.split(json_file)
                move_path = os.path.join(pth, 'completed', jsn)
                try:
                    shutil.move(json_file, move_path)
                except Exception:
                    logger.warning("JSON file failed to move to completed folder: %s.", json_file)

    logger.info("======== END Black Pearl validate/CID media record END ========")


def process_files(autoingest, job_id, bucket, bucket_list, session):
    '''
    Receive ingest fpath then JSON has confirmed files ingested to tape
    and this function handles CID media record check/creation and move
    '''
    for key, val in LOG_PATHS.items():
        if key in autoingest:
            wpath = val

    folderpath = os.path.join(autoingest, job_id)
    file_list = [x for x in os.listdir(folderpath) if os.path.isfile(os.path.join(folderpath, x))]
    logger.info("%s files found in folderpath %s", len(file_list), folderpath)
    logger.info("Preservation bucket: %s Buckets in use for validation checks: %s", bucket, ', '.join(bucket_list))

    check_list = []
    adjusted_list = file_list
    for file in file_list:
        file = file.strip()
        fpath = os.path.join(autoingest, job_id, file)
        logger.info("*** %s - processing file", fpath)
        byte_size = utils.get_size(fpath)
        object_number = utils.get_object_number(file)
        duration = utils.get_duration(fpath)
        duration_ms = utils.get_ms(fpath)
        if duration or duration_ms:
            logger.info("Duration: %s MS: %s", duration, duration_ms)

        # Handle string returns - back up to CSV
        if not duration:
            duration = ''
        elif 'N/A' in str(duration):
            duration = ''
        if not duration_ms:
            duration_ms = ''
        elif "N/A" in str(duration_ms):
            duration_ms = ''
        if not byte_size:
            byte_size = ''
        duration_size_log(file, object_number, duration, byte_size, duration_ms)

        # Run series of BP checks here - any failures no CID media record made
        confirmed, remote_md5, length = bp.get_confirmation_length_md5(file, bucket, bucket_list)
        if confirmed is None:
            logger.warning('Problem retrieving Black Pearl TapeList. Skipping')
            continue
        elif confirmed is False:
            logger.warning("Assigned to storage domain is FALSE: %s", fpath)
            persistence_log_message("BlackPearl has not persisted file to data tape but ObjectList exists", fpath, wpath, file)
            continue
        elif confirmed is True:
            logger.info("Retrieved BP data: Confirmed %s BP MD5: %s Length: %s", confirmed, remote_md5, length)
        elif 'No object list' in confirmed or 'No tape list' in confirmed:
            logger.warning("ObjectList could not be extracted from BP for file: %s", fpath)
            persistence_log_message("No BlackPearl ObjectList returned from BlackPearl API query", fpath, wpath, file)
            # Move file back to black_pearl_ingest folder
            try:
                logger.warning("Failed ingest: File %s ObjectList not found in BlackPearl, re-ingesting file.", file)
                reingest_path = os.path.join(autoingest, file)
                shutil.move(fpath, reingest_path)
                logger.info("** %s file moved back into black_pearl_ingest. Removed from file_list to allow completion of job processing.")
                persistence_log_message("Renewed ingest of file will be attempted. Moved file back to BlackPearl ingest folder.", fpath, wpath, file)
                adjusted_list.remove(file)
            except Exception as err:
                logger.warning("Unable to move failed ingest to black_pearl_ingest: %s\n%s", fpath, err)
            continue

        local_md5 = get_md5(file)
        if not local_md5:
            logger.warning("No Local MD5 found: %s", fpath)
            continue
        # Make global log message [ THIS MESSAGE TO BE DEPRECATED, KEEPING FOR TIME BEING FOR CONSISTENCY ]
        logger.info("Writing persistence checking message to persistence_queue.csv.")
        persistence_log_message("Ready for persistence checking", fpath, wpath, file)

        if int(byte_size) != int(length):
            logger.warning("FILES BYTE SIZE DO NOT MATCH: Local %s and Remote %s", byte_size, length)
            persistence_log_message("Filesize does not match BlackPearl object length", fpath, wpath, file)
            continue
        if remote_md5 != local_md5:
            logger.warning("MD5 FILES DO NOT MATCH: Local MD5 %s and Remote MD5 %s", local_md5, remote_md5)
            persistence_log_message("Failed fixity check: checksums do not match", fpath, wpath, file)
            md5_match = False
        else:
            logger.info("MD5 MATCH: Local %s and BP ETag %s", local_md5, remote_md5)
            md5_match = True

        # Prepare move path to not include Netflix/Amazon for transcoding
        root_path = os.path.split(autoingest)[0]
        if 'black_pearl_netflix_ingest' in autoingest and not file.endswith(('.mov', '.MOV')):
            move_path = os.path.join(root_path, 'completed', file)
        elif 'black_pearl_amazon_ingest' in autoingest:
            move_path = os.path.join(root_path, 'completed', file)
        else:
            move_path = os.path.join(root_path, 'transcode', file)

        # New section here to check for Media Record first and clean up file if found
        logger.info("Checking if Media record already exists for file: %s", file)
        media_priref, access_mp4 = check_for_media_record(file, session)
        if media_priref:
            logger.info("Media record %s already exists for file: %s", media_priref, fpath)
            # Check for previous 'deleted' message in global.log
            deletion_confirm = utils.check_global_log(file, 'Successfully deleted file')
            reingest_confirm = utils.check_global_log(file, 'Renewed ingest of file will be attempted')
            if deletion_confirm:
                logger.info("DELETING DUPLICATE: File has Media record, and deletion confirmation in global.log \n%s", deletion_confirm)
                try:
                    os.remove(fpath)
                    logger.info("Deleted file: %s", fpath)
                    check_list.append(file)
                except Exception as err:
                    logger.warning("Unable to delete asset: %s %s", fpath, err)
                    logger.warning("Manual inspection of asset required")
            elif reingest_confirm and md5_match:
                logger.info("File is being reingested following failed attempt. MD5 checks have passed. Moving to transcode folder and updating global.log for deletion.")
                persistence_log_message("Persistence checks passed: delete file", fpath, wpath, file)
                # Move to next folder for autoingest deletion - may not be duplicate
                try:
                    shutil.move(fpath, move_path)
                    check_list.append(file)
                except Exception:
                    logger.warning("MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s", fpath, move_path)
            elif md5_match and not access_mp4:
                persistence_log_message("Persistence checks passed: delete file", fpath, wpath, file)
                logger.info("File has media record but has no Access MP4. Moving to transcode folder and updating global.log for deletion.")
                # Move to next folder for autoingest deletion - may not be duplicate
                try:
                    shutil.move(fpath, move_path)
                    check_list.append(file)
                except Exception:
                    logger.warning("MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s", fpath, move_path)
            else:
                logger.warning("Problem with file %s: Has media record but no deletion message in global.log", fpath)
            continue

        # Create CID media record only if all BP checks pass and no CID Media record already exists
        if not md5_match:
            continue
        logger.info("No Media record found for file: %s", file)
        logger.info("Creating media record and linking via object_number: %s", object_number)
        media_priref = create_media_record(object_number, duration, byte_size, file, bucket, session)
        print(media_priref)

        if media_priref:
            check_list.append(file)
            # Move file to transcode folder
            try:
                shutil.move(fpath, move_path)
            except Exception:
                logger.warning("MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s", fpath, move_path)

            # Make global log message
            logger.info("Writing persistence checking message to persistence_queue.csv.")
            persistence_log_message("Persistence checks passed: delete file", fpath, wpath, file)
        else:
            logger.warning("File %s has no associated CID media record created.", file)
            logger.warning("File will be left in folder for manual intervention.")

    check_list.sort()
    adjusted_list.sort()
    if check_list == adjusted_list:
        return f'Job complete {job_id}'
    # For mismatched lists, some failed to create CID records return filenames
    set_diff = set(adjusted_list) - set(check_list)
    return list(set_diff)


def persistence_log_message(message, path, wpath, file):
    '''
    Output confirmation to persistence_queue.csv
    '''
    datestamp = str(datetime.now())[:19]

    with open(PERSISTENCE_LOG, 'a') as of:
        writer = csv.writer(of)
        writer.writerow([path, message, datestamp])

    if file:
        with open(os.path.join(LOG_PATH, 'persistence_confirmation.log'), 'a') as of:
            of.write(f"{datestamp} INFO\t{path}\t{wpath}\t{file}\t{message}\n")


def duration_size_log(filename, ob_num, duration, size, ms):
    '''
    Save outcome message to duration_size_media_records.csv
    '''
    datestamp = str(datetime.now())[:-7]
    written = False

    with open(MEDIA_REC_CSV, 'r') as doc:
        readme = csv.reader(doc)
        for row in readme:
            if filename in str(row):
                written = True

    if not written:
        with open(MEDIA_REC_CSV, 'a') as doc:
            writer = csv.writer(doc)
            writer.writerow([filename, ob_num, str(duration), str(size), datestamp, str(ms)])


def create_media_record(ob_num, duration, byte_size, filename, bucket, session):
    '''
    Media record creation for BP ingested file
    '''
    record_data = []
    part, whole = utils.check_part_whole(filename)
    if not part:
        return None
    record_data = ([{'input.name': 'datadigipres'},
                    {'input.date': str(datetime.now())[:10]},
                    {'input.time': str(datetime.now())[11:19]},
                    {'input.notes': 'Digital preservation ingest - automated bulk documentation.'},
                    {'reference_number': filename},
                    {'imagen.media.original_filename': filename},
                    {'container.file_size.total_bytes': int(byte_size)},
                    {'object.object_number': ob_num},
                    {'imagen.media.part': part},
                    {'imagen.media.total': whole},
                    {'preservation_bucket': bucket}])
    print(record_data)
    record_data_xml = adlib.create_record_data(CID_API, 'media', session, '', record_data)
    logger.info(record_data_xml)

    try:
        item_rec = adlib.post(CID_API, record_data_xml, 'media', 'insertrecord', session)
        if item_rec:
            try:
                media_priref = adlib.retrieve_field_name(item_rec, 'priref')[0]
                print(f'** CID media record created with Priref {media_priref}')
                logger.info('CID media record created with priref %s', media_priref)
            except Exception:
                logger.exception("CID media record failed to retrieve priref")
                return None
    except Exception:
        print(f"\nUnable to create CID media record for {ob_num}")
        logger.exception("Unable to create CID media record!")
        return None

    return media_priref


if __name__ == "__main__":
    main()
