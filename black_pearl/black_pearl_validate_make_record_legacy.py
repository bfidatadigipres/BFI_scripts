#!/usr/bin/env python3

"""
QNAP-05 legacy ingest script
Script to clean up Black Pearl PUT jobs by retrieving
JSON notifications, matching job id to folder name in
black_pearl_ingest paths.

Iterates QNAP-05/Public autoingest path looking for folders that
don't start with 'ingest_', 'error_' or 'blob'. Extract folder
name and look for JSON file matching. Open matching JSON.

If JSON indicates that some files haven't successfully
written to tape, then those matching items are removed
(using dictionary enclosed in JSON file 'ObjectsNotPersisted')
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
   - Find matching CID media record (using object number) and get priref
     Add bucket and imagen.media.original_filename field content
     Move finished filename to autoingest/transcode folder
2. Once completed above move JSON to Logs/black_pearl/completed folder.
   The empty job id folder is deleted if empty, if not prepended 'error_'

JMW NOTE:
PENDING QUESTION ABOUT CLEARING ALL MEDIA RECORDS
FOR THE CSV FILES. PAUSED DEVELOPMENT FOR TIME BEING.

2026
"""

import csv
import glob
import json
import logging
import os
import shutil
import sys
from datetime import datetime
from typing import Optional

import bp_utils as bp
import requests

CODE_PATH = os.environ["CODE"]
sys.path.append(CODE_PATH)
import adlib_v3_sess as adlib
import utils

# Global variables
BPINGEST = os.environ["BP_INGEST"]
LOG_PATH = os.environ["LOG_PATH"]
JSON_PATH = os.path.join(LOG_PATH, "black_pearl")
CID_API = utils.get_current_api()
INGEST_CONFIG = os.path.join(os.environ.get("CODE_DEPENDS"), "black_pearl/dpi_ingests.yaml")
MEDIA_REC_CSV = os.path.join(LOG_PATH, "duration_size_media_records.csv")
PERSISTENCE_LOG = os.path.join(LOG_PATH, "autoingest", "persistence_queue.csv")
CSV_PATH = "" # Path for catching failed digital media updates - may not be needed

# Setup logging
logger = logging.getLogger("black_pearl_validate_make_record_legacy")
HDLR = logging.FileHandler(
    os.path.join(LOG_PATH, "black_pearl_validate_make_record_legacy.log")
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)

LOG_PATHS = {
    os.environ["QNAP_05"]: os.environ["L_QNAP05"]
}


def retrieve_json_data(foldername: str) -> str:
    """
    Look for matching JSON file
    """
    json_file = [x for x in os.listdir(JSON_PATH) if str(foldername) in str(x)]
    if json_file:
        return os.path.join(JSON_PATH, json_file[0])


def json_check(json_pth: str) -> Optional[str]:
    """
    Open json and return value for ObjectsNotPersisted
    Has to be a neater way than this!
    """
    with open(json_pth) as file:
        dct = json.load(file)
        for k, v in dct.items():
            if k == "Notification":
                for ky, vl in v.items():
                    if ky == "Event":
                        for key, val in vl.items():
                            if key == "ObjectsNotPersisted":
                                return val


def get_md5(filename: str) -> Optional[str]:
    """
    Retrieve the local_md5 from checksum_md5 folder
    """
    file_match = [
        fn
        for fn in glob.glob(os.path.join(LOG_PATH, "checksum_md5/*"))
        if filename in str(fn)
    ]
    if not file_match:
        return None

    filepath = os.path.join(LOG_PATH, "checksum_md5", f"{filename}.md5")
    print(f"Found matching MD5: {filepath}")

    try:
        with open(filepath, "r") as text:
            contents = text.readline()
            split = contents.split(" - ")
            local_md5 = split[0]
            local_md5 = str(local_md5)
            text.close()
    except (IOError, IndexError, TypeError) as err:
        print(f"FILE NOT FOUND: {filepath}")
        print(err)

    if local_md5.startswith("None"):
        return None
    else:
        return local_md5


def check_for_media_record(obj: str, session: requests.Session) -> Optional[str, str]:
    """
    Check if media record already exists
    In which case the file may be a duplicate
    """
    priref = ref_num = ""
    search = (
        f"object.object_number='{obj}'"
    )

    try:
        result = adlib.retrieve_record(
            CID_API, "media", search, "0", session, ["priref", "reference_number"]
        )[1]
    except Exception as err:
        logger.exception("CID check for media record failed: %s", err)

    if result:
        try:
            priref = adlib.retrieve_field_name(result[0], "priref")[0]
        except (KeyError, IndexError):
            pass
        try:
            ref_num = adlib.retrieve_field_name(
                result[0], "reference_number"
            )[0]
        except (KeyError, IndexError):
            pass
    return priref, ref_num


def main():
    """
    Load dpi_ingest.yaml
    Iterate host paths looking in black_pearl_ingest/ for folders
    not starting with 'ingest_'. When found, check in json path for
    matching folder names to json filename
    """
    if not utils.check_control("black_pearl") or not utils.check_control(
        "pause_scripts"
    ):
        logger.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        logger.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")
    ingest_data = utils.read_yaml(INGEST_CONFIG)
    hosts = ingest_data["Host_size"]
    sess = adlib.create_session()
    autoingest_list = []
    for host in hosts:
        if not "/mnt/qnap_05/Public" in str(host):
            continue
        for pth in host.keys():
            autoingest_list.append(os.path.join(pth, BPINGEST))

    print(autoingest_list)
    for autoingest in autoingest_list:
        if not os.path.exists(autoingest):
            print(f"**** Path does not exist: {autoingest}")
            continue
        if not utils.check_storage(autoingest):
            logger.info(
                "Skipping path - storage_control.json returned ‘False’ for path %s",
                autoingest
            )
            continue

        bucket, bucket_list = bp.get_buckets("bfi")
        folders = [
            x
            for x in os.listdir(autoingest)
            if os.path.isdir(os.path.join(autoingest, x))
        ]
        if not folders:
            continue

        for folder in folders:
            if not utils.check_control("black_pearl"):
                logger.info(
                    "Script run prevented by downtime_control.json. Script exiting."
                )
                sys.exit(
                    "Script run prevented by downtime_control.json. Script exiting."
                )
            if folder.startswith(("ingest_", "error_", "blob", ".")):
                continue
            logger.info(
                "======== START Black Pearl validate/CID Media record START ========"
            )
            logger.info(
                "Folder found that is not an ingest folder, or has failed or errored files within: %s",
                folder,
            )
            json_file = success = ""

            failed_folder = None
            if folder.startswith("pending_"):
                fpath = os.path.join(autoingest, folder)
                logger.info(
                    "Failed folder found, will pass on for repeat processing. No JSON needed: %s",
                    folder,
                )
                failed_folder = folder.split("_")[-1]

            elif len(folder) == 73:
                logger.info("Concatenated job IDs! %s", folder)

                folders = folder.split("_")
                if not len(folders) == 2:
                    success = None
                    continue
                if len(folders[0]) != 36 or len(folders[1]) != 36:
                    success = None
                    continue

                # Iterate through JOB IDs
                for fld in folders:
                    fpath = os.path.join(autoingest, fld)
                    json_file = retrieve_json_data(fld)
                    if not json_file:
                        logger.info("No matching JSON found for folder.")
                        continue

                    logger.info("Matching JSON found for BP Job ID: %s", fld)
                    # Check in JSON for failed BP job object
                    failed_files = json_check(json_file)
                    if failed_files:
                        for ffile in failed_files:
                            for key, value in ffile.items():
                                if key == "Name":
                                    logger.info(
                                        "FAILED: Moving back into Black Pearl ingest folder:\n%s",
                                        value,
                                    )
                                    print(
                                        f"shutil.move({os.path.join(fpath, value)}, {os.path.join(autoingest, value)})"
                                    )
                                    try:
                                        shutil.move(
                                            os.path.join(fpath, value),
                                            os.path.join(autoingest, value),
                                        )
                                    except Exception as exc:
                                        print(exc)
                                        logger.warning(
                                            "Failed ingest file %s couldn't be moved out of path: %s",
                                            value,
                                            fpath,
                                        )
                                        pass
                    else:
                        logger.info("No files failed transfer to BP data tape")

            else:
                fpath = os.path.join(autoingest, folder)
                logger.info(
                    "Folder found that is not ingest or errored folder. Checking if JSON exists for %s.",
                    folder,
                )
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
                            if key == "Name":
                                logger.info(
                                    "FAILED: Moving back into Black Pearl ingest folder:\n%s",
                                    value,
                                )
                                print(
                                    f"shutil.move({os.path.join(fpath, value)}, {os.path.join(autoingest, value)})"
                                )
                                try:
                                    shutil.move(
                                        os.path.join(fpath, value),
                                        os.path.join(autoingest, value),
                                    )
                                except Exception as exc:
                                    print(exc)
                                    logger.warning(
                                        "Failed ingest file %s couldn't be moved out of path: %s",
                                        value,
                                        fpath,
                                    )
                                    pass
                else:
                    logger.info("No files failed transfer to BP data tape")

            success = process_files(autoingest, folder, bucket, bucket_list, sess)
            if not success:
                continue

            if "Job complete" in success:
                logger.info(
                    "All files in %s have completed processing successfully", folder
                )
                # Check job folder is empty, if so delete else leave and prepend 'error_'
                if len(os.listdir(fpath)) == 0:
                    logger.info(
                        "All files moved to completed. Deleting empty job folder: %s.",
                        folder,
                    )
                    os.rmdir(fpath)
                else:
                    logger.warning(
                        "Folder %s is not empty as expected. Adding 'error_{}' to folder and leaving.",
                        folder,
                    )
                    if folder.startswith("failed_"):
                        efolder = f"error_{failed_folder}"
                    else:
                        efolder = f"error_{folder}"
                    try:
                        os.rename(
                            os.path.join(autoingest, folder),
                            os.path.join(autoingest, efolder),
                        )
                    except OSError as err:
                        logger.warning(
                            "Unable to rename folder %s to %s - please handle this manually. %s",
                            folder,
                            efolder,
                            err,
                        )

            elif "Not complete" in success:
                logger.warning(
                    "BP tape confirmation not yet complete. Leaving until next pass: %s",
                    folder,
                )
                continue

            else:
                if len(success) > 0:
                    # Where CID records not made, files in this list left in job folder and folder renamed
                    logger.warning(
                        "List of files returned that didn't get CID media records: %s.",
                        success,
                    )
                    logger.warning(
                        "Leaving in job folder. Prepending folder with 'pending_{}."
                    )
                    if folder.startswith("pending_"):
                        ffolder = f"pending_{failed_folder}"
                    else:
                        ffolder = f"pending_{folder}"
                    try:
                        os.rename(
                            os.path.join(autoingest, folder),
                            os.path.join(autoingest, ffolder),
                        )
                    except Exception:
                        logger.warning(
                            "Unable to rename folder %s to %s - please handle this manually",
                            folder,
                            ffolder,
                        )

            # Moving JSON to completed folder
            if json_file:
                logger.info("Moving JSON file to completed folder: %s", json_file)
                pth, jsn = os.path.split(json_file)
                move_path = os.path.join(pth, "completed", jsn)
                try:
                    shutil.move(json_file, move_path)
                except Exception:
                    logger.warning(
                        "JSON file failed to move to completed folder: %s.", json_file
                    )

    logger.info("======== END Black Pearl validate/CID media record END ========")


def process_files(
    autoingest: str,
    job_id: str,
    bucket: str,
    bucket_list: list[str],
    session: requests.Session,
) -> str | list[str]:
    """
    Receive ingest fpath then JSON has confirmed files ingested to tape
    and this function handles CID media record check/creation and move
    """
    wpath = ""
    for key, val in LOG_PATHS.items():
        if key in autoingest:
            wpath = val

    folderpath = os.path.join(autoingest, job_id)
    file_list = [
        x for x in os.listdir(folderpath) if os.path.isfile(os.path.join(folderpath, x))
    ]
    logger.info("%s files found in folderpath %s", len(file_list), folderpath)
    logger.info(
        "Preservation bucket: %s Buckets in use for validation checks: %s",
        bucket,
        ", ".join(bucket_list),
    )

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
            duration = ""
        elif "N/A" in str(duration):
            duration = ""
        if not duration_ms:
            duration_ms = ""
        elif "N/A" in str(duration_ms):
            duration_ms = ""
        if not byte_size:
            byte_size = ""
        print(file, object_number, duration, byte_size, duration_ms)

        # Run series of BP checks here - any failures no CID media record made
        confirmed, remote_md5, length = bp.get_confirmation_length_md5(
            file, bucket, bucket_list
        )
        if confirmed is None:
            logger.warning("Problem retrieving Black Pearl TapeList. Skipping")
            continue
        elif confirmed is False:
            logger.warning("Assigned to storage domain is FALSE: %s", fpath)
            persistence_log_message(
                "BlackPearl has not persisted file to data tape but ObjectList exists",
                fpath,
                wpath,
                file,
            )
            continue
        elif confirmed is True:
            logger.info(
                "Retrieved BP data: Confirmed %s BP MD5: %s Length: %s",
                confirmed,
                remote_md5,
                length,
            )
        elif "No object list" in confirmed:
            logger.warning(
                "ObjectList could not be extracted from BP for file: %s", fpath
            )
            persistence_log_message(
                "No BlackPearl ObjectList returned from BlackPearl API query",
                fpath,
                wpath,
                file,
            )
            # Move file back to black_pearl_ingest folder
            try:
                logger.warning(
                    "Failed ingest: File %s ObjectList not found in BlackPearl, re-ingesting file.",
                    file,
                )
                reingest_path = os.path.join(autoingest, file)
                shutil.move(fpath, reingest_path)
                logger.info(
                    "** %s file moved back into black_pearl_ingest. Removed from file_list to allow completion of job processing."
                )
                persistence_log_message(
                    "Renewed ingest of file will be attempted. Moved file back to BlackPearl ingest folder.",
                    fpath,
                    wpath,
                    file,
                )
                adjusted_list.remove(file)
            except Exception as err:
                logger.warning(
                    "Unable to move failed ingest to black_pearl_ingest: %s\n%s",
                    fpath,
                    err,
                )
            continue

        local_md5 = get_md5(file)
        if not local_md5:
            logger.warning("No Local MD5 found: %s", fpath)
            continue
        if not length:
            logger.warning("Length could not be found for file: %s", file)
            continue
        # Make global log message [ THIS MESSAGE TO BE DEPRECATED, KEEPING FOR TIME BEING FOR CONSISTENCY ]
        logger.info("Writing persistence checking message to persistence_queue.csv.")
        persistence_log_message("Ready for persistence checking", fpath, wpath, file)

        if int(byte_size) != int(length):
            logger.warning(
                "FILES BYTE SIZE DO NOT MATCH: Local %s and Remote %s",
                byte_size,
                length,
            )
            persistence_log_message(
                "Filesize does not match BlackPearl object length", fpath, wpath, file
            )
            continue
        if remote_md5 != local_md5:
            logger.warning(
                "MD5 FILES DO NOT MATCH: Local MD5 %s and Remote MD5 %s",
                local_md5,
                remote_md5,
            )
            persistence_log_message(
                "Failed fixity check: checksums do not match", fpath, wpath, file
            )
            md5_match = False
        else:
            logger.info("MD5 MATCH: Local %s and BP ETag %s", local_md5, remote_md5)
            md5_match = True

        if not md5_match:
            continue

        # Prepare move path to not include Netflix/Amazon for transcoding
        root_path = os.path.split(autoingest)[0]
        move_path = os.path.join(root_path, "transcode", file)

        # JMW THIS WILL LIKELY NEED REVERTING TO FAILING IF CID DIGITAL MEDIA REC FOUND
        # New section here to check for Media Record first and clean up file if found
        logger.info("Checking if Media record already exists for file: %s", file)
        media_priref, ref_num = check_for_media_record(object_number, session)
        if not media_priref:
            logger.warninig(
                "SKIPPING: Media record could not be matched to file %s %s", object_number, fpath
            )
            continue

        # Create CID media record only if all BP checks pass and no CID Media record already exists
        logger.info("Media record found for file: %s - %s", file, media_priref)
        logger.info(
            "** Attempting update of media record for %s, %s, %s, %s",
            file,
            object_number,
            byte_size,
            bucket,
        )
        imagen_fname = update_media_record(
            media_priref, object_number, byte_size, file, bucket, session
        )

        if imagen_fname:
            logger.info("Media record %s updated: %s", media_priref)
            check_list.append(file)
        else:
            logger.warning("File %s was not updated to CID media record %s", file, media_priref)
            logger.warning("Manual update will be needed from CSV file: %s", CSV_PATH)
            
        # Move file to transcode folder
        try:
            shutil.move(fpath, move_path)
        except Exception as err:
            logger.warning(
                "MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s %s",
                fpath,
                move_path,
                err
            )

        # Make global log message
        logger.info(
            "Writing persistence checking message to persistence_queue.csv."
        )
        persistence_log_message(
            "Persistence checks passed: delete file", fpath, wpath, file
        )

    check_list.sort()
    adjusted_list.sort()
    if check_list == adjusted_list:
        return f"Job complete {job_id}"
    # For mismatched lists, some failed to create CID records return filenames
    set_diff = set(adjusted_list) - set(check_list)
    return list(set_diff)


def persistence_log_message(message: str, path: str, wpath: str, file: str) -> None:
    """
    Output confirmation to persistence_queue.csv
    """
    datestamp = str(datetime.now())[:19]

    with open(PERSISTENCE_LOG, "a") as of:
        writer = csv.writer(of)
        writer.writerow([path, message, datestamp])

    if file:
        with open(os.path.join(LOG_PATH, "persistence_confirmation.log"), "a") as of:
            of.write(f"{datestamp} INFO\t{path}\t{wpath}\t{file}\t{message}\n")


def failed_update_log(
    filename: str, ob_num: str, size: int, part: int, whole: int, bucket: str
) -> None:
    """
    Save failed data for update later
    """
    datestamp = str(datetime.now())[:-7]

    with open(CSV_PATH, "a") as doc:
        writer = csv.writer(doc)
        writer.writerow(
            [filename, ob_num, str(size), str(part), str(whole), bucket, datestamp]
        )


## JMW THIS WILL LIKELY NEED REVERTING TO CREATE MEDIA REC FUNCTION AGAIN
def update_media_record(
    priref: str,
    ob_num: Optional[str],
    byte_size: int,
    filename: Optional[str],
    bucket: Optional[str],
    session: requests.Session,
) -> Optional[str]:
    """
    Media record creation for BP ingested file
    """
    record_data = []
    part, whole = utils.check_part_whole(filename)
    logger.info("Part: %s Whole: %s", part, whole)
    if not part:
        return None
    record_data = [
        {"edit.name": "datadigipres"},
        {"edit.date": str(datetime.now())[:10]},
        {"edit.time": str(datetime.now())[11:19]},
        {"edit.notes": "Digital preservation ingest - automated bulk documentation."},
        {"imagen.media.original_filename": filename},
        {"container.file_size.total_bytes": int(byte_size)},
        {"imagen.media.part": part},
        {"imagen.media.total": whole},
        {"preservation_bucket": bucket},
    ]

    imagen_fname = ""
    logger.info(record_data)
    record_data_xml = adlib.create_record_data(
        CID_API, "media", session, priref, record_data
    )
    logger.info("Record data XML: %s", record_data_xml)
    try:
        media_rec = adlib.post(
            CID_API, record_data_xml, "media", "updaterecord", session
        )
        logger.info("Media record: %s", media_rec)
        if media_rec:
            try:
                imagen_fname = adlib.retrieve_field_name(media_rec, "imagen.media.original_filename")[0]
                print(f"** CID media record updated {imagen_fname}")
                logger.info("CID media record updated %s", imagen_fname)
            except Exception as err:
                logger.exception("CID media record failed to retrieve priref %s", err)
    except Exception as err:
        print(f"\nUnable to update CID media record for {ob_num}")
        logger.exception("Unable to create CID media record! %s", err)

    return imagen_fname


if __name__ == "__main__":
    main()
