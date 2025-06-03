#!/usr/bin/env python3

"""
Script which cleans up Black Pearl PUT jobs by retrieving
JSON notifications, matching job id to folder name in
black_pearl_ingest paths.

Iterates all autoingest paths looking for folders that
don't start with 'ingest_'. Extract folder name and look
for JSON file matching. Open matching JSON.

If JSON indicates that some files haven't successfully
written to tape, then those matching items need removing
from folder and placing back into black_pearl_ingest top
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

Joanna White / Stephen McConnachie
2022
"""

import csv
import glob
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime

import yaml
from ds3 import ds3

# Local import
CODE_PATH = os.environ["CODE"]
sys.path.append(CODE_PATH)
import adlib

# Global variables
BPINGEST = "autoingest/bp_reingest"
LOG_PATH = os.environ["LOG_PATH"]
JSON_PATH = os.path.join(LOG_PATH, "black_pearl")
CONTROL_JSON = os.path.join(LOG_PATH, "downtime_control.json")
CID_API = os.environ["CID_API3"]
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)
INGEST_CONFIG = os.path.join(CODE_PATH, "black_pearl/dpi_ingests.yaml")
MEDIA_REC_CSV = os.path.join(LOG_PATH, "duration_size_media_records.csv")
PERSISTENCE_LOG = os.path.join(LOG_PATH, "autoingest", "persistence_queue.csv")
GLOBAL_LOG = os.path.join(LOG_PATH, "autoingest", "global.log")
CLIENT = ds3.createClientFromEnv()
BUCKET = "imagen"

# Setup logging
logger = logging.getLogger("black_pearl_validate_reingest")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "black_pearl_validate_reingest.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)

LOG_PATHS = {
    "/mnt/qnap_08": os.environ["L_QNAP08"],
    "/mnt/qnap_10": os.environ["L_QNAP10"],
    "/mnt/qnap_h22/Public": os.environ["L_QNAP02"],
    "/mnt/grack_h22": os.environ["L_GRACK02"],
    "/mnt/qnap_06": os.environ["L_QNAP06"],
    "/mnt/qnap_film/Public": os.environ["L_QNAP03"],
    "/mnt/isilon/special_collections/Finished": os.environ["L_IS_SPEC"],
    "/mnt/isilon/film_operations/Finished": os.environ["L_IS_FILM"],
    "/mnt/isilon/video_operations/Finished": os.environ["L_IS_VID"],
    "/mnt/isilon/ingest/media": os.environ["L_IS_MED"],
    "/mnt/isilon/audio_operations/Finished": os.environ["L_IS_AUD"],
    "/mnt/isilon/digital_operations/Finished": os.environ["L_IS_DIGI"],
    "/mnt/qnap_07": os.environ["L_QNAP07"],
    "/mnt/qnap_video/Public/F47": os.environ["L_QNAP01"],
}


def check_control():
    """
    Check control json for downtime requests
    """
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j["black_pearl"]:
            logger.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")


def cid_check():
    """
    Tests if CID active before all other operations commence
    """
    try:
        CUR = adlib.Cursor(CID)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        logger.critical("Cannot establish CID session, exiting script")
        sys.exit()


def load_yaml(file):
    """
    Load yaml and return safe_load()
    """
    with open(file) as config_file:
        return yaml.safe_load(config_file)


def retrieve_json_data(foldername):
    """
    Look for matching JSON file
    """
    json_file = [x for x in os.listdir(JSON_PATH) if str(foldername) in str(x)]
    if json_file:
        return os.path.join(JSON_PATH, json_file[0])


def json_check(json_pth):
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


def get_file_size(filepath):
    """
    Retrieve size of path item in bytes
    """
    return os.path.getsize(filepath)


def make_object_num(fname):
    """
    Receive a filename remove ext,
    find part whole and return as object number
    """
    name = os.path.splitext(fname)[0]
    name_split = name.split("_")

    if len(name_split) == 3:
        return f"{name_split[0]}-{name_split[1]}"
    elif len(name_split) == 4:
        return f"{name_split[0]}-{name_split[1]}-{name_split[2]}"
    else:
        return None


def get_part_whole(fname):
    """
    Receive a filename extract part whole from end
    Return items split up
    """
    name = os.path.splitext(fname)[0]
    name_split = name.split("_")
    part_whole = name_split[-1]
    if "of" not in part_whole:
        return (None, None)

    part, whole = part_whole.split("of")
    if part[0] == "0":
        part = part[1]
    if whole[0] == "0":
        whole = whole[1]

    return (part, whole)


def get_ms(filepath):
    """
    Retrieve duration as milliseconds if possible
    """
    duration = ""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        filepath,
    ]

    try:
        duration = subprocess.check_output(cmd)
        duration = duration.decode("utf-8")
    except Exception as err:
        logger.info("Unable to extract duration: %s", err)
    if duration:
        return duration.rstrip("\n")
    else:
        return None


def get_duration(filepath):
    """
    Retrieve duration field if possible
    """
    duration = ""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        "-sexagesimal",
        filepath,
    ]
    try:
        duration = subprocess.check_output(cmd)
        duration = duration.decode("utf-8")
    except Exception as err:
        logger.info("Unable to extract duration: %s", err)
    if duration:
        return duration.rstrip("\n")
    else:
        return None


def get_md5(filename):
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
        with open(filepath) as text:
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


def check_for_media_record(fname):
    """
    Check if media record already exists
    In which case the file may be a duplicate
    """

    search = f"imagen.media.original_filename='{fname}'"
    print(f"****** SEARCH USED: {search}")

    query = {
        "database": "media",
        "search": search,
        "limit": "0",
        "output": "json",
        "fields": "access_rendition.mp4",
    }

    try:
        result = CID.get(query)
        print(f"************** RESULT FROM CID CHECK:\n{result.records}")
    except Exception as err:
        logger.exception("CID check for media record failed: %s", err)
        result = None
    try:
        priref = result.records[0]["priref"][0]
    except (KeyError, IndexError):
        priref = ""
    try:
        access_mp4 = result.records[0]["access_rendition.mp4"][0]
    except (KeyError, IndexError):
        access_mp4 = ""
    if access_mp4:
        return (priref, access_mp4)

    try:
        access_mp4 = result.records[0]["Access_rendition"][0]["access_rendition.mp4"][0]
    except (KeyError, IndexError):
        access_mp4 = ""
    return (priref, access_mp4)


def check_global_log(fname):
    """
    Read global log lines and look for a
    confirmation of deletion from autoingest
    """
    with open(GLOBAL_LOG, "r") as data:
        rows = csv.reader(data, delimiter="\t")
        for row in rows:
            if fname in str(row) and "Successfully deleted file" in str(row):
                print(row)
                return row


def check_global_log_again(fname):
    """
    Read global log lines and look for a
    confirmation of reingest of file
    """
    with open(GLOBAL_LOG, "r") as data:
        rows = csv.reader(data, delimiter="\t")
        for row in rows:
            if fname in str(row) and "Renewed ingest of file will be attempted" in str(
                row
            ):
                print(row)
                return row


def main():
    """
    Load dpi_ingest.yaml
    Iterate host paths looking in black_pearl_ingest/ for folders
    not starting with 'ingest_'. When found, check in json path for
    matching folder names to json filename
    """
    cid_check()
    ingest_data = load_yaml(INGEST_CONFIG)
    hosts = ingest_data["Host_size"]
    print(hosts)
    for host in hosts:
        logger.info(host)

        for pth in host.keys():
            autoingest = os.path.join(pth, BPINGEST)
            print(autoingest)
            if not os.path.exists(autoingest):
                print(f"**** Path does not exist here: {autoingest}")
                continue
            logger.info("======== START Black Pearl validate reingest START ========")
            logger.info("Looking for folders in Autoingest path: %s", autoingest)

            folders = [
                x
                for x in os.listdir(autoingest)
                if os.path.isdir(os.path.join(autoingest, x))
            ]
            if not folders:
                logger.info("No folders available to check.")
                continue

            for folder in folders:
                check_control()
                if folder.startswith(("ingest_", "error_")):
                    continue

                logger.info(
                    "Folder found that is not an ingest folder, or has failed or errored files within: %s",
                    folder,
                )
                json_file = json_file1 = json_file2 = success = ""

                failed_folder = None
                if folder.startswith("pending_"):
                    fpath = os.path.join(autoingest, folder)
                    logger.info(
                        "Failed folder found, will pass on for repeat processing. No JSON needed: %s",
                        folder,
                    )
                    failed_folder = folder.split("_")[-1]

                # Process double job_id
                elif len(folder) > 36 and len(folder) < 74:
                    folder1, folder2 = folder.split("_")
                    logger.info(
                        "Folder has two job ID's associated with this one PUT: %s  -  %s",
                        folder1,
                        folder2,
                    )

                    # Make double paths and check both present/not faults before processing
                    fpath = os.path.join(autoingest, folder)
                    json_file1 = retrieve_json_data(folder1)
                    json_file2 = retrieve_json_data(folder2)
                    if not json_file1 and not json_file2:
                        logger.info("Both JSON files are still absent")
                        continue
                    if not json_file1 and json_file2:
                        logger.info("One of the JSON files are still absent")
                        continue
                    if json_file1 and not json_file2:
                        logger.info("One of the JSON files are still absent")
                        continue

                    failed_files1 = json_check(json_file1)
                    if failed_files1:
                        logger.info(
                            "FAILED: Moving back into black_pearl_ingest folder:\n%s",
                            failed_files1,
                        )
                        for failure in failed_files1:
                            logger.info("Moving failed BP file")
                            shutil.move(
                                os.path.join(fpath, failure),
                                os.path.join(autoingest, failure),
                            )
                    else:
                        logger.info(
                            "No files failed transfer to BP data tape for %s", folder1
                        )

                    failed_files2 = json_check(json_file2)
                    if failed_files2:
                        logger.info(
                            "FAILED: Moving back into black_pearl_ingest folder:\n%s",
                            failed_files2,
                        )
                        for failure in failed_files2:
                            logger.info("Moving failed BP file")
                            shutil.move(
                                os.path.join(fpath, failure),
                                os.path.join(autoingest, failure),
                            )
                    else:
                        logger.info(
                            "No files failed transfer to BP data tape for %s", folder2
                        )

                    # Follow up check that Status agrees with received notification
                    status, size, cached = get_job_status(folder1)
                    if "COMPLETED" in status and size == cached:
                        logger.info(
                            "Completed check < %s >.  Sizes match < %s : %s >",
                            status,
                            size,
                            cached,
                        )
                        # Iterate through files in folders and extract data / write logs / create CID record
                    else:
                        logger.info(
                            "Completed check failed: %s. Size %s. Cached size %s",
                            status,
                            size,
                            cached,
                        )
                        # continue

                    status, size, cached = get_job_status(folder2)
                    if "COMPLETED" in status and size == cached:
                        logger.info(
                            "Completed check < %s >.  Sizes match < %s : %s >",
                            status,
                            size,
                            cached,
                        )
                    else:
                        logger.info(
                            "Completed check failed: %s. Size %s. Cached size %s",
                            status,
                            size,
                            cached,
                        )
                        continue

                    # Iterate through files in folders and extract data / write logs / create CID record
                    success = process_files(autoingest, folder, "")

                elif len(folder) > 74:
                    logger.info("Too many concatenated job IDs - skipping! %s", folder)
                    success = None
                    continue

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
                    # Check in JSON for failed BP job objects
                    failed_files = json_check(json_file)
                    if failed_files:
                        logger.info(
                            "FAILED: Moving back into black_pearl_ingest folder:\n%s",
                            failed_files,
                        )
                        for failure in failed_files:
                            logger.info("Moving failed BP file")
                            shutil.move(
                                os.path.join(fpath, failure),
                                os.path.join(autoingest, failure),
                            )
                    else:
                        logger.info("No files failed transfer to BP data tape")

                    # Follow up check that Status agrees with received notification
                    status, size, cached = get_job_status(folder)
                    if "COMPLETED" in status and size == cached:
                        logger.info(
                            "Completed check < %s >.  Sizes match < %s : %s >",
                            status,
                            size,
                            cached,
                        )
                    else:
                        logger.info(
                            "Completed absent or size mismatch:  %s. Size %s = %s",
                            status,
                            size,
                            cached,
                        )
                        # continue

                success = process_files(autoingest, folder, "")
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
                        except Exception:
                            logger.warning(
                                "Unable to rename folder %s to %s - please handle this manually.",
                                folder,
                                efolder,
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
                            "JSON file failed to move to completed folder: %s.",
                            json_file,
                        )
                if json_file1:
                    logger.info(
                        "Moving JSON files to completed folder: %s - %s.",
                        json_file1,
                        json_file2,
                    )
                    pth, jsn1 = os.path.split(json_file1)
                    jsn2 = os.path.split(json_file2)[1]
                    move_path1 = os.path.join(pth, "completed", jsn1)
                    move_path2 = os.path.join(pth, "completed", jsn2)
                    try:
                        shutil.move(json_file1, move_path1)
                        shutil.move(json_file2, move_path2)
                    except Exception:
                        logger.warning(
                            "JSON files failed to move to completed folder: %s - %s.",
                            json_file1,
                            json_file2,
                        )

    logger.info("======== END Black Pearl validate reingest END ========")


def process_files(autoingest, job_id, arg):
    """
    Receive ingest fpath and argument 'check' or empty argument.
    If empty arg then JSON has confirmed files ingested to tape
    If 'check' then BP query needs sending to confirm
    successful ingest to BP
    """
    for key, val in LOG_PATHS.items():
        if key in autoingest:
            wpath = val

    folderpath = os.path.join(autoingest, job_id)
    file_list = [
        x for x in os.listdir(folderpath) if os.path.isfile(os.path.join(folderpath, x))
    ]
    logger.info("%s files found in folderpath %s", len(file_list), folderpath)

    if arg == "check":
        # Get status
        status = get_job_status(job_id)
        if status != "COMPLETED":
            logger.info("%s - Job ID has not completed: %s", job_id, status)
            return "Not complete"

    check_list = []
    adjusted_list = file_list
    for file in file_list:
        file = file.strip()
        fpath = os.path.join(autoingest, job_id, file)
        logger.info("*** %s - processing file", fpath)
        byte_size = get_file_size(fpath)
        object_number = make_object_num(file)
        duration = get_duration(fpath)
        duration_ms = get_ms(fpath)
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
        duration_size_log(file, object_number, duration, byte_size, duration_ms)

        # Run series of BP checks here - any failures no CID media record made
        confirmed, remote_md5, length = get_object_list(file)
        if confirmed is None:
            logger.warning("Problem retrieving Black Pearl ObjectList. Skipping")
            continue
        logger.info(
            "Retrieved BP data: Confirmed %s BP MD5: %s Length: %s",
            confirmed,
            remote_md5,
            length,
        )
        if "No object list" in confirmed:
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

        if "False" in confirmed:
            logger.warning("Assigned to storage domain is FALSE: %s", fpath)
            persistence_log_message(
                "BlackPearl has not persisted file to data tape but ObjectList exists",
                fpath,
                wpath,
                file,
            )
            continue

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
        local_md5 = get_md5(file)
        if not local_md5:
            logger.warning("No Local MD5 found: %s", fpath)
            persistence_log_message(
                "MD5 checksum does not yet exist for this file. Skipping",
                fpath,
                wpath,
                file,
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

        # New section here to check for Media Record first and clean up file if found
        logger.info("Checking if Media record already exists for file: %s", file)
        media_priref, access_mp4 = check_for_media_record(file)
        root_path = os.path.split(autoingest)[0]

        if media_priref:
            logger.info("Media record %s exists for file: %s", media_priref, fpath)
            # Check md5_match, then check if MP4 found in media record
            if not md5_match:
                logger.warning(
                    "MD5 checksums do not match for reingest file %s. Leaving in place manual assistance needed.",
                    file,
                )
                continue
            logger.info("MD5 checksums match for reingested file: %s", file)
            persistence_log_message(
                "Persistence checks passed: delete file", fpath, wpath, file
            )
            # Configure correct path for file
            if str(access_mp4) in str(file):
                logger.info(
                    "MP4 transcode has been created and confirmation added to Media Record: %s",
                    access_mp4,
                )
                logger.info("Moving file straight to completed path.")
                move_path = os.path.join(root_path, "completed", file)
            else:
                logger.info(
                    "File is being reingested and Media record exists but no MP4 file found. Moving to transcode folder."
                )
                move_path = os.path.join(root_path, "transcode", file)
            # Move to transcode/completed folder for autoingest deletion - may not be duplicate
            try:
                shutil.move(fpath, move_path)
                check_list.append(file)
            except Exception:
                logger.warning(
                    "MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s",
                    fpath,
                    move_path,
                )
            continue

        # Create CID media record only if all BP checks pass and no CID Media record already exists
        if not md5_match:
            continue
        logger.info("No Media record found for file: %s", file)
        logger.info(
            "Creating media record and linking via object_number: %s", object_number
        )
        media_priref = create_media_record(object_number, duration, byte_size, file)
        print(media_priref)

        if media_priref:
            check_list.append(file)
            # Make path to transcode folder
            move_path = os.path.join(root_path, "transcode", file)

            # Move file to transcode folder
            try:
                shutil.move(fpath, move_path)
            except Exception:
                logger.warning(
                    "MOVE FAILURE: %s DID NOT MOVE TO TRANSCODE FOLDER: %s",
                    fpath,
                    move_path,
                )

            # Make global log message
            logger.info(
                "Writing persistence checking message to persistence_queue.csv."
            )
            persistence_log_message(
                "Persistence checks passed: delete file", fpath, wpath, file
            )
        else:
            logger.warning("File %s has no associated CID media record created.", file)
            logger.warning("File will be left in folder for manual intervention.")

    check_list.sort()
    adjusted_list.sort()
    if check_list == adjusted_list:
        return f"Job complete {job_id}"
    # For mismatched lists, some failed to create CID records return filenames
    set_diff = set(adjusted_list) - set(check_list)
    return list(set_diff)


def get_job_status(job_id):
    """
    Fetch job status for specific ID
    """
    size = cached = status = ""

    job_status = CLIENT.get_job_spectra_s3(ds3.GetJobSpectraS3Request(job_id))

    if job_status.result["CompletedSizeInBytes"]:
        size = job_status.result["CompletedSizeInBytes"]
    if job_status.result["CachedSizeInBytes"]:
        cached = job_status.result["CachedSizeInBytes"]
    if job_status.result["Status"]:
        status = job_status.result["Status"]
    return (status, size, cached)


def get_object_list(fname):
    """
    Get all details to check file persisted
    """
    confirmed, md5, length = "", "", ""
    request = ds3.GetObjectsWithFullDetailsSpectraS3Request(
        name=f"{fname}", include_physical_placement=True
    )
    try:
        result = CLIENT.get_objects_with_full_details_spectra_s3(request)
        data = result.result
    except Exception as err:
        return None

    if not data["ObjectList"]:
        return "No object list", None, None
    if "'TapeList': [{'AssignedToStorageDomain': 'true'" in str(data):
        confirmed = "True"
    elif "'TapeList': [{'AssignedToStorageDomain': 'false'" in str(data):
        confirmed = "False"
    try:
        md5 = data["ObjectList"][0]["ETag"]
    except (TypeError, IndexError):
        pass
    try:
        length = data["ObjectList"][0]["Blobs"]["ObjectList"][0]["Length"]
    except (TypeError, IndexError):
        pass

    return confirmed, md5, length


def persistence_log_message(message, path, wpath, file):
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


def duration_size_log(filename, ob_num, duration, size, ms):
    """
    Save outcome message to duration_size_media_records.csv
    """
    datestamp = str(datetime.now())[:-7]
    written = False

    with open(MEDIA_REC_CSV, "r") as doc:
        readme = csv.reader(doc)
        for row in readme:
            if filename in str(row):
                written = True

    if not written:
        with open(MEDIA_REC_CSV, "a") as doc:
            writer = csv.writer(doc)
            writer.writerow(
                [filename, ob_num, str(duration), str(size), datestamp, str(ms)]
            )


def create_media_record(ob_num, duration, byte_size, filename):
    """
    Media record creation for BP ingested file
    """
    record_data = []
    part, whole = get_part_whole(filename)

    record_data = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.now())[:10]},
        {"input.time": str(datetime.now())[11:19]},
        {"input.notes": "Digital preservation ingest - automated bulk documentation."},
        {"reference_number": filename},
        {"imagen.media.original_filename": filename},
        {"object.object_number": ob_num},
        {"imagen.media.part": part},
        {"imagen.media.total": whole},
    ]

    media_priref = ""

    try:
        i = CUR.create_record(
            database="media", data=record_data, output="json", write=True
        )
        if i.records:
            try:
                media_priref = i.records[0]["priref"][0]
                print(f"** CID media record created with Priref {media_priref}")
                logger.info("CID media record created with priref %s", media_priref)
            except Exception:
                logger.exception("CID media record failed to retrieve priref")
    except Exception:
        print(f"\nUnable to create CID media record for {ob_num}")
        logger.exception("Unable to create CID media record!")

    return media_priref


if __name__ == "__main__":
    main()
