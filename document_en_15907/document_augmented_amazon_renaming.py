#!/usr/bin/ spython3

"""
Script to receive MOV
Amazon files named with UID.
UID placed into Item record
digital.acquired_filename field
of CID item record.

Receives folder of MOVs complete with
video and audio desc also MOV wrapped.

1. Searches STORAGE for 'rename_amazon'
   folders and extracts CID item record
   number from enclosed folders.
2. Iterates folders, finds match for folder name
   with CID item record, ie 'N-123456'. Creates
   N_123456_filename prefix
3. Retrieves metadata for each MOV (or possibly
   other file wrapper) in folder
4. Uses CID item record filename prefix to name
   the AV MOV file with UHD HDR content (colour
   primaries denote HDR with BT.2020)
5. New CID item records are made for the remaining
   files (anticipated a UHD SDR colourspace BT.709,
   and mov with audio description only, no video stream)
6. These files are renamed with the CID item record
   object_number, likely all with 01of01.
7. Adds original filename and to existing and new
   CID item records in 'digital.acquired_filename'
   field. Formatting for this:
   "<Original Filename> - Renamed to: N_123456_01of06.mxf"
8. Renamed files are moved to autoingest new
   black_pearl_amazon_ingest path where new put
   scripts ensure file is moved to amazon01 bucket.

2024
"""

import datetime
import logging
# Public packages
import os
import shutil
import subprocess
import sys
from typing import Any, Final, Optional

# Local packages
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Global variables
STORAGE_PTH: Final = os.environ.get("PLATFORM_INGEST_PTH")
AMZ_PTH: Final = os.environ.get("AMAZON_PATH")
AMZ_INGEST: Final = os.environ.get("AMAZON_INGEST")
AUTOINGEST: Final = os.path.join(STORAGE_PTH, AMZ_INGEST)
STORAGE: Final = os.path.join(STORAGE_PTH, AMZ_PTH)
ADMIN: Final = os.environ.get("ADMIN")
LOGS: Final = os.path.join(ADMIN, "Logs")
CODE: Final = os.environ.get("CODE_PATH")
CONTROL_JSON: Final = os.path.join(LOGS, "downtime_control.json")
CID_API: Final = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("document_augmented_amazon_renaming")
HDLR = logging.FileHandler(os.path.join(LOGS, "document_augmented_amazon_renaming.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_check_fname(object_number: str) -> Optional[str]:
    """
    Looks up object_number and retrieves title
    and other data for new timed text record
    """
    search: str = f"object_number='{object_number}'"
    try:
        record = adlib.retrieve_record(CID_API, "items", search, "1")[1]
    except Exception as err:
        print(
            f"cid_check_fname(): Unable to match CID record to folder: {object_number} {err}"
        )
        record = None

    return record


def walk_folders() -> list[str]:
    """
    Collect list of folderpaths
    for files named rename_amazon
    """

    rename_folders: list[str] = []
    for root, dirs, _ in os.walk(STORAGE):
        for dir in dirs:
            if "rename_amazon" == dir:
                rename_folders.append(os.path.join(root, dir))
    print(f"{len(rename_folders)} rename folder(s) found")
    folder_list = []
    for rename_folder in rename_folders:
        print(rename_folder)
        folders: list[str] = os.listdir(rename_folder)
        if not folders:
            LOGGER.info(
                "Amazon file renaming script. Skipping as rename folder empty: %s",
                rename_folder,
            )
            continue
        for folder in folders:
            print(folder)
            fpath = os.path.join(rename_folder, folder)
            if os.path.isdir(fpath):
                folder_list.append(os.path.join(rename_folder, folder))
            else:
                LOGGER.warning(
                    "Amazon file renaming script. Non-folder item found in rename_amazon path: %s",
                    fpath,
                )

    return folder_list


def retrieve_metadata(fpath: str, mfile: str) -> str:
    """
    Retrieve metadata for each file
    """
    cmd: list[str] = [
        "mediainfo",
        "-f",
        "--Language=raw",
        "--Output=Video;%colour_primaries%",
        os.path.join(fpath, mfile),
    ]

    cmd2: list[str] = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=index:stream_tags=language",
        "-of",
        "compact=p=0:nk=1",
        os.path.join(fpath, mfile),
    ]

    colour_prim = subprocess.check_output(cmd).decode("utf-8")
    audio_spec = subprocess.check_output(cmd2).decode("utf-8")

    if "BT.2020" in str(colour_prim):
        return "HDR"
    if "BT.709" in str(colour_prim):
        return "SDR"
    if "eng" in str(audio_spec):
        return "Audio Description"
    return None


def main():
    """
    Check watch folder for folder containing
    MOV files. Look to match to folder name with
    CID item record.
    Where matched, identify HDR and process as
    digital asset for matched CID item record
    then create additional CID item records for
    remaining video/audio files (wrapped .mov)
    """
    if not utils.check_control("pause_scripts"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    folder_list: list[str] = walk_folders()
    if len(folder_list) == 0:
        LOGGER.info("Amazon file renaming script. No folders found.")
        sys.exit()

    LOGGER.info("== Document augmented Amazon renaming start =================")
    for fpath in folder_list:
        folder = os.path.split(fpath)[1].strip()
        LOGGER.info("Folder path found: <%s>", fpath)
        record = cid_check_fname(folder)
        if record is None:
            LOGGER.warning("Skipping: Record could not be matched with object_number")
            continue
        priref: str = adlib.retrieve_field_name(record[0], "priref")[0]
        ob_num: str = adlib.retrieve_field_name(record[0], "object_number")[0]

        LOGGER.info("Folder matched to CID Item record: %s | %s ", folder, priref)
        mov_list: list[str] = [
            x for x in os.listdir(fpath) if x.endswith((".mov", ".MOV"))
        ]
        print(mov_list)
        all_items: list[str] = [
            x for x in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, x))
        ]
        if len(mov_list) != len(all_items):
            LOGGER.warning("Folder contains files that are not MOV: %s", fpath)
            continue

        # Retrieving metadata for all MOV file
        for mov_file in mov_list:
            ext = mov_file.split(".")[1]
            item_xml = ""
            if ext.lower() != "mov":
                LOGGER.warning(
                    "Extension of file found that is not MOV. Skipping: %s", mov_file
                )
                continue
            metadata = retrieve_metadata(fpath, mov_file)
            if "HDR" in metadata:
                LOGGER.info("UHD HDR file found: %s", mov_file)
                new_filename = f"{ob_num.replace('-', '_')}_01of01.{ext}"
                new_fpath = os.path.join(fpath, new_filename)
                digital_note = f"{mov_file} - Renamed to: {new_filename}"

                success: bool = create_digital_original_filenames(
                    priref, folder.strip(), digital_note
                )
                if not success:
                    LOGGER.warning(
                        "Skipping further actions. Acquired filename not written to CID item record: %s",
                        priref,
                    )
                    break
                LOGGER.info(
                    "CID item record <%s> filenames appended to digital.acquired_filenamed field",
                    priref,
                )
                LOGGER.info("Renaming file %s to %s", mov_file, new_filename)
                os.rename(os.path.join(fpath, mov_file), new_fpath)
                if os.path.exists(new_fpath):
                    LOGGER.info(
                        "File renamed successfully. Moving to autoingest/ingest/amazon"
                    )
                    shutil.move(new_fpath, os.path.join(AUTOINGEST, new_filename))
                    continue
                else:
                    LOGGER.warning(
                        "Failed to rename file. Leaving in folder for manual intervention."
                    )
                    continue
            elif "SDR" in metadata:
                LOGGER.info("UHD SDR file found: %s", mov_file)
                # Build dictionary from CID item record
                item_data = make_item_record_dict(priref, mov_file, record, "SDR")
                if item_data is None:
                    LOGGER.info(
                        "Skipping: Creation of Item record dictionary failed for file %s",
                        mov_file,
                    )
                    continue
                item_xml = adlib.create_record_data(CID_API, "items", "", item_data)
                qcomm = "UHD SDR version"
            elif "Audio Description" in metadata:
                LOGGER.info("Audio Description file found: %s", mov_file)
                # Build dictionary from CID item record
                item_data = make_item_record_dict(
                    priref, mov_file, record, "Audio Description"
                )
                if item_data is None:
                    LOGGER.info(
                        "Skipping: Creation of Item record dictionary failed for file %s",
                        mov_file,
                    )
                    continue
                item_xml = adlib.create_record_data(CID_API, "items", "", item_data)
                qcomm = "Audio Description"
            else:
                LOGGER.warning(
                    "File found with metadata not recognised. Skipping this item."
                )
                continue

            # Make new item record
            new_record = adlib.post(CID_API, item_xml, "items", "insertrecord")
            if new_record is None:
                LOGGER.warning(
                    "Creation of new CID item record failed with XML: \n%s", item_xml
                )
                continue
            new_priref = adlib.retrieve_field_name(new_record, "priref")[0]
            new_ob_num = adlib.retrieve_field_name(new_record, "object_number")[0]
            LOGGER.info("** CID Item record created: %s - %s", new_priref, new_ob_num)

            new_filename = f"{new_ob_num.replace('-', '_')}_01of01.{ext}"
            new_fpath = os.path.join(fpath, new_filename)
            digital_note = f"{mov_file} - Renamed to: {new_filename}"
            success = create_digital_original_filenames(
                new_priref, folder.strip(), digital_note
            )
            if not success:
                LOGGER.warning(
                    "Skipping further actions. Asset item list not written to CID item record: %s",
                    new_priref,
                )
                continue
            LOGGER.info(
                "CID item record <%s> filenames appended to digital.acquired_filenamed field",
                new_priref,
            )
            LOGGER.info("Renaming file %s to %s", mov_file, new_filename)
            LOGGER.info(
                'Appending quality comments "%s" to CID item record: %s',
                qcomm,
                new_priref,
            )
            qc_success = adlib.add_quality_comments(CID_API, new_priref, qcomm)
            if not qc_success:
                LOGGER.warning(
                    "Quality Comment 'UHD SDR' write failed to CID item record: %s",
                    new_priref,
                )
            os.rename(os.path.join(fpath, mov_file), new_fpath)
            if os.path.exists(new_fpath):
                LOGGER.info(
                    "File renamed successfully. Moving to autoingest/ingest/amazon"
                )
                shutil.move(new_fpath, os.path.join(AUTOINGEST, new_filename))
                continue
            else:
                LOGGER.warning(
                    "Failed to rename file. Leaving in folder for manual intervention."
                )
                continue

        # Check folder is empty and delete
        contents = list(os.listdir(fpath))
        if len(contents) == 0:
            os.rmdir(fpath)
            LOGGER.info("Amazon folder empty, deleting %s", fpath)
        else:
            LOGGER.warning(
                "Amazon folder not empty, leaving in place for checks: %s", fpath
            )

    LOGGER.info("== Document augmented Amazon renaming end ===================\n")


def make_item_record_dict(
    priref: str, file: str, record: str, arg: str
) -> Optional[list[dict[str, str]]]:
    """
    Get CID item record for source and borrow data
    for creation of new CID item record
    """
    item = []
    record_default = defaults()
    item.extend(record_default)
    item.append({"record_type": "ITEM"})
    item.append({"item_type": "DIGITAL"})
    item.append({"copy_status": "M"})
    item.append({"copy_usage.lref": "131560"})
    item.append({"accession_date": str(datetime.datetime.now())[:10]})

    if "Title" in str(record):
        mov_title = adlib.retrieve_field_name(record[0], "title")[0]
        if "Audio Description" in arg:
            item.append({"title": f"{mov_title} ({arg})"})
        else:
            item.append({"title": mov_title})
        if "title.article" in str(record):
            item.append(
                {
                    "title.article": adlib.retrieve_field_name(
                        record[0], "title.article"
                    )[0]
                }
            )
        item.append({"title.language": "English"})
        item.append({"title.type": "05_MAIN"})
    else:
        LOGGER.warning("No title data retrieved. Aborting record creation")
        return None
    if "part_of_reference" in str(record):
        item.append(
            {
                "part_of_reference.lref": record[0]["Part_of"][0]["part_of_reference"][
                    0
                ]["priref"][0]["spans"][0]["text"]
            }
        )
    else:
        LOGGER.warning("No part_of_reference data retrieved. Aborting record creation")
        return None
    item.append({"related_object.reference.lref": priref})
    item.append({"related_object.notes": f"{arg} for"})
    if "SDR" in arg:
        item.append({"file_type.lref": "114307"})
        item.append({"language.lref": "74129"})
        item.append({"language.type": "DIALORIG"})
    elif "Audio Description" in arg:
        item.append({"file_type.lref": "114307"})
        item.append({"language.lref": "74129"})
        item.append({"language.type": "AUDDES"})
    if "acquisition.date" in str(record):
        item.append(
            {
                "acquisition.date": adlib.retrieve_field_name(
                    record[0], "acquisition.date"
                )[0]
            }
        )
    if "acquisition.method" in str(record):
        item.append(
            {
                "acquisition.method.lref": adlib.retrieve_field_name(
                    record[0], "acquisition.method.lref"
                )[0]
            }
        )
    if "Acquisition_source" in str(record):
        item.append(
            {
                "acquisition.source.lref": adlib.retrieve_field_name(
                    record[0], "acquisition.source.lref"
                )[0]
            }
        )
        item.append(
            {
                "acquisition.source.type": adlib.retrieve_field_name(
                    record[0], "acquisition.source.type"
                )[0]
            }
        )
    item.append(
        {
            "access_conditions": "Access requests for this collection are subject to an approval process. "
            "Please raise a request via the Collections Systems Service Desk, describing your specific use."
        }
    )
    item.append({"access_conditions.date": str(datetime.datetime.now())[:10]})
    if "grouping" in str(record):
        item.append({"grouping": adlib.retrieve_field_name(record[0], "grouping")[0]})
    if "language" in str(record):
        item.append({"language": adlib.retrieve_field_name(record[0], "language")[0]})
        item.append(
            {"language.type": adlib.retrieve_field_name(record[0], "language.type")[0]}
        )
    if len(file) > 1:
        item.append({"digital.acquired_filename": file})

    return item


def create_digital_original_filenames(
    priref: str, folder: str, digital_note: str
) -> bool:
    """
    Create entries for digital.acquired_filename
    and append to the CID item record.
    """
    payload = f"<adlibXML><recordList><record priref='{priref}'>"
    pay_mid = f"<Acquired_filename><digital.acquired_filename>{digital_note}</digital.acquired_filename><digital.acquired_filename.type>FILE</digital.acquired_filename.type></Acquired_filename>"
    pay_edit = f"<Edit><edit.name>datadigipres</edit.name><edit.date>{str(datetime.datetime.now())[:10]}</edit.date><edit.time>{str(datetime.datetime.now())[11:19]}</edit.time><edit.notes>Amazon automated digital acquired filename update</edit.notes></Edit>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload + pay_mid + pay_edit + payload_end

    LOGGER.info("** Appending data to work record now...")
    LOGGER.info(payload)

    try:
        result = adlib.post(CID_API, payload, "items", "updaterecord")
        print(f"Item appended successful! {priref}\n{result}")
        print(f"Item appended successful! {priref}")
        LOGGER.info(
            "Successfully appended MOV digital.acquired_filenames to Item record %s %s",
            priref,
            folder,
        )
        return True
    except Exception as err:
        print(err)
        LOGGER.warning(
            "Failed to append MOV digital.acquired_filenames to Item record %s %s",
            priref,
            folder,
        )
        print(f"CID item record append FAILED!! {priref}")
        return False


def defaults() -> list[dict[str, str]]:
    """
    Build defaults for new CID item records
    """
    record = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {"input.notes": "Amazon metadata integration - automated bulk documentation"},
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"record_access.user": "System Management"},
        {"record_access.rights": "3"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"record_access.user": "Information Specialist"},
        {"record_access.rights": "3"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"record_access.user": "Digital Operations"},
        {"record_access.rights": "2"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"record_access.user": "Documentation"},
        {"record_access.rights": "2"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"record_access.user": "Curator"},
        {"record_access.rights": "2"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"record_access.user": "Special Collections"},
        {"record_access.rights": "2"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"record_access.user": "Librarian"},
        {"record_access.rights": "2"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        # {'record_access.user': '$REST'},
        # {'record_access.rights': '1'},
        # {'record_access.reason': 'SENSITIVE_LEGAL'},
        {"grouping.lref": "401361"},
        {"record_type": "ITEM"},
        {"item_type": "DIGITAL"},
        {"copy_status": "M"},
        {"copy_usage.lref": "131560"},
        {"accession_date": str(datetime.datetime.now())[:10]},
    ]

    return record


if __name__ == "__main__":
    main()
