#!/usr/bin/env python3

"""
THIS SCRIPT DEPENDS ON PYTHON ENV PATH

Create CID record hierarchies for Work-Manifestation-Item
using STORA created csv metadata source and traversing filesystem paths to files
    1. Create work-manifestation-item for each csv in the path
    2. Add the WebVTT subtitles to the Item record using requests (to avoid escape
       characters being introduced in Python3 adlib.py [Deprecated feature]
    3. Rename the MPEG transport stream file with the Item object number, into autoingest
    4. Rename the subtitles.vtt file with the Item object number, move to Isilon folder
    5. Identify the folder as completed by renaming the csv with .documented suffix

Refactored 2023
"""

# Public packages
import os
import sys
import csv
import shutil
import logging
import datetime
import requests
from time import sleep
from typing import Optional, Final, Any

# Private packages
sys.path.append(os.environ["CODE"])
import adlib_v3_sess as adlib
import utils

# Global variables
STORAGE = os.environ["STORA_PATH"]
AUTOINGEST_PATH = os.environ["STORA_AUTOINGEST"]
CODE_PATH = os.environ["CODE_DDP"]
LOG_PATH = os.environ["LOG_PATH"]
CONTROL_JSON = os.path.join(LOG_PATH, "downtime_control.json")
SUBS_PTH = os.environ["SUBS_PATH2"]
CID_API = os.environ["CID_API3"]
FAILURE_COUNTER = 0

# Setup logging
logger = logging.getLogger("document_stora")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "document_stora.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Path date variables
TODAY = datetime.date.today()
YEST = TODAY - datetime.timedelta(days=1)
YEST_CLEAN = YEST.strftime("%Y-%m-%d")
YEAR = YEST_CLEAN[0:4]
# YEAR = '2024'
STORAGE_PATH = os.path.join(STORAGE, YEAR)


def csv_retrieve(fullpath: str) -> Optional[dict[str, str]]:
    """
    Fall back for missing descriptions, and output all content to utb field
    """
    data = {}
    print(f"csv_retrieve(): PATH: {fullpath}")
    if not os.path.exists(fullpath):
        logger.warning("No info.csv file found. Skipping CSV retrieve")
        print("No info.csv file found. Skipping CSV retrieve")
        return None
    print("*** Check CSV data reading well ***")
    with open(fullpath, "r", encoding="latin-1") as inf:
        rows = csv.reader(inf)
        for row in rows:
            print(row)
            data = {
                "channel": row[0],
                "title": row[1],
                "description": row[2],
                "title_date_start": row[3],
                "time": row[4],
                "duration": row[5],
                "actual_duration": row[6],
            }
            logger.info("%s\tCSV being processed: %s", fullpath, data["title"])

    return data


def generate_variables(data) -> tuple[str, str, str, str, int, int, int, str, str, str]:
    """
    Take CSV data and generate variable for CID records
    """
    channel = data["channel"]
    title = data["title"]
    if "programmes start" in title:
        title = f"{channel} {title}"
    description = data["description"]
    description = description.replace("'", "'")
    title_date_start = data["title_date_start"]
    time = data["time"]

    broadcast_company = code_type = ""
    if "BBC" in channel or "CBeebies" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "454"
    if "ITV" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "20425"
    if channel == "More4" or channel == "Film4" or channel == "E4":
        code_type = "MPEG-2"
        broadcast_company = "73319"
    if channel == "Channel4":
        code_type = "MPEG-4 AVC"
        broadcast_company = "73319"
    if "5" in channel or "Five" in channel:
        code_type = "MPEG-2"
        broadcast_company = "24404"
    if "Al Jazeera" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "125338"
    if "GB News" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "999831694"
    if "Sky News" in channel:
        code_type = "MPEG-2"
        broadcast_company = "78200"
    if "Talk TV" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "999883795"
    if "Sky Arts" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "150001"
    if "Sky Mix HD" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "999939366"
    if "U&Dave" in channel:
        code_type = "MPEG-2"
        broadcast_company = "999929397"
    if "U&Drama" in channel:
        code_type = "MPEG-2"
        broadcast_company = "999929393"
    if "U&Yesterday" in channel:
        code_type = "MPEG-2"
        broadcast_company = "999929396"
    if "QVC" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "999939374"
    if "TogetherTV" in channel:
        code_type = "MPEG-4 AVC"
        broadcast_company = "999939362"

    duration = data["duration"]
    duration_hours, duration_minutes = duration.split(":")[:2]
    duration_hours_integer = int(duration_hours)
    duration_minutes_integer = int(duration_minutes)
    duration_total = (duration_hours_integer * 60) + duration_minutes_integer

    actual_duration = data['actual_duration']
    if ':' in str(actual_duration):
        actual_duration_hours, actual_duration_minutes, actual_duration_seconds = actual_duration.split(':')
    elif '-' in str(actual_duration):
        actual_duration_hours, actual_duration_minutes, actual_duration_seconds = actual_duration.split('-')
    else:
        return (title, description, title_date_start, time, duration_total, duration_total, 0, channel, broadcast_company, code_type)

    actual_duration_hours_integer = int(actual_duration_hours)
    actual_duration_minutes_integer = int(actual_duration_minutes)
    actual_duration_total = (actual_duration_hours_integer * 60) + actual_duration_minutes_integer
    actual_duration_seconds_integer = int(actual_duration_seconds)

    return (
        title,
        description,
        title_date_start,
        time,
        duration_total,
        actual_duration_total,
        actual_duration_seconds_integer,
        channel,
        broadcast_company,
        code_type,
    )


def build_defaults(
    title: str,
    description: str,
    title_date_start: str,
    time: str,
    duration_total: int,
    actual_duration_total: int,
    actual_duration_seconds_integer: int,
    channel: str,
    broadcast_company: str,
    code_type: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[object], list[dict[str, str]]]:
    """
    Get detailed information
    and build record_defaults dict
    """
    record = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {
            "input.notes": "STORA off-air television capture - automated bulk documentation"
        },
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"grouping.lref": "398775"},
        {"title": title},
        {"title.language": "English"},
        {"title.type": "05_MAIN"},
    ]

    work = [
        {"record_type": "WORK"},
        {"worklevel_type": "MONOGRAPHIC"},
        {"work_type": "T"},
        {"description.type.lref": "100298"},
        {"title_date_start": title_date_start},
        {"title_date.type": "04_T"},
        {"description": description},
        {"description.type": "Synopsis"},
        {"description.date": str(datetime.datetime.now())[:10]},
    ]

    work_restricted = [
        {"application_restriction": "MEDIATHEQUE"},
        {"application_restriction.date": str(datetime.datetime.now())[:10]},
        {"application_restriction.reason": "STRATEGIC"},
        {"application_restriction.duration": "PERM"},
        {"application_restriction.review_date": "2030-01-01"},
        {"application_restriction.authoriser": "mcconnachies"},
        {
            "application_restriction.notes": "Automated off-air television capture - pending discussion"
        },
    ]

    manifestation = [
        {"record_type": "MANIFESTATION"},
        {"manifestationlevel_type": "TRANSMISSION"},
        {"format_high_level": "Video - Digital"},
        {"colour_manifestation": "C"},
        {"sound_manifestation": "SOUN"},
        {"language.lref": "74129"},
        {"language.type": "DIALORIG"},
        {"transmission_date": title_date_start},
        {"transmission_start_time": time},
        {"transmission_duration": duration_total},
        {"runtime": actual_duration_total},
        {"runtime_seconds": actual_duration_seconds_integer},
        {"broadcast_channel": channel},
        {"broadcast_company.lref": broadcast_company},
        {"transmission_coverage": "DIT"},
        {"aspect_ratio": "16:9"},
        {"country_manifestation": "United Kingdom"},
        {
            "notes": "Manifestation representing the UK Freeview television broadcast of the Work."
        },
    ]

    item = [
        {"record_type": "ITEM"},
        {"item_type": "DIGITAL"},
        {"copy_status": "M"},
        {"copy_usage.lref": "131560"},
        {"file_type": "MPEG-TS"},
        {"code_type": code_type},
        {"source_device": "STORA"},
        {"acquisition.method": "Off-Air"},
    ]

    return (record, work, work_restricted, manifestation, item)


def main() -> None:
    """
    Iterate through all info.csv.redux / info.csv.stora
    which have no matching EPG data. Create CID work - manifestation - item records
    """

    if not utils.check_control("pause_scripts") or not utils.check_control("stora"):
        logger.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        logger.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    logger.info(
        "========== STORA documentation script STARTED ==============================================="
    )
    session = adlib.create_session()

    # Iterate through all info.csv.stora creating CID records
    for root, _, files in os.walk(STORAGE_PATH):
        for file in files:
            if FAILURE_COUNTER > 5:
                logger.critical(
                    "Multiple CID item record creation failures. Script exiting."
                )
                sys.exit(
                    "Multiple CID item record creation failures detected. Script exiting."
                )
            # Check control json for STORA false
            if not utils.check_control("pause_scripts") or not utils.check_control(
                "stora"
            ):
                logger.info(
                    "Script run prevented by downtime_control.json. Script exiting."
                )
                sys.exit(
                    "Script run prevented by downtime_control.json. Script exiting."
                )

            if not file.endswith(".stora"):
                continue

            fullpath = os.path.join(root, file)
            print(f"Processing path: {fullpath}")
            data = csv_retrieve(fullpath)
            if len(data) > 0:
                print(f"* CSV found and being processed - {fullpath}")
                logger.info("%s\tCSV being processed: %s", fullpath, data["title"])
                print(f"* Data parsed from csv: {data}")

            # Create variables from csv sources
            var_data = generate_variables(data)
            title = var_data[0]
            description = var_data[1]
            title_date_start = var_data[2]
            time = var_data[3]
            duration_total = var_data[4]
            actual_duration_total = var_data[5]
            actual_duration_seconds_integer = var_data[6]
            channel = var_data[7]
            broadcast_company = var_data[8]
            code_type = var_data[9]
            acquired_filename = os.path.join(root, "stream.mpeg2.ts")

            # Create defaults for all records in hierarchy
            record, work, work_restricted, manifestation, item = build_defaults(
                title,
                description,
                title_date_start,
                time,
                duration_total,
                actual_duration_total,
                actual_duration_seconds_integer,
                channel,
                broadcast_company,
                code_type,
            )

            # create a Work-Manifestation CID record hierarchy
            work_id = create_work(
                fullpath, title, session, record, work, work_restricted
            )
            man_id = create_manifestation(
                work_id, fullpath, title, session, record, manifestation
            )
            if work_id is None or man_id is None:
                print(
                    "* Work or Manifestation record failed to create. Marking with DELETE warning"
                )
                mark_for_deletion(work_id, man_id, fullpath, session)
                continue
            if len(work_id) == 0 or len(man_id) == 0:
                print(
                    "* Work or Manifestation record failed to create. Marking with DELETE warning"
                )
                mark_for_deletion(work_id, man_id, fullpath, session)
                continue

            # Create CID record for Item, first managing subtitles text if present
            old_webvtt = os.path.join(root, "subtitles.vtt")
            webvtt_payload = build_webvtt_dct(old_webvtt)

            item_id, item_ob_num = create_item(
                man_id, fullpath, title, session, acquired_filename, record, item
            )
            if not item_id:
                print(
                    "* Item record failed to create. Marking Work and Manifestation with DELETE warning"
                )
                mark_for_deletion(work_id, man_id, fullpath, session)
                continue
            """
            # Build webvtt payload [Deprecated]
            if webvtt_payload:
                success = push_payload(item_id, session, webvtt_payload)
                if not success:
                    logger.warning("Unable to push webvtt_payload to CID Item %s: %s", item_id, webvtt_payload)
            """
            # Rename csv with .documented
            documented = f"{fullpath}.documented"
            print(f"* Renaming {fullpath} to {documented}")
            try:
                os.rename(fullpath, f"{fullpath}.documented")
            except Exception as err:
                print(f"** PROBLEM: Could not rename {fullpath} to {documented}. {err}")
                logger.critical("%s\tCould not rename to %s", fullpath, documented)

            # Rename transport stream file with Item object number
            item_object_number_underscore = item_ob_num.replace("-", "_")
            new_filename = f"{item_object_number_underscore}_01of01.ts"
            destination = os.path.join(AUTOINGEST_PATH, new_filename)
            print(f"* Renaming {acquired_filename} to {destination}")
            try:
                shutil.move(acquired_filename, destination)
                logger.info(
                    "%s\tRenamed %s to %s", fullpath, acquired_filename, destination
                )
            except Exception as err:
                print(
                    f"** PROBLEM: Could not rename {acquired_filename} to {destination}. {err}"
                )
                logger.critical(
                    "%s\tCould not rename %s to %s. Error: %s",
                    fullpath,
                    acquired_filename,
                    destination,
                    err,
                )

            # Rename GOOD subtitle file with Item object number and move to Isilon for use later in MTQ workflow
            if webvtt_payload:
                logger.info(
                    "%s\tWebVTT subtitles data included in Item %s", fullpath, item_id
                )
                old_vtt = fullpath.replace(file, "subtitles.vtt")
                new_vtt_name = f"{item_object_number_underscore}_01of01.vtt"
                new_vtt = f"{SUBS_PTH}{new_vtt_name}"
                print(f"* Renaming {old_vtt} to {new_vtt}")
                try:
                    shutil.move(old_vtt, new_vtt)
                    logger.info("%s\tRenamed %s to %s", fullpath, old_vtt, new_vtt)
                except Exception as err:
                    print(f"** PROBLEM: Could not rename {old_vtt} to {new_vtt}. {err}")
                    logger.critical(
                        "%s\tCould not rename %s to %s. Error: %s",
                        fullpath,
                        old_vtt,
                        new_vtt,
                        err,
                    )
            else:
                print(
                    "Subtitle data is absent. Subtitle.vtt file will not be renamed or moved"
                )

    logger.info(
        "========== STORA documentation script END ===================================================\n"
    )


def create_work(
    fullpath: str, title: str, session: requests.Session, record_defaults: list[dict[str, str]], work_defaults: list[dict[str, str]], work_restricted_defaults: list[dict[str, str]]
) -> Optional[str]:
    """
    Create CID record for Work
    """
    work_values = []
    work_id = ""
    object_number = ""
    work_values.extend(record_defaults)
    work_values.extend(work_defaults)
    work_values.extend(work_restricted_defaults)
    print(work_values)
    work_values_xml = adlib.create_record_data(
        CID_API, "works", session, "", work_values
    )
    if work_values_xml is None:
        return None
    print("***************************")
    print(work_values_xml)

    try:
        sleep(2)
        logger.info("Attempting to create Work record for item %s", title)
        data = adlib.post(CID_API, work_values_xml, "works", "insertrecord", session)
        try:
            work_id = adlib.retrieve_field_name(data, "priref")[0]
            object_number = adlib.retrieve_field_name(data, "object_number")[0]
            print(
                f"* Work record created with Priref <{work_id}> Object number <{object_number}>"
            )
            logger.info("%s\tWork record created with priref %s", fullpath, work_id)
        except (IndexError, TypeError, KeyError) as err:
            logger.critical("Failed to retrieve Priref from record created using: 'works', 'insertrecord' for %s", title)
            raise Exception(
                "Failed to retrieve Priref/Object Number from record creation."
            ).with_traceback(err.__traceback__)

    except Exception as err:
        print(f"* Unable to create Work record for <{title}>")
        print(err)
        logger.critical("%s\tUnable to create Work record for <%s>", fullpath, title)
        logger.critical(err)
        raise Exception("Unable to write Work record.").with_traceback(
            err.__traceback__
        )

    return work_id


def create_manifestation(
    work_id: str, fullpath: Optional[str], title: str, session: requests.Session, record_defaults: list[dict[str, str]], manifestation_defaults: list[dict[str, str]]
) -> str:
    """
    Create CID record for Manifestation
    """
    manifestation_id, object_number = "", ""
    manifestation_values = []
    manifestation_values.extend(record_defaults)
    manifestation_values.extend(manifestation_defaults)
    manifestation_values.append({"part_of_reference.lref": work_id})

    man_values_xml = adlib.create_record_data(
        CID_API, "manifestations", session, "", manifestation_values
    )
    if man_values_xml is None:
        return None
    print("***************************")
    print(man_values_xml)

    try:
        sleep(2)
        logger.info("Attempting to create Manifestation record for item %s", title)
        data = adlib.post(
            CID_API, man_values_xml, "manifestations", "insertrecord", session
        )
        try:
            manifestation_id = adlib.retrieve_field_name(data, "priref")[0]
            object_number = adlib.retrieve_field_name(data, "object_number")[0]
            print(
                f"* Manifestation record created with Priref {manifestation_id} Object number {object_number}"
            )
            logger.info(
                "%s\tManifestation record created with priref %s",
                fullpath,
                manifestation_id,
            )
        except (IndexError, TypeError, KeyError) as err:
            logger.critical("Failed to retrieve Priref from record created using: 'works', 'insertrecord' for %s", title)
            raise Exception(
                "Failed to retrieve Priref/Object Number from record creation."
            ).with_traceback(err.__traceback__)
    except Exception as err:
        if "bool" in str(err):
            logger.critical(
                "Unable to write manifestation record <%s>", manifestation_id
            )
            print(f"Unable to write manifestation record - error: {err}")
            raise Exception("Unable to write manifestation record.").with_traceback(
                err.__traceback__
            )
        print(f"*** Unable to write manifestation record: {err}")
        logger.critical(
            "Unable to write manifestation record <%s> %s", manifestation_id, err
        )
        raise Exception("Unable to write manifestation record.").with_traceback(
            err.__traceback__
        )

    return manifestation_id


def build_webvtt_dct(old_webvtt: str) -> Optional[str]:
    """
    Open WEBVTT and if content present
    append to CID item record
    """

    print("Attempting to open and read subtitles.vtt")
    if not os.path.exists(old_webvtt):
        print(f"subtitles.vtt not found: {old_webvtt}")
        return None

    with open(old_webvtt, encoding="latin-1") as webvtt_file:
        webvtt_payload = webvtt_file.read()
        webvtt_file.close()

    if not webvtt_payload:
        print("subtitles.vtt could not be open")
        logger.warning("Unable to open subtitles.vtt - file absent")
        return None

    if not "-->" in webvtt_payload:
        print("subtitles.vtt has no data present in file")
        logger.warning("subtitles.vtt data is absent")
        return None

    return webvtt_payload.replace("'", "'")


def create_item(
    manifestation_id: str,
    fullpath: str,
    title: str,
    session: requests.Session,
    acquired_filename: str,
    record_defaults: list[dict[str, str]],
    item_defaults:list[dict[str, str]]
):
    """
    Create item record, and if failure of item record
    creation then add delete warning to work and manifestation records
    """
    item_id, item_object_number = "", ""
    item_values = []
    item_values.extend(record_defaults)
    item_values.extend(item_defaults)
    item_values.append({"part_of_reference.lref": manifestation_id})
    item_values.append({"digital.acquired_filename": acquired_filename})

    item_values_xml = adlib.create_record_data(
        CID_API, "items", session, "", item_values
    )
    if item_values_xml is None:
        return None
    print("***************************")
    print(item_values_xml)

    try:
        sleep(2)
        logger.info("Attempting to create CID item record for item %s", title)
        data = adlib.post(CID_API, item_values_xml, "items", "insertrecord", session)
        try:
            item_id = adlib.retrieve_field_name(data, "priref")[0]
            item_object_number = adlib.retrieve_field_name(data, "object_number")[0]
            print(
                f"* Item record created with Priref {item_id} Object number {item_object_number}"
            )
            logger.info("%s\tItem record created with priref %s", fullpath, item_id)
        except (TypeError, IndexError, KeyError) as err:
            logger.critical("Failed to retrieve Priref from record created using: 'works', 'insertrecord' %s", title)
            raise Exception(
                "Failed to retrieve Priref/Object Number from record creation."
            ).with_traceback(err.__traceback__)
    except Exception as err:
        print(f"** PROBLEM: Unable to create Item record for <{title}> {err}")
        logger.critical(
            "%s\tPROBLEM: Unable to create Item record for <%s>, marking Work and Manifestation records for deletion",
            fullpath,
            title,
        )
        raise Exception("Unable to write Item record.").with_traceback(
            err.__traceback__
        )

    return item_id, item_object_number


def mark_for_deletion(work_id: str, manifestation_id: Optional[str], fullpath: str, session: requests.Session) -> None:
    """
    Update work and manifestation records with deletion prompt in title
    """
    global FAILURE_COUNTER
    FAILURE_COUNTER += 1
    payload_start = f"<adlibXML><recordList><record priref='{work_id}'>"
    payload_mid = (
        f"<Title><title>DELETE - STORA record creation problem</title></Title>"
    )
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_start + payload_mid + payload_end
    try:
        response = adlib.post(CID_API, payload, "works", "updaterecord", session)
        if response:
            logger.info(
                "%s\tRenamed Work %s with deletion prompt in title, for bulk deletion",
                fullpath,
                work_id,
            )
        else:
            logger.warning(
                "%s\tUnable to rename Work %s with deletion prompt in title, for bulk deletion",
                fullpath,
                work_id,
            )
    except Exception as err:
        logger.warning(
            "%s\tUnable to rename Work %s with deletion prompt in title, for bulk deletion. Error: %s",
            fullpath,
            work_id,
            err,
        )

    payload_start = f"<adlibXML><recordList><record priref='{manifestation_id}'>"
    payload_mid = (
        f"<Title><title>DELETE - STORA record creation problem</title></Title>"
    )
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_start + payload_mid + payload_end
    try:
        response = adlib.post(
            CID_API, payload, "manifestations", "updaterecord", session
        )
        if response:
            logger.info(
                "%s\tRenamed Manifestation %s with deletion prompt in title",
                fullpath,
                manifestation_id,
            )
        else:
            logger.warning(
                "%s\tUnable to rename Manifestation %s with deletion prompt in title",
                fullpath,
                manifestation_id,
            )
    except Exception as err:
        logger.warning(
            "%s\tUnable to rename Manifestation %s with deletion prompt in title. Error: %s",
            fullpath,
            manifestation_id,
            err,
        )


def push_payload(item_id: str, session: requests.Session, webvtt_payload: str) -> Optional[bool]:
    """
    Push webvtt payload separately to Item record
    creation, to manage escape character injects
    """

    label_type = "SUBWEBVTT"
    label_source = "Extracted from MPEG-TS created by STORA recording"
    # Make payload
    pay_head = f'<adlibXML><recordList><record priref="{item_id}">'
    label_type_addition = f"<label.type>{label_type}</label.type>"
    label_addition = f"<label.source>{label_source}</label.source><label.text><![CDATA[{webvtt_payload}]]></label.text>"
    pay_end = "</record></recordList></adlibXML>"
    payload = pay_head + label_type_addition + label_addition + pay_end

    try:
        post_resp = adlib.post(CID_API, payload, "items", "updaterecord", session)
        if post_resp:
            return True
    except Exception as err:
        logger.warning(
            "push_payload(): Error returned from requests post: %s %s %s",
            item_id,
            payload,
            err,
        )
        return False


if __name__ == "__main__":
    main()
