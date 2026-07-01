import os
import sys
from datetime import datetime, timedelta, time
sys.path.append(os.environ["CODE"])
import utils
import adlib_v3 as adlib
import adlib_v3_sess as adlib_sess
import shutil
from pathlib import Path
from typing import Optional
import logging
import re
from dataclasses import dataclass
import argparse
import time
CID_API = os.environ['CID_API3']
LOG_PATH = os.environ["LOG_PATH"]
SUBTITLE_FOLDER = os.path.join(os.environ.get("ADMIN"), "off_air_tv/subtitles_not_in_cid")

PROCESSED_FOLDER = Path(
    os.getenv("PROCESSED_FOLDER", os.path.join(os.environ.get("ADMIN"), "off_air_tv/subtitles"))
)

EDITOR_NAME = "datadigipres"
EDITOR_NOTES = "Automated subtitle relocation project"
SUBTITLE_TYPE = "WEBVTT_C"

TIME_FORMAT = "%H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

logger = logging.getLogger("subtitle_relocation")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "subtitle_relocation.log"))
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
logger.info("Logger initialised")

_SAFE_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.\- ]+$")


@dataclass
class TransmissionInfo:
    date: str
    start_time: str
    end_time: str

def is_safe_search_value(value: str) -> bool:
    return bool(_SAFE_VALUE_RE.fullmatch(value))


def safe_search_query(field: str, value: str) -> str:
    if not is_safe_search_value(value):
        raise ValueError(f"Unsafe search value for {field}={value!r}")
    return f"{field}='{value}'"

def retrieve_single_record(
    database: str,
    search_field: str,
    search_value: str,
    fields: Optional[list[str]] = None,
) -> Optional[list[dict]]:
    query = safe_search_query(search_field, search_value)
    hits, records = adlib.retrieve_record(
        CID_API, database, query, "1", fields=fields
    )
    if not hits or not records:
        return None
    return records


def get_field(record: dict, field_name: str) -> Optional[str]:
    values = adlib.retrieve_field_name(record, field_name)
    return values[0] if values else None


def get_item_priref(object_number: str) -> Optional[str]:
    records = retrieve_single_record("items", "object_number", object_number)
    if not records:
        logger.warning("No item found for object_number=%s", object_number)
        return None
    return get_field(records[0], "priref")

def get_manifestation_priref(item_priref: str) -> Optional[str]:
    records = retrieve_single_record("items", "priref", item_priref)
    if not records:
        logger.warning(
            "No manifestation record for item_priref=%s", item_priref
        )
        return None
    return get_field(records[0], "part_of_reference.lref")

def get_transmission_info(
    manifestation_priref: str,
) -> Optional[TransmissionInfo]:
    records = retrieve_single_record(
        "items",
        "priref",
        manifestation_priref,
        fields=[
            "transmission_date",
            "transmission_end_time",
            "transmission_start_time",
        ],
    )
    if not records:
        logger.warning(
            "No transmission info for priref=%s", manifestation_priref
        )
        return None

    trans_date = get_field(records[0], "transmission_date")
    end_time = get_field(records[0], "transmission_end_time")
    start_time = get_field(records[0], "transmission_start_time")

    if not all([trans_date, end_time, start_time]):
        logger.error(
            "Incomplete transmission data for priref=%s "
            "(date=%s, end=%s, start=%s)",
            manifestation_priref,
            trans_date,
            end_time,
            start_time,
        )
        return None

    return TransmissionInfo(date=trans_date, start_time=start_time, end_time=end_time)

def working_day_check(dt: datetime) -> bool:
    """Check for clash with working week"""
    work_days = {0, 1, 2, 3, 4}
    start = time(8, 00, 0)
    end = time(19, 55, 0)
        
    if dt.weekday() not in work_days:
        return False
    current_time = dt.time()
    return start <= current_time <= end

def adjust_date_for_midnight(info: TransmissionInfo) -> str:
    try:
        end = datetime.strptime(info.end_time, TIME_FORMAT)
        start = datetime.strptime(info.start_time, TIME_FORMAT)
    except ValueError as exc:
        raise ValueError(f"Invalid time format in {info}") from exc

    date = datetime.strptime(info.date, DATE_FORMAT)
    if end < start:
        date += timedelta(days=1)
        logger.info(
            "Show ran past midnight - date adjusted to %s",
            date.strftime(DATE_FORMAT),
        )
    return date.strftime(DATE_FORMAT)

def build_subtitle_edit_xml(
    priref: str, subtitle_date: str, vtt_text: str
) -> str:
    now = datetime.now()
    edit_entries = [
        {"edit.date": now.strftime("%Y-%m-%d")},
        {"edit.name": EDITOR_NAME},
        {"edit.notes": EDITOR_NOTES},
        {"edit.time": now.strftime("%H:%M:%S")},
        {"subtitle.date": subtitle_date},
        {"subtitle.text": vtt_text},
        {"subtitle.type": SUBTITLE_TYPE},
    ]
    return adlib.create_grouped_data(priref, "Edit", [edit_entries])

def post_xml_to_cid(edit_xml) -> tuple[bool, str]:
    try:
        record = adlib_sess.post(CID_API, edit_xml, "items", "updaterecord", None)
    except Exception as err:
        if hasattr(err, '__cause__'):
            reason = f"Cause: {err.__cause__}"
        elif hasattr(err, 'last_attempt'):
            reason = f"Underlying exception: {err.last_attempt.exception()}"
        else:
            reason = str(err)
        logger.error("Failed to post edit record: %s", reason)
        return False, reason

    if record is None:
        return False, "record is None"
    if isinstance(record, dict) and "@attribute" in record:
        return True, ""
    if isinstance(record, dict) and "'error': {'message':" in record:
        reason = "error found in record"
        logger.error("Failed to post edit record: %s", reason)
        return False, reason
    return True, ""

def main():  
    parser = argparse.ArgumentParser(
        description="Relocate subtitle VTT files into the CID database."
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Process at most N files (default: 0 = all files)"
    )
    args = parser.parse_args()

    #if working_day_check(datetime.now()):
    #    sys.exit("Exiting: Cannot operate in working hours")
    logger.info(
        "========== subtitle creation script STARTED ==============================================="
    )
    list_files= [
        f for f in os.listdir(SUBTITLE_FOLDER)
        if f.endswith(".vtt")
    ]
    if args.limit:
        list_files = list_files[:args.limit]

    total = len(list_files)
    successes = 0
    errors =0
    #os.listdir(SUBTITLE_FOLDER)[20:30]

    for file in list_files:
        object_number = utils.get_object_number(file)
        logger.info("PROCESSING start | file=%s | object_number=%s", file, object_number)
        time.sleep(2)
        if not is_safe_search_value(object_number):
            logger.error(
                "Rejecting unsafe object_number=%s from filename=%s", object_number, file
            )
            errors += 1
            continue

        item_priref = get_item_priref(object_number)
        logger.info("Item priref: %s", item_priref)
        logger.info("PROCESSING item_priref | file=%s | priref=%s", file, item_priref)
        if item_priref is None:
            logger.error("Skipping %s: no item priref found", file)
            errors += 1
            continue


        mani_priref = get_manifestation_priref(item_priref)
        logger.info("PROCESSING manifestation | file=%s | priref=%s", file, mani_priref)
        if mani_priref is None:
            logger.error("Skipping %s: no manifestation priref for item %s", file, item_priref)
            errors += 1
            continue
        logger.info("manifestation priref: %s", mani_priref)

        trans_info = get_transmission_info(mani_priref)
        if not trans_info:
            logger.error("Skipping %s: no/incomplete transmission data for manifestation %s", file, mani_priref)
            errors += 1
            continue
        logger.info("transmission_date: %s", trans_info.date)
        logger.info("transmission_end_time: %s", trans_info.end_time)
        logger.info("transmission_start_time: %s", trans_info.start_time)

        try:
            subtitle_date = adjust_date_for_midnight(trans_info)
            logger.info("subtitle_date: %s", subtitle_date)
        except ValueError as exc:
            logger.error(
            "Date adjustment failed for %s: %s", file, exc
                )
            errors += 1
            continue

        file_path = os.path.join(SUBTITLE_FOLDER, file)
        try:
             with open(file_path, encoding="utf-8") as webvtt_file:
                webvtt_payload = webvtt_file.read()
        except (OSError, UnicodeDecodeError) as exc:
            logger.error("Failed to read %s: %s", file_path, exc)
            errors += 1
            continue

        xml_payload = build_subtitle_edit_xml(
        item_priref, subtitle_date, webvtt_payload
        )

        logger.debug("XML payload:\n%s", xml_payload)

        success, reason = post_edit_record(xml_payload)
        if success:
            successes += 1
            logger.info("SUCCESS | Post Successful")
        else:
            logger.error("FAIL | reason=%s", reason)
            errors += 1

        #shutil.move(file_path, str(PROCESSED_FOLDER / file))
        #logger.info("Moved %s -> %s", file, PROCESSED_FOLDER / file)

        logger.info("PROCESSED ok | file=%s | object_number=%s", file, object_number)
        successes += 1
    logger.info("SUMMARY: %d / %d succeeded | %d errors", successes, total, errors)
    if errors:
        logger.warning("Review errors above for %d failed file(s)", errors)
    logger.info(
        "========== subtitle creation script END ==============================================="
    )



if __name__ == "__main__":
    main()
