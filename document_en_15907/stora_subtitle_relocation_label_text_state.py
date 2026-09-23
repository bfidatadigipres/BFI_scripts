import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import re
from requests import Session

sys.path.append(os.environ["CODE"])
import utils
import adlib_v3 as adlib
import adlib_v3_sess as adlib_sess
import logging
import time as clock

CID_API = utils.get_current_api()
LOG_PATH = os.environ["LOG_PATH"]

logger = logging.getLogger("stora_subtitle_relocation_label_text")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "stora_subtitle_relocation_label_text.log"))
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
logger.info("Logger initialised")

CONFIG_PATH = os.path.join(LOG_PATH, "subtitle_transfer_config.json")
START_DATE = "2020-01-01"
STOP_DATE = "2024-12-31"
MONTH_STEP = 4
DATE_FORMAT = "%Y-%m-%d"

_SAFE_VALUE_RE = re.compile(r"^[a-zA-Z0-9_\-.*?()' /:]+$")

def is_safe_search_value(value: str) -> bool:
    return bool(_SAFE_VALUE_RE.fullmatch(value))

def safe_search_query(field: str, value: str) -> str:
    if not is_safe_search_value(value):
        raise ValueError(f"Unsafe search value for {field}={value!r}")
    return f"{field}='{value}'"

def get_field(record: dict, field_name: str) -> Optional[str]:
    values = adlib_sess.retrieve_field_name(record, field_name)
    if values and values[0] is not None:
        return values[0]

    if "." in field_name:
        top_key, rest = field_name.split(".", 1)
        try:
            items = record[top_key]
        except (KeyError, TypeError):
            return None
        for item in items:
            result = get_field(item, rest)
            if result is not None:
                return result
        return None

    return None

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


def post_xml_to_cid(edit_xml, database, session) -> tuple[bool, str]:
    try:
        record = adlib_sess.post(CID_API, edit_xml, database, "updaterecord", session)
    except Exception as err:
        if hasattr(err, "__cause__"):
            reason = f"Cause: {err.__cause__}"
        elif hasattr(err, "last_attempt"):
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

def build_subtitle_edit_xml(
        priref: str, input_date: str, subtitle_text: str, subtitle_source: str, subtitle_type:str, manifestation=False
) -> str:
    """Build XML edit record payload with subtitle metadata and VTT content."""
    now = datetime.now()
    edit_entries = [
            {"edit.date": now.strftime("%Y-%m-%d")},
            {"edit.name": "datadigipres"},
            {"edit.notes": "Automated subtitle relocation project"},
            {"edit.time": now.strftime("%H:%M:%S")},
            {"subtitle.date": input_date},
            {"subtitle.text": subtitle_text.replace("ï»¿", "")},
            {"subtitle.type": subtitle_type},
            {"subtitle.source": subtitle_source},
        ]
    if manifestation:
        edit_entries = [{"accessibility_resource": "SUBTITLES"}]
    return adlib_sess.create_grouped_data(priref, "Edit", [edit_entries])

def get_manifestation_priref(item_priref: str) -> Optional[str]:
    """Look up the parent manifestation priref for a given item priref."""
    records = retrieve_single_record("items", "priref", item_priref)
    if not records:
        logger.warning("No manifestation record for item_priref=%s", item_priref)
        return None
    return get_field(records[0], "part_of_reference.lref")


def read_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        config = {
            "start_date": START_DATE,
            "stop_date": STOP_DATE,
            "completed_ranges": [],
            "in_progress": None,
        }
        write_config(config)
        logger.info("Created new config file: %s", CONFIG_PATH)
        return config
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def write_config(config: dict) -> None:
    tmp_path = CONFIG_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(config, f, indent=2)
    os.rename(tmp_path, CONFIG_PATH)


def add_months(source: datetime, months: int) -> datetime:
    month = source.month - 1 + months
    year = source.year + month // 12
    month = month % 12 + 1
    day = min(
        source.day,
        [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
         31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1],
    )
    return source.replace(year=year, month=month, day=day)


def generate_ranges(start_str: str, stop_str: str, step_months: int = 4) -> list[dict]:
    ranges = []
    cursor = datetime.strptime(start_str, DATE_FORMAT)
    stop = datetime.strptime(stop_str, DATE_FORMAT)
    while cursor < stop:
        end = add_months(cursor, step_months)
        if end > stop:
            end = stop
        ranges.append({"start": cursor.strftime(DATE_FORMAT), "end": end.strftime(DATE_FORMAT)})
        cursor = end
    return ranges


def build_query(start: str, end: str, last_priref: Optional[str] = None) -> str:
    date_clause = f"input.date>='{start}' and input.date<'{end}'"
    base = f"({safe_search_query('grouping.lref', '398775')} and {safe_search_query('label.type', '*VTT')} and {date_clause})"
    if last_priref:
        return f"(priref>{last_priref}) and {base}"
    return base


def next_unprocessed_range(config: dict) -> Optional[dict]:
    all_ranges = generate_ranges(config["start_date"], config["stop_date"], MONTH_STEP)
    completed = {(r["start"], r["end"]) for r in config["completed_ranges"]}
    for r in all_ranges:
        if (r["start"], r["end"]) not in completed:
            return r
    return None


def range_remaining_count(start: str, end: str) -> int:
    query = build_query(start, end)
    hits, _ = adlib.retrieve_record(CID_API, "items", query, "1", fields=["priref"])
    return hits


def process_range(
    session: Session,
    fields: list[str],
    current_range: dict,
    last_priref: Optional[str],
    limit: int = 0,
    config: Optional[dict] = None,
) -> None:
    start = current_range["start"]
    end = current_range["end"]

    query = build_query(start, end, last_priref)
    hits, _ = adlib.retrieve_record(CID_API, "items", query, "1", fields=fields)
    logger.info("Records in range %s to %s: %d", start, end, hits)

    if limit:
        hits = min(hits, limit)

    successes = 0
    errors = 0

    for i in range(hits):
        if last_priref is None:
            search = build_query(start, end)
        else:
            search = build_query(start, end, last_priref)

        clock.sleep(0.3)
        _, item_record = adlib.retrieve_record(
            CID_API, "items", search, "1", fields=fields
        )

        if not item_record:
            logger.info("No more records in range %s to %s", start, end)
            break

        priref_values = adlib.retrieve_field_name(item_record[0], "priref")
        if not priref_values:
            logger.error("Skipping: no priref found")
            errors += 1
            continue

        current_priref = priref_values[0]

        try:
            input_date = get_field(item_record[0], "input.date")
            subtitle_text = get_field(item_record[0], "label.text")
            subtitle_type = get_field(item_record[0], "label.type")
            subtitle_source = get_field(item_record[0], "label.source")

            if not all([input_date, subtitle_text, subtitle_type, subtitle_source]):
                logger.error(
                    "Skipping priref=%s: missing subtitle fields (text=%s type=%s source=%s)",
                    current_priref, subtitle_text, subtitle_type, subtitle_source,
                )
                errors += 1
                last_priref = current_priref
                if config:
                    config["in_progress"]["last_priref"] = current_priref
                    write_config(config)
                continue

            edit_xml = build_subtitle_edit_xml(
                current_priref, input_date, subtitle_text, subtitle_source, subtitle_type,
            )

            mani_priref = get_field(item_record[0], "Part_of.part_of_reference.priref")
            logger.info("Manifestation priref for item %s: %s", current_priref, mani_priref)

            manifestation_xml = None
            if mani_priref:
                manifestation_xml = build_subtitle_edit_xml(
                    mani_priref, "", "", "", "", manifestation=True,
                )

            logger.info("Record %d/%d priref=%s", i + 1, hits, current_priref)

            success, reason = post_xml_to_cid(edit_xml, "items", session)
            if success:
                logger.info("Items post OK | record %d/%d priref=%s", i + 1, hits, current_priref)
                successes += 1
            else:
                logger.error(
                    "Items post FAIL | record %d/%d priref=%s | reason=%s",
                    i + 1, hits, current_priref, reason,
                )
                errors += 1

            if manifestation_xml:
                mani_success, mani_reason = post_xml_to_cid(
                    manifestation_xml, "manifestations", session,
                )
                if mani_success:
                    logger.info(
                        "Manifestation post OK | record %d/%d priref=%s",
                        i + 1, hits, current_priref,
                    )
                    successes += 1
                else:
                    logger.error(
                        "Manifestation post FAIL | record %d/%d priref=%s | reason=%s",
                        i + 1, hits, current_priref, mani_reason,
                    )
                    errors += 1

        except Exception as exc:
            logger.exception("Unexpected error processing priref=%s: %s", current_priref, exc)
            errors += 1

        last_priref = current_priref
        if config:
            config["in_progress"]["last_priref"] = current_priref
            write_config(config)

    logger.info("Range %s to %s done: %d succeeded, %d failed", start, end, successes, errors)


def main():
    parser = argparse.ArgumentParser(
        description="Relocate subtitle VTT files into the CID database."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N records (default: 0 = all records in range)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use hardcoded test range 2022-09-01 to 2022-09-10, ignore config file",
    )
    args = parser.parse_args()

    logger.info(
        "========== Transfer subtitle fields script STARTED ==============================================="
    )

    session = adlib_sess.create_session()
    fields = ["label.type", "label.text", "label.source", "input.date", "priref", "part_of_reference"]

    if args.test:
        logger.info("TEST MODE: using range 2022-09-01 to 2022-09-10, no state file touched")
        test_range = {"start": "2022-09-01", "end": "2022-09-10"}
        process_range(session, fields, test_range, last_priref=None, limit=args.limit)
        logger.info(
            "========== Transfer subtitle fields script END ==============================================="
        )
        return

    config = read_config()
    logger.info(
        "Config loaded: start=%s stop=%s completed=%d in_progress=%s",
        config["start_date"],
        config["stop_date"],
        len(config["completed_ranges"]),
        config["in_progress"],
    )

    if config["in_progress"] is not None:
        current_range = config["in_progress"]
        last_priref = current_range.get("last_priref")
        logger.info(
            "Resuming range %s to %s from priref=%s",
            current_range["start"], current_range["end"], last_priref,
        )
    else:
        next_range = next_unprocessed_range(config)
        if next_range is None:
            logger.info("All ranges completed up to %s. Nothing to do.", config["stop_date"])
            logger.info(
                "========== Transfer subtitle fields script END ==============================================="
            )
            return
        current_range = next_range
        last_priref = None
        config["in_progress"] = {
            "start": current_range["start"],
            "end": current_range["end"],
            "last_priref": None,
        }
        write_config(config)
        logger.info("Starting new range %s to %s", current_range["start"], current_range["end"])

    process_range(session, fields, current_range, last_priref, args.limit, config)

    remaining = range_remaining_count(current_range["start"], current_range["end"])
    if remaining == 0:
        config["completed_ranges"].append({
            "start": current_range["start"],
            "end": current_range["end"],
        })
        config["in_progress"] = None
        write_config(config)
        logger.info(
            "Range %s to %s fully completed. Total ranges done: %d",
            current_range["start"], current_range["end"],
            len(config["completed_ranges"]),
        )
    else:
        logger.info(
            "Range %s to %s partially processed (%d records remaining). Next run will resume.",
            current_range["start"], current_range["end"], remaining,
        )

    logger.info(
        "========== Transfer subtitle fields script END ==============================================="
    )


if __name__ == "__main__":
    main()

