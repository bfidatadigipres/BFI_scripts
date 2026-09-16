import argparse
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

def get_manifestation_priref(item_priref: str, session: Session) -> Optional[str]:
    """Look up the parent manifestation priref for a given item priref."""
    records = retrieve_single_record("items", "priref", item_priref, session)
    if not records:
        logger.warning("No manifestation record for item_priref=%s", item_priref)
        return None
    return get_field(records[0], "part_of_reference.lref")

def main():
    # adding limit for testing purposes
    session = adlib_sess.create_session()
    parser = argparse.ArgumentParser(
        description="Relocate subtitle VTT files into the CID database."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N files (default: 0 = all files)",
    )
    args = parser.parse_args()

    logger.info(
        "========== Transfer subtitle fields script STARTED ==============================================="
    )
    search_query = (
        f"({safe_search_query('grouping.lref', '398775')} and "
        f"{safe_search_query('label.type', '*VTT')} and "
        f"input.date>'2022-09-01' and input.date<'2022-09-10')"
    )
    fields = ["label.type", "label.text", "label.source", "input.date", "priref", "part_of_reference"]
    hits, item_record = adlib.retrieve_record(
        CID_API, "items", search_query, "1", fields=fields
    )
    logger.info("item_record: %s", item_record)
    logger.info("hits: %s", hits)
    if args.limit:
        hits = args.limit

    total = hits * 2
    current_priref = None
    successes = 0
    errors = 0

    session = adlib_sess.create_session()
    for i in range(hits):
        if current_priref is None:
            search = search_query
        else:
            search = f"(priref>{current_priref}) and {search_query}"

        clock.sleep(0.3)
        _, item_record = adlib.retrieve_record(
            CID_API, "items", search, "1", fields=fields
        )

        if not item_record:
            break

        priref_values = adlib.retrieve_field_name(item_record[0], "priref")
        if not priref_values:
            logger.error("Skipping: no priref found")
            errors += 1
            continue
        current_priref = priref_values[0]
        # Extract fields and create XML
        input_date = get_field(item_record[0], "input.date")
        subtitle_text = get_field(item_record[0], "label.text")
        subtitle_type = get_field(item_record[0], "label.type")
        subtitle_source = get_field(item_record[0], "label.source")

        if not all([input_date, subtitle_text, subtitle_type, subtitle_source]):
            logger.error("Skipping priref=%s: missing subtitle fields", current_priref)
            logger.error("subtitle_text: %s", subtitle_text)
            logger.error("subtitle_type: %s", subtitle_type)
            logger.error("subtitle_source: %s", subtitle_source)
            errors += 1
            continue

        edit_xml = build_subtitle_edit_xml(current_priref, input_date, subtitle_text, subtitle_source, subtitle_type)
        #get manifestation priref :)
        mani_priref = get_field(item_record[0], "Part_of.part_of_reference.priref")
        print(f"manifestation priref: {mani_priref}")
        logger.info("manifestation priref: %s", mani_priref)
        manifestation_xml = build_subtitle_edit_xml(mani_priref,"", "",  "", "", True)
        logger.info("(%d/%d) priref=%s", i + 1, hits, current_priref)

        success, reason = post_xml_to_cid(edit_xml, "items", session)
        if success:
            logger.info("OK | (%d/%d) priref=%s", i + 1, total, current_priref)
            successes += 1
        else:
            logger.error(
                "FAIL | (%d/%d) priref=%s | reason=%s",
                i + 1,
                total,
                current_priref,
                reason,
            )
            errors += 1
        manifestation_success, manifestation_reason = post_xml_to_cid(manifestation_xml, "manifestations", session)
        if manifestation_success:
            successes += 1
            logger.info("SUCCESS | Manifestation Post Successful")
        else:
            logger.error(
                "FAIL | (%d/%d) priref=%s | reason=%s",
                i + 1,
                total,
                current_priref,
                manifestation_reason,
            )
            errors += 1

    logger.info("SUMMARY: %d / %d succeeded | %d errors", successes, total, errors)
    logger.info(
        "========== Transfer subtitle fields script END ==============================================="
    )


if __name__ == "__main__":
    main()

