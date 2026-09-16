import argparse
import json
import os
import sys
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

CID_API = os.environ["CID_API3"]
LOG_PATH = os.environ["LOG_PATH"]

logger = logging.getLogger("stora_manifestation_epg_improvement")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "stora_manifestation_epg_improvement.log"))
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


# --- Checkpoint functions ---

def load_checkpoint(checkpoint_file: str) -> dict:
    """Load checkpoint from file, or return default structure."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return json.load(f)
    return {
        "current_window_start": "2021-08-06",
        "current_window_end": None,
        "last_processed_priref": None,
        "last_completed_date": None,
        "status": "running",
        "updated_at": None,
        "failed_records": [],
    }


def save_checkpoint(checkpoint_file: str, checkpoint: dict):
    """Save checkpoint to file."""
    checkpoint["updated_at"] = datetime.now().isoformat()
    with open(checkpoint_file, "w") as f:
        json.dump(checkpoint, f, indent=2)


def advance_window(checkpoint: dict, end_date: str):
    """Advance to the next 4-month window."""
    from dateutil.relativedelta import relativedelta

    current_start = datetime.strptime(checkpoint["current_window_start"], "%Y-%m-%d")
    current_end = datetime.strptime(checkpoint["current_window_end"], "%Y-%m-%d")

    new_start = current_end
    new_end = new_start + relativedelta(months=4)

    if new_start >= datetime.strptime(end_date, "%Y-%m-%d"):
        checkpoint["status"] = "finished"
        logger.info("All windows processed. Finished.")
        return

    if new_end > datetime.strptime(end_date, "%Y-%m-%d"):
        new_end = datetime.strptime(end_date, "%Y-%m-%d")

    checkpoint["current_window_start"] = new_start.strftime("%Y-%m-%d")
    checkpoint["current_window_end"] = new_end.strftime("%Y-%m-%d")
    checkpoint["last_processed_priref"] = None
    checkpoint["last_completed_date"] = None
    checkpoint["status"] = "running"
    logger.info("Advanced to window: %s → %s", checkpoint["current_window_start"], checkpoint["current_window_end"])


def compute_initial_window(start_date: str) -> tuple[str, str]:
    """Compute the first 4-month window from start_date."""
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = start + relativedelta(months=4)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# --- Main ---

DEFAULT_CHECKPOINT_FILE = "/mnt/qnap_04/Admin/Logs/epg_improvement_config.json" 

def main():
    parser = argparse.ArgumentParser(
        description="Automated EPG metadata improvement with checkpoint-based scheduling."
    )
    parser.add_argument(
        "--checkpoint-file",
        default=DEFAULT_CHECKPOINT_FILE,
        help="Path to the checkpoint JSON file. Defaults to DEFAULT_CHECKPOINT_FILE in script.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N records per run (default: 0 = all records in window).",
    )
    args = parser.parse_args()

    if not args.checkpoint_file:
        parser.error("--checkpoint-file is required. Set DEFAULT_CHECKPOINT_FILE in script or pass via CLI.")

    END_DATE = "2025-12-31"

    logger.info(
        "========== EPG metadata script STARTED ==============================================="
    )

    # Load checkpoint
    checkpoint = load_checkpoint(args.checkpoint_file)

    # If no window end set (fresh start), compute initial window
    if checkpoint["current_window_end"] is None:
        start, end = compute_initial_window(checkpoint["current_window_start"])
        checkpoint["current_window_start"] = start
        checkpoint["current_window_end"] = end
        logger.info("Initial window: %s → %s", start, end)

    # If previous window was completed, advance
    if checkpoint["status"] == "completed":
        advance_window(checkpoint, END_DATE)
        if checkpoint["status"] == "finished":
            save_checkpoint(args.checkpoint_file, checkpoint)
            logger.info("SUMMARY: All windows processed. Nothing to do.")
            return

    save_checkpoint(args.checkpoint_file, checkpoint)

    window_start = checkpoint["current_window_start"]
    window_end = checkpoint["current_window_end"]
    last_priref = checkpoint["last_processed_priref"]

    logger.info("Processing window: %s → %s (resuming from priref=%s)", window_start, window_end, last_priref)

    # Build search query
    search_query = (
        f"({safe_search_query('grouping.lref', '398775')} and "
        f"input.date>='{window_start}' and input.date<'{window_end}')"
    )

    fields = ["priref", "utb.content", "utb.fieldname"]

    # Initial hit count
    if last_priref is None:
        initial_search = search_query
    else:
        initial_search = f"(priref>{last_priref}) and {search_query}"

    hits, _ = adlib.retrieve_record(CID_API, "manifestations", initial_search, "1", fields=fields)
    logger.info("Remaining hits in window: %s", hits)

    if args.limit:
        hits = args.limit

    session = adlib_sess.create_session()
    total = hits 
    current_priref = last_priref
    successes = 0
    errors = 0

    for i in range(hits):
        if current_priref is None:
            search = search_query
        else:
            search = f"(priref>{current_priref}) and {search_query}"

        clock.sleep(0.3)
        _, manifestation_record = adlib.retrieve_record(CID_API, "manifestations", search, "1", fields=fields)

        if not manifestation_record:
            break

        priref_values = adlib.retrieve_field_name(manifestation_record[0], "priref")
        if not priref_values:
            logger.error("Skipping: no priref found")
            errors += 1
            continue
        current_priref = priref_values[0]

        # Extract fields
        utb_content = get_field(manifestation_record[0], "utb.content")
        utb_fieldname = get_field(manifestation_record[0], "utb.fieldname")

        if utb_content is None and utb_fieldname is None:
            logger.error("Skipping priref=%s: missing utb.content and utb.fieldname", current_priref)
            logger.error("utb_content: %s", utb_content)
            logger.error("utb_fieldname: %s", utb_fieldname)
            checkpoint["failed_records"].append({
                "priref": str(current_priref),
                "post_type": "manifestation",
                "reason": "missing utb.content and utb.fieldname",
                "failed_at": datetime.now().isoformat(),
            })
            save_checkpoint(args.checkpoint_file, checkpoint)
            errors += 1
            continue

        # Build edit entries from utb_content
        edit_entries = []
        if utb_content is not None:
            if 'repeat' in utb_content:
                edit_entries.append({"schedule_context": "REPEAT"})
            if 'omnibus' in utb_content:
                edit_entries.append({"schedule_context": "OMNIBUS"})
            if 'premiere' in utb_content:
                edit_entries.append({"schedule_context": "PREMIERE"})
            if 'returning' in utb_content:
                edit_entries.append({"schedule_context": "RETURNING"})
            if 'continued' in utb_content:
                edit_entries.append({"schedule_context": "CONTINUED"})
            if 'follow on' in utb_content:
                edit_entries.append({"schedule_context": "FOLLOW"})
            if 'new' in utb_content:
                edit_entries.append({"schedule_context": "NEW"})
            if "subtitles" in utb_content:
                edit_entries.append({"accessibility_resource": "SUBTITLES"})
            if "sign-language" in utb_content:
                edit_entries.append({"accessibility_resource": "SIGN_LANGUAGE"})
            if "audio-description" in utb_content:
                edit_entries.append({"accessibility_resource": "AUDIO_DES"})

        logger.info("manifestation priref: %s", current_priref)
        logger.info("(%d/%d) priref=%s", i + 1, hits, current_priref)

        if not edit_entries:
            logger.error("Skipping priref=%s: no schedule_context matches in utb.content", current_priref)
            logger.error("utb_content: %s", utb_content)
            checkpoint["failed_records"].append({
                "priref": str(current_priref),
                "post_type": "manifestation",
                "reason": "no schedule_context matches in utb.content",
                "failed_at": datetime.now().isoformat(),
            })
            save_checkpoint(args.checkpoint_file, checkpoint)
            errors += 1
            continue

        manifestation_xml = adlib_sess.create_record_data(CID_API, "manifestations", session, current_priref, edit_entries)
        logger.info("manifestation_xml: %s", manifestation_xml)

        # Post manifestation
        manifestation_success, manifestation_reason = post_xml_to_cid(manifestation_xml, "manifestations", session)
        if manifestation_success:
            successes += 1
            logger.info("OK | (%d/%d) priref=%s", i + 1, total, current_priref)
        else:
            logger.error("FAIL | (%d/%d) priref=%s | reason=%s", i + 1, total, current_priref, manifestation_reason)
            checkpoint["failed_records"].append({
                "priref": str(current_priref),
                "post_type": "manifestation",
                "reason": manifestation_reason,
                "failed_at": datetime.now().isoformat(),
            })
            save_checkpoint(args.checkpoint_file, checkpoint)
            errors += 1

        # Save checkpoint after each successful record
        checkpoint["last_processed_priref"] = current_priref
        save_checkpoint(args.checkpoint_file, checkpoint)

    # Mark window as completed
    checkpoint["status"] = "completed"
    save_checkpoint(args.checkpoint_file, checkpoint)

    logger.info("SUMMARY: %d / %d succeeded | %d errors", successes, total, errors)
    logger.info("Window %s → %s completed.", window_start, window_end)
    logger.info(
        "========== EPG metadata script END ==============================================="
    )


if __name__ == "__main__":
    main()

