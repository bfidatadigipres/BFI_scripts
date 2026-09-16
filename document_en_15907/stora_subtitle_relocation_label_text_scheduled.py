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

CID_API = os.environ["CID_API4"]
LOG_PATH = os.environ["LOG_PATH"]

logger = logging.getLogger("stora_subtitle_relocation_label_text_scheduled")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "stora_subtitle_relocation_label_text_scheduled.log"))
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
) -> Optional[list[dict]]:
    query = safe_search_query(search_field, search_value)
    hits, records = adlib.retrieve_record(
        CID_API, database, query, "1"
    )
    if not hits or not records:
        return None
    return records


def post_xml_to_cid(edit_xml, database, session, search_value: str = "") -> tuple[bool, str]:
    try:
        record = adlib_sess.post_with_verify(
            CID_API, edit_xml, database, "updaterecord", session,
            search_value=search_value, max_retries=3, retry_delay=10
        )
    except Exception as err:
        if hasattr(err, "__cause__"):
            reason = f"Cause: {err.__cause__}"
        elif hasattr(err, "last_attempt"):
            reason = f"Underlying exception: {err.last_attempt.exception()}"
        else:
            reason = str(err)
        logger.error("Failed to post edit record: %s", reason)
        return False, reason

    logger.debug("Post response for database=%s: %s", database, record)

    if record is None:
        return False, "record is None after retries"
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


# --- Checkpoint functions ---

def load_checkpoint(checkpoint_file: str) -> dict:
    """Load checkpoint from file, or return default structure."""
    default_checkpoint = {
        "current_window_start": "2021-08-06",
        "current_window_end": None,
        "last_processed_priref": None,
        "last_completed_date": None,
        "status": "running",
        "updated_at": None,
        "failed_records": [],
    }
    
    if not os.path.exists(checkpoint_file):
        return default_checkpoint
    
    try:
        with open(checkpoint_file, "r") as f:
            content = f.read().strip()
            if not content:
                logger.warning("Checkpoint file is empty, using default checkpoint")
                return default_checkpoint
            return json.loads(content)
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON in checkpoint file, using default checkpoint: %s", e)
        return default_checkpoint

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
CODE_PATH = os.environ["CODE_DEPENDS"]
DEFAULT_CHECKPOINT_FILE = os.path.join(CODE_PATH, "document_en_15907/historical_stora_subtitle.json")

def main():
    parser = argparse.ArgumentParser(
        description="Automated subtitle relocation with checkpoint-based scheduling."
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
        help="Process at most N files per run (default: 0 = all files in window).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview which records would be skipped or pushed without making changes.",
    )
    args = parser.parse_args()

    if not args.checkpoint_file:
        parser.error("--checkpoint-file is required. Set DEFAULT_CHECKPOINT_FILE in script or pass via CLI.")

    END_DATE = "2025-12-31"

    logger.info(
        "========== Transfer subtitle fields script STARTED ==============================================="
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
        f"{safe_search_query('label.type', '*VTT')} and "
        f"input.date>='{window_start}' and input.date<'{window_end}')"
    )

    fields = ["label.type", "label.text", "label.source", "subtitle.text", "subtitle.type", "subtitle.source", "accessibility_resource", "input.date", "priref", "part_of_reference"]

    # Initial hit count
    if last_priref is None:
        initial_search = search_query
    else:
        initial_search = f"(priref>{last_priref}) and {search_query}"

    hits, _ = adlib.retrieve_record(CID_API, "items", initial_search, "1", fields=fields)
    logger.info("Remaining hits in window: %s", hits)

    if args.limit:
        hits = args.limit

    session = adlib_sess.create_session()
    total = hits * 2
    current_priref = last_priref
    successes = 0
    step = 0
    errors = 0

    if args.dry_run:
        logger.info("========== DRY RUN MODE - No changes will be made ==========")

    for i in range(hits):
        if current_priref is None:
            search = search_query
        else:
            search = f"(priref>{current_priref}) and {search_query}"

        clock.sleep(0.3)
        _, item_record = adlib.retrieve_record(CID_API, "items", search, "1", fields=fields)

        if not item_record:
            break

        priref_values = adlib.retrieve_field_name(item_record[0], "priref")
        if not priref_values:
            logger.error("Skipping: no priref found")
            errors += 1
            continue
        current_priref = priref_values[0]

        # Extract fields
        input_date = get_field(item_record[0], "input.date")
        subtitle_text = get_field(item_record[0], "label.text")
        subtitle_type = get_field(item_record[0], "label.type")
        subtitle_source = get_field(item_record[0], "label.source")

        if not all([input_date, subtitle_text, subtitle_type, subtitle_source]):
            logger.error("Skipping priref=%s: missing subtitle fields", current_priref)
            logger.error("subtitle_text: %s", subtitle_text)
            logger.error("subtitle_type: %s", subtitle_type)
            logger.error("subtitle_source: %s", subtitle_source)
            checkpoint["failed_records"].append({
                "priref": str(current_priref),
                "post_type": "items",
                "reason": "missing subtitle fields",
                "date": input_date or "unknown",
                "failed_at": datetime.now().isoformat(),
            })
            save_checkpoint(args.checkpoint_file, checkpoint)
            errors += 1
            continue

        # Pre-check: skip if item already has all subtitle fields populated
        existing_subtitle_text = get_field(item_record[0], "subtitle.text")
        existing_subtitle_type = get_field(item_record[0], "subtitle.type")
        existing_subtitle_source = get_field(item_record[0], "subtitle.source")
        if existing_subtitle_text and existing_subtitle_type and existing_subtitle_source:
            logger.info("SKIP priref=%s: already has subtitle data (text=%s, type=%s, source=%s)",
                        current_priref, existing_subtitle_text, existing_subtitle_type, existing_subtitle_source)
            successes += 1
            if not args.dry_run:
                checkpoint["last_processed_priref"] = current_priref
                checkpoint["last_completed_date"] = input_date
                save_checkpoint(args.checkpoint_file, checkpoint)
            continue

        edit_xml = build_subtitle_edit_xml(current_priref, input_date, subtitle_text, subtitle_source, subtitle_type)
        mani_priref = get_field(item_record[0], "Part_of.part_of_reference.priref")
        logger.info("manifestation priref: %s", mani_priref)

        # Pre-check: skip manifestation if accessibility_resource already set
        existing_accessibility = get_field(item_record[0], "accessibility_resource")
        manifestation_already_pushed = existing_accessibility == "SUBTITLES"

        if not manifestation_already_pushed:
            manifestation_xml = build_subtitle_edit_xml(mani_priref, "", "", "", "", True)
        logger.info("(%d/%d) priref=%s", i + 1, hits, current_priref)

        if args.dry_run:
            logger.info("DRY RUN | Would push ITEM for priref=%s", current_priref)
            if manifestation_already_pushed:
                logger.info("DRY RUN | SKIP MANIFESTATION: already has accessibility_resource=SUBTITLES | priref=%s", mani_priref)
            else:
                logger.info("DRY RUN | Would push MANIFESTATION for priref=%s", mani_priref)
            successes += 2
            continue

        # Post item
        step += 1
        success, reason = post_xml_to_cid(edit_xml, "items", session, search_value=f"priref='{current_priref}'")
        if success:
            logger.info("SUCCESS | ITEM PUSHED: OK | (%d/%d) priref=%s", step, total, current_priref)
            successes += 1
        else:
            logger.error("FAIL TO PUSH ITEM | (%d/%d) priref=%s | reason=%s", step, total, current_priref, reason)
            checkpoint["failed_records"].append({
                "priref": str(current_priref) if current_priref else "unknown priref",
                "post_type": "items",
                "reason": reason,
                "date": input_date,
                "failed_at": datetime.now().isoformat(),
            })
            save_checkpoint(args.checkpoint_file, checkpoint)
            errors += 1

        # Post manifestation
        step += 1
        if manifestation_already_pushed:
            logger.info("SKIP MANIFESTATION: already has accessibility_resource=SUBTITLES | priref=%s", mani_priref)
            successes += 1
        else:
            manifestation_success, manifestation_reason = post_xml_to_cid(
                manifestation_xml, "manifestations", session, search_value=f"priref='{mani_priref}'"
            )
            if manifestation_success:
                successes += 1
                logger.info("SUCCESS | MANIFESTATION | Manifestation Post Successful | (%d/%d) priref=%s", step, total, mani_priref)
            else:
                logger.error("FAIL TO PUSH MANIFESTATION| (%d/%d) priref=%s | reason=%s", step, total, current_priref, manifestation_reason)
                checkpoint["failed_records"].append({
                    "priref": str(mani_priref) if mani_priref else "unknown",
                    "post_type": "manifestation",
                    "reason": manifestation_reason,
                    "date": input_date,
                    "failed_at": datetime.now().isoformat(),
                })
                save_checkpoint(args.checkpoint_file, checkpoint)
                errors += 1

        # Save checkpoint after each successful record
        checkpoint["last_processed_priref"] = current_priref
        checkpoint["last_completed_date"] = input_date
        save_checkpoint(args.checkpoint_file, checkpoint)

    # Mark window as completed
    checkpoint["status"] = "completed"
    save_checkpoint(args.checkpoint_file, checkpoint)

    logger.info("SUMMARY: %d / %d succeeded | %d errors", successes, total, errors)
    logger.info("Window %s → %s completed.", window_start, window_end)
    logger.info(
        "========== Transfer subtitle fields script END ==============================================="
    )


if __name__ == "__main__":
    main()


