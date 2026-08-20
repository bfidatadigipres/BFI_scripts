import os
import sys
from datetime import datetime

sys.path.append(os.environ["CODE"])
import utils
import adlib_v3 as adlib
import adlib_v3_sess as adlib_sess
import shutil
import logging
import time

CID_API = os.environ["CID_API3"]
LOG_PATH = os.environ["LOG_PATH"]

logger = logging.getLogger("subtitle_fields_transfer")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "subtitle_fields_transfer.log"))
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
logger.info("Logger initialised")


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


def get_field(record: dict, field_name: str) -> Optional[str]:
    values = adlib.retrieve_field_name(record, field_name)
    return values[0] if values else None


def retrieve_single_record(
    database: str,
    search_field: str,
    search_value: str,
    fields: Optional[list[str]] = None,
) -> Optional[list[dict]]:
    query = safe_search_query(search_field, search_value)
    hits, records = adlib.retrieve_record(CID_API, database, query, "1", fields=fields)
    if not hits or not records:
        return None
    return records


def post_xml_to_cid(edit_xml) -> tuple[bool, str]:
    try:
        record = adlib_sess.post(CID_API, edit_xml, "items", "updaterecord", None)
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


def main():
    logger.info(
        "========== Transfer subtitle fields script STARTED ==============================================="
    )
    search_query = "(grouping.lref='398775' and label.type='*VTT' and input.date>'2022-09-01' and input.date<'2022-09-10')"
    fields = ["label.type", "label.text", "label.source", "input.date", "priref"]
    hits, item_record = adlib.retrieve_record(
        CID_API, "items", search_query, "1", fields=fields
    )
    logger.info("hits: %s", hits)

    total = hits
    current_priref = None
    successes = 0
    errors = 0

    for i in range(hits):
        if current_priref is None:
            search = search_query
        else:
            search = f"(priref>{current_priref}) and {search_query}"

        time.sleep(0.3)
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
        input_date = adlib.retrieve_field_name(item_record[0], "input.date")
        subtitle_text = adlib.retrieve_field_name(item_record[0], "label.text")
        subtitle_type = adlib.retrieve_field_name(item_record[0], "label.type")
        subtitle_source = adlib.retrieve_field_name(item_record[0], "label.source")

        if not all([input_date, subtitle_text, subtitle_type, subtitle_source]):
            logger.error("Skipping priref=%s: missing subtitle fields", current_priref)
            errors += 1
            continue
        now = datetime.now()
        item_edit_data = [
            {"edit.date": now.strftime("%Y-%m-%d")},
            {"edit.name": "datadigipres"},
            {"edit.notes": "Automated subtitle relocation project"},
            {"edit.time": now.strftime("%H:%M:%S")},
            {"subtitle.date": input_date[0]},
            {"subtitle.text": subtitle_text[0].replace("ï»¿", "")},
            {"subtitle.type": subtitle_type[0]},
            {"subtitle.source": subtitle_source[0]},
        ]

        edit_xml = adlib.create_grouped_data(current_priref, "Edit", [item_edit_data])
        logger.info("(%d/%d) priref=%s", i + 1, hits, current_priref)

        success, reason = post_xml_to_cid(edit_xml)
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

    logger.info("SUMMARY: %d / %d succeeded | %d errors", successes, total, errors)
    logger.info(
        "========== Transfer subtitle fields script END ==============================================="
    )


if __name__ == "__main__":
    main()
