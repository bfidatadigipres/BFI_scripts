#!/usr/bin/env python3

"""
Adverts record creation pass two:
- Iterate daily advert creation CSV
  looking for each independent advert
  timing, placement within advert block 
  and channel information
- Using Film Code link to single work
  inherit title from this(?) but copy
  original descriptive fields to UTB
  for long-term reference
- Consider quantity of manifestations
  linked to a single work, how we manage
  large volumes.

Dependencies:
2 week delay for TechEdge full
metadata enrichment.

Parser needs updating when CSV finalised

Consider:
If a new advert does not find
a match to Work - way to retry
individual CSV entries

2026
"""

# Public packages
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
from time import sleep
import tenacity
from typing import Optional

sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils
from parsers import techedge_csv as te

# Global variables
STORAGE = # Path to CSVs
LOG_PATH = os.environ.get("LOG_PATH")
CID_API = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("document_augmented_work_adverts")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "document_augmented_work_adverts.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


@tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_attempt(10))
def advert_exists_query(film_code: str) -> Optional[str]:
    """
    Sends request for advert hit
    """

    search = f'alternative_number="{film_code}"'
    try:
        hit_count, record = adlib.retrieve_record(
            CID_API, "works", search, "1", ["alternative_number.type"]
        )
    except Exception as err:
        print(err)
        raise Exception

    print(f"Hits {hit_count}\n{record}")
    if hit_count is None:
        print(
            f"Unable to match film code: {film_code}"
        )
        return None
    if hit_count == 0:
        print(f"No match found for Film Code {film_code}")
        return False
    if "alternative_number.type" in str(record):
        antype = adlib.retrieve_field_name(record[0], "alternative_number.type")[0]
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
        if "Unique advert identifier - TechEdge" == antype:
            return priref
        
    return None


def genre_match(major: str, mid: str, minor: str) -> Optional[str]:
    """
    Match major, mid, minor categories to thesaurus terms
    Unsure where these will sit, assume Work.
    """
    dict_matches = {
        "Entertainment & leisure": "THESAURUS",
        "Leisure activities": "THESAURUS",
        "Theatres musicals & plays": "THESAURUS",
    }

    genres = {}

    for k, v in dict_matches.items():
        if k == major:
            genres["Major"] = v
        if k == mid:
            genres["Middle"] = v
        if k == minor:
            genres["Minor"] = v
    return genres


def get_utc(date_start: str, start_time: str) -> Optional[str]:
    """
    Passes datetime through timezone change
    for London, adding +1 hours during BST
    Must receive data formatted %Y-%m-%d %H:%M:%S
    """
    format = "%Y-%m-%d %H:%M:%S"
    try:
        make_time = f"{date_start} {start_time}"
        dt_time = datetime.strptime(make_time, format).replace(tzinfo=ZoneInfo("Europe/London"))
        UTC_timestamp = datetime.strftime(dt_time.astimezone(ZoneInfo("UTC")), format)
    except Exception as err:
        print(err)
        UTC_timestamp = None

    return UTC_timestamp


def split_title(title_article):
    """
    An exception needs adding for "Die " as German language content
    This list is not comprehensive.
    """
    if title_article.startswith(
        (
            "A ",
            "An ",
            "Am ",
            "Al-",
            "As ",
            "Az ",
            "Bir ",
            "Das ",
            "De ",
            "Dei ",
            "Den ",
            "Der ",
            "Det ",
            "Di ",
            "Dos ",
            "Een ",
            "Eene",
            "Ei ",
            "Ein ",
            "Eine",
            "Eit ",
            "El ",
            "el-",
            "En ",
            "Et ",
            "Ett ",
            "Het ",
            "Il ",
            "Na ",
            "A'",
            "L'",
            "La ",
            "Le ",
            "Les ",
            "Los ",
            "The ",
            "Un ",
            "Une ",
            "Uno ",
            "Y ",
            "Yr ",
        )
    ):
        title_split = title_article.split()
        ttl = title_split[1:]
        title = " ".join(ttl)
        title_art = title_split[0]
        return title, title_art

    return title_article, ""


def main():
    """
    Iterates through .csv files in TechEdge folders of storage_path
    extracts necessary data into variables. Checks if advert is repeat
    if yes - make manifestation only and link to work_priref
    if no - need to consider skipping or seeking out work data from other CSV
    
    JMW - check if no requirement for different channel ads to be listed separately
    """
    if not utils.check_storage(STORAGE):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    LOGGER.info(
        "========== Adverts manifestation documentation script STARTED ==========================="
    )

    for row in te.iter_techedge_rows(STORAGE):
        if not utils.check_control("pause_scripts"):
            LOGGER.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")
        if not utils.cid_check(CID_API):
            LOGGER.warning("* Cannot establish CID session, exiting script")
            sys.exit("* Cannot establish CID session, exiting script")

        LOGGER.info("Processing row: %s", ", ".join(row))

        # Check if parent work exists
        film_code = row.film_code or ""
        wpriref = advert_exists_query(film_code)
        if not wpriref:
            LOGGER.warning("Work does not exist for Advert %s - %s", film_code, wpriref)
            continue

        # Get defaults as lists of dictionary pairs
        rec_def, man_def, item_def = build_defaults(row, wpriref)

        # Create CID manifestation record
        manifestation_values = []
        manifestation_values.extend(rec_def)
        manifestation_values.extend(man_def)
        mpriref = create_manifestation(wpriref, manifestation_values)

        if not mpriref:
            print(
                f"CID Manifestation priref not retrieved for manifestation: {mpriref}"
            )
            sys.exit("Exiting for failure to create new manifestations")


        # JMW - Create CID item record for these??
        # Still to decide access method / file type
        # Possibly just the first Work will have manifestation / item and no others
        item_values = []
        item_values.extend(rec_def)
        item_values.extend(item_def)
        item_data = create_cid_item_record(
            wpriref,
            mpriref,
            item_values,
            )
        print(f"item_object_number: {item_data}")

        if item_data is None:
            print(
                f"CID Item object number not retrieved for manifestation: {mpriref}"
            )
            print(
                f"*** Manual clean up needed for Manifestation {mpriref}"
            )
            continue
        if len(item_data[0]) == 0 or len(item_data[1]) == 0:
            print(
                f"Error retrieving Item record priref and object number. Skipping completion of this programme, manual clean up of records needed."
            )
            print(
                f"*** Manual clean up needed for Manifestation {mpriref}"
            )
            continue

    LOGGER.info(
        "========== Adverts documentation script END ===============================\n"
    )


def build_records(row):
    """
    Extraction of CSV data:
    "channel": row[0],
    "date": row[1]
    "start_time": row[2]
    "film_code": row[3],
    "break_code": row[4],
    "advertiser": row[5],
    "brand": row[6],
    "agency": row[7],
    "holding_company": row[8],
    "barb_before": row[9],
    "barb_after": row[10],
    "sales_house": row[11],
    "major_cat": row[12],
    "mid_cat": row[13],
    "minor_cat": row[14],
    "all_pib_rel": row[15],
    "all_pib_pos": row[16],
    "log_station": row[17],
    "impacts_a4": row[18],
    "whole_data_string": row[19]
    JMW - to check if these map from Stephen / Will we add whole original entry to Work utb?
    """

    title_art = row.brand or ""
    # JMW Likely to need any title splits for "A" or "The"?
    title, title_article = split_title(title_art)

    alternative_number = row.film_code or ""
    alternative_number.type = "Unique advert identifier - TechEdge"
    title_date_start = datetime.strftime(datetime.strptime(row.date, "%d/%m/%Y"), "%Y-%m-%d")
    transmission_start_time = row.start_time or ""
    utc_timestamp = get_utc(title_date_start, transmission_start_time)

    # Broadcast details
    channel = row.channel or ""
    for k, v in CHANNELS.items():
        if k == channel:
            broadcast_channel = v[0]
            broadcast_company = v[1]

    record = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {
            "input.notes": "TechEdge Adverts record creation - automated bulk documentation"
        }, # JMW - check with Stephen
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"grouping.lref": ""}, # JMW New grouping needed
        {"title": title},
        {"title.article": title_article},
        {"title.language": "English"},
        {"title.type": "05_MAIN"},
    ]

    manifestation = [
        {"record_type": "MANIFESTATION"},
        {"manifestationlevel_type": "TRANSMISSION"},
        {"format_high_level": "Video - Digital"},
        {"colour_manifestation": "C"},
        {"sound_manifestation": "SOUN"},
        {"transmission_date": title_date_start},
        {"transmission_start_time": transmission_start_time},
        {"UTC_timestamp": utc_timestamp},
        {"broadcast_channel": broadcast_channel},
        {"broadcast_company": broadcast_company},
        {"transmission_coverage": "DIT"},
        {"aspect_ratio": "16:9"},
        {"country_manifestation": "United Kingdom"},
        {
            "notes": "Manifestation representing the UK Freeview television advert of the Work." # JMW - check with Stephen
        },
        {"alternative_number": row[3]},
        {"alternative_number.type": "Unique advert identifier - TechEdge"},
        {"utb.content": row.pib_rel or ""},
        {"utb.fieldname": "PIB position"},
        {"utb.content": row.barb_before or ""},
        {"utb.fieldname": "BARB Prog Before"},
        {"utb.content": row.barb_after or ""},
        {"utb.fieldname": "BARB Prog After"},
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

    return record, work, work_restricted, people, manifestation, item


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_work(record: dict, work: dict, work_restriction: dict) -> Optional[str]:
    """
    Build the dictionary and pass to CID for XML conversion
    POST to Axiell and return Priref
    """
    work_values = []
    work_values.extend(record)
    work_values.extend(work)
    work_values.extend(work_restriction)
    title = work[0].get("title")

    work_id = work_rec = ""
    # Start creating CID Work record
    sleep(1)
    work_values_xml = adlib.create_record_data(CID_API, "works", "", work_values)
    if work_values_xml is None:
        return None
    try:
        sleep(1)
        LOGGER.info("Attempting to create Work record for item %s", title)
        work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
        print(f"create_work(): {work_rec}")
    except Exception as err:
        print(f"* Unable to create Work record for <{title}>\n{err}")
        LOGGER.warning(
            "%s\tUnable to create Work record for <%s>", title
        )
        LOGGER.warning(err)

    # Allow for retry if record priref creation crash:
    if len(work_rec) == 0:
        raise Exception("Recycle of API exception raised.")

    if "Duplicate key in unique index 'invno':" in str(work_rec):
        try:
            sleep(1)
            LOGGER.info(
                "Attempting to create Work record for item %s", title
            )
            work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
            print(f"create_work(): {work_rec}")
        except Exception as err:
            print(f"* Unable to create Work record for <{title}>\n{err}")
            LOGGER.warning(
                "Unable to create Work record for <%s>", title
            )
            LOGGER.warning(err)

    try:
        print("Populating work_id and object_number variables")
        work_id = adlib.retrieve_field_name(work_rec, "priref")[0]
        object_number = adlib.retrieve_field_name(work_rec, "object_number")[0]
        print(
            f"* Work record created with Priref {work_id} Object number {object_number}"
        )
        LOGGER.info("Work record created with priref %s", work_id)
    except (IndexError, TypeError, KeyError) as err:
        LOGGER.warning(
            "Failed to retrieve Priref from record created using: 'works', 'insertrecord' for %s",
            title,
        )
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)

    if not work_id:
        return None

    # JMW May need to append genres here

    return work_id


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_manifestation(work_priref: str, record: dict, manifestation: dict) -> Optional[str]:
    """
    Create a manifestation record,
    linked to work_priref
    """
    manifestation_id = title = ""
    for dct in manifestation:
        if "title" in str(dct):
            title = dct.get("title")

    manifestation_values = []
    manifestation_values.extend(record)
    manifestation_values.extend(manifestation)
    manifestation_values.append({"part_of_reference.lref": work_priref})

    # Convert into XML
    man_values_xml = adlib.create_record_data(
        CID_API, "manifestations", "", manifestation_values
    )
    print("=================================")
    print(manifestation_values)
    print(man_values_xml)
    print("=================================")
    if man_values_xml is None:
        return None
    try:
        sleep(1)
        LOGGER.info("Attempting to create Manifestation record for item %s", title)
        man_rec = adlib.post(CID_API, man_values_xml, "manifestations", "insertrecord")
        print(f"create_manifestation(): {man_rec}")
    except Exception as err:
        print(f"*** Unable to write manifestation record: {err}")
        LOGGER.warning(
            "Unable to write manifestation record <%s> %s", manifestation_id, err
        )

    # Allow for retry if record priref creation crash:
    if "Duplicate key in unique index 'invno':" in str(man_rec):
        try:
            sleep(1)
            LOGGER.info("Attempting to create Manifestation record for item %s", title)
            man_rec = adlib.post(
                CID_API, man_values_xml, "manifestations", "insertrecord"
            )
            print(f"create_manifestation(): {man_rec}")
        except Exception as err:
            print(f"*** Unable to write manifestation record: {err}")
            LOGGER.warning(
                "Unable to write manifestation record <%s> %s", manifestation_id, err
            )

    if man_rec is False:
        raise Exception("Recycle of API exception raised.")
    try:
        manifestation_id = adlib.retrieve_field_name(man_rec, "priref")[0]
        object_number = adlib.retrieve_field_name(man_rec, "object_number")[0]
        print(
            f"* Manifestation record created with Priref {manifestation_id} Object number {object_number}"
        )
        LOGGER.info(
            "Manifestation record created with priref %s",
            manifestation_id,
        )
    except (IndexError, KeyError, TypeError) as err:
        LOGGER.warning("Failed to retrieve Priref from record created for - %s", title)
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)

    return manifestation_id


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_cid_item_record(work_id: str, manifestation_id: str, record: dict, items: dict) -> Optional[str]:
    """
    Create CID Item record
    """
    item_id = title = item_object_number = ""
    for dct in items:
        if "title" in str(dct):
            title = dct.get("title")

    item_value = []
    item_value.extend(record)
    item_value.extend(items)
    item_value.append({"part_of_reference.lref": manifestation_id})

    item_values_xml = adlib.create_record_data(CID_API, "items", "", item_value)
    if item_values_xml is None:
        return None

    try:
        sleep(1)
        LOGGER.info(
            "Attempting to create CID item record for item %s", title
        )
        item_rec = adlib.post(CID_API, item_values_xml, "items", "insertrecord")
        print(f"create_cid_item_record(): {item_rec}")
    except Exception as err:
        LOGGER.warning(
            "PROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
            title,
        )
        print(f"** PROBLEM: Unable to create Item record for {err}")

    # Allow for retry if record priref creation crash:
    if "Duplicate key in unique index 'invno':" in str(item_rec):
        try:
            sleep(1)
            LOGGER.info(
                "Attempting to create CID item record for item %s", title
            )
            item_rec = adlib.post(CID_API, item_values_xml, "items", "insertrecord")
            print(f"create_cid_item_record(): {item_rec}")
        except Exception as err:
            LOGGER.warning(
                "%s\tPROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
                title,
            )
            print(f"** PROBLEM: Unable to create Item record for {title} {err}")

    if item_rec is False:
        raise Exception("Recycle of API exception raised.")
    try:
        item_id = adlib.retrieve_field_name(item_rec, "priref")[0]
        item_object_number = adlib.retrieve_field_name(item_rec, "object_number")[0]
        print(
            f"* Item record created with Priref {item_id} Object number {item_object_number}"
        )
        LOGGER.info("Item record created with priref %s", item_id)
    except (IndexError, KeyError, TypeError) as err:
        LOGGER.warning("Failed to retrieve Priref from record created %s", err)
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)
    if item_rec is None:
        LOGGER.warning(
            "PROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
            title,
        )
        print(f"** PROBLEM: Unable to create Item record for {title}")
        return None

    return item_object_number, item_id


if __name__ == "__main__":
    main()
