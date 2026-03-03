#!/usr/bin/env python3

"""
Adverts record creation pass one:
- Work through CSV of unique adverts
  with LLM enhanced descriptive metadata.
- Validate CSV row data with Pydantic
- Inform creation of Work and People
  record using the row data.

Dependencies:
2 week delay for TechEdge full
metadata enrichment and additional
parsing of TechEdge data through LLM
for cleaning/amendment to descriptive
columns.

Parser needs updating when CSV finalised

Only new adverts (decided by unique
film_code value) are to have new works
created. Creation pass two to make
manifestations for channel appearances

2026
"""

# Public packages
import os
import sys
from datetime import datetime
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


@tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_attempt(10))
def advertiser_exists(name: str) -> Optional[str]:
    """
    Sends request for advert hit
    JMW check this search field names
    """

    search = f'name="{name}"'
    try:
        hit_count, record = adlib.retrieve_record(
            CID_API, "people", search, "1"
        )
    except Exception as err:
        print(err)
        raise Exception

    print(f"Hits {hit_count}\n{record}")
    if hit_count is None:
        print(
            f"Unable to match People name: {name}"
        )
        return None
    if hit_count == 0:
        print(f"No match found for Name {name}")
        return False
    if "priref" in str(record):
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
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


def main():
    """
    Iterates through LLM cleaned CSV files in TechEdge folders of STORAGE
    extracts necessary data into variables. Checks if advert is repeat
    if yes - skip and note priref / film code
    if no - make work record
          - make people record if needed
    """

    if not utils.check_storage(STORAGE):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    LOGGER.info(
        "========== Adverts work documentation script STARTED ==============================================="
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

        # Work out parsing rows pydantic

        # Check for People record
        name = row.name or ""
        people_priref = advertiser_exists(name)
        if people_priref:
            LOGGER.info("People record already exists for Advertiser %s - %s", name, ppriref)
            continue
        
        # Create People
        people_values = []
        people_values.extend(rec_def)
        people_values.extend(people)
        ppriref = create_work(
            people_values,
        )

        if not ppriref:
            print(
                f"People record creation error for data: {people_values}"
            )
            continue

        LOGGER.info("New People record created for name %s - %s", name, ppriref)

        # Check if file exists
        film_code = row.film_code or ""
        wpriref = advert_exists_query(film_code)
        if wpriref:
            LOGGER.info("Work already exists for Advert %s - %s", film_code, wpriref)
            continue

        # Get defaults as lists of dictionary pairs
        rec_def, work_def, work_res_def, people = build_rec_details(row)

        # Create Work JMW - need people priref?
        work_values = []
        work_values.extend(rec_def)
        work_values.extend(work_def)
        work_values.extend(work_res_def)
        wpriref = create_work(
            work_values, ppriref
        )

        if not wpriref:
            print(
                f"Work creation error for data: {work_values}"
            )
            continue

        LOGGER.info("New work record created for film code %s - %s", film_code, wpriref)

    LOGGER.info(
        "========== Adverts work documentation script END =======================================================\n"
    )


def build_rec_details(row, ppriref):
    """
    Extraction of CSV data
    and create record layouts
    """

    title = row.brand or ""
    title_date_start = row.start_time or ""
    alternative_number = row.film_code or ""
    alternative_number.type = "Unique advert identifier - TechEdge"
    credit_name1 = row.advertiser or ""
    credit_name2 = row.agency or ""
    product_category = row.minor_category or ""
    description = row.description = "" # Possible space for LLM description

    # Organise thesaurus terms
    major = row.major_category or ""
    mid = row.mid_category or ""
    minor = row.minor_category or ""
    genre_dct = genre_match(major, mid, minor)

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
        {"title.language": "English"},
        {"title.type": "05_MAIN"},
    ]

    work = [
        {"record_type": "WORK"},
        {"worklevel_type": "MONOGRAPHIC"},
        {"work_type": "T"},
        {"title_date_start": title_date_start},
        {"title_date.type": "04_T"},
        {"nfa_category": "D"}, # JMW Is this needed? Non-fiction
        {"credit.name": credit_name1},
        {"credit.type": "Advertiser"},
        {"activity_type": "Sponsor"},
        {"party.class": "ORGANISATION"},
        {"source": "TechEdge adverts data supply"}
        {"credit.name": credit_name2},
        {"credit.type": "Advertising Agency"},
        {"activity_type": "Advertising Agency"},
        {"party.class": "ORGANISATION"},
        {"source": "TechEdge adverts data supply"},
        {"credit.name.lref": ppriref},
        {"credit.type": "Advertising Agency"},
        {"activity_type": "Advertising Agency"},
        {"party.class": "ORGANISATION"},
        {"source": "TechEdge adverts data supply"},
        {"product_category": product_category}
    ]

    work_restricted = [
        {"application_restriction": "MEDIATHEQUE"},
        {"application_restriction.date": str(datetime.datetime.now())[:10]},
        {"application_restriction.reason": "STRATEGIC"},
        {"application_restriction.duration": "PERM"},
        {"application_restriction.review_date": "2030-01-01"}, # JMW
        {"application_restriction.authoriser": "mcconnachies"}, # JMW
        {
            "application_restriction.notes": "Automated Advert creation - pending discussion"
        }, # JMW
    ]

    people = [
        {"name": holding_company},
        {"activity_type": "Sponsor"},
        {"party.class": "ORGANISATION"},
        {"source": "TechEdge adverts data supply"}
    ]

    title_date_start = datetime.strftime(datetime.strptime(row[1], "%d/%m/%Y"), "%Y-%m-%d")

    return record, work, work_restricted, people


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

    return work_id


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def people_record_creation():
    """
    Possibly create Advertiser
    people record here
    """
    pass


if __name__ == "__main__":
    main()
