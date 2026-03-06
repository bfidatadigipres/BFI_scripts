#!/usr/bin/env python3

"""
Adverts record creation pass one:
- Work through CSV of unique adverts
  with LLM enhanced descriptive metadata.
- Validate CSV row data with Pydantic
- Inform creation of Work, People and
  single manifestation for the first ad
  using CSV row data.

Long-term dependencies:
2 week delay for TechEdge full
metadata enrichment and additional
parsing of TechEdge data through LLM
for cleaning/amendment to descriptive
columns.

Notes:
Parser needs updating when CSV finalised

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

# Global variable
STORAGE = os.path.join(os.environ.get("ADMIN"), "datasets")
LOG_PATH = os.environ.get("LOG_PATH")
CID_API = utils.get_current_api()
ADMIN = os.environ.get("ADMIN")
HOLDING_COMP_DOC = os.path.join(STORAGE, "techedge_holding_company_change.yaml")

# Setup logging
LOGGER = logging.getLogger("document_augmented_work_adverts")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "document_augmented_work_adverts.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

CHANNELS = {
    "ITV1": "ITV HD",
    "ITV1 HD": "ITV HD",
    "ITV2": "ITV2",
    "ITV3": "ITV3",
    "ITV4": "ITV4",
    "ITVBe": "ITV Be",
    "ITVQuiz": "ITV Be", # JMW We record QUIZ for a while as ITVBe do we leave?
    "CITV": "CiTV", # May not need this one
    "CH4": "Channel 4 HD",
    "More4": "More4",
    "E4": "E4",
    "Film4": "Film4",
    "Channel 5": "Channel 5 HD",
    "5": "Channel 5 HD",
    "5STAR": "5STAR",
}

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


def get_utc(date_start: str, start_time: str) -> Optional[str]:
    """
    Passes datetime through timezone change
    for London, adding +1 hours during BST
    Must receive data formatted %Y-%m-%d %H:%M:%S
    """
    fmt = "%Y-%m-%d %H:%M:%S"
    try:
        make_time = f"{date_start} {start_time}"
        dt_time = datetime.strptime(make_time, fmt).replace(tzinfo=ZoneInfo("Europe/London"))
        UTC_timestamp = datetime.strftime(dt_time.astimezone(ZoneInfo("UTC")), fmt)
    except Exception as err:
        print(err)
        UTC_timestamp = None

    return UTC_timestamp


def manage_product_category(major: str, mid: str, minor: str) -> Optional[str]:
    """
    Search for product_category entry that matches 'minor' entry
    If found return priref | If not found, fetch priref for product_category entry match
    If not present - create parent thesaurus entry using PROD_CAT priref
    If not present - create grandparent thesaurus entry using parent priref
    """
    search = f"term='{minor}' and source='TechEdge adverts data supply'"
    hits, rec = adlib.retrieve_record(
        CID_API, "thesaurus", search, "0"
    )
    if hits >= 1:
        minor_priref = adlib.retrieve_field_name(rec[0], "priref")
        if mid in str(rec[0].get("broader_term")):
            LOGGER.info("%s matched to thesaurus priref with broader term %s: %s", minor, mid, minor_priref)
            return minor_priref

    LOGGER.info("Advertiser product_catogory %s not found in thesaurus. Creating heirarchy.", minor)
    minordct = [
        {"term": minor},
        {"term.type": "PROD_CAT"},
        {"source": "TechEdge adverts data supply"} # Indexed but not open to API
    ]

    minor_xml = adlib.create_record_data(CID_API, "thesaurus", "", minordct)
    minor_rec = adlib.post(CID_API, minor_xml, "thesaurus", "insertrecord")
    minor_priref = adlib.retrieve_field_name(minor_rec, "priref")[0]
    if minor_priref:
        LOGGER.info("New thesaurus entry created for Product Category %s", minor)
    else:
        LOGGER.warning("Failed to create Thesaurus record for %s:\n%s", minor, minor_rec)
        return None, None, None

    search = f"term='{mid}' and source='TechEdge adverts data supply'"
    hits, rec = adlib.retrieve_record(
        CID_API, "thesaurus", search, "1"
    )
    if hits:
        mid_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        LOGGER.info("Mid catergory found already created, no further record creation required - %s priref %s", mid, mid_priref)
    else:
        middct = [
            {"term": mid},
            {"term.type": "PROD_CAT"},
            {"narrower_term.lref": minor_priref},
            {"source": "TechEdge adverts data supply"} # Indexed but not open to API
        ]
        mid_xml = adlib.create_record_data(CID_API, "thesaurus", "", middct)
        mid_rec = adlib.post(CID_API, mid_xml, "thesaurus", "insertrecord")
        mid_priref = adlib.retrieve_field_name(mid_rec, "priref")[0]
        if mid_priref:
            LOGGER.info("New broader term thesaurus entry created for %s: %s - priref %s", minor_priref, mid, mid_priref)
        else:
            LOGGER.warning("Failed to create broader term Thesaurus record: %s - %s", mid, mid_priref)
            mid_priref = None

    search = f"term='{major}' and source='TechEdge adverts data supply'"
    hits, rec = adlib.retrieve_record(
        CID_API, "thesaurus", search, "1"
    )
    if hits:
        maj_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        LOGGER.info("Major catergory found already created, no further record creation required - %s priref %s", major, maj_priref)
    else:
        majdct = [
            {"term": major},
            {"term.type": "PROD_CAT"},
            {"narrower_term.lref": mid_priref},
            {"source": "TechEdge adverts data supply"} # Indexed but not open to API
        ]
        maj_xml = adlib.create_record_data(CID_API, "thesaurus", "", majdct)
        maj_rec = adlib.post(CID_API, maj_xml, "thesaurus", "insertrecord")
        maj_priref = adlib.retrieve_field_name(maj_rec, "priref")[0]
        if maj_priref:
            LOGGER.info("New broader term thesaurus entry created for %s: %s - priref %s", mid_priref, major, maj_priref)
        else:
            LOGGER.warning("Failed to create broader term Thesaurus record: %s - %s", major, maj_priref)
            maj_priref = None

    LOGGER.info(
        "All thesaurus categories made for product categories:\n%s - priref %s\n%s - priref %s\n%s - priref %s",
        minor,
        minor_priref,
        mid,
        mid_priref,
        major,
        maj_priref
    )

    return minor_priref, mid_priref, maj_priref


def manage_advertiser_people(advertiser: str, holding_comp: str, agency: str) -> Optional[tuple[str, str, str]]:
    """
    Update Holding Company data when Advertiser child ownership
    appears to have changed for a TechEdge advertiser
    """

    search = f"name='{agency}' and source='TechEdge adverts data supply'"
    hits, rec = adlib.retrieve_record(
        CID_API, "people", search, "1"
    )
    if hits == 1:
        agency_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        LOGGER.info("Agency matched to name %s - %s.", agency, agency_priref)
    else:
        agency_dct = [
            {"name": agency},
            {"name.type": "CASTCREDIT"},
            {"activity_type": "Advertising Agency"},
            {"party.class": "ORGANISATION"},
            {"source": "TechEdge adverts data supply"}
        ]
        agency_xml = adlib.create_record_data(CID_API, "people", "", agency_dct)
        agency_rec = adlib.post(CID_API, agency_xml, "people", "insertrecord")
        agency_priref = adlib.retrieve_field_name(agency_rec, "priref")[0]
        if agency_priref:
            LOGGER.info("New Agency person record created for %s: %s", agency, agency_priref)
        else:
            LOGGER.warning("Failed to create Agency people record: %s - %s", agency, agency_priref)
            agency_priref = ""

    make_hc = False
    make_ad = False

    search = f"name='{advertiser}' and source='TechEdge adverts data supply'"
    hits, rec = adlib.retrieve_record(
        CID_API, "people", search, "1"
    )
    if hits == 1:
        ad_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        ad_parent_pri = adlib.retrieve_field_name(rec[0], "part_of.lref")[0]
        ad_parent = adlib.retrieve_field_name(rec[0], "part_of")[0]
        LOGGER.info("Advertiser matched to name %s - %s and parent priref found %s.", advertiser, ad_priref, ad_parent_pri)
    else:
        make_ad = True
        ad_priref = ad_parent_pri = ad_parent = ""

    search = f"name='{holding_comp}' and source='TechEdge adverts data supply'"
    hits, rec = adlib.retrieve_record(
        CID_API, "people", search, "1"
    )
    if hits == 1:
        hc_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        parts_priref = adlib.retrieve_field_name(rec[0], "parts.lref")
    else:
        make_hc = True
        hc_priref = parts_priref = ""

    if make_hc is False and make_ad is False:
        if hc_priref != ad_parent_pri:
            LOGGER.warning(
                "Holding Company retrieved priref %s does not match parent of retrieved Advertiser %s - Updating logs",
                hc_priref, ad_parent
            )
            make_hc = True
        elif ad_priref not in parts_priref:
            LOGGER.warning(
                "Advertiser record %s and parent Holding Company parts found %s - No parent/child relations evident - Updating log",
                ad_priref, parts_priref
            )
            make_hc = True
        else:
            LOGGER.info(
                "Found Holding Company and matching Advertiser data already created: %s - %s / %s - %s",
                advertiser, ad_priref, holding_comp, hc_priref
            )
            return agency_priref, hc_priref, ad_priref

    if make_hc is False and make_ad is True:
        # Make new Advertiser and link to Holding Company parent
        LOGGER.info("Advertiser not found but Holding Company found - making new Ad People and linking to parent")
        ad_dct = [
            {"name": advertiser},
            {"name.type": "CASTCREDIT"},
            {"activity_type": "Sponsor"},
            {"part_of.lref": hc_priref},
            {"party.class": "ORGANISATION"},
            {"source": "TechEdge adverts data supply"}
        ]

        ad_xml = adlib.create_record_data(CID_API, "people", "", ad_dct)
        ad_rec = adlib.post(CID_API, ad_xml, "people", "insertrecord")
        ad_priref = adlib.retrieve_field_name(ad_rec, "priref")[0]
        if ad_priref:
            LOGGER.info("New Agency person record created for %s: %s", advertiser, ad_priref)
        else:
            LOGGER.warning("Failed to create Agency people record: %s - %s", advertiser, ad_priref)
            ad_priref = None

        return agency_priref, hc_priref, ad_priref

    elif make_hc is True and make_ad is False:
        # Make new Holding Company and update Advertiser record with change to Holding company / new part.lref overwrite
        LOGGER.info("Advertiser %s found but change in Holding Company old %s - to new %s", advertiser, ad_parent, holding_comp)

        hc_dct = [
            {"name": holding_comp},
            {"activity_type": "Agency Holding Company"}, # JMW New enumeration requestd
            {"party.class": "ORGANISATION"},
            {"source": "TechEdge adverts data supply"}
        ]
        hc_xml = adlib.create_record_data(CID_API, "people", "", hc_dct)
        hc_rec = adlib.post(CID_API, hc_xml, "people", "insertrecord")
        hc_priref = adlib.retrieve_field_name(hc_rec, "priref")[0]
        if hc_priref:
            LOGGER.info("New Agency person record created for %s: %s", holding_comp, hc_priref)
        else:
            LOGGER.warning("Failed to create Agency people record: %s - %s", holding_comp, hc_priref)
            hc_priref = None

        ad_dct_update = [
            {"priref": ad_priref},
            {"part_of.lref": hc_priref},
            # JMW - FIELD AND NOTIFICATION DATA TO COME FROM LOUISE
            {"TBC": f"Holding company changed from {ad_parent} - {ad_parent_pri}"},
            {"TBC_DATE": str(datetime.now())[:19]} # YYYY-MM-DD HH:MM:SS
        ]
        ad_xml = adlib.create_record_data(CID_API, "people", "", ad_dct_update)
        ad_rec = adlib.post(CID_API, ad_xml, "people", "updaterecord")

    elif make_hc is True and make_ad is True:
        ad_dct = [
            {"name": advertiser},
            {"name.type": "CASTCREDIT"},
            {"activity_type": "Sponsor"},
            {"part_of.lref": hc_priref},
            {"party.class": "ORGANISATION"},
            {"source": "TechEdge adverts data supply"}
        ]
        ad_xml = adlib.create_record_data(CID_API, "people", "", ad_dct)
        ad_rec = adlib.post(CID_API, ad_xml, "people", "insertrecord")
        ad_priref = adlib.retrieve_field_name(ad_rec, "priref")[0]
        if ad_priref:
            LOGGER.info("New Agency person record created for %s: %s", advertiser, ad_priref)
        else:
            LOGGER.warning("Failed to create Agency people record: %s - %s", advertiser, ad_priref)
            ad_priref = None

        hc_dct = [
            {"name": holding_comp},
            {"activity_type": "Agency Holding Company"},
            {"party.class": "ORGANISATION"},
            {"parts.lref": ad_priref},
            {"source": "TechEdge adverts data supply"}
        ]
        hc_xml = adlib.create_record_data(CID_API, "people", "", hc_dct)
        hc_rec = adlib.post(CID_API, hc_xml, "people", "insertrecord")
        hc_priref = adlib.retrieve_field_name(hc_rec, "priref")[0]
        if hc_priref:
            LOGGER.info("New Agency person record created for %s: %s", holding_comp, hc_priref)
        else:
            LOGGER.warning("Failed to create Agency people record: %s - %s", holding_comp, hc_priref)
            hc_priref = None

    return agency_priref, hc_priref, ad_priref


def make_credit_data_for_work(ad_priref, agency_priref):
    """
    Append data into dict for Work POST
    JMW following credit.sequence_sort from
    STORA credit name creation
    """
    work_creds = [
        {"credit.name.lref": agency_priref},
        {"credit.type": "Advertising Agency"},
        {"credit.sequence": "05"},
        {"credit.sequence.sort": "20750005"},
        {"credit.section": "[normal credit]"},
        {"credit.name.lref": ad_priref},
        {"credit.type": "Advertiser"},
        {"credit.sequence": "10"},
        {"credit.sequence.sort": "20900010"},
        {"credit.section": "[normal credit]"}
    ]
    return work_creds


def main():
    """
    Iterates through LLM cleaned CSV files in TechEdge folders of STORAGE
    extracts necessary data into variables. Checks if advert is repeat
    if yes - skip and note priref / film codes
    if no - make work record
          - make people record if needed
          - make first Manifestation only for now
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

        # Check if unique film code already exists
        film_code = row.film_code or ""
        wpriref = advert_exists_query(film_code)
        if wpriref:
            LOGGER.info("Skipping: Work already exists for Advert %s - %s", film_code, wpriref)
            continue

        # Get defaults as lists of dictionary pairs
        rec_def, work_def, work_cred_dct, work_res_def, manifestation = build_rec_details(row)

        # Create Work JMW - need people priref?
        work_values = []
        work_values.extend(rec_def)
        work_values.extend(work_def)
        work_values.extend(work_cred_dct)
        work_values.extend(work_res_def)
        print(work_values)

        wpriref = create_work(work_values)
        if not wpriref:
            print(
                f"Work creation error for data: {work_values}"
            )
            continue
        LOGGER.info("New work record created for film code %s - %s", film_code, wpriref)

        man_values = []
        man_values.extend(rec_def)
        man_values.append({"part_of_reference.lref": wpriref})
        man_values.extend(manifestation)
        print(man_values)

        mpriref = create_manifestation(man_values)
        if not mpriref:
            print(f"Manifesatation creation error data data: {manifestation}")
            LOGGER.warning("Failed to make new manifestation and link to work: %s", wpriref)
        LOGGER.info("New manifestation record created %s - linked to work %s", mpriref, wpriref)

    LOGGER.info(
        "========== Adverts work documentation script END =======================================================\n"
    )


def time_to_secs(timestamp):
    """ Calculate seconds from string """
    dt = datetime.strptime(timestamp, "%H:%M:%S")
    return dt.hour * 3600 + dt.minute * 60 + dt.second


def get_duration_total_parts(title_date_start: str, transmission_start_time: str, alternative_number: str):
    """
    Fetch and read specific CSV row and following rows
    to get the duration and the part total value
    """
    csv_path = os.path.join(STORAGE, f"adverts_techedge_no_dupes/{title_date_start}_BFIExport.csv")
    rows = []
    with open(csv_path, "r", encoding="latin1") as file:
        for lines in file:
            parts = lines.strip().split(",")
            try:
                start = parts[2]
                alt_num = parts[3]
                part_total = parts[-3]
            except (IndexError, ValueError):
                continue

            rows.append({
                "start_time": start,
                "alt_num": alt_num,
                "part_total": part_total,
            })

        # Get part unit total value
        target_index = next(i for i, r in enumerate(rows) if r["alt_num"] == alternative_number and r["start_time"] == transmission_start_time)
        row = rows[target_index]
        print(row)
        part_unit = row["part_total"]
        part_unit_total = row["part_total"]

        for i in range(target_index + 1, len(rows)):
            if rows[i]["part_total"] <= rows[i-1]["part_total"]:
                break
            part_unit_total = rows[i]["part_total"]

        if part_unit == part_unit_total:
            # LOGGER.infp("Duration cannot be calculated for end item")
            return part_unit, part_unit_total, "", ""
        elif part_unit > part_unit_total:
            # LOGGER.warning("Code broken, part unit total %s is smaller than part unit %s", part_unit_total, part_unit)
            return part_unit, "", "", ""

        # LOGGER.info("Calculating duration using next row in sequence")
        dur_row = rows[target_index + 1]
        stop_time = dur_row["start_time"]
        dur_start_secs = time_to_secs(row["start_time"])
        duration_stop_secs = time_to_secs(stop_time)
        duration = duration_stop_secs - dur_start_secs
        rows = []

        return str(part_unit), str(part_unit_total), str(duration), stop_time


def build_rec_details(row):
    """
    Extraction of CSV data
    and create record layouts
    """

    title_art = row.brand or ""
    title, title_article = utils.split_title(title_art)
    title_date_start = row.start_time or ""
    alternative_number = row.film_code or ""
    alternative_number.type = "Unique advert identifier - TechEdge"
    title_date_start = datetime.strftime(datetime.strptime(row.date, "%d/%m/%Y"), "%Y-%m-%d")
    transmission_start_time = row.start_time or ""
    utc_timestamp = get_utc(title_date_start, transmission_start_time)

    # Broadcast details
    broadcast_company = broadcast_channel = ""
    channel = row.channel or ""
    for k, v in CHANNELS.items():
        if k == channel:
            broadcast_channel = v[0]
            broadcast_company = v[1]

    # Get part unit value total and duration
    part_unit, part_unit_total, duration, stop_time = get_duration_total_parts(
        title_date_start,
        transmission_start_time,
        alternative_number
    )

    record = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.now())[:10]},
        {"input.time": str(datetime.now())[11:19]},
        {"input.notes": "Automated bulk record creation using data supplied by TechEdge"},
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"grouping.lref": ""}, # JMW New grouping needed
        {"title": title},
        {"title.article": title_article},
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
    ]

    # Organise category terms
    major = row.major_category or None
    mid = row.mid_category or None
    minor = row.minor_category or None
    if major and mid and minor:
        minor_pri, mid_pri, maj_pri = manage_product_category(major, mid, minor)
        if minor_pri:
            work.append({"product_category": minor_pri})
        if mid_pri:
            work.append({"product_category": mid_pri})
        if maj_pri:
            work.append({"product_category": maj_pri})

    # Organise credit data
    advertiser = row.advertiser or ""
    holding_comp = row.hold_comp or ""
    agency = row.agency or ""
    agency_priref, hc_priref, ad_priref = manage_advertiser_people(advertiser, holding_comp, agency)
    work_cred_dct = make_credit_data_for_work(ad_priref, agency_priref)

    work_restricted = [
        {"application_restriction": "MEDIATHEQUE"},
        {"application_restriction.date": str(datetime.now())[:10]},
        {"application_restriction.reason": "STRATEGIC"},
        {"application_restriction.duration": "PERM"},
        {"application_restriction.review_date": "2030-01-01"}, # JMW
        {"application_restriction.authoriser": "mcconnachies"}, # JMW
        {"application_restriction.notes": "Automated Advert creation - pending discussion"}, # JMW
    ]

    manifestation = [
        {"record_type": "MANIFESTATION"},
        {"manifestationlevel_type": "TRANSMISSION"},
        {"format_high_level": "Video - Digital"},
        {"colour_manifestation": "C"},
        {"sound_manifestation": "SOUN"},
        {"transmission_date": title_date_start},
        {"transmission_start_time": transmission_start_time},
        {"transmission_end_time": stop_time},
        {"transmission_duration": duration},
        {"UTC_timestamp": utc_timestamp},
        {"broadcast_channel": broadcast_channel},
        {"broadcast_company": broadcast_company},
        {"transmission_coverage": "DIT"},
        {"aspect_ratio": "16:9"},
        {"country_manifestation": "United Kingdom"},
        {"notes": "Manifestation representing advert broadcast time and date."}, # JMW check with Stephen
        {"alternative_number.type": "Unique advert identifier - TechEdge"},
        {"alternative_number": alternative_number},
        {"utb.fieldname": "Advert sequence in commercial break block"},
        {"utb.content": f"{part_unit.zfill(2)}of{part_unit_total.zfill(2)}"},
        {"utb.fieldname": "BARB Prog Before"}, # JMW
        {"utb.content": row.barb_before or ""},
        {"utb.fieldname": "BARB Prog After"}, # JMW
        {"utb.content": row.barb_after or ""},
    ]

    return record, work, work_cred_dct, work_restricted, manifestation


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_work(work_values: dict) -> Optional[str]:
    """
    Build the dictionary and pass to CID for XML conversion
    POST to Axiell and return Priref
    """
    title = work_values[0].get("title")

    work_id = work_rec = ""
    # Start creating CID Work record
    sleep(1)
    work_values_xml = adlib.create_record_data(CID_API, "works", "", work_values)
    if work_values_xml is None:
        return None

    print("=================================")
    print(work_values)
    print(work_values_xml)
    print("=================================")

    try:
        LOGGER.info("Attempting to create Work record for item %s", title)
        work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
        print(f"create_work(): {work_rec}")
    except Exception as err:
        print(f"* Unable to create Work record for <{title}>\n{err}")
        LOGGER.warning("Unable to create Work record for <%s>", title)
        LOGGER.warning(err)

    # Allow for retry if record priref creation crash:
    if len(work_rec) == 0:
        sleep(1)
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
def create_manifestation(manifestation_values: dict) -> Optional[str]:
    """
    Create a manifestation record,
    linked to work_priref
    """
    title = manifestation_values[0].get("title")
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
            "Unable to write manifestation record <%s> %s", title, err
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
                "Unable to write manifestation record <%s> %s", title, err
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


if __name__ == "__main__":
    main()
