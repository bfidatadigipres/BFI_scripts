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

2026
"""

# Public packages
import os
import sys
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import logging
from time import sleep
from typing import Optional, Iterator, List, Dict
import tenacity

sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils
from parsers import techedge_csv as te

# Global variable
STORAGE = os.path.join(os.environ.get("ADMIN"), "datasets")
CSV_PATH = os.path.join(
    STORAGE, "adverts_techedge_unique/Unique_adverts_BFIExport_CLEANED.csv"
)
LOG_PATH = os.environ.get("LOG_PATH")
CID_API = utils.get_current_api()
ADMIN = os.environ.get("ADMIN")
HOLDING_COMP_DOC = os.path.join(STORAGE, "techedge_holding_company_change.yaml")

# Setup logging
LOGGER = logging.getLogger("document_augmented_work_adverts")
HDLR = logging.FileHandler(
    os.path.join(LOG_PATH, "document_augmented_work_adverts.log")
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

CHANNELS = {
    "ITV1": ["ITV HD", "20425"],
    "ITV1 HD": ["ITV HD", "20425"],
    "ITV2": ["ITV2", "20425"],
    "ITV3": ["ITV3", "20425"],
    "ITV4": ["ITV4", "20425"],
    "ITVBe": ["ITV Be", "20425"],
    "ITVQuiz": ["ITV Be", "20425"],  # ITVQuiz recorded as `ITVBe`
    # "CITV": ["CiTV", "20425"], # TBC
    "CH4": ["Channel 4 HD", "73319"],
    "More4": ["More4", "73319"],
    "E4": ["E4", "73319"],
    "Film4": ["Film4", "73319"],
    "Channel 5": ["Channel 5 HD", "24404"],
    "5": ["Channel 5 HD", "24404"],
    "5STAR": ["5STAR", "24404"],
}


def working_day_check(dt: datetime) -> bool:
    """ Check for clash with working week """
    work_days = {0, 1, 2, 3, 4}
    start = time(8, 0, 0)
    end = time(20, 0, 0)
    
    if dt.weekday() not in work_days:
        return False
    current_time = dt.time()
    return start <= current_time <= end


@tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_attempt(10))
def advert_exists_query(film_code: str) -> Optional[str]:
    """
    Sends request for advert hit
    """

    search = f"(Df=WORK and alternative_number='{film_code}' and alternative_number.type='Unique advert identifier - TechEdge')"
    try:
        hit_count, record = adlib.retrieve_record(CID_API, "works", search, "0")
    except Exception as err:
        print(err)
        raise Exception

    print(f"Hits {hit_count}\n{record}")
    if hit_count is None:
        print(f"Unable to match film code: {film_code}")
        return None
    if hit_count == 0:
        print(f"No match found for Film Code {film_code}")
        return False
    if hit_count >= 1:
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
        return priref

    return None


def manifestation_exists_query(
    film_code: str, utc_timestamp: str, parent_priref: str
) -> Optional[str]:
    """
    Check if manifestation is a duplicate
    """
    search = f"(Df=MANIFESTATION and alternative_number='{film_code}' and UTC_timestamp='{utc_timestamp}')"
    try:
        hit_count, record = adlib.retrieve_record(
            CID_API, "manifestations", search, "0"
        )
    except Exception as err:
        print(err)
        raise Exception

    print(f"Hits {hit_count}\n{record}")
    if hit_count is None:
        print(
            f"Unable to match film code and UTC timestamp: {film_code} | {utc_timestamp}"
        )
        return None

    if hit_count == 0:
        print(
            f"No match found for Film Code {film_code} | UTC timestamp {utc_timestamp}"
        )
        return False

    if "TechEdge" in str(record) and parent_priref in str(record):
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
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
        dt_time = datetime.strptime(make_time, fmt).replace(
            tzinfo=ZoneInfo("Europe/London")
        )
        utc_timestamp = datetime.strftime(dt_time.astimezone(ZoneInfo("UTC")), fmt)
    except Exception as err:
        print(err)
        utc_timestamp = None

    return utc_timestamp


def get_existing_terms(
    term_type: str, priref: str, new_term: str
) -> List[Dict[str, str]]:
    """
    Call up thesaurus and retrieve all broader or
    narrower terms to allow update of new with
    existing terms. To avoid updaterecord overwrite
    """
    search = f"priref='{priref}"
    rec = adlib.retrieve_record(CID_API, "thesaurus", search, 1)[1]
    if not rec:
        return []

    entries = rec[0].get(term_type)
    if not entries:
        return []
    length = len(entries)
    LOGGER.info(
        "%s %s entries found in thesaurus record <%s>", length, term_type, priref
    )
    get_terms = []
    for term in entries:
        try:
            num = term.get("priref")[0].get("spans")[0].get("text")
            if num:
                get_terms.append(num)
        except (IndexError, TypeError):
            pass

    terms = []
    for t in get_terms:
        terms.append({f"{term_type}.lref": t})
    terms.append({f"{term_type}.lref": new_term})

    return terms


def manage_product_category(major: str, mid: str, minor: str) -> Optional[str]:
    """
    Search for product_category entry that matches 'minor' entry
    ASSUMPTION: If 'minor' is presents parents are made/linked
    If found return priref | If not found, fetch priref for product_category entry match
    If not present - create parent thesaurus entry using PROD_CAT priref
    If not present - create grandparent thesaurus entry using parent priref
    ASSUMPTION: Mid and Major could exist where Minor does not - check before creation
    """
    search = f"(term='{minor}' and term.type='PROD_CAT')"
    print(search)
    hits, rec = adlib.retrieve_record(CID_API, "thesaurus", search, "1")
    if hits == 1:
        minor_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
    else:
        minordct = [
            {"term": minor},
            {"term.type": "PROD_CAT"},
            {"term.status": "1"},
            {"source": "TechEdge adverts data supply"},
            {"record_access.user": "BFIiispublic"},
            {"record_access.rights": "0"},
            {"input.name": "datadigipres"},
            {"input.date": str(datetime.now())[:10]},
            {"input.time": str(datetime.now())[11:19]},
            {
                "input.notes": "Automated bulk record creation using data supplied by TechEdge"
            },
        ]
        sleep(0.25)
        minor_xml = adlib.create_record_data(CID_API, "thesaurus", "", minordct)
        minor_rec = adlib.post(CID_API, minor_xml, "thesaurus", "insertrecord")
        minor_priref = adlib.retrieve_field_name(minor_rec, "priref")[0]
        if minor_priref:
            LOGGER.info(
                "* New thesaurus entry created for Product Category '%s'", minor
            )
        else:
            LOGGER.warning(
                "Failed to create Thesaurus record for '%s':\n%s", minor, minor_rec
            )
            return None, None, None

    search = f"(term='{mid}' and term.type='PROD_CAT')"
    print(search)
    hits, rec = adlib.retrieve_record(CID_API, "thesaurus", search, "1")
    if hits == 1:
        mid_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        if minor_priref in str(rec):
            LOGGER.info("Minor term %s is linked to Mid term %s already", minor, mid)
        else:
            term_dct = get_existing_terms("narrower_term", mid_priref, minor_priref)
            mid_xml = adlib.create_record_data(
                CID_API, "thesaurus", mid_priref, term_dct
            )
            print(mid_xml)
            mid_rec = adlib.post(CID_API, mid_xml, "thesaurus", "updaterecord")
            print(mid_rec)
    else:
        middct = [
            {"term": mid},
            {"term.type": "PROD_CAT"},
            {"term.status": "1"},
            {"narrower_term.lref": minor_priref},
            {"source": "TechEdge adverts data supply"},
            {"record_access.user": "BFIiispublic"},
            {"record_access.rights": "0"},
            {"input.name": "datadigipres"},
            {"input.date": str(datetime.now())[:10]},
            {"input.time": str(datetime.now())[11:19]},
            {
                "input.notes": "Automated bulk record creation using data supplied by TechEdge"
            },
        ]
        sleep(0.25)
        mid_xml = adlib.create_record_data(CID_API, "thesaurus", "", middct)
        mid_rec = adlib.post(CID_API, mid_xml, "thesaurus", "insertrecord")
        mid_priref = adlib.retrieve_field_name(mid_rec, "priref")[0]
        if mid_priref:
            LOGGER.info(
                "* New broader term thesaurus entry created for '%s': '%s' - priref %s",
                minor,
                mid,
                mid_priref,
            )
        else:
            LOGGER.warning("Failed to create broader term Thesaurus record: '%s'", mid)
            mid_priref = None

    search = f"(term='{major}' and term.type='PROD_CAT')"
    print(search)
    hits, rec = adlib.retrieve_record(CID_API, "thesaurus", search, "1")
    if hits == 1:
        maj_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        if mid_priref in str(rec):
            LOGGER.info("Mid term %s is linked to Major term %s", mid, major)
        else:
            term_dct = get_existing_terms("narrower_term", maj_priref, mid_priref)
            maj_xml = adlib.create_record_data(
                CID_API, "thesaurus", maj_priref, term_dct
            )
            print(maj_xml)
            maj_rec = adlib.post(CID_API, maj_xml, "thesaurus", "updaterecord")
            print(maj_rec)
    else:
        majdct = [
            {"term": major},
            {"term.type": "PROD_CAT"},
            {"term.status": "1"},
            {"narrower_term.lref": mid_priref},
            {"source": "TechEdge adverts data supply"},
            {"record_access.user": "BFIiispublic"},
            {"record_access.rights": "0"},
            {"input.name": "datadigipres"},
            {"input.date": str(datetime.now())[:10]},
            {"input.time": str(datetime.now())[11:19]},
            {
                "input.notes": "Automated bulk record creation using data supplied by TechEdge"
            },
        ]
        sleep(0.25)
        maj_xml = adlib.create_record_data(CID_API, "thesaurus", "", majdct)
        maj_rec = adlib.post(CID_API, maj_xml, "thesaurus", "insertrecord")
        maj_priref = adlib.retrieve_field_name(maj_rec, "priref")[0]
        if maj_priref:
            LOGGER.info(
                "* New broader term thesaurus entry created for '%s': '%s' - priref %s",
                mid,
                major,
                maj_priref,
            )
        else:
            LOGGER.warning(
                "Failed to create broader term Thesaurus record: '%s'", major
            )
            maj_priref = None

    return minor_priref, mid_priref, maj_priref


def manage_advertiser_people(
    advertiser: str, holding_comp: str, agency: str
) -> Optional[tuple[str, str, str]]:
    """
    Update Holding Company data when Advertiser child ownership
    appears to have changed for a TechEdge advertiser
    People record parts, parts.category (both linked)
    and parts.date.start/end not linked <- API document change?
    """

    if agency is None:
        LOGGER.info("Skipping Agency as 'Missing' found in field")
        agency_priref = ""
    elif agency.lower() == "missing":
        LOGGER.info("Skipping Agency as 'Missing' found in field")
        agency_priref = ""
    else:
        search = f"(name='{agency.strip()}' and activity_type='Advertising Agency' and source='TechEdge adverts data supply')"
        hits, rec = adlib.retrieve_record(CID_API, "people", search, "1")
        if hits >= 1:
            agency_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
            LOGGER.info("Agency matched to name %s - %s.", agency, agency_priref)
        else:
            agency_dct = [
                {"name": agency},
                {"name.type": "CASTCREDIT"},
                {"activity_type": "Advertising Agency"},
                {"party.class": "ORGANISATION"},
                {"source": "TechEdge adverts data supply"},
                {"record_access.user": "BFIiispublic"},
                {"record_access.rights": "0"},
                {"record_access.reason": "SENSITIVE_LEGAL"},
                {"input.name": "datadigipres"},
                {"input.date": str(datetime.now())[:10]},
                {"input.time": str(datetime.now())[11:19]},
                {
                    "input.notes": "Automated bulk record creation using data supplied by TechEdge"
                },
            ]
            sleep(0.25)
            agency_xml = adlib.create_record_data(CID_API, "people", "", agency_dct)
            agency_rec = adlib.post(CID_API, agency_xml, "people", "insertrecord")
            agency_priref = adlib.retrieve_field_name(agency_rec, "priref")[0]
            if agency_priref:
                LOGGER.info(
                    "* New Agency person record created for '%s': %s",
                    agency,
                    agency_priref,
                )
            else:
                LOGGER.warning(
                    "Failed to create Agency people record: %s - %s",
                    agency,
                    agency_priref,
                )
                agency_priref = ""

    make_hc = False
    make_ad = False

    search = f"(name='{advertiser}' and activity_type='Sponsor' and source='TechEdge adverts data supply' and part_of='*')"
    hits, rec = adlib.retrieve_record(CID_API, "people", search, "0")
    if hits >= 1:
        ad_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        ad_parent_pri = adlib.retrieve_field_name(rec[0], "part_of.lref")[0]
        ad_parent = adlib.retrieve_field_name(rec[0], "part_of")[0] # Name field
        LOGGER.info(
            "Advertiser matched to name '%s' - '%s' and parent %s priref found '%s'.",
            advertiser,
            ad_priref,
            ad_parent,
            ad_parent_pri, #Correct
        )
    else:
        make_ad = True
        ad_priref = ad_parent_pri = ad_parent = ""

    hc_priref = ""
    # Check ad_parent_pri has name same as holding_comp
    if ad_parent_pri and ad_parent:
        if ad_parent.startswith(holding_comp) or holding_comp.startswith(ad_parent):
            hc_priref = ad_parent_pri

    if not hc_priref:
        search = f"(name='{holding_comp}' and activity_type='Sponsor' and source='TechEdge adverts data supply' and parts='*')"
        hits, rec = adlib.retrieve_record(CID_API, "people", search, "0")
        if hits >= 1:
            hc_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
        else:
            make_hc = True

    if ad_parent_pri and hc_priref:
        print(f"******* Advert parent priref {ad_parent_pri} / Holding Company priref {hc_priref} *********")
        if ad_parent_pri != hc_priref:
            # Overwrite the link to old holding company
            LOGGER.info("*** Holding Company %s update for existing Advertiser %s ***", hc_priref, ad_priref)
            ad_update = [{"part_of.lref": hc_priref}]
            sleep(0.25)
            ad_update_xml = adlib.create_record_data(CID_API, "people", ad_priref, ad_update)
            ad_update_rec = adlib.post(CID_API, ad_update_xml, "people", "updaterecord")
            if hc_priref in str(ad_update_rec):
                LOGGER.info(
                    "* New Holding Company priref updated to Advertiser People record"
                )
            else:
                LOGGER.warning(
                    "Failure to update new Holding Company priref to Advertiser record: %s",
                    ad_update_xml,
                )

            # Move connection broken above to relationship field
            date_now = str(datetime.now())[:10]
            old_hc_dct_update = [
                {"relationship.lref": ad_priref},
                {"relationship.date.end": date_now},
                {
                    "relationship.notes": f"TechEdge Holding Company changed from <{ad_parent_pri}> - <{hc_priref}>"
                },
            ]
            sleep(0.25)
            old_hc_xml = adlib.create_record_data(
                CID_API, "people", ad_parent_pri, old_hc_dct_update
            )
            LOGGER.info(old_hc_xml)
            hc_rec = adlib.post(CID_API, old_hc_xml, "people", "updaterecord")
            if date_now in str(hc_rec):
                LOGGER.info(
                    "* Old Holding Company relationship updated for Advertiser People record"
                )
            else:
                LOGGER.warning(
                    "Failure to update old Holding Company relationships for Advertiser record: %s",
                    ad_update_xml,
                )

    elif make_hc is False and make_ad is False:
        LOGGER.info(
            "Found Holding Company and matching Advertiser data already created: '%s' - %s / '%s' - %s",
            advertiser,
            ad_priref,
            holding_comp,
            hc_priref,
        )
        return agency_priref, hc_priref, ad_priref

    if make_hc is False and make_ad is True:
        # Make new Advertiser and link to Holding Company parent
        LOGGER.info(
            "Advertiser not found but Holding Company found - making new Ad People and linking to parent"
        )
        ad_dct = [
            {"name": advertiser},
            {"name.type": "CASTCREDIT"},
            {"activity_type": "Sponsor"},
            {"part_of.lref": hc_priref},
            {"party.class": "ORGANISATION"},
            {"source": "TechEdge adverts data supply"},
            {"record_access.user": "BFIiispublic"},
            {"record_access.rights": "0"},
            {"record_access.reason": "SENSITIVE_LEGAL"},
            {"input.name": "datadigipres"},
            {"input.date": str(datetime.now())[:10]},
            {"input.time": str(datetime.now())[11:19]},
            {
                "input.notes": "Automated bulk record creation using data supplied by TechEdge"
            },
        ]
        sleep(0.25)
        ad_xml = adlib.create_record_data(CID_API, "people", "", ad_dct)
        ad_rec = adlib.post(CID_API, ad_xml, "people", "insertrecord")
        ad_priref = adlib.retrieve_field_name(ad_rec, "priref")[0]
        if ad_priref:
            LOGGER.info(
                "* New Agency person record created for '%s': %s", advertiser, ad_priref
            )
        else:
            LOGGER.warning(
                "Failed to create Agency people record: '%s' - %s",
                advertiser,
                ad_priref,
            )
            ad_priref = None

        return agency_priref, hc_priref, ad_priref

    if make_hc is True and make_ad is False:
        # Make new Holding Company and update Advertiser record with change to Holding company / new part.lref overwrite
        LOGGER.info(
            "Advertiser '%s' found but change in Holding Company old '%s' - to new '%s'",
            advertiser,
            ad_parent,
            holding_comp,
        )

        hc_dct = [
            {"name": holding_comp},
            {"activity_type": "Sponsor"},
            {"party.class": "ORGANISATION"},
            {"source": "TechEdge adverts data supply"},
            {"record_access.user": "BFIiispublic"},
            {"record_access.rights": "0"},
            {"record_access.reason": "SENSITIVE_LEGAL"},
            {"input.name": "datadigipres"},
            {"input.date": str(datetime.now())[:10]},
            {"input.time": str(datetime.now())[11:19]},
            {
                "input.notes": "Automated bulk record creation using data supplied by TechEdge"
            },
        ]
        sleep(0.25)
        hc_xml = adlib.create_record_data(CID_API, "people", "", hc_dct)
        hc_rec = adlib.post(CID_API, hc_xml, "people", "insertrecord")
        hc_priref = adlib.retrieve_field_name(hc_rec, "priref")[0]
        if hc_priref:
            LOGGER.info(
                "* New Holding Company person record created for '%s': %s",
                holding_comp,
                hc_priref,
            )
        else:
            LOGGER.warning(
                "Failed to create Holding Company people record: '%s' - %s",
                holding_comp,
                hc_priref,
            )
            hc_priref = None

    if make_hc is True and make_ad is True:
        ad_dct = [
            {"name": advertiser},
            {"name.type": "CASTCREDIT"},
            {"activity_type": "Sponsor"},
            {"party.class": "ORGANISATION"},
            {"source": "TechEdge adverts data supply"},
            {"record_access.user": "BFIiispublic"},
            {"record_access.rights": "0"},
            {"record_access.reason": "SENSITIVE_LEGAL"},
            {"input.name": "datadigipres"},
            {"input.date": str(datetime.now())[:10]},
            {"input.time": str(datetime.now())[11:19]},
            {
                "input.notes": "Automated bulk record creation using data supplied by TechEdge"
            },
        ]
        sleep(0.25)
        ad_xml = adlib.create_record_data(CID_API, "people", "", ad_dct)
        ad_rec = adlib.post(CID_API, ad_xml, "people", "insertrecord")
        ad_priref = adlib.retrieve_field_name(ad_rec, "priref")[0]
        if ad_priref:
            LOGGER.info(
                "* New Advertiser person record created for '%s': %s",
                advertiser,
                ad_priref,
            )
        else:
            LOGGER.warning(
                "Failed to create Advertiser people record: '%s' - %s",
                advertiser,
                ad_priref,
            )
            ad_priref = None

        hc_dct = [
            {"name": holding_comp},
            {"activity_type": "Sponsor"},
            {"party.class": "ORGANISATION"},
            {"parts.lref": ad_priref},
            {"source": "TechEdge adverts data supply"},
            {"record_access.user": "BFIiispublic"},
            {"record_access.rights": "0"},
            {"record_access.reason": "SENSITIVE_LEGAL"},
            {"input.name": "datadigipres"},
            {"input.date": str(datetime.now())[:10]},
            {"input.time": str(datetime.now())[11:19]},
            {
                "input.notes": "Automated bulk record creation using data supplied by TechEdge"
            },
        ]
        sleep(0.25)
        hc_xml = adlib.create_record_data(CID_API, "people", "", hc_dct)
        hc_rec = adlib.post(CID_API, hc_xml, "people", "insertrecord")
        hc_priref = adlib.retrieve_field_name(hc_rec, "priref")[0]
        if hc_priref:
            LOGGER.info(
                "* New Holding Company person record created for '%s': %s",
                holding_comp,
                hc_priref,
            )
        else:
            LOGGER.warning(
                "Failed to create Holding Company people record: '%s' - %s",
                holding_comp,
                hc_priref,
            )
            hc_priref = None

    return agency_priref, hc_priref, ad_priref


def make_credit_data_for_work(ad_priref, agency_priref, wpriref):
    """
    Append data into dict for Work POST
    Not using credit.sequence_sort following
    STORA credit name creation method
    """
    work_creds = []
    if agency_priref != "":
        work_creds.append(
            {
                "credit.name.lref": agency_priref,
                "credit.type": "Advertising Agency",
                "credit.sequence": "05",
                "credit.section": "[normal credit]",
            }
        )
    if ad_priref != "":
        work_creds.append(
            {
                "credit.name.lref": ad_priref,
                "credit.type": "Advertiser",
                "credit.sequence": "10",
                "credit.section": "[normal credit]",
            }
        )

    cred_xml = adlib.create_grouped_data(wpriref, "credits", work_creds)
    return cred_xml


def make_utb_data_for_man(row, mpriref):
    """
    Append data into dict for Work POST
    Not using credit.sequence_sort following
    STORA credit name creation method
    """
    title_date_start = datetime.strftime(
        datetime.strptime(row.date, "%d/%m/%Y"), "%Y-%m-%d"
    )
    part_unit, part_unit_total, _, _ = get_duration_total_parts(
        title_date_start, row.start_time, row.film_code
    )

    utb_dct = []
    if part_unit and part_unit_total:
        utb_dct.append(
            {
                "utb.fieldname": "Advert sequence in commercial break block",
                "utb.content": f"{part_unit}of{part_unit_total}",
            }
        )
    utb_dct.append(
        {
            "utb.fieldname": "Programme before (BARB via TechEdge)",
            "utb.content": row.barb_before,
        }
    )
    utb_dct.append(
        {
            "utb.fieldname": "Programme after (BARB via TechEdge)",
            "utb.content": row.barb_after,
        }
    )

    if len(row.original) > 1:
        orig_list = ", ".join(
            str(row.original).rsplit(":", maxsplit=1)[-1].strip().split("-")
        )
        utb_dct.append(
            {
                "utb.fieldname": "Original Advertiser, Brand, Agency and Holding Company values from TechEdge",
                "utb.content": orig_list,
            }
        )
    print(utb_dct)
    utb_xml = adlib.create_grouped_data(mpriref, "utb", utb_dct)
    return utb_xml


def date_range(start_date: str, end_date: str) -> Iterator[str]:
    """
    Set date range, and yield one
    at a time back to main.
    Args received must be:
    datetime.date(2015, 1, 1)
    """

    days = int((end_date - start_date).days)
    for n in range(days):
        yield str(start_date + timedelta(n))


def main():
    """
    Iterates through LLM cleaned CSV supply (single or date dependent)
    extracts necessary data into variables. Checks if Work advert exists
    if yes - skip Work creation and create manifestation if needed
    if no - make work record
          - make people record if needed
          - make Manifestation
    """

    if not utils.check_storage(STORAGE):
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not utils.check_control("pause_scripts"):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if working_day_check(datetime.now()):
        sys.exit("Exiting: Cannot operate in working hours")
    LOGGER.info(
        "========== Adverts work documentation script STARTED ==============================================="
    )

    for row in te.iter_techedge_rows(CSV_PATH):
        if working_day_check(datetime.now()):
            LOGGER.info("Exiting: Cannot operate in working hours")
            sys.exit("Exiting: Cannot operate in working hours")
        first_showing = False
        if not utils.check_control("pause_scripts"):
            LOGGER.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")
        if not utils.cid_check(CID_API):
            LOGGER.warning("* Cannot establish CID session, exiting script")
            sys.exit("* Cannot establish CID session, exiting script")

        # Check if unique film code already exists
        film_code = row.film_code
        wpriref = advert_exists_query(film_code)
        if wpriref is False:
            LOGGER.info(
                "Processing row: %s, %s, %s, %s, %s, %s, %s, %s,",
                row.channel,
                row.date,
                row.start_time,
                row.film_code,
                row.advertiser,
                row.brand,
                row.agency,
                row.hold_comp,
            )

            # Get defaults as lists of dictionary pairs
            first_showing = True
            rec_def, work_def, work_res_def, _ = build_rec_details(row)

            work_values = []
            work_values.extend(rec_def)
            work_values.extend(work_def)
            work_values.extend(work_res_def)
            print(work_values)

            wpriref = create_work(row, work_values)
            if not wpriref:
                print(f"Work creation error for data: {work_values}")
                continue

        title_date_start = datetime.strftime(
            datetime.strptime(row.date, "%d/%m/%Y"), "%Y-%m-%d"
        )
        utc_timestamp = get_utc(title_date_start, row.start_time)
        mpriref = manifestation_exists_query(film_code, utc_timestamp, wpriref)
        if mpriref is False:
            LOGGER.info(
                "Manifestation match not found '%s' - %s %s",
                row.brand,
                row.date,
                row.start_time,
            )

            rec_def, _, _, manifestation = build_rec_details(row)
            man_values = []
            man_values.extend(rec_def)
            man_values.append({"part_of_reference.lref": wpriref})
            man_values.extend(manifestation)
            print(man_values)

            mpriref = create_manifestation(first_showing, row, man_values)
            if not mpriref:
                print(f"Manifesatation creation error data data: {manifestation}")
                LOGGER.warning(
                    "Failed to make new manifestation and link to work: %s\n", wpriref
                )
        else:
            print("SKIPPING: Manifestation exists for this Ad.")

    LOGGER.info(
        "========== Adverts work documentation script END =======================================================\n"
    )


def time_to_secs(timestamp):
    """Calculate seconds from string"""
    dt = datetime.strptime(timestamp, "%H:%M:%S")
    return dt.hour * 3600 + dt.minute * 60 + dt.second


def convert_transmission_time(transmission_start_time: str) -> str:
    """
    Handle cases where times supplied greater
    than 23:59:59, eg 27:35:50
    """
    hours = int(transmission_start_time.split(":")[0])
    if hours > 23:
        start_time_int = int(transmission_start_time.split(":")[0]) - 24
        adjusted_start_time = ":".join(
            [str(start_time_int).zfill(2)] + transmission_start_time.split(":")[1:]
        )
        return adjusted_start_time
    else:
        return transmission_start_time


def get_duration_total_parts(
    title_date_start: str, transmission_start_time: str, alternative_number: str
):
    """
    Fetch and read specific CSV row and following rows
    to get the duration and the part total value
    """
    csv_path = os.path.join(
        STORAGE, f"adverts_techedge_no_dupes/{title_date_start}_BFIExport.csv"
    )
    print(f"Targeting path for next data: {csv_path}")
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as file:
        for lines in file:
            parts = lines.strip().split(",")
            try:
                start = parts[2]
                alt_num = parts[3]
                part_total = parts[-3]
            except (IndexError, ValueError):
                continue

            rows.append(
                {
                    "start_time": start,
                    "alt_num": alt_num,
                    "part_total": part_total,
                }
            )

        # Get part unit total value
        target_index = next(
            (
                i
                for i, r in enumerate(rows)
                if r["alt_num"] == alternative_number
                and convert_transmission_time(r["start_time"])
                == transmission_start_time
            ),
            None,
        )
        print(target_index)
        if target_index is None:
            return None, None, None, None
        row = rows[target_index]
        part_unit = int(row["part_total"])
        part_unit_total = int(row["part_total"])

        for i in range(target_index + 1, len(rows)):
            if int(rows[i]["part_total"]) <= int(rows[i - 1]["part_total"]):
                break
            part_unit_total = int(rows[i]["part_total"])
        if int(part_unit) == int(part_unit_total):
            LOGGER.info("Duration cannot be calculated for end item")
            return str(part_unit).zfill(2), str(part_unit_total).zfill(2), "", ""
        if int(part_unit) > int(part_unit_total):
            LOGGER.warning(
                "Code broken, part unit total %s is smaller than part unit %s",
                part_unit_total,
                part_unit,
            )
            return str(part_unit).zfill(2), "", "", ""

        dur_row = rows[target_index + 1]
        stop_time = dur_row["start_time"]
        converted_stop_time = convert_transmission_time(stop_time)
        converted_start_time = convert_transmission_time(row["start_time"])
        dur_start_secs = time_to_secs(converted_start_time)
        duration_stop_secs = time_to_secs(converted_stop_time)
        duration = duration_stop_secs - dur_start_secs
        rows = []

        return (
            str(part_unit).zfill(2),
            str(part_unit_total).zfill(2),
            str(duration),
            converted_stop_time,
        )


def build_rec_details(row):
    """
    Extraction of CSV data
    and create record layouts
    """

    title_art = row.brand or ""
    title, title_article = utils.split_title(title_art)
    title_date_start = row.start_time
    alternative_number = row.film_code
    title_date_start = datetime.strftime(
        datetime.strptime(row.date, "%d/%m/%Y"), "%Y-%m-%d"
    )
    transmission_start_time = row.start_time
    utc_timestamp = get_utc(title_date_start, transmission_start_time)

    # Broadcast details
    broadcast_company = broadcast_channel = ""
    channel = row.channel or ""
    for k, v in CHANNELS.items():
        if k == channel:
            broadcast_channel = v[0]
            broadcast_company = v[1]

    # Get part unit value total and duration
    _, _, duration, stop_time = get_duration_total_parts(
        title_date_start, transmission_start_time, alternative_number
    )

    record = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.now())[:10]},
        {"input.time": str(datetime.now())[11:19]},
        {
            "input.notes": "Automated bulk record creation using data supplied by TechEdge"
        },
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {
            "grouping.lref": "402585"
        },  # Digital Acquisition: Off-Air TV Recording: Automated - Adverts
        {"title": title},
        {"title.article": title_article},
        {"title.language": "English"},
        {"title.type": "05_MAIN"},
        {"alternative_number.type": "Unique advert identifier - TechEdge"},
        {"alternative_number": alternative_number},
    ]

    work = [
        {"record_type": "WORK"},
        {"worklevel_type": "MONOGRAPHIC"},
        {"work_type": "T"},
        {"content.genre.lref": "110138"},  # Adverts
        {"title_date_start": title_date_start},
        {"title_date.type": "04_T"},
        {"nfa_category": "D"},
    ]

    # Organise category terms
    major = row.major_category or None
    mid = row.mid_category or None
    minor = row.minor_category or None
    if major and mid and minor:
        minor_pri, mid_pri, maj_pri = manage_product_category(major, mid, minor)
        if minor_pri:
            work.append({"product_category.lref": minor_pri})
        else:
            LOGGER.warning("Minor product category priref absent!")
        if mid_pri:
            work.append({"product_category.lref": mid_pri})
        else:
            LOGGER.warning("Mid product category priref absent!")
        if maj_pri:
            work.append({"product_category.lref": maj_pri})
        else:
            LOGGER.warning("Major product category priref absent!")

    work_restricted = [
        {"application_restriction": "MEDIATHEQUE"},
        {"application_restriction.date": str(datetime.now())[:10]},
        {"application_restriction.reason": "STRATEGIC"},
        {"application_restriction.duration": "PERM"},
        {"application_restriction.review_date": "2030-01-01"},
        {"application_restriction.authoriser": "mcconnachies"},
        {
            "application_restriction.notes": "Automated Advert creation - pending discussion"
        },
    ]

    manifestation = [
        {"record_type": "MANIFESTATION"},
        {"manifestationlevel_type": "TRANSMISSION"},
        {"language.lref": "74129"},
        {"language.type": "DIALORIG"},
        {"format_high_level": "Video - Digital"},
        {"colour_manifestation": "C"},
        {"sound_manifestation": "SOUN"},
        {"transmission_date": title_date_start},
        {"transmission_start_time": transmission_start_time},
        {"UTC_timestamp": utc_timestamp},
        {"broadcast_channel": broadcast_channel},
        {"broadcast_company.lref": broadcast_company},
        {"transmission_coverage": "DIT"},
        {"aspect_ratio": "16:9"},
        {"country_manifestation": "United Kingdom"},
    ]
    if stop_time:
        manifestation.append({"transmission_end_time": stop_time})
    if duration:
        manifestation.append({"runtime_seconds": duration})

    return record, work, work_restricted, manifestation


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_work(row, work_values: dict) -> Optional[str]:
    """
    Build the dictionary and pass to CID for XML conversion
    POST to Axiell and return Priref
    """
    title = work_values[8].get("title")

    work_id = work_rec = ""
    # Start creating CID Work record
    sleep(0.25)
    work_values_xml = adlib.create_record_data(CID_API, "works", "", work_values)
    if work_values_xml is None:
        return None

    print("=================================")
    print(work_values)
    print(work_values_xml)
    print("=================================")
    try:
        sleep(0.25)
        LOGGER.info("Attempting to create Work record for item '%s'...", title)
        work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
        print(f"create_work(): {work_rec}")
    except Exception as err:
        print(f"* Unable to create Work record for <{title}>\n{err}")
        LOGGER.warning("Unable to create Work record for '%s'", title)
        LOGGER.warning(err)

    # Allow for retry if record priref creation crash:
    if len(work_rec) == 0:
        sleep(0.25)
        raise Exception("Recycle of API exception raised.")

    if "Duplicate key in unique index 'invno':" in str(work_rec):
        try:
            sleep(0.25)
            LOGGER.info("Attempting to create Work record for item %s", title)
            work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
            print(f"create_work(): {work_rec}")
        except Exception as err:
            print(f"* Unable to create Work record for <{title}>\n{err}")
            LOGGER.warning("Unable to create Work record for <%s>", title)
            LOGGER.warning(err)

    try:
        print("Populating work_id and object_number variables")
        work_id = adlib.retrieve_field_name(work_rec, "priref")[0]
        object_number = adlib.retrieve_field_name(work_rec, "object_number")[0]
        print(
            f"* Work record created with Priref {work_id} Object number {object_number}"
        )
        LOGGER.info("* Work record created with priref %s", work_id)
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

    # Append work credits here
    advertiser = row.advertiser
    holding_comp = row.hold_comp
    agency = row.agency
    agency_priref, _, ad_priref = manage_advertiser_people(
        advertiser, holding_comp, agency
    )
    work_cred_xml = make_credit_data_for_work(ad_priref, agency_priref, work_id)

    print("=================================")
    print(work_cred_xml)
    print("=================================")

    try:
        sleep(0.25)
        # adlib.write_lock(CID_API, work_id, "works")
        LOGGER.info("Attempting to create credit data for Work record '%s'...", work_id)
        work_rec = adlib.post(CID_API, work_cred_xml, "works", "updaterecord")
        print(f"create_work(): {work_rec}")
    except Exception as err:
        # adlib.unlock_record(CID_API, work_id, "works")
        print(f"* Unable to create Work record for <{title}>\n{err}")
        LOGGER.warning("Unable to create Work record for <%s>", title)
        LOGGER.warning(err)
    if advertiser in str(work_rec):
        LOGGER.info("Successfully updated Advert credit data to work.")

    return work_id


def over_two_weeks(first_showing: bool, date_start: str) -> bool:
    """
    JMW remove when BAU work starts
    Temporary function to be removed
    for notes fixed to 'first showing'
    """
    if first_showing is False:
        return False
    date_obj = datetime.strptime(date_start, "%d/%m/%Y")
    start_dt = datetime(2016, 1, 1) + timedelta(weeks=2)
    return date_obj > start_dt


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_manifestation(
    first_showing, row, manifestation_values: dict
) -> Optional[str]:
    """
    Create a manifestation record,
    linked to work_priref
    """

    # JMW BAU just check first_showing is True for "first" addition
    confirm = over_two_weeks(first_showing, row.date)
    if confirm is False:
        manifestation_values.append(
            {"notes": "Manifestation representing advert broadcast time and date."}
        )
    else:
        manifestation_values.append(
            {
                "notes": "Manifestation representing advert first broadcast time and date."
            }
        )

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
        sleep(0.25)
        LOGGER.info(
            "Attempting to create Manifestation record for item '%s'...", row.brand
        )
        man_rec = adlib.post(CID_API, man_values_xml, "manifestations", "insertrecord")
        print(f"create_manifestation(): {man_rec}")
    except Exception as err:
        print(f"Unable to write manifestation record: {err}")
        LOGGER.warning("Unable to write manifestation record '%s'\n%s", row.brand, err)

    # Allow for retry if record priref creation crash:
    if "Duplicate key in unique index 'invno':" in str(man_rec):
        try:
            sleep(0.25)
            LOGGER.info(
                "Retry creation of Manifestation record for item '%s'...", row.brand
            )
            man_rec = adlib.post(
                CID_API, man_values_xml, "manifestations", "insertrecord"
            )
            print(f"create_manifestation(): {man_rec}")
        except Exception as err:
            print(f"Unable to write manifestation record: {err}")
            LOGGER.warning(
                "Unable to write manifestation record '%s'\n%s", row.brand, err
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
            "* Manifestation record created with priref '%s' and object_number '%s'",
            manifestation_id,
            object_number,
        )
    except (IndexError, KeyError, TypeError) as err:
        LOGGER.warning(
            "Failed to retrieve Priref from record created for - %s", row.brand
        )
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)

    # Append UTB data to record
    utb_xml = make_utb_data_for_man(row, manifestation_id)
    print("=================================")
    print(utb_xml)
    print("=================================")

    try:
        sleep(0.25)
        LOGGER.info("Attempting to append UTB data to record '%s'...", manifestation_id)
        man_rec_update = adlib.post(CID_API, utb_xml, "manifestations", "updaterecord")
        print(f"create_manifestation(): {man_rec_update}")
    except Exception as err:
        print(f"* Unable to update UTB to record <{manifestation_id}>\n{err}")
        LOGGER.warning("Unable to update Manifestation record <%s>", manifestation_id)
        LOGGER.warning("%s\n", err)
    if row.barb_before in str(man_rec_update):
        LOGGER.info("* Successfully updated Advert credit data to work.\n")

    return manifestation_id


if __name__ == "__main__":
    main()
