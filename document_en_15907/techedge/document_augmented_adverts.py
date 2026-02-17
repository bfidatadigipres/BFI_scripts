#!/usr/bin/env python3

"""
WIP:

Quickly worked through functions
main() not yet completed
People record creation / appending needs adding
Thesaurus terms need appending where indicated

2026
"""

# Public packages
import os
import csv
import sys
import glob
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import shutil
from time import sleep
import tenacity
import yaml

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils
from parsers import techedge_csv as tec

# Global variables
CODE_PATH = os.environ["CODE"]
GENRE_MAP = os.path.join(CODE_PATH, "document_en_15907/EPG_genre_mapping.yaml")
SERIES_LIST = os.path.join(CODE_PATH, "document_en_15907/series_list.json")
LOG_PATH = os.environ["LOG_PATH"]
CONTROL_JSON = os.path.join(LOG_PATH, "downtime_control.json")
CID_API = utils.get_current_api()
FAILURE_COUNTER = 0

# Setup logging
logger = logging.getLogger("document_augmented_adverts")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "document_augmented_adverts.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Some date variables for path configuration - PROB NEEDED FOR BAU WORK
# TODAY = datetime.date.today()
# YESTERDAY = TODAY - datetime.timedelta(days=1)
# YESTERDAY_CLEAN = YESTERDAY.strftime("%Y-%m-%d")
# YEAR_PATH = YESTERDAY_CLEAN[:4]
# YEAR_PATH = '2025'
# STORAGE_PATH = STORAGE + YEAR_PATH

# Match CSV channel to list: STORA channel name, code_type, broadcast_company
CHANNELS = {
    "ITV1": ["ITV HD", "MPEG-4 AVC", "20425"],
    "ITV2": ["ITV2", "MPEG-4 AVC", "20425"],
    "ITV3": ["ITV3", "MPEG-4 AVC", "20425"],
    "ITV4": ["ITV4", "MPEG-4 AVC", "20425"],
    "ITVQuiz": ["ITVBe", "MPEG-4 AVC", "20425"], # JMW Do we need this one any longer Stephen?
    "CH4": [
        "Channel 4 HD", 
        "MPEG-4 AVC",
        "73319"
    ],
    "More4": ["More4", "MPEG-2", "73319"],
    "E4": ["E4", "MPEG-2", "73319"],
    "Film4": ["Film4", "MPEG-2", "73319"],
    "5": ["Channel 5 HD", "MPEG-2", "24404"],
    "5STAR": ["5STAR", "MPEG-2", "24404"],
}


def look_up_series_list(alternative_num):
    """
    Check if series requires annual series creation
    JMW KEEP THIS FOR ADVERTS
    """

    if alternative_num.strip() == "2af14f77-ef15-517c-a463-04dc0a7c81ad":
        return "BBC News"
    with open(SERIES_LIST, "r") as file:
        slist = json.load(file)
        if alternative_num in slist:
            return True
    return False


@tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_attempt(10))
def cid_series_query(series_id):
    """
    Sends CID request for series_id data
    JMW KEEP THIS
    """

    print(f"CID SERIES QUERY: {series_id}")
    search = f'alternative_number="{series_id}"'
    sleep(1)
    try:
        hit_count, series_query_result = adlib.retrieve_record(
            CID_API, "works", search, "1"
        )
    except Exception as err:
        print(err)
        raise Exception

    print(f"cid_series_query(): {hit_count}\n{series_query_result}")
    if hit_count is None or hit_count == 0:
        print(
            f"cid_series_query(): Unable to access series data from CID using Series ID: {series_id}"
        )
        print(
            "cid_series_query(): Series hit count and series priref will return empty strings"
        )
        return hit_count, ""
    if "priref" in str(series_query_result):
        series_priref = adlib.retrieve_field_name(series_query_result[0], "priref")[0]
        print(f"cid_series_query(): Series priref: {series_priref}")
    else:
        print("cid_series_query(): Unable to access series_priref")
        return hit_count, ""

    return hit_count, series_priref


@tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_attempt(10))
def find_repeats(asset_id):
    """
    Use asset_id to check in CID for duplicate
    advert showings of a manifestation
    JMW KEEP THIS FOR FILM CODE CHECKS
    """

    search = f'alternative_number="{asset_id}" AND alternative_number.type="Unique advert identifier - TechEdge"'
    sleep(1)
    hits, result = adlib.retrieve_record(CID_API, "manifestations", search, "1")
    print(f"*** find_repeats(): {hits}\n{result}")
    if hits is None:
        print(f"CID API could not be reached for Manifestations search: {search}")
        return None
    if hits == 0:
        return 0
    try:
        man_priref = adlib.retrieve_field_name(result[0], "priref")[0]
    except (IndexError, TypeError, KeyError):
        return None
    sleep(1)
    full_result = adlib.retrieve_record(
        CID_API,
        "manifestations",
        f'priref="{man_priref}"',
        "1",
        ["alternative_number.type", "part_of_reference.lref"],
    )[1]
    if not full_result:
        return None
    try:
        print(full_result[0])
        alt_num_type = adlib.retrieve_field_name(
            full_result[0], "alternative_number.type"
        )[0]
    except (IndexError, TypeError, KeyError):
        alt_num_type = ""
    try:
        ppriref = adlib.retrieve_field_name(full_result[0], "part_of_reference.lref")[0]
    except (IndexError, TypeError, KeyError):
        ppriref = ""

    print(f"********** Alternative number type: {alt_num_type} ************")
    if ppriref is None:
        return None
    print(
        f"Priref with matching asset_id in CID: {man_priref} / Parent Work: {ppriref}"
    )
    if len(ppriref) > 1:
        return ppriref


def genre_retrieval(category_code, description, title):
    """
    Retrieve genre data, return as list
    """
    with open(GENRE_MAP, "r", encoding="utf8") as files:
        data = yaml.load(files, Loader=yaml.FullLoader)
        print(
            f"genre_retrieval(): The genre data is being retrieved for: {category_code}"
        )
    for _ in data:
        if category_code in data["genres"]:
            genre_one = {}
            genre_two = {}
            subject_one = {}
            subject_two = {}

            genre_one = data["genres"][category_code.strip("u")]["Genre"]
            print(f"genre_retrieval(): Genre one: {genre_one}")
            if "Undefined" in str(genre_one):
                print(
                    f"genre_retrieval(): Undefined category_code discovered: {category_code}"
                )
                with open(os.path.join(GENRE_PTH, "redux_undefined_genres.txt"), "a") as genre_log:
                    genre_log.write("\n")
                    genre_log.write(
                        f"Category: {category_code}     Title: {title}     Description: {description}"
                    )
                genre_one_priref = ""
            else:
                for _, val in genre_one.items():
                    genre_one_priref = val
                print(f"genre_retrieval(): Key value for genre_one_priref: {genre_one_priref}")
            try:
                genre_two = data["genres"][category_code.strip("u")]["Genre2"]
                for _, val in genre_two.items():
                    genre_two_priref = val
            except (IndexError, KeyError):
                genre_two_priref = ""

            try:
                subject_one = data["genres"][category_code.strip("u")]["Subject"]
                for _, val in subject_one.items():
                    subject_one_priref = val
                print(
                    f"genre_retrieval(): Key value for subject_one_priref: {subject_one_priref}"
                )
            except (IndexError, KeyError):
                subject_one_priref = ""

            try:
                subject_two = data["genres"][category_code.strip("u")]["Subject2"]
                for _, val in subject_two.items():
                    subject_two_priref = val
                print(f"genre_retrieval(): Key value for subject_two_priref: {subject_two_priref}")
            except (IndexError, KeyError):
                subject_two_priref = ""

            return [
                genre_one_priref,
                genre_two_priref,
                subject_one_priref,
                subject_two_priref,
            ]

        else:
            logger.warning(
                "%s -- New category not in EPG_genre_map.yaml: %s", category_code, title
            )
            with open(os.path.join(GENRE_PTH, "redux_undefined_genres.txt"), "a") as genre_log:
                genre_log.write("\n")
                genre_log.write(
                    f"Category: {category_code}     Title: {title}     Description: {description}"
                )
            return []


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


def yield_csv(fullpath):
    """
    Read CSV and return one at a time
    skipping lines not required

    Args:
        fullpath (str): path to CSV
    """
    with open(fullpath, "r", encoding="utf-8") as inf:
        rows = csv.reader(inf)
        for row in rows:
            return row


def main():
    """
    Iterates through .json files in STORA folders of storage_path
    extracts necessary data into variable. Checks if show is repeat
    if yes - make manifestation/item only and link to work_priref
    if no - make series/work/manifestation and item record
    """
    if not utils.check_storage(STORAGE):
        logger.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    logger.info(
        "========== STORA documentation script STARTED ==============================================="
    )

    file_list = glob.glob(f"{STORAGE_PATH}/**/*.json", recursive=True)
    file_list.sort()
    print(f"Found JSON file total: {len(file_list)}")

    for fullpath in file_list:
        if FAILURE_COUNTER > 2:
            logger.critical(
                "Multiple CID item record creation failures. Script exiting."
            )
            sys.exit(
                "Multiple CID item record creation failures detected. Script exiting."
            )
        if not utils.check_control("pause_scripts") or not utils.check_control("stora"):
            logger.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")
        if not utils.cid_check(CID_API):
            logger.warning("* Cannot establish CID session, exiting script")
            sys.exit("* Cannot establish CID session, exiting script")

        root, file = os.path.split(fullpath)
        if not os.path.exists(os.path.join(root, "stream.mpeg2.ts")):
            logger.info("Skipping: No stream file found in path: %s", root)
            continue
        if not os.path.exists(fullpath):
            continue
        if not file.endswith(".json") or not file.startswith("info_"):
            continue
        new_work = False

        print(f"\nFullpath for file being handled: {fullpath}")
        with open(fullpath, "r", encoding="utf-8") as inf:
            json_data = inf.read()

        # Retrieve all data needed from JSON
        if json_data:
            generic, epg_dict = fetch_lines(fullpath, json_data)
        else:
            print("No EPG dictionary found. Skipping!")
            continue
        if generic is True:
            print("Generic episode title found")
        title = epg_dict["title"]
        print(f"Title: {title}")
        description = epg_dict["description"]
        print(f"Longest Description: {description}")
        broadcast_channel = ""
        if "channel" in epg_dict:
            channel = epg_dict["channel"]
            print(f"Channel selected: {channel}")
        if "broadcast_channel" in epg_dict:
            broadcast_channel = epg_dict["broadcast_channel"]
            print(f"Broadcaster: {broadcast_channel}")

        # CSV data gather
        csv_data = csv_retrieve(os.path.join(root, "info.csv"))
        if csv_data:
            try:
                csv_description = csv_data[0]
                csv_actual_duration = csv_data[1]
                print(f"** CSV DESCRIPTION: {csv_description}")
                print(f"** CSV ACTUAL DURATION: {csv_actual_duration}")
                csv_dump = csv_data[2]
                print(f"** CSV DATA FOR UTB: {csv_dump}")
            except (IndexError, TypeError, KeyError) as err:
                csv_data = []
                csv_description = ""
                csv_actual_duration = ""
                csv_dump = ""
                print(err)
        else:
            csv_data = []
            csv_description = ""
            csv_actual_duration = ""
            csv_dump = ""

        # Get defaults as lists of dictionary pairs
        rec_def, ser_def, work_def, work_res_def, man_def, item_def = build_defaults(
            epg_dict
        )

        # Asset id check here
        work_priref = ""
        if "asset_id" in epg_dict:
            print(f"Checking if this asset_id already in CID: {epg_dict['asset_id']}")
            work_priref = find_repeats(epg_dict["asset_id"])
            print(work_priref)
        if work_priref is None:
            print(
                "Cannot retrieve Work parent data. Maybe missing in CID or problems accessing dB via API. Skipping"
            )
            logger.warning(
                "Skipping further actions: Failed to retrieve response from CID API for asset_id search: \n%s",
                epg_dict["asset_id"],
            )
            continue
        elif work_priref == 0:
            new_work = True
        elif len(work_priref) > 4:
            print(
                f"**** JSON file found to have repeated Asset ID, previous work: {work_priref}"
            )
            if generic is True:
                print("Generic in title, assuming programme is new content")
                new_work = True
            else:
                logger.info(
                    "** Programme found to be a repeat. Making manifestation/item only and linking to Priref: %s",
                    work_priref,
                )

        # Check file health with policy verification - skip if broken MPEG file
        acquired_filename = os.path.join(root, "stream.mpeg2.ts")
        print(f"Path for programme stream content: {acquired_filename}")
        """
        success, response = utils.get_mediaconch(acquired_filename, MPEG_TS_POLICY)
        if success is False:
            # Fix 'BROKEN' to folder name, update failure CSV
            logger.warning(
                "File found that has failed MPEG-TS policy:\n%s", acquired_filename
            )
            logger.warning("Marking JSON with .PROBLEM")
            mark_broken_stream(fullpath, acquired_filename)
            logger.warning("Marking stream.mpeg2.ts.BROKEN and updating CSV")
            update_broken_ts(acquired_filename, work_priref, response, epg_dict)
            continue
        logger.info("MPEG-TS passed MediaConch check: %s", success)
        print(response)
        """

        # Make news channels new works for all live programming
        if channel in NEWS_CHANNELS:
            new_work = True

        if new_work is True:
            # Create the Work record here, and populate work_priref
            print(
                "JSON file does not have repeated asset_id. Creating new work record..."
            )
            series_return = []
            series_work_id = ""
            if "series_id" in epg_dict:
                print("Series ID exists, trying to retrieve series data from CID")
                # Check if series already in CID and/or series_cache, if not generate series_cache json
                series_chck = look_up_series_list(epg_dict["series_id"])
                month = ""
                if series_chck == "BBC News":
                    bbc_split = True
                    month = root.split("/")[-4]
                    series_id = f"{YEAR_PATH}_{month}_{epg_dict['series_id']}"
                elif series_chck is False:
                    series_id = epg_dict["series_id"]
                    bbc_split = False
                elif series_chck is True:
                    series_id = f"{YEAR_PATH}_{epg_dict['series_id']}"
                    logger.info("Series found for annual refresh: %s", series_chck)
                    bbc_split = False

                series_return = cid_series_query(series_id)
                if series_return[0] is None:
                    print(f"CID Series data not retrieved: {epg_dict['series_id']}")
                    logger.warning(
                        "Skipping further actions: Failed to retrieve response from CID API for series_work_id search: \n%s",
                        epg_dict["series_id"],
                    )
                    continue

                hit_count = series_return[0]
                series_work_id = series_return[1]
                if hit_count == 0:
                    print(
                        "This Series does not exist yet in CID - attempting creation now"
                    )
                    # Launch create series function
                    series_work_id = create_series(
                        fullpath,
                        ser_def,
                        work_res_def,
                        epg_dict,
                        series_id,
                        month,
                        bbc_split,
                    )
                    if not series_work_id:
                        logger.warning(
                            "Skipping further actions: Creation of series failed as no series_work_id found: \n%s",
                            epg_dict["series_id"],
                        )
                        continue

            # Create Work
            work_values = []
            work_values.extend(rec_def)
            work_values.extend(work_def)
            work_values.extend(work_res_def)
            work_priref = create_work(
                fullpath,
                series_work_id,
                work_values,
                csv_description,
                csv_dump,
                epg_dict,
            )

        if not work_priref:
            print(
                f"Work error, priref not numeric from new file creation: {work_priref}"
            )
            continue
        if not work_priref.isnumeric() and new_work is True:
            print(
                f"Work error, priref not numeric from new file creation: {work_priref}"
            )
            continue

        # Create CID manifestation record
        manifestation_values = []
        manifestation_values.extend(rec_def)
        manifestation_values.extend(man_def)
        manifestation_priref = create_manifestation(
            fullpath, work_priref, csv_actual_duration, manifestation_values, epg_dict
        )

        if not manifestation_priref:
            print(
                f"CID Manifestation priref not retrieved for manifestation: {manifestation_priref}"
            )
            if new_work:
                print(f"*** Manual clean up needed for Work {work_priref}")
            sys.exit("Exiting for failure to create new manifestations")

        # Check if subtitles are populated
        old_webvtt = os.path.join(root, "subtitles.vtt")
        webvtt_payload = build_webvtt_dct(old_webvtt)

        # Create CID item record
        item_values = []
        item_values.extend(rec_def)
        item_values.extend(item_def)
        item_data = create_cid_item_record(
            work_priref,
            manifestation_priref,
            acquired_filename,
            fullpath,
            file,
            new_work,
            item_values,
            epg_dict,
        )
        print(f"item_object_number: {item_data}")

        if item_data is None:
            print(
                f"CID Item object number not retrieved for manifestation: {manifestation_priref}"
            )
            if new_work:
                print(
                    f"*** Manual clean up needed for Work {work_priref} and Manifestation {manifestation_priref}"
                )
                continue
            else:
                print(
                    f"*** Manual clean up needed for Manifestation {manifestation_priref}"
                )
                continue
        if len(item_data[0]) == 0 or len(item_data[1]) == 0:
            print(
                f"Error retrieving Item record priref and object number. Skipping completion of this programme, manual clean up of records needed."
            )
            if new_work:
                print(
                    f"*** Manual clean up needed for Work {work_priref} and Manifestation {manifestation_priref}"
                )
                continue
            else:
                print(
                    f"*** Manual clean up needed for Manifestation {manifestation_priref}"
                )
                continue

        """
        # Build webvtt payload [deprecated]
        if webvtt_payload:
            success = push_payload(item_data[1], webvtt_payload)
            if not success:
                logger.warning("Unable to push webvtt_payload to CID Item %s", item_data[1])
        """
        # Rename JSON with .documented
        documented = f"{fullpath}.documented"
        print(f"* Renaming {fullpath} to {documented}")
        try:
            os.rename(fullpath, f"{fullpath}.documented")
        except Exception as err:
            print(f"** PROBLEM: Could not rename {fullpath} to {documented}")
            logger.warning(
                "%s\tCould not rename to %s. Error: %s", fullpath, documented, err
            )

        # Rename transport stream file with Item object number and move to autoingest
        item_object_number_underscore = item_data[0].replace("-", "_")
        new_filename = f"{item_object_number_underscore}_01of01.ts"
        destination = f"{AUTOINGEST_PATH}{new_filename}"
        print(f"* Renaming {acquired_filename} to {destination}")
        try:
            shutil.move(acquired_filename, destination)
            logger.info(
                "%s\tRenamed %s to %s", fullpath, acquired_filename, destination
            )
        except Exception as err:
            print(
                f"** PROBLEM: Could not rename & move {acquired_filename} to {destination}"
            )
            logger.warning(
                "%s\tCould not rename & move %s to %s. Error: %s",
                fullpath,
                acquired_filename,
                destination,
                err,
            )

        # Rename .vtt subtitle file with Item object number and move to Isilon for use later in MTQ workflow
        if webvtt_payload is not None:
            new_vtt_name = f"{item_object_number_underscore}_01of01.vtt"
            new_vtt = f"{SUBS_PTH}{new_vtt_name}"
            print(f"* Renaming {old_webvtt} to {new_vtt}")
            try:
                shutil.move(old_webvtt, new_vtt)
                logger.info("%s\tRenamed %s to %s", fullpath, old_webvtt, new_vtt)
            except Exception as err:
                print(f"** PROBLEM: Could not rename {old_webvtt} to {new_vtt}")
                logger.warning(
                    "%s\tCould not rename %s to %s. Error: %s",
                    fullpath,
                    old_webvtt,
                    new_vtt,
                    err,
                )
    logger.info(
        "========== STORA documentation script END ===================================================\n"
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
        {"title": row[6]},
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
        {"credit.name": row[5]},
        {"credit.type": "Advertiser"},
        {"activity_type": "Sponsor"},
        {"party.class": "ORGANISATION"},
        {"source": "TechEdge adverts data supply"}
        {"credit.name": row[7]},
        {"credit.type": "Advertising Agency"},
        {"activity_type": "Advertising Agency"},
        {"party.class": "ORGANISATION"},
        {"source": "TechEdge adverts data supply"},
        {"product_category": row[14]},
        {"utb.fieldname": "Freeview EPG"},
        {"utb.content": row[19]},

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
        {"name": row[8]},
        {"activity_type": "Sponsor"},
        {"party.class": "ORGANISATION"},
        {"source": "TechEdge adverts data supply"}
    ]

    title_date_start = datetime.strftime(datetime.strptime(row[1], "%d/%m/%Y"), "%Y-%m-%d")
    transmission_start_time = row[2]
    utc_timestamp = get_utc(title_date_start, transmission_start_time)

    # Broadcast details
    for key, val in CHANNELS.items():
        if key == row[0]:
            try:
                channel = val[0]
                code_type = val[1]
                broadcast_company = val[2]
                print(f"Broadcast channel data: {channel} {code_type} {broadcast_company}")
            except (IndexError, TypeError, KeyError) as err:
                print(err)

    manifestation = [
        {"record_type": "MANIFESTATION"},
        {"manifestationlevel_type": "TRANSMISSION"},
        {"format_high_level": "Video - Digital"},
        {"colour_manifestation": "C"},
        {"sound_manifestation": "SOUN"},
        {"transmission_date": title_date_start},
        {"transmission_start_time": row[2]},
        {"UTC_timestamp": utc_timestamp},
        {"broadcast_channel": channel},
        {"broadcast_company": broadcast_company},
        {"transmission_coverage": "DIT"},
        {"aspect_ratio": "16:9"},
        {"country_manifestation": "United Kingdom"},
        {
            "notes": "Manifestation representing the UK Freeview television advert of the Work."
        },
        {"alternative_number": row[3]},
        {"alternative_number.type": "Unique advert identifier - TechEdge"},
        {"utb.content": row[[16]]},
        {"utb.fieldname": "PIB position"},
        {"utb.content": row[9]},
        {"utb.fieldname": "BARB Prog Before"},
        {"utb.content": row[10]},
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
        logger.info("Attempting to create Work record for item %s", title)
        work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
        print(f"create_work(): {work_rec}")
    except Exception as err:
        print(f"* Unable to create Work record for <{title}>\n{err}")
        logger.warning(
            "%s\tUnable to create Work record for <%s>", title
        )
        logger.warning(err)

    # Allow for retry if record priref creation crash:
    if len(work_rec) == 0:
        raise Exception("Recycle of API exception raised.")

    if "Duplicate key in unique index 'invno':" in str(work_rec):
        try:
            sleep(1)
            logger.info(
                "Attempting to create Work record for item %s", title
            )
            work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
            print(f"create_work(): {work_rec}")
        except Exception as err:
            print(f"* Unable to create Work record for <{title}>\n{err}")
            logger.warning(
                "Unable to create Work record for <%s>", title
            )
            logger.warning(err)

    try:
        print("Populating work_id and object_number variables")
        work_id = adlib.retrieve_field_name(work_rec, "priref")[0]
        object_number = adlib.retrieve_field_name(work_rec, "object_number")[0]
        print(
            f"* Work record created with Priref {work_id} Object number {object_number}"
        )
        logger.info("Work record created with priref %s", work_id)
    except (IndexError, TypeError, KeyError) as err:
        logger.warning(
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
        logger.info("Attempting to create Manifestation record for item %s", title)
        man_rec = adlib.post(CID_API, man_values_xml, "manifestations", "insertrecord")
        print(f"create_manifestation(): {man_rec}")
    except Exception as err:
        print(f"*** Unable to write manifestation record: {err}")
        logger.warning(
            "Unable to write manifestation record <%s> %s", manifestation_id, err
        )

    # Allow for retry if record priref creation crash:
    if "Duplicate key in unique index 'invno':" in str(man_rec):
        try:
            sleep(1)
            logger.info("Attempting to create Manifestation record for item %s", title)
            man_rec = adlib.post(
                CID_API, man_values_xml, "manifestations", "insertrecord"
            )
            print(f"create_manifestation(): {man_rec}")
        except Exception as err:
            print(f"*** Unable to write manifestation record: {err}")
            logger.warning(
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
        logger.info(
            "Manifestation record created with priref %s",
            manifestation_id,
        )
    except (IndexError, KeyError, TypeError) as err:
        logger.warning("Failed to retrieve Priref from record created for - %s", title)
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
        logger.info(
            "Attempting to create CID item record for item %s", title
        )
        item_rec = adlib.post(CID_API, item_values_xml, "items", "insertrecord")
        print(f"create_cid_item_record(): {item_rec}")
    except Exception as err:
        logger.warning(
            "PROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
            title,
        )
        print(f"** PROBLEM: Unable to create Item record for {err}")

    # Allow for retry if record priref creation crash:
    if "Duplicate key in unique index 'invno':" in str(item_rec):
        try:
            sleep(1)
            logger.info(
                "Attempting to create CID item record for item %s", title
            )
            item_rec = adlib.post(CID_API, item_values_xml, "items", "insertrecord")
            print(f"create_cid_item_record(): {item_rec}")
        except Exception as err:
            logger.warning(
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
        logger.info("Item record created with priref %s", item_id)
    except (IndexError, KeyError, TypeError) as err:
        logger.warning("Failed to retrieve Priref from record created %s", err)
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)
    if item_rec is None:
        logger.warning(
            "PROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
            title,
        )
        print(f"** PROBLEM: Unable to create Item record for {title}")
        return None

    return item_object_number, item_id


if __name__ == "__main__":
    main()
