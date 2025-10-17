#!/usr/bin/env python3

"""
THIS SCRIPT DEPENDS ON PYTHON ENV PATH

Create CID record hierarchies for Work-Manifestation-Item
using augmented metadata supply (JSON via API) and traversing filesystem paths to files
    1. Create work-manifestation-item for each JSON in the path and link to a
       series work if the programme is episodic. Where the programme is a repeated showing
       check for episode title starting 'Generic' (PA Media assure us this identifies a repeated
       asset_id that has a new episode). If 'Generic' present at start of episode title create new work,
       otherwise only create manifestation and item, and link to existing work.
       If new series (if episodic and series data present) create new series from downloaded
       EPG series data, then link work-manifestion-item to it.
    2. Add the WebVTT subtitles to the Item record (utb and label.text) using requests library
       push to avoid problems with escape characters through adlib.py method. [Deprecated feature]
    3. Rename the MPEG transport stream file with the Item object number, into autoingest
    4. Rename the subtitles.vtt file with Item object number and move to Isilon folder
    5. Identify the folder as completed by renaming the JSON with .documented suffix

    NOTE: this assumes a separate script - fetch_stora_augmented.py - will fetch the JSON
    for each programme from the API and place it in paths to be used here. Where none is matched
    document_stora.py will update to CID from the info.csv generated from the STORA TS file metadata.

Refactored Py3 2023
"""

# Public packages
import csv
import datetime
import glob
import json
import logging
import os
import shutil
import sys
from time import sleep

import tenacity
import yaml
from series_retrieve import check_id, retrieve

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils
from helpers import stora_helper

# Global variables
STORAGE = os.environ["STORA_PATH"]
AUTOINGEST_PATH = os.environ["STORA_AUTOINGEST"]
SERIES_CACHE_PATH = os.path.join(STORAGE, "series_cache")
CODE_PATH = os.environ["CODE"]
GENRE_MAP = os.path.join(CODE_PATH, "document_en_15907/EPG_genre_mapping.yaml")
SERIES_LIST = os.path.join(CODE_PATH, "document_en_15907/series_list.json")
LOG_PATH = os.environ["LOG_PATH"]
CONTROL_JSON = os.path.join(LOG_PATH, "downtime_control.json")
CSV_FAILURES = os.path.join(LOG_PATH, "failed_mpeg_ts_files.csv")
MPEG_TS_POLICY = os.path.join(os.environ["MEDIACONCH"], "mpeg_ts_policy.xml")
SUBS_PTH = os.environ["SUBS_PATH2"]
GENRE_PTH = os.path.split(SUBS_PTH)[0]
CID_API = utils.get_current_api()
FAILURE_COUNTER = 0

# Setup logging
logger = logging.getLogger("document_augmented_stora")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "document_augmented_stora.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Some date variables for path configuration
TODAY = datetime.date.today()
YESTERDAY = TODAY - datetime.timedelta(days=1)
YESTERDAY_CLEAN = YESTERDAY.strftime("%Y-%m-%d")
YEAR_PATH = YESTERDAY_CLEAN[:4]
# YEAR_PATH = '2024'
STORAGE_PATH = STORAGE + YEAR_PATH

NEWS_CHANNELS = ["Al Jazeera", "BBC NEWS HD", "Sky News", "GB News", "QVC"]

CHANNELS = {
    "bbconehd": [
        "BBC One HD",
        "BBC News",
        "BBC One joins the BBC's rolling news channel for a night of news [S][HD]",
    ],
    "bbctwohd": [
        "BBC Two HD",
        "This is BBC Two",
        "Highlights of programmes BBC Two. [HD]",
    ],
    "bbcthree": [
        "BBC Three HD",
        "This is BBC Three",
        "Programmes start at 7:00pm. [HD]",
    ],
    "bbcfourhd": [
        "BBC Four HD",
        "This is BBC Four",
        "Programmes start at 7:00pm. [HD]",
    ],
    "bbcnewshd": [
        "BBC NEWS HD",
        "BBC News HD close",
        "Programmes will resume shortly.",
    ],
    "cbbchd": [
        "CBBC HD",
        "This is CBBC!",
        "This is CBBC! Join the CBBC crew for all your favourite programmes. Tune into CBBC every day from 7.00am. [HD]",
    ],
    "cbeebieshd": ["CBeebies HD", "CBeebies HD", "Programmes start at 6.00am."],
    "itv1": ["ITV HD", "ITV Nightscreen", "Text-based information service."],
    "itv2": ["ITV2", "ITV2 Nightscreen", "Text-based information service."],
    "itv3": ["ITV3", "ITV3 Nightscreen", "Text-based information service."],
    "itv4": ["ITV4", "ITV4 Nightscreen", "Text-based information service."],
    "itvbe": ["ITV Be", "ITV Be Nightscreen", "Text-basd information service."],
    "citv": ["CiTV", "CiTV close", "Programmes start at 6:00am."],
    "channel4": [
        "Channel 4 HD",
        "Channel 4 HD close",
        "Programming will resume shortly.",
    ],
    "more4": ["More4", "More4 close", "Programmes will resume shortly."],
    "e4": ["E4", "E4 close", "Programmes will resume shortly."],
    "film4": ["Film4", "Film4 close", "Programmes will resume shortly."],
    "five": ["Channel 5 HD", "Channel 5 close", "Programmes will resume shortly."],
    "5star": ["5STAR", "5STAR close", "Programmes will resume shortly."],
    "al_jazeera": [
        "Al Jazeera",
        "Al Jazeera close",
        "This is a 24 hour broadcast news channel.",
    ],
    "gb_news": [
        "GB News",
        "GB News close",
        "This is a 24 hour broadcast news channel.",
    ],
    "sky_news": [
        "Sky News",
        "Sky News close",
        "This is a 24 hour broadcast news channel.",
    ],
    "skyarts": ["Sky Arts", "Sky Arts close", "Programmes will resume shortly."],
    "skymixhd": ["Sky Mix HD", "Sky Mix HD close", "Programmes will resume shortly."],
    "qvc": ["QVC", "QVC close", "Programmes will resume shortly."],
    "togethertv": [
        "Together TV",
        "Together TV close",
        "Programmes will resume shortly.",
    ],
    "u_dave": ["U&Dave", "U & Dave close", "Programmes will resume shortly."],
    "u_drama": ["U&Drama", "U & Drama close", "Programmes will resume shortly."],
    "u_yesterday": [
        "U&Yesterday",
        "U & Yesterday close",
        "Programmes will resume shortly.",
    ],
}


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


def look_up_series_list(alternative_num):
    """
    Check if series requires annual series creation
    """
    with open(SERIES_LIST, "r") as file:
        slist = json.load(file)
        if alternative_num in slist:
            return slist[alternative_num]
    return False


@tenacity.retry(wait=tenacity.wait_fixed(5), stop=tenacity.stop_after_attempt(10))
def cid_series_query(series_id):
    """
    Sends CID request for series_id data
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
    PATV showings of a manifestation
    """

    search = f'alternative_number="{asset_id}"'
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

    print(f"********** Alternative number types: {alt_num_type} ************")
    if "Amazon" in alt_num_type:
        logger.warning(
            "Matching episode work found to be an Amazon work record: %s", man_priref
        )
    if "Netflix" in alt_num_type:
        logger.warning(
            "Matching episode work found to be a Netflix work record: %s", man_priref
        )
    if ppriref is None:
        return None
    print(
        f"Priref with matching asset_id in CID: {man_priref} / Parent Work: {ppriref}"
    )
    if len(ppriref) > 1:
        return ppriref


def series_check(series_id):
    """
    Separate function that looks up series info when called in script
    """

    for files in os.listdir(SERIES_CACHE_PATH):
        if series_id not in files:
            continue
        if not files.endswith(".json"):
            continue
        filename = os.path.splitext(files)[0]
        fullpath = os.path.join(SERIES_CACHE_PATH, files)
        print(f"series_check(): MATCH! {filename} with Series_ID {series_id}")
        print(
            f"series_check(): Json to be opened and read for series data retrieval: {fullpath}"
        )
        with open(fullpath, "r", encoding="utf-8") as inf:
            lines = json.load(inf)
            if "ResourceNotFoundError" in str(lines):
                continue
            for _ in lines:
                series_descriptions = []
                try:
                    series_short = lines["summary"]["short"]
                    series_descriptions.append(series_short)
                except (IndexError, TypeError, KeyError):
                    series_short = ""
                    series_descriptions.append(series_short)
                try:
                    series_medium = lines["summary"]["medium"]
                    series_descriptions.append(series_medium)
                except (IndexError, TypeError, KeyError):
                    series_medium = ""
                    series_descriptions.append(series_medium)
                try:
                    series_long = lines["summary"]["long"]
                    series_descriptions.append(series_long)
                except (IndexError, TypeError, KeyError):
                    series_long = ""
                    series_descriptions.append(series_long)
                # Sort and return longest of descriptions
                series_descriptions.sort(key=len, reverse=True)
                series_description = series_descriptions[0]
                print(
                    f"series_check(): Series description longest: {series_description}"
                )
                try:
                    series_title_full = lines["title"]
                    print(f"series_check(): Series title full: {series_title_full}")
                except (IndexError, TypeError, KeyError):
                    series_title_full = ""
                series_category_codes = []
                # series category codes, unsure if there's always two parts to category, selects longest
                try:
                    series_category_code_one = lines["category"][0]["code"]
                    series_category_codes.append(series_category_code_one)
                except (IndexError, TypeError, KeyError):
                    series_category_code_one = ""
                    series_category_codes.append(series_category_code_one)
                try:
                    series_category_code_two = lines["category"][1]["code"]
                    series_category_codes.append(series_category_code_two)
                except (IndexError, TypeError, KeyError):
                    series_category_code_two = ""
                    series_category_codes.append(series_category_code_two)
                series_category_codes.sort(key=len, reverse=True)
                series_category_code = series_category_codes[0]
                print(
                    f"series_check(): Series category code, longest: {series_category_code}"
                )

                return (
                    series_description,
                    series_short,
                    series_medium,
                    series_long,
                    series_title_full,
                    series_category_code,
                )


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
                genre_one = []
                genre_two = []
                subject_one = []
                subject_two = []
                try:
                    genre_one = data["genres"][category_code.strip("u")]["Genre"]
                    print(f"genre_retrieval(): Genre one: {genre_one}")
                    if "Undefined" in genre_one:
                        print(
                            f"genre_retrieval(): Undefined category_code discovered: {category_code}"
                        )
                        with open(
                            f"{GENRE_PTH}redux_undefined_genres.txt", "a"
                        ) as genre_log:
                            print(
                                "genre_retrieval(): Writing Undefined category details to genre log"
                            )
                            genre_log.write("\n")
                            genre_log.write(
                                f"Category: {category_code}     Title: {title}     Description: {description}"
                            )
                        genre_one_priref = ""
                    else:
                        for _, val in genre_one.items():
                            genre_one_priref = val
                        print(
                            f"genre_retrieval(): Key value for genre_one_priref: {genre_one_priref}"
                        )
                except (IndexError, TypeError, KeyError):
                    genre_one_priref = ""
                try:
                    genre_two = data["genres"][category_code.strip("u")]["Genre2"]
                    for _, val in genre_two.items():
                        genre_two_priref = val
                    print(
                        f"genre_retrieval(): Key value for genre_two_priref: {genre_two_priref}"
                    )
                except (IndexError, TypeError, KeyError):
                    genre_two_priref = ""
                try:
                    subject_one = data["genres"][category_code.strip("u")]["Subject"]
                    for _, val in subject_one.items():
                        subject_one_priref = val
                    print(
                        f"genre_retrieval(): Key value for subject_one_priref: {subject_one_priref}"
                    )
                except (IndexError, TypeError, KeyError):
                    subject_one_priref = ""
                try:
                    subject_two = data["genres"][category_code.strip("u")]["Subject2"]
                    for _, val in subject_two.items():
                        subject_two_priref = val
                    print(
                        f"genre_retrieval(): Key value for subject_two_priref: {subject_two_priref}"
                    )
                except (IndexError, TypeError, KeyError):
                    subject_two_priref = ""
                return [
                    genre_one_priref,
                    genre_two_priref,
                    subject_one_priref,
                    subject_two_priref,
                ]

            logger.warning(
                "%s -- New category not in EPG_genre_map.yaml: %s", category_code, title
            )


def csv_retrieve(fullpath):
    """
    Fall back for missing descriptions, and output all content to utb field
    """
    csv_dump = ""
    csv_desc = ""
    print(f"csv_retrieve(): PATH: {fullpath}")
    if not os.path.exists(fullpath):
        logger.warning("No info.csv file found. Skipping CSV retrieve")
        print("No info.csv file found. Skipping CSV retrieve")
        return None
    if os.stat(fullpath).st_size == 0:
        logger.warning("Empty info.csv file found. Skipping CSV retrieve")
        print("No entry in info.csv. Skipping CSV retrieve")
        return None

    print("**** Check CSV reading correctly ****")
    with open(fullpath, "r", encoding="utf-8") as inf:
        rows = csv.reader(inf)
        for row in rows:
            print(row)
            data = {
                "channel": row[0],
                "title": row[1],
                "description": row[2],
                "title_date_start": row[3],
                "time": row[4],
                "duration": row[5],
                "actual_duration": row[6],
            }
            logger.info("%s\tCSV being processed: %s", fullpath, data["title"])
            csv_chan = data["channel"]
            csv_title = data["title"]
            csv_desc = data["description"]
            csv_date = data["title_date_start"]
            csv_time = data["time"]
            csv_dur = data["duration"]
            csv_act = data["actual_duration"]
            csv_dump = f"{csv_chan}, {csv_title}, {csv_desc}, Date start: {csv_date}, Time: {csv_time}, \
                         Duration: {csv_dur}, Actual duration: {csv_act}"

    return (csv_desc, csv_act, csv_dump)


def fetch_lines(fullpath, lines):
    """
    Function to extract all required fields
    and return as a dictionary
    """
    epg_dict = {}
    generic = False
    for _ in lines:
        print(f"Fullpath for file being handled: {fullpath}")
        try:
            title_whole = lines["item"][0]["asset"]["title"]
        except (IndexError, KeyError, TypeError):
            title_whole = ""
        try:
            title_new = lines["item"][0]["title"]
        except (IndexError, KeyError, TypeError):
            title_new = ""

        # This block is for correct title formatting, and flagging 'Generic'
        if title_whole.title().startswith("Generic"):
            print(title_whole)
            title_for_split = title_new
            generic = True
        elif title_whole == "":
            title_for_split = title_new
        else:
            title_bare = "".join(str for str in title_whole if str.isalnum())
            if title_bare.isnumeric():
                title_for_split = title_new
            else:
                title_for_split = title_whole

        # Form title and return all but ASCII [ THIS NEEDS REPLACING ]
        title, title_article = split_title(title_for_split)
        title = title.replace("\xe2\x80\x99", "'")

        description = []
        try:
            d_short = lines["item"][0]["summary"]["short"]
            d_short = d_short.replace("\xe2\x80\x99", "'")
            epg_dict["d_short"] = d_short
            description.append(d_short)
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            d_medium = lines["item"][0]["summary"]["medium"]
            d_medium = d_medium.replace("\xe2\x80\x99", "'")
            epg_dict["d_medium"] = d_medium
            description.append(d_medium)
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            d_long = lines["item"][0]["summary"]["long"]
            d_long = d_long.replace("\xe2\x80\x99", "'")
            epg_dict["d_long"] = d_long
            description.append(d_long)
        except (IndexError, KeyError, TypeError) as err:
            print(err)

        # Sorts to longest first which populates description var
        description.sort(key=len, reverse=True)
        if len(description) > 0:
            description = description[0]
        else:
            description = ""

        # For closed programming, overwrites title/desc var
        if title == "Close":
            print(f"Title has 'Close' as name: {fullpath}")
            for key, val in CHANNELS.items():
                if f"/{key}/" in fullpath:
                    print(f"Key that's in fullpath: {key}")
                    title = val[1]
                    description = val[2]
                    print(f"Replacement title for 'Close': {title}")
                    print(f"Replacement description: {description}")

        epg_dict["title"] = title
        if title_article:
            epg_dict["title_article"] = title_article
        epg_dict["description"] = description

        try:
            title_date_start_full = lines["item"][0]["dateTime"]
            title_date_start = str(title_date_start_full)[0:10]
            epg_dict["title_date_start"] = title_date_start
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            time_full = lines["item"][0]["dateTime"]
            time = str(time_full)[11:19]
            epg_dict["time"] = time
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            duration = lines["item"][0]["duration"]
            duration_total = str(duration)
            epg_dict["duration_total"] = duration_total
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            certification = lines["item"][0]["certification"]["bbfc"]
            epg_dict["certification"] = [certification]
        except (IndexError, KeyError, TypeError):
            certification = ""
        try:
            group = lines["item"][0]["meta"]["group"]
            group = str(group)
            epg_dict["group"] = [group]
        except (IndexError, KeyError, TypeError):
            group = ""
        try:
            attribute = lines["item"][0]["attribute"]
            asset_attribute = lines["item"][0]["asset"]["attribute"]
            epg_dict["attribute"] = attribute
            epg_dict["asset_attribute"] = asset_attribute
            list_attributes = attribute + asset_attribute + [group] + [certification]
            epg_dict["epg_attribute"] = ", ".join(
                str(x) for x in list_attributes if len(x) > 0
            )
        except (IndexError, KeyError, TypeError) as err:
            print(err)

        if "black-and-white" in str(asset_attribute):
            colour_manifestation = "B"
            print("This is a black and white item")
        else:
            colour_manifestation = "C"
            print("This is being classed as a colour item")

        epg_dict["colour_manifestation"] = colour_manifestation

        try:
            asset_id = lines["item"][0]["asset"]["id"]
            epg_dict["asset_id"] = asset_id
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            related_content = lines["item"][0]["asset"]["related"]
            related_content = str(related_content)
            epg_dict["related_content"] = related_content
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            series_number = lines["item"][0]["asset"]["related"][0]["number"]
            epg_dict["series_number"] = int(series_number)
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            series_id = lines["item"][0]["asset"]["related"][1]["id"]
            epg_dict["series_id"] = str(series_id)
        except (IndexError, KeyError, TypeError):
            series_id = None
        nested_id = check_id(fullpath)
        if nested_id is None:
            pass
        else:
            if len(nested_id) == 36 and nested_id != series_id:
                logger.warning(
                    "Retrieved Series ID %s likely not 'series' - exchanged for %s",
                    series_id,
                    nested_id,
                )
                series_id = nested_id
                epg_dict["series_id"] = str(series_id)
        try:
            episode_total = lines["item"][0]["asset"]["meta"]["episodeTotal"]
            epg_dict["episode_total"] = episode_total
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            episode_number = lines["item"][0]["asset"]["meta"]["episode"]
            logger.info("Episode number: %s", episode_number)
        except (IndexError, KeyError, TypeError):
            episode_number = ""
        if "&" in str(episode_number):
            episode_number = episode_number.split("&")
            logger.info(
                "& found in episode_number: %s - %s",
                episode_number,
                type(episode_number),
            )
            print(
                f"Episode number contains '&' and has been split {len(episode_number)} times"
            )
        epg_dict["episode_number"] = episode_number
        category_codes = []
        try:
            category_code_one = lines["item"][0]["asset"]["category"][0]["code"]
            category_codes.append(category_code_one)
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        try:
            category_code_two = lines["item"][0]["asset"]["category"][1]["code"]
            category_codes.append(category_code_two)
        except (IndexError, KeyError, TypeError) as err:
            print(err)
        # Sort for longest
        category_codes.sort(key=len, reverse=True)
        if len(category_codes) > 1:
            category_code = category_codes[0]
        else:
            category_code = category_codes
        epg_dict["category_code"] = category_code
        try:
            work = lines["item"][0]["asset"]["type"]
            epg_dict["work"] = work
        except (IndexError, KeyError, TypeError):
            work = ""

        # Broadcast details
        if "bbc" in fullpath or "cbeebies" in fullpath or "cbbc" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "454"
            print(f"Broadcast company set to BBC in {fullpath}")
        elif "itv" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "20425"
            print(f"Broadcast company set to ITV in {fullpath}")
        elif "more4" in fullpath or "film4" in fullpath or "/e4/" in fullpath:
            code_type = "MPEG-2"
            broadcast_company = "73319"
            print(f"Broadcast company set to Channel4 in {fullpath}")
        elif "channel4" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "73319"
            print(f"Broadcast company set to Channel4 in {fullpath}")
        elif "5star" in fullpath or "five" in fullpath:
            code_type = "MPEG-2"
            broadcast_company = "24404"
            print(f"Broadcast company set to Five in {fullpath}")
        elif "sky_news" in fullpath:
            code_type = "MPEG-2"
            broadcast_company = "78200"
            print(f"Broadcast company set to Sky News in {fullpath}")
        elif "skyarts" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "150001"
            print(f"Broadcast company set to Sky Arts in {fullpath}")
        elif "skymixhd" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "999939366"
            print(f"Broadcast company set to Sky Mix in {fullpath}")
        elif "al_jazeera" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "125338"
            print(f"Broadcast company set to Al Jazeera in {fullpath}")
        elif "gb_news" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "999831694"
            print(f"Broadcast company set to GB News in {fullpath}")
        elif "talk_tv" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "999883795"
            print(f"Broadcast company set to Talk TV in {fullpath}")
        elif "/u_dave" in fullpath:
            code_type = "MPEG-2"
            broadcast_company = "999929397"
            print(f"Broadcast company set to U&Dave in {fullpath}")
        elif "/u_drama" in fullpath:
            code_type = "MPEG-2"
            broadcast_company = "999929393"
            print(f"Broadcast company set to U&Drama in {fullpath}")
        elif "/u_yesterday" in fullpath:
            code_type = "MPEG-2"
            broadcast_company = "999929396"
            print(f"Broadcast company set to U&Yesterday in {fullpath}")
        elif "qvc" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "999939374"
            print(f"Broadcast company set to QVC UK in {fullpath}")
        elif "togethertv" in fullpath:
            code_type = "MPEG-4 AVC"
            broadcast_company = "999939362"
            print(f"Broadcast company set to Together TV in {fullpath}")
        else:
            broadcast_company = None

        if broadcast_company:
            epg_dict["broadcast_company"] = broadcast_company
        if code_type:
            epg_dict["code_type"] = code_type

        # Broadcast details
        for key, val in CHANNELS.items():
            if f"/{key}/" in fullpath:
                try:
                    channel = val[0]
                    print(f"Broadcast channel is {channel}")
                    epg_dict["channel"] = channel
                except (IndexError, TypeError, KeyError) as err:
                    print(err)

        # Sort category data
        try:
            category_data = genre_retrieval(category_code, description, title)
            print(f"Category data from genre_retrieval(): {category_data}")
            work_genre_one = category_data[0]
            print(f"Genre one work priref: {work_genre_one}")
            epg_dict["work_genre_one"] = work_genre_one
            if len(category_data[1]) > 0:
                work_genre_two = category_data[1]
                print(f"Genre two work priref: {work_genre_two}")
                epg_dict["work_genre_two"] = work_genre_two
            if len(category_data[2]) > 0:
                work_subject_one = category_data[2]
                print(f"Subject one work priref: {work_subject_one}")
                epg_dict["work_subject_one"] = work_subject_one
            if len(category_data[3]) > 0:
                work_subject_two = category_data[3]
                print(f"Subject two work priref: {work_subject_two}")
                epg_dict["work_subject_two"] = work_subject_two
        except (IndexError, TypeError, KeyError) as err:
            print(err)

        if "factual-topics" in category_code:
            nfa_category = "D"
        elif "movie-drama" in category_code:
            nfa_category = "F"
        elif "news-current-affairs" in category_code:
            nfa_category = "D"
        elif "sports:" in category_code:
            nfa_category = "D"
        elif "music-ballet" in category_code:
            nfa_category = "D"
        elif "arts-culture:" in category_code:
            nfa_category = "D"
        elif "social-political-issues" in category_code:
            nfa_category = "D"
        elif "leisure-hobbies" in category_code:
            nfa_category = "D"
        elif "show-game-show" in category_code:
            nfa_category = "D"
        elif "quiz-show" in category_code:
            nfa_category = "D"
        else:
            nfa_category = "F"
        epg_dict["nfa_category"] = nfa_category

        # Generate work_type
        if "work" in epg_dict:
            if work in ("episode", "one-off"):
                work_type = "T"
            elif work == "movie":
                work_type = "F"
            print(f"Work type = {work_type}")
            epg_dict["work_type"] = work_type

    return generic, epg_dict


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
        if not os.path.exists(fullpath):
            continue
        if not file.endswith(".json") or not file.startswith("info_"):
            continue
        new_work = False

        print(f"\nFullpath for file being handled: {fullpath}")
        with open(fullpath, "r", encoding="utf-8") as inf:
            lines = json.load(inf)

        # Retrieve all data needed from JSON
        if lines:
            generic, epg_dict = fetch_lines(fullpath, lines)
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
                if series_chck is False:
                    series_id = epg_dict["series_id"]
                else:
                    series_id = f"{YEAR_PATH}_{epg_dict['series_id']}"
                    logger.info("Series found for annual refresh: %s", series_chck)

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
                        fullpath, ser_def, work_res_def, epg_dict, series_id
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


def create_series(
    fullpath, series_work_defaults, work_restricted_def, epg_dict, series_id
):
    """
    Call function series_check(series_id) and build all data needed
    to make new series. Return boole for success/fail
    """
    new_series_list = False
    if series_id.startswith(YEAR_PATH):
        new_series_list = True

    series_work_id = ""
    series_data = series_check(epg_dict["series_id"])
    if series_data is None:
        print(
            f"Attempting to retrieve series data from EPG API using function 'retrieve({fullpath})'"
        )
        retrieve(fullpath)
    series_data = series_check(epg_dict["series_id"])
    print(series_data)
    if series_data is None:
        print("No series data found in CID or in cache")
        return None

    # Data found
    series_description = series_data[0]
    series_short = series_data[1]
    series_medium = series_data[2]
    series_long = series_data[3]
    series_title_full = series_data[4]
    series_category_code = series_data[5]
    print(f"Extracting series data: {series_data}")

    if len(series_title_full) == 0:
        print(
            "There is no series data available in Cache or CID, unable to access data"
        )
        return None
    try:
        series_title = split_title(series_title_full)[0]
        series_title_article = split_title(series_title_full)[1]
        print(f"***** Series title: {series_title}, and article {series_title_article}")
    except (IndexError, TypeError, KeyError):
        series_title = series_title_full
        series_title_article = ""
        print(f"** Series title: {series_title}")

    series_category_data = []
    try:
        series_category_data = genre_retrieval(
            series_category_code, series_description, series_title
        )
        print(
            f"*** SERIES CATEGORY DATA FROM GENRE_RETRIEVAL(): {series_category_data}"
        )
    except (IndexError, TypeError, KeyError):
        print("Unable to retrieve series category data from genre_retrieval() function")
    try:
        series_genre_one = series_category_data[0]
        print(f"Genre one series priref: {series_genre_one}")
    except (IndexError, TypeError, KeyError):
        series_genre_one = ""
    try:
        series_genre_two = series_category_data[1]
        print(f"Genre two series priref: {series_genre_two}")
    except (IndexError, TypeError, KeyError):
        series_genre_two = ""
    try:
        series_subject_one = series_category_data[2]
        print(f"Subject one series priref: {series_subject_one}")
    except (IndexError, TypeError, KeyError):
        series_subject_one = ""
    try:
        series_subject_two = series_category_data[3]
        print(f"Subject two series priref: {series_subject_two}")
    except (IndexError, TypeError, KeyError):
        series_subject_two = ""
    try:
        if "factual-topics" in series_category_code:
            nfa_category = "D"
        elif "news-current-affairs" in series_category_code:
            nfa_category = "D"
        elif "sports:" in series_category_code:
            nfa_category = "D"
        elif "music-ballet" in series_category_code:
            nfa_category = "D"
        elif "arts-culture:" in series_category_code:
            nfa_category = "D"
        elif "social-political-issues" in series_category_code:
            nfa_category = "D"
        elif "leisure-hobbies" in series_category_code:
            nfa_category = "D"
        elif "show-game-show" in series_category_code:
            nfa_category = "D"
        elif "quiz-show" in series_category_code:
            nfa_category = "D"
        else:
            nfa_category = "F"
    except (IndexError, TypeError, KeyError):
        nfa_category = ""

    # Series work value extensions for missing series data
    series_work_values = []
    series_work_values.extend(series_work_defaults)
    series_work_values.extend(work_restricted_def)

    # Add series title and article
    if new_series_list is True:
        series_title = f"{series_title} ({YEAR_PATH})"
    series_work_values.append({"title": series_title})
    series_work_values.append({"nfa_category": nfa_category})
    try:
        series_work_values.append({"title.article": series_title_article})
    except (IndexError, TypeError, KeyError):
        print("There is no series title article")
    try:
        series_work_values.append(
            {"alternative_number.type": "EBS augmented EPG supply"}
        )
        series_work_values.append({"alternative_number": series_id})
    except (IndexError, TypeError, KeyError):
        print("series_id will not be added to series_work_values")

    if new_series_list is True:
        if len(series_description) > 0:
            try:
                series_work_values.append(
                    {
                        "description": f"Specific serial work created for {YEAR_PATH}. {series_description}"
                    }
                )
                series_work_values.append({"description.type": "Synopsis"})
                series_work_values.append(
                    {"description.date": str(datetime.datetime.now())[:10]}
                )
                series_work_values.append(
                    {
                        "production.notes": "This is a series record created for one year of this programme."
                    }
                )
            except (IndexError, TypeError, KeyError):
                print(
                    "Series description LONGEST will not be added to series_work_values"
                )
    else:
        if len(series_description) > 0:
            try:
                series_work_values.append({"description": series_description})
                series_work_values.append({"description.type": "Synopsis"})
                series_work_values.append(
                    {"description.date": str(datetime.datetime.now())[:10]}
                )
            except (IndexError, TypeError, KeyError):
                print(
                    "Series description LONGEST will not be added to series_work_values"
                )

    # Start creating CID Work Series record with no nested groups
    series_values_xml = adlib.create_record_data(
        CID_API, "works", "", series_work_values
    )
    if series_values_xml is None:
        return None
    sleep(1)
    try:
        logger.info("Attempting to create CID series record for %s", series_title_full)
        work_rec = adlib.post(CID_API, series_values_xml, "works", "insertrecord")
    except Exception as err:
        print(f"* Unable to create Series Work record for <{series_title_full}> {err}")
        logger.warning(
            "%s\tUnable to create Series Work record for <%s>",
            fullpath,
            series_title_full,
        )

    # Allow for retry if record priref creation crash:
    if "Duplicate key in unique index 'invno':" in str(work_rec):
        try:
            logger.info(
                "Attempting to create CID series record for %s", series_title_full
            )
            work_rec = adlib.post(CID_API, series_values_xml, "works", "insertrecord")
        except Exception as err:
            print(
                f"* Unable to create Series Work record for <{series_title_full}> {err}"
            )
            logger.warning(
                "%s\tUnable to create Series Work record for <%s>",
                fullpath,
                series_title_full,
            )

    if work_rec is False:
        raise Exception("Recycle of API exception raised.")
    print(f"create_series(): {work_rec}")
    try:
        series_work_id = adlib.retrieve_field_name(work_rec, "priref")[0]
        object_number = adlib.retrieve_field_name(work_rec, "object_number")[0]
        print(f"* Series record created with Priref {series_work_id}")
        print(f"* Series record created with Object number {object_number}")
        logger.info("%s\tWork record created with priref %s", fullpath, series_work_id)
    except (IndexError, TypeError, KeyError) as err:
        print(f"* Unable to create Series Work record for <{series_title_full}>\n{err}")
        logger.warning(
            "%s\tUnable to create Series Work record for <%s>",
            fullpath,
            series_title_full,
        )
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)

    if not series_work_id:
        return None

    # Append Content genres to record
    series_content_genres = []
    genre = False
    if len(str(series_genre_one)) > 0:
        genre = True
        series_content_genres.append([{"content.genre.lref": series_genre_one}])
    if len(str(series_genre_two)) > 0:
        genre = True
        series_content_genres.append([{"content.genre.lref": series_genre_two}])
    if genre is True:
        genre_xml = adlib.create_grouped_data(
            series_work_id, "Content_genre", series_content_genres
        )
        print(genre_xml)
        update_rec = adlib.post(CID_API, genre_xml, "works", "updaterecord")
        if update_rec is False:
            raise Exception("Recycle of API exception raised.")
        if "Content_genre" in str(update_rec):
            logger.info(
                "Label text successfully updated to Series Work %s", series_work_id
            )

    # Append Content subject to record
    series_content_subject = []
    content = False
    if len(str(series_subject_one)) > 0:
        content = True
        series_content_subject.append([{"content.subject.lref": series_subject_one}])
    if len(str(series_subject_two)) > 0:
        content = True
        series_content_subject.append([{"content.subject.lref": series_subject_two}])
    if content is True:
        subject_xml = adlib.create_grouped_data(
            series_work_id, "Content_subject", series_content_subject
        )
        print(subject_xml)
        update_rec = adlib.post(CID_API, subject_xml, "works", "updaterecord")
        if update_rec is False:
            raise Exception("Recycle of API exception raised.")
        if "Content_subject" in str(update_rec):
            logger.info(
                "Label text successfully updated to Series Work %s", series_work_id
            )

    # Append group data contents to record
    label_fields = []
    labels = False
    if len(series_short) > 0:
        labels = True
        label_fields.append(
            [
                {"label.type": "EPGSHORT"},
                {"label.text": series_short},
                {"label.source": "EBS augmented EPG supply"},
                {"label.date": str(datetime.datetime.now())[:10]},
            ]
        )
        print("Series description short will not be added to series_work_values")
    if len(series_medium) > 0:
        labels = True
        label_fields.append(
            [
                {"label.type": "EPGMEDIUM"},
                {"label.text": series_medium},
                {"label.source": "EBS augmented EPG supply"},
                {"label.date": str(datetime.datetime.now())[:10]},
            ]
        )
        print("Series description medium will not be added to series_work_values")
    if len(series_long) > 0:
        labels = True
        label_fields.append(
            [
                {"label.type": "EPGLONG"},
                {"label.text": series_long},
                {"label.source": "EBS augmented EPG supply"},
                {"label.date": str(datetime.datetime.now())[:10]},
            ]
        )
        print("Series description long will not be added to series_work_values")
    if labels is True:
        label_xml = adlib.create_grouped_data(series_work_id, "Label", label_fields)
        print(label_xml)
        update_rec = adlib.post(CID_API, label_xml, "works", "updaterecord")
        if update_rec is False:
            raise Exception("Recycle of API exception raised.")
        if "Label" in str(update_rec):
            logger.info(
                "Label text successfully updated to Series Work %s", series_work_id
            )

    return series_work_id


def build_defaults(epg_dict):
    """
    Get detailed information
    and build record_defaults dict
    """

    # BST time conversion
    utc_date = epg_dict.get("title_date_start")
    utc_time = epg_dict.get("time")
    if len(utc_date) > 4 and len(utc_time) > 4:
        utc_timestamp = f"{utc_date} {utc_time}"
        bst_data = utils.check_bst_adjustment(utc_timestamp)
        if len(bst_data) != 2:
            LOGGER.warning(
                "BST date time conversion failed. Resorting to UTC time stamps"
            )
            bst_date = epg_dict.get("title_date_start")
            bst_time = epg_dict.get("time")
        else:
            bst_date = bst_data[0]
            bst_time = bst_data[1]
    if epg_dict["duration_total"] == "":
        epg_dict["duration_total"] = "0"
    # getting stoptime
    end_time = ""
    if "duration_total" in epg_dict:
        end_time = stora_helper.calculate_transmission_stoptime(
            epg_dict["duration_total"], bst_time
        )
        print(f"*** END TIME: {end_time}")

    record = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {
            "input.notes": "STORA off-air television capture - automated bulk documentation"
        },
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"grouping.lref": "398775"},
        {"title": epg_dict["title"]},
        {"title.language": "English"},
        {"title.type": "05_MAIN"},
    ]

    series_work = [
        {"record_type": "WORK"},
        {"worklevel_type": "SERIAL"},
        {"work_type": "T"},
        {"description.type.lref": "100298"},
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {
            "input.notes": "STORA off-air television capture - automated bulk documentation"
        },
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"grouping.lref": "398775"},
        {"title.language": "English"},
        {"title.type": "05_MAIN"},
    ]

    work = [
        {"record_type": "WORK"},
        {"worklevel_type": "MONOGRAPHIC"},
        {"work_type": epg_dict["work_type"]},
        {"description.type.lref": "100298"},
        {"title_date_start": bst_date},
        {"title_date.type": "04_T"},
        {"nfa_category": epg_dict["nfa_category"]},
    ]

    work_restricted = [
        {"application_restriction": "MEDIATHEQUE"},
        {"application_restriction.date": str(datetime.datetime.now())[:10]},
        {"application_restriction.reason": "STRATEGIC"},
        {"application_restriction.duration": "PERM"},
        {"application_restriction.review_date": "2030-01-01"},
        {"application_restriction.authoriser": "mcconnachies"},
        {
            "application_restriction.notes": "Automated off-air television capture - pending discussion"
        },
    ]

    manifestation = [
        {"record_type": "MANIFESTATION"},
        {"manifestationlevel_type": "TRANSMISSION"},
        {"format_high_level": "Video - Digital"},
        {"colour_manifestation": epg_dict["colour_manifestation"]},
        {"sound_manifestation": "SOUN"},
        {"transmission_duration": epg_dict["duration_total"]},
        {"transmission_date": bst_date},
        {"transmission_start_time": bst_time},
        {"transmission_end_time": end_time},
        {"UTC_timestamp": utc_timestamp},
        {"broadcast_channel": epg_dict["channel"]},
        {"transmission_coverage": "DIT"},
        {"aspect_ratio": "16:9"},
        {"country_manifestation": "United Kingdom"},
        {
            "notes": "Manifestation representing the UK Freeview television broadcast of the Work."
        },
    ]

    item = [
        {"record_type": "ITEM"},
        {"item_type": "DIGITAL"},
        {"copy_status": "M"},
        {"copy_usage.lref": "131560"},
        {"file_type": "MPEG-TS"},
        {"code_type": epg_dict.get("code_type")},
        {"source_device": "STORA"},
        {"acquisition.method": "Off-Air"},
    ]

    return (record, series_work, work, work_restricted, manifestation, item)


def build_webvtt_dct(old_webvtt):
    """
    Open WEBVTT and if content present
    append to CID item record
    """

    print("Attempting to open and read subtitles.vtt")
    if not os.path.exists(old_webvtt):
        print(f"subtitles.vtt not found: {old_webvtt}")
        return None

    with open(old_webvtt, encoding="utf-8") as webvtt_file:
        webvtt_payload = webvtt_file.read()
        webvtt_file.close()

    if not webvtt_payload:
        print("subtitles.vtt could not be open")
        logger.warning("Unable to open subtitles.vtt - file absent")
        return None

    if not "-->" in webvtt_payload:
        print("subtitles.vtt has no data present in file")
        logger.warning("subtitles.vtt data is absent")
        return None

    return webvtt_payload.replace("'", "'")


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_work(
    fullpath, series_work_id, work_values, csv_description, csv_dump, epg_dict
):
    """
    Create work records
    """
    if "title_article" in epg_dict:
        work_values.append({"title.article": epg_dict["title_article"]})
    if series_work_id:
        work_values.append({"part_of_reference.lref": series_work_id})
    if "episode_number" in epg_dict:
        ep_num = epg_dict["episode_number"]
        if isinstance(ep_num, str):
            work_values.append({"part_unit": "EPISODE"})
            work_values.append({"part_unit.value": ep_num})
        if isinstance(ep_num, list):
            work_values.append({"part_unit": "EPISODE"})
            work_values.append({"part_unit.value": ep_num[0]})
            try:
                if ep_num[1]:
                    work_values.append({"part_unit": "EPISODE"})
                    work_values.append({"part_unit.value": ep_num[1]})
                if ep_num[2]:
                    work_values.append({"part_unit": "EPISODE"})
                    work_values.append({"part_unit.value": ep_num[2]})
                if ep_num[3]:
                    work_values.append({"part_unit": "EPISODE"})
                    work_values.append({"part_unit.value": ep_num[3]})
            except (IndexError, TypeError, KeyError) as err:
                print(err)
        if "episode_total" in epg_dict:
            work_values.append({"part_unit.valuetotal": epg_dict["episode_total"]})
    if "series_number" in epg_dict:
        series_num = epg_dict["series_number"]
        if series_num < 1900:
            print("Writing series number to part_unit field, not season detail")
            work_values.append({"part_unit": "SERIES"})
            work_values.append({"part_unit.value": str(series_num)})
        elif series_num > 1900:
            work_values.append({"production.notes": str(series_num) + " season"})
            print(f"***** Series number is {str(series_num)} season (in year format)")
        else:
            print("Series number isn't present, skipping.")

    if "description" in epg_dict:
        description = epg_dict["description"]
        work_values.append({"description": description})
        work_values.append({"description.type": "Synopsis"})
        work_values.append({"description.date": str(datetime.datetime.now())[:10]})
    else:
        if csv_description:
            work_values.append({"description": csv_description})
            work_values.append({"description.type": "Synopsis"})
            work_values.append({"description.date": str(datetime.datetime.now())[:10]})
        if csv_dump:
            work_values.append({"utb.fieldname": "Freeview EPG"})
            work_values.append({"utb.content": csv_dump})

    work_id = work_rec = ""
    # Start creating CID Work record
    sleep(1)
    work_values_xml = adlib.create_record_data(CID_API, "works", "", work_values)
    if work_values_xml is None:
        return None
    try:
        sleep(1)
        logger.info("Attempting to create Work record for item %s", epg_dict["title"])
        work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
        print(f"create_work(): {work_rec}")
    except Exception as err:
        print(f"* Unable to create Work record for <{epg_dict['title']}>\n{err}")
        logger.warning(
            "%s\tUnable to create Work record for <%s>", fullpath, epg_dict["title"]
        )
        logger.warning(err)

    # Allow for retry if record priref creation crash:
    if len(work_rec) == 0:
        raise Exception("Recycle of API exception raised.")

    if "Duplicate key in unique index 'invno':" in str(work_rec):
        try:
            sleep(1)
            logger.info(
                "Attempting to create Work record for item %s", epg_dict["title"]
            )
            work_rec = adlib.post(CID_API, work_values_xml, "works", "insertrecord")
            print(f"create_work(): {work_rec}")
        except Exception as err:
            print(f"* Unable to create Work record for <{epg_dict['title']}>\n{err}")
            logger.warning(
                "%s\tUnable to create Work record for <%s>", fullpath, epg_dict["title"]
            )
            logger.warning(err)

    try:
        print("Populating work_id and object_number variables")
        work_id = adlib.retrieve_field_name(work_rec, "priref")[0]
        object_number = adlib.retrieve_field_name(work_rec, "object_number")[0]
        print(
            f"* Work record created with Priref {work_id} Object number {object_number}"
        )
        logger.info("%s\tWork record created with priref %s", fullpath, work_id)
    except (IndexError, TypeError, KeyError) as err:
        logger.warning(
            "Failed to retrieve Priref from record created using: 'works', 'insertrecord' for %s",
            epg_dict["title"],
        )
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)

    if not work_id:
        return None

    # Append Content genres to record
    content_genres = []
    genre = False
    if "work_genre_one" in epg_dict:
        genre = True
        content_genres.append([{"content.genre.lref": epg_dict["work_genre_one"]}])
    if "work_genre_two" in epg_dict:
        genre = True
        content_genres.append([{"content.genre.lref": epg_dict["work_genre_two"]}])
    if genre is True:
        genre_xml = adlib.create_grouped_data(work_id, "Content_genre", content_genres)
        print(genre_xml)
        update_rec = adlib.post(CID_API, genre_xml, "works", "updaterecord")
        if update_rec is False:
            raise Exception("Recycle of API exception raised.")
        if "Content_genre" in str(update_rec):
            logger.info("Label text successfully updated to Series Work %s", work_id)

    # Append Content subject to record
    content_subject = []
    content = False
    if "work_subject_one" in epg_dict:
        content = True
        content_subject.append([{"content.subject.lref": epg_dict["work_subject_one"]}])
    if "work_subject_two" in epg_dict:
        content = True
        content_subject.append([{"content.subject.lref": epg_dict["work_subject_two"]}])
    if content is True:
        subject_xml = adlib.create_grouped_data(
            work_id, "Content_subject", content_subject
        )
        print(subject_xml)
        update_rec = adlib.post(CID_API, subject_xml, "works", "updaterecord")
        if update_rec is False:
            raise Exception("Recycle of API exception raised.")
        if "Content_subject" in str(update_rec):
            logger.info("Label text successfully updated to Series Work %s", work_id)

    # Append group data contents to record
    label_fields = []
    labels = False
    if "d_short" in epg_dict:
        labels = True
        label_fields.append(
            [
                {"label.type": "EPGSHORT"},
                {"label.text": epg_dict["d_short"]},
                {"label.source": "EBS augmented EPG supply"},
                {"label.date": str(datetime.datetime.now())[:10]},
            ]
        )
        print("Work description short will not be added to series_work_values")
    if "d_medium" in epg_dict:
        labels = True
        label_fields.append(
            [
                {"label.type": "EPGMEDIUM"},
                {"label.text": epg_dict["d_medium"]},
                {"label.source": "EBS augmented EPG supply"},
                {"label.date": str(datetime.datetime.now())[:10]},
            ]
        )
        print("Work description medium will not be added to series_work_values")
    if "d_long" in epg_dict:
        labels = True
        label_fields.append(
            [
                {"label.type": "EPGLONG"},
                {"label.text": epg_dict["d_long"]},
                {"label.source": "EBS augmented EPG supply"},
                {"label.date": str(datetime.datetime.now())[:10]},
            ]
        )
        print("Work description long will not be added to series_work_values")
    if labels is True:
        label_xml = adlib.create_grouped_data(work_id, "Label", label_fields)
        print(label_xml)
        update_rec = adlib.post(CID_API, label_xml, "works", "updaterecord")
        if update_rec is False:
            raise Exception("Recycle of API exception raised.")
        if "Label" in str(update_rec):
            logger.info("Label text successfully updated to Series Work %s", work_id)

    return work_id


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_manifestation(
    fullpath, work_priref, actual_duration, manifestation_defaults, epg_dict
):
    """
    Create a manifestation record,
    linked to work_priref
    """
    manifestation_id = ""
    title = epg_dict["title"]
    manifestation_values = []
    manifestation_values.extend(manifestation_defaults)

    manifestation_values.append({"part_of_reference.lref": work_priref})
    manifestation_values.append({"alternative_number.type": "PATV asset id"})
    if "asset_id" in epg_dict:
        manifestation_values.append({"alternative_number": epg_dict["asset_id"]})
    if "title_article" in epg_dict:
        manifestation_values.append({"title.article": epg_dict["title_article"]})
    if "epg_attribute" in epg_dict:
        manifestation_values.append({"utb.fieldname": "EPG attributes"})
        manifestation_values.append({"utb.content": epg_dict["epg_attribute"]})
    if "broadcast_company" in epg_dict:
        manifestation_values.append(
            {"broadcast_company.lref": epg_dict["broadcast_company"]}
        )

    # MAKE SURE RUNTIME REFLECTS ACTUAL DURATIONS / MINS & SECS
    actual_data = None
    if len(actual_duration) > 0 and ":" in str(actual_duration):
        actual_data = actual_duration.split(":")
    elif len(actual_duration) > 0 and "-" in str(actual_duration):
        actual_data = actual_duration.split("-")

    if actual_data is not None:
        actual_minutes = (int(actual_data[0]) * 60) + int(actual_data[1])
        actual_seconds = (actual_minutes * 60) + int(actual_data[2])
        print(f"** Actual minutes {actual_minutes} - actual seconds {actual_seconds}")
        manifestation_values.append({"runtime": actual_minutes})
        manifestation_values.append({"runtime_seconds": actual_seconds})

    else:
        duration_mins = epg_dict["duration_total"]
        if duration_mins.isdigit():
            duration_secs = str(int(duration_mins) * 60)
            manifestation_values.append({"runtime": epg_dict["duration_total"]})
            manifestation_values.append({"runtime_seconds": duration_secs})

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
            "%s\tManifestation record created with priref %s",
            fullpath,
            manifestation_id,
        )
    except (IndexError, KeyError, TypeError) as err:
        logger.warning("Failed to retrieve Priref from record created for - %s", title)
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)

    return manifestation_id


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def create_cid_item_record(
    work_id,
    manifestation_id,
    acquired_filename,
    fullpath,
    file,
    new_work,
    item_values,
    epg_dict,
):
    """
    Create CID Item record
    """
    item_id = ""
    item_object_number = ""
    item_values.append({"part_of_reference.lref": manifestation_id})
    item_values.append({"digital.acquired_filename": acquired_filename})

    try:
        title_article = epg_dict["title_article"]
        if len(title_article) > 0:
            item_values.append({"title.article": title_article})
    except (KeyError, IndexError, TypeError):
        print("Title article is not present")

    item_values_xml = adlib.create_record_data(CID_API, "items", "", item_values)
    if item_values_xml is None:
        return None

    try:
        sleep(1)
        logger.info(
            "Attempting to create CID item record for item %s", epg_dict["title"]
        )
        item_rec = adlib.post(CID_API, item_values_xml, "items", "insertrecord")
        print(f"create_cid_item_record(): {item_rec}")
    except Exception as err:
        logger.warning(
            "%s\tPROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
            fullpath,
            file,
        )
        print(f"** PROBLEM: Unable to create Item record for {fullpath} {err}")

    # Allow for retry if record priref creation crash:
    if "Duplicate key in unique index 'invno':" in str(item_rec):
        try:
            sleep(1)
            logger.info(
                "Attempting to create CID item record for item %s", epg_dict["title"]
            )
            item_rec = adlib.post(CID_API, item_values_xml, "items", "insertrecord")
            print(f"create_cid_item_record(): {item_rec}")
        except Exception as err:
            logger.warning(
                "%s\tPROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
                fullpath,
                file,
            )
            print(f"** PROBLEM: Unable to create Item record for {fullpath} {err}")

    if item_rec is False:
        raise Exception("Recycle of API exception raised.")
    try:
        item_id = adlib.retrieve_field_name(item_rec, "priref")[0]
        item_object_number = adlib.retrieve_field_name(item_rec, "object_number")[0]
        print(
            f"* Item record created with Priref {item_id} Object number {item_object_number}"
        )
        logger.info("%s\tItem record created with priref %s", fullpath, item_id)
    except (IndexError, KeyError, TypeError) as err:
        logger.warning("Failed to retrieve Priref from record created %s", err)
        raise Exception(
            "Failed to retrieve Priref/Object Number from record creation."
        ).with_traceback(err.__traceback__)
    if item_rec is None:
        logger.warning(
            "%s\tPROBLEM: Unable to create Item record for <%s> marking Work and Manifestation records for deletion",
            fullpath,
            file,
        )
        print(f"** PROBLEM: Unable to create Item record for {fullpath}")
        success = clean_up_work_man(fullpath, manifestation_id, new_work, work_id)
        logger.warning(
            "Data cleaned following failure of Item record creation: %s", success
        )
        return None

    return item_object_number, item_id


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def clean_up_work_man(fullpath, manifestation_id, new_work, work_id):
    """
    Item record creation failed
    Update manifestation records with deletion prompt in title
    """
    payload_start = f"<adlibXML><recordList><record priref='{manifestation_id}'>"
    payload_mid = "<Title><title>DELETE - STORA record creation problem</title></Title>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_start + payload_mid + payload_end
    try:
        sleep(1)
        response = adlib.post(CID_API, payload, "manifestations", "updaterecord")
        if response:
            logger.info(
                "%s\tRenamed Manifestation %s with deletion prompt in title",
                fullpath,
                manifestation_id,
            )
        else:
            logger.warning(
                "%s\tUnable to rename Manifestation %s with deletion prompt in title",
                fullpath,
                manifestation_id,
            )
    except Exception as err:
        logger.warning(
            "%s\tUnable to rename Manifestation %s with deletion prompt in title. Error: %s",
            fullpath,
            manifestation_id,
            err,
        )

    # Update work record with deletion prompt in title
    if new_work is True:
        payload_start = f"<adlibXML><recordList><record priref='{work_id}'>"
        payload_mid = (
            "<Title><title>DELETE - STORA record creation problem</title></Title>"
        )
        payload_end = "</record></recordList></adlibXML>"
        payload = payload_start + payload_mid + payload_end
        try:
            sleep(1)
            response = adlib.post(CID_API, payload, "works", "updaterecord")
            if "priref" in str(response):
                logger.info(
                    "%s\tRenamed Work %s with deletion prompt in title, for bulk deletion",
                    fullpath,
                    work_id,
                )
            else:
                logger.warning(
                    "%s\tUnable to rename Work %s with deletion prompt in title, for bulk deletion",
                    fullpath,
                    work_id,
                )
        except Exception as err:
            logger.warning(
                "%s\tUnable to rename Work %s with deletion prompt in title, for bulk deletion. Error: %s",
                fullpath,
                work_id,
                err,
            )

    success = mark_problem_json(fullpath)
    logger.info("Successfully updated JSON with PROBLEM: %s", success)


def mark_problem_json(fullpath):
    """
    Rename JSON with .PROBLEM to prevent retry
    """
    problem = f"{fullpath}.PROBLEM"
    print(f"* Renaming {fullpath} to {problem}")
    logger.info("%s\t Renaming JSON to %s", fullpath, problem)
    global FAILURE_COUNTER
    FAILURE_COUNTER += 1
    try:
        os.rename(fullpath, problem)
    except Exception as err:
        print(f"** PROBLEM: Could not rename {fullpath} to {problem}")
        logger.warning(
            "%s\tCould not rename JSON to %s. Error: %s", fullpath, problem, err
        )
    if os.path.exists(problem):
        return True


def mark_broken_stream(json_path, vpath):
    """
    Rename JSON with .PROBLEM to prevent retry
    and append ts.BROKEN to video file
    """

    vpath_broken = f"{vpath}.BROKEN"
    problem = f"{json_path}.PROBLEM"
    print(f"* Renaming {json_path} to {problem}")
    print(f"* Renaming {vpath} to {vpath_broken}")
    logger.info("%s\t Renaming JSON to %s", json_path, problem)
    logger.info("%s\t Renaming MPEG-TS to %s", vpath, vpath_broken)

    if os.path.exists(json_path):
        os.rename(json_path, problem)
    else:
        logger.warning("Path not found, unable to append '.PROBLEM': %s", json_path)
    if os.path.exists(vpath):
        os.rename(vpath, vpath_broken)
    else:
        logger.warning("Path not found, unable to append '.BROKEN': %s", vpath)


def update_broken_ts(vpath, work_priref, response, epg_dict=None):
    """
    Update broken MPEG-TS file to
    CSV along with date/channel/policy
    """
    broadcast_channel = epg_dict.get("broadcast_channel")
    title_art = epg_dict.get("title_article")
    if title_art:
        title_art = f"{title_art} "
    else:
        title_art = ""
    title = epg_dict.get("title")
    date_start = epg_dict.get("title_date_start")
    time = epg_dict.get("time")
    duration = epg_dict.get("duration")
    asset_id = epg_dict.get("asset_id")
    episode_number = epg_dict.get("episode_number")
    series_id = epg_dict.get("series_id")
    code_type = epg_dict.get("code_type")
    data = [
        broadcast_channel,
        f"{title_art}{title}",
        date_start,
        time,
        duration,
        asset_id,
        episode_number,
        series_id,
        work_priref,
        code_type,
        vpath,
        response,
    ]

    with open(CSV_FAILURES, "a") as failures:
        writer = csv.writer(failures)
        writer.writerow(data)


@tenacity.retry(stop=tenacity.stop_after_attempt(1))
def push_payload(item_id, webvtt_payload):
    """
    DEPRECATED
    Push webvtt payload separately to Item record
    creation, to manage escape character injects
    """
    label_type = "SUBWEBVTT"
    label_source = "Extracted from MPEG-TS created by STORA recording"
    # Make payload
    pay_head = f'<adlibXML><recordList><record priref="{item_id}">'
    label_type_addition = f"<label.type>{label_type}</label.type>"
    label_addition = f"<label.source>{label_source}</label.source><label.text><![CDATA[{webvtt_payload}]]></label.text>"
    pay_end = "</record></recordList></adlibXML>"
    payload = pay_head + label_type_addition + label_addition + pay_end

    try:
        post_resp = adlib.post(CID_API, payload, "items", "updaterecord")
    except Exception as err:
        logger.warning(
            "push_payload()): Unable to write Webvtt to record %s \n%s", item_id, err
        )
    if post_resp is False:
        raise Exception("Recycle of API exception raised.")
    if post_resp:
        return True
    else:
        return False


if __name__ == "__main__":
    main()
