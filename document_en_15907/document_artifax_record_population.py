#!/usr/bin/env python3

'''
Follow up script to document_artifax_record_creation.py

1. Updating of CID work record
   - Fetch Artifax season_id batch work_ids from earlier script 1 download, displaying today's date
     (each morning new priref examples generated day before appear on new json extractions)
   - Extract season from json, iterate through JSON looking for work_ids with priref field populated
     and final version 'flagged'
   - Populate variables for lock, season_id, work_id, priref.
   - Extract credit data from dct['Credits'], check CID for existing people and get PRIREFs
   - If no people exist, create new people record and pass all PRIREFs to work_append_dct
   - Where no 'lock' file present in record, proceed to append CID work record
2. Generation of Manifestations for season
   - Check custom field for presence of Q&A information, if present create work record and manifestation
     INTERNET Q&A type
   - Check if season name of json matches season description in work_copy (skipped altogether if
     already present in CID Work record season grouping)
       If yes, proceed to create manifestion as child of work, with manifestation defaults
       ( WRITE season grouping to Work record )
       If no, append new season_id to Work record in CID and create manifestation with manifestation
       defaults, and link to Work record and season grouping
   - When completed push date lock back to work_id  (to prevent any future work record appending).

NOTE: Updated for Adlib V3

2021
'''

# Public packages
import os
import sys
import json
import logging
import shutil
import datetime
import requests
import yaml
import tenacity
from typing import Final, Optional, Any

# Local packages
import title_article
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Local date vars for script comparison/activation
TODAY = str(datetime.date.today())
TODAY_TIME = str(datetime.datetime.now())
TIME = TODAY_TIME[11:19]

# Global variables
STORAGE_PATH: Final = os.environ['SEASONS_PATH']
LANGUAGE_MAP: Final = os.environ['LANGUAGE_YAML']
JSON_COMPLETED: Final = os.environ['COMPLETED_PATH']
LOG_PATH: Final = os.environ['LOG_PATH']
CONTROL_JSON: Final = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = utils.get_current_api()

# Setup logging (running from bk-qnap-video)
LOGGER = logging.getLogger('document_artifax_record_population')
hdlr = logging.FileHandler(os.path.join(LOG_PATH, 'document_artifax_record_population.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)

# Artifax url paths and header data
WORK_URL: Final = os.environ['ARTIFAX_WORK']
WORK_COPY_URL: Final = os.environ['ARTIFAX_WORK_COPY']
WORK_SEASON_URL: Final = os.environ['ARTIFAX_WORK_SEASON']
CUSTOM_URL: Final = os.environ['ARTIFAX_CUSTOM']
HEADERS = {
    "X-API-KEY": f"{os.environ['ARTIFAX_API_KEY']}"
}

FESTIVALS = {'FL2021': ["119", "Flare 2021", "399108"],
             'FFF2021': ["134", "Future Film Festival 2021", "399083"],
             'LFF2021': ["129", "LFF 2021", "399145"],
             'FL2022': ["145", "Flare 2022", "399151"],
             'FFF2022': ["155", "Future Film Festival 2022", "399156"],
             'LFF2022': ["150", "LFF 2022", "399146"],
             'FL2023': ["146", "Flare 2023", "399152"],
             'FFF2023': ["156", "Future Film Festival 2023", "399157"],
             'LFF2023': ["151", "LFF 2023", "399147"],
             'FOFF23': ["281", "Film on Film Festival - June 2023", "400889"],
             'FL2024': ["147", "Flare 2024", "399153"],
             'FFF2024': ["157", "Future Film Festival 2024", "399158"],
             'LFF2024': ["152", "LFF 2024", "399148"],
             'FL2025': ["148", "Flare 2025", "399154"],
             'FFF2025': ["158", "Future Film Festival 2025", "399159"],
             'LFF2025': ["153", "LFF 2025", "399149"],
             'FL2026': ["149", "Flare 2026", "399155"],
             'FFF2026': ["159", "Future Film Festival 2026", "399160"],
             'LFF2026': ["154", "LFF 2026", "399150"]
             }

FORMATS = {'DVD': ['DVD (Digital Versatile Disc)', '73390'],
           '8mm film': ['8mm Film', '74333'],
           'Betacam SP videocassette': ['Betacam SP', '74252'],
           'VHS videocassette': ['VHS Videocassette', '74275'],
           'Digital Betacam': ['Digital Betacam', '74277'],
           '16mm film': ['16mm Film', '74305'],
           '35mm film': ['35mm Film', '74314'],
           '70mm film': ['70mm Film', '74325'],
           'HDCAM': ['HDCAM', '74445'],
           'Blu-ray Disc': ['Blu-ray', '109726'],
           'Digital Cinema Package': ['Digital Cinema Package (DCP)', '110477'],
           'HDCAM SR': ['HDCAM SR', '394912'],
           'Digital file': ['ProRes QuickTime', '399231']
           }


def get_country(code: str) -> Optional[str]:
    '''
    Use language.yaml to extract Country name from ISO code
    '''
    code = code.lower()
    with open(LANGUAGE_MAP, 'r') as files:
        data = (yaml.load(files, Loader=yaml.FullLoader))
        dct = data["languages"]
        for key, val in dct.items():
            if code == key:
                return str(val)


def firstname_split(person: str) -> str:
    '''
    Splits 'firstname surname' and returns 'surname, firstname'
    '''
    person = person.title()
    name_list: list[str] = person.split()
    count: int = len(person.split())
    if count > 2:
        firstname, *rest, surname = name_list
        rest_names = ' '.join(rest)
        return surname + ", " + firstname + " " + rest_names
    elif count > 1:
        firstname, surname = name_list
        return surname + ", " + firstname
    else:
        return person


def fetch_work_copy(work_id: str) -> str:
    '''
    Retrieve the work_copy for individual work_id returns path of downloaded file
    '''
    params: dict[str, str] = {"work_id": work_id}
    dct = requests.request("GET", WORK_COPY_URL, headers=HEADERS, params=params)
    fname = os.path.join(STORAGE_PATH, f"{work_id}_work_copy.json")
    # Outputs response files to storage_path, named as work_id_work_copy
    with open(fname, 'w') as file:
        json.dump(dct.json(), file, indent=4)
    return fname


def fetch_work_season(work_id: str) -> str:
    '''
    Retrieve the work_season for individual work_id returns path of downloaded file
    '''
    params = {"work_id": work_id}
    dct = requests.request("GET", WORK_SEASON_URL, headers=HEADERS, params=params)
    fname = os.path.join(STORAGE_PATH, f"{work_id}_work_season.json")
    # Outputs response files to storage_path, named as work_id_work_season
    with open(fname, 'w') as file:
        json.dump(dct.json(), file, indent=4)
    return fname


def return_mins(secs: int | str) -> str:
    '''
    Convert seconds to minutes
    '''
    minutes = int(secs) // 60
    return str(minutes)


def json_name_split(file: str) -> tuple[str, str]:
    '''
    Splits name of work record downloaded with prefix season_code in title
    '''
    filename: str = os.path.splitext(file)[0]
    split: list[str] = filename.split('_', 3)
    try:
        season_code: str = split[0]
        season_id: str = split[1]
    except (KeyError, IndexError):
        LOGGER.warning("Unable to split json file %s", file)
        season_code = ''
        season_id = ''

    return (season_code, season_id)


def move_file(fname: str) -> None:
    '''
    Move json files dealt with to completed/ folder
    '''
    if os.path.exists(fname):
        filename: str = os.path.basename(fname)
        file_completed: str = os.path.join(JSON_COMPLETED, filename)
        try:
            shutil.move(fname, file_completed)
            LOGGER.info("Moved %s to completed folder", fname)
        except Exception as err:
            LOGGER.warning("Unable to move %s to completed folder %s\n%s", fname, file_completed, err)


def cid_retrieve(search: str) -> Optional[tuple[str | list, str, str]]:
    '''
    Retrieve grouping data abd title using priref of work dictionary
    '''
    record = adlib.retrieve_record(CID_API, 'works', search, '0', ['grouping.lref', 'title', 'edit.name'])[1]
    LOGGER.info("cid_retrieve(): Making CID query request with:\n %s", search)
    if not record:
        LOGGER.exception("cid_retrieve(): Unable to retrieve data")
        return None
    try:
        groupings = adlib.retrieve_field_name(record[0], 'grouping.lref')
        LOGGER.info("********* Groupings: %s", groupings)
    except (KeyError, IndexError):
        groupings = []
        LOGGER.warning("cid_retrieve(): Unable to access grouping")

    try:
        title = adlib.retrieve_field_name(record[0], 'title')[0]
        title = title.rstrip('\n')
    except (KeyError, IndexError):
        title = ""
        LOGGER.warning("cid_retrieve(): Unable to access title")
    try:
        edit_name = adlib.retrieve_field_name(record[0], 'edit.name')[0]
    except (KeyError, IndexError):
        edit_name = ""
        LOGGER.warning("cid_retrieve(): Unable to access edit name")

    return groupings, title, edit_name


def work_quick_check(dct=None) -> tuple[str, str, str, str]:
    '''
    Retrieve work_id, priref and art_form from existing Artifax work
    '''
    if dct is None:
        dct = []
        LOGGER.warning("No dictionary data passed through to work_quick_check()")

    if dct['work_id']:
        work_id: str = dct['work_id']
    else:
        work_id = ''
        LOGGER.warning("Unable to retrieve work_id")
    print(f"Work ID: {work_id}")
    if dct['art_form']:
        art_form: str = dct['art_form']
    else:
        art_form = ''
        LOGGER.warning("Unable to retrieve art_form")

    priref = cid_import = ''
    for custom_dct in dct['custom_forms'][0]['custom_form_sections'][0]['custom_form_elements']:
        if custom_dct['custom_form_element_id'] == 1004:
            print(f"Custom dictionary: {custom_dct}")
            priref = custom_dct['custom_form_data_value']
        if custom_dct['custom_form_element_id'] == 1307:
            cid_import = custom_dct['custom_form_data_value']

    return (work_id, priref, cid_import, art_form)


def work_season_retrieve(fname: str, supplied_season_id: str) -> tuple[str]:
    '''
    Obtain various statuses from work_season for work_id, if season_id match
    '''

    with open(fname, 'r') as inf:
        dcts = json.load(inf)
        for dct in dcts:
            if not isinstance(dct, dict):
                return None

            season_id: str = ''
            if dct['season_id']:
                season_id = dct['season_id']
            print(f"Season ID: {season_id}")
            if str(season_id) not in str(supplied_season_id):
                continue

            work_season_id: str = ''
            if dct['work_on_season_id']:
                work_season_id = dct['work_on_season_id']
            start_date = ""
            if dct['date_first_confirmed_public_event']:
                start_date = dct['date_first_confirmed_public_event']

            # Iterate custom entries for custom data values
            for custom_dct in dct['custom_forms'][0]['custom_form_sections'][0]['custom_form_elements']:
                advanced_confirm = ""
                if custom_dct['custom_form_element_id'] == 1148:
                    advanced_confirm = custom_dct['custom_form_data_value']
                qanda_confirm = ""
                if custom_dct['custom_form_element_id'] == 1143:
                    qanda_confirm = custom_dct['custom_form_data_value']
                qanda_date = ""
                if custom_dct['custom_form_element_id'] == 1144:
                    qanda_date = custom_dct['custom_form_data_value']
                nfa_category = ""
                if custom_dct['custom_form_element_id'] == 1142:
                    nfa_category = custom_dct['custom_form_data_value']

            for custom_dct in dct['custom_forms'][1]['custom_form_sections'][0]['custom_form_elements']:
                cid_import_date = ""
                if custom_dct['custom_form_element_id'] == 1176: # Changed from 1312
                    cid_import_date = custom_dct['custom_form_data_value']

        inf.close()
        move_file(fname)

        return (advanced_confirm, qanda_confirm, qanda_date, cid_import_date, nfa_category, start_date, work_season_id)


def work_copy_extraction(fname: str, current_festival):
    '''
    Extracts data from newly downloaded work_copy for specified work_id
    Only returns data if the festival matches that being processed in main() dct
    Two if statements allow for both festival and internet manifestations to be generated where both present
    '''
    manifestation_internet_dct = []
    manifestation_festival_dct = []
    with open(fname, 'r') as inf:
        dcts = json.load(inf)
        if str(dcts).startswith('No results'):
            return 'No results'

        for dct in dcts:
            description = ''
            if dct['description']:
                description = dct['description']

            cid_format_type = ''
            for custom_dct in dct['custom_forms'][0]['custom_form_sections'][0]['custom_form_elements']:
                if custom_dct['custom_form_element_id'] == 1177:
                    cid_format_type = custom_dct['custom_form_data_value']

            # Check description in dct matches current festival only
            if description.upper() in current_festival.upper():
                # Extract matches that flagged as 'internet'
                if (
                    cid_format_type != ''
                    and 'internet' in cid_format_type.lower()
                ):
                    try:
                        film_format_name = dct['film_format_name']
                    except (KeyError, IndexError):
                        film_format_name = ''
                    for key, val in FORMATS.items():
                        if (
                            film_format_name != ''
                            and str(film_format_name) in key
                        ):
                            manifestation_internet_dct.append({'format_low_level.lref': val[1]})
                    try:
                        colour_data = dct['colour']
                    except (KeyError, IndexError):
                        colour_data = ''
                    if 'colour' in colour_data.lower():
                        manifestation_internet_dct.append({'colour_manifestation': 'C'})
                    if 'black and white' in colour_data.lower():
                        manifestation_internet_dct.append({'colour_manifestation': 'B'})
                    try:
                        duration_secs = dct['duration']
                        duration = return_mins(duration_secs)
                        manifestation_internet_dct.append({'runtime': duration})
                    except (KeyError, IndexError):
                        duration_secs = ''
                        duration = ''
                    try:
                        aspect_ratio = dct['aspect_ratio_name']
                        # Handling null return
                        if aspect_ratio:
                            manifestation_internet_dct.append({'aspect_ratio': aspect_ratio})
                        else:
                            manifestation_internet_dct.append({'aspect_ratio': ''})
                    except (KeyError, IndexError):
                        aspect_ratio = ''
                    try:
                        work_copy_id = dct['work_copy_id']
                        manifestation_internet_dct.append({'alternative_number.type': 'Artifax work_copy id number'})
                        manifestation_internet_dct.append({'alternative_number': work_copy_id})
                    except (KeyError, IndexError):
                        work_copy_id = ''
                # Extract matches that flagged as 'festival'
                if (
                    cid_format_type != ''
                    and 'festival' in cid_format_type.lower()
                ):
                    try:
                        film_format_name = dct['film_format_name']
                    except (KeyError, IndexError):
                        film_format_name = ''
                    for key, val in FORMATS.items():
                        if (
                            film_format_name != ''
                            and str(film_format_name) in key
                        ):
                            manifestation_festival_dct.append({'format_low_level.lref': val[1]})
                    try:
                        colour_data = dct['colour']
                    except (KeyError, IndexError):
                        colour_data = ''
                    if 'colour' in colour_data.lower():
                        manifestation_festival_dct.append({'colour_manifestation': 'C'})
                    elif 'black and white' in colour_data.lower():
                        manifestation_festival_dct.append({'colour_manifestation': 'B'})
                    try:
                        duration_secs = dct['duration']
                        duration = return_mins(duration_secs)
                        manifestation_festival_dct.append({'runtime': duration})
                    except (KeyError, IndexError):
                        duration = ''
                    try:
                        aspect_ratio = dct['aspect_ratio_name']
                        if aspect_ratio:
                            manifestation_festival_dct.append({'aspect_ratio': aspect_ratio})
                        else:
                            manifestation_festival_dct.append({'aspect_ratio': ''})
                    except (KeyError, IndexError):
                        aspect_ratio = ''
                    try:
                        work_copy_id = dct['work_copy_id']
                        manifestation_festival_dct.append({'alternative_number.type': 'Artifax work_copy id number'})
                        manifestation_festival_dct.append({'alternative_number': work_copy_id})
                    except (KeyError, IndexError):
                        work_copy_id = ''
            else:
                LOGGER.info("work_copy_extraction(): SKIPPING as festivals don't align.")

        inf.close()
        move_file(fname)

        return (manifestation_internet_dct, manifestation_festival_dct)


def work_extraction(season_id, dct=None):
    '''
    Outputting data from Artifax work dct, as manifestation data or work data formatted for CID
    Also outputting
    '''
    if dct is None:
        dct = []
        LOGGER.warning("No dictionary data passed through to work_extraction()")

    work_append_dct = ([{'edit.name': 'datadigipres'},
                        {'edit.date': TODAY},
                        {'edit.time': str(datetime.datetime.now())[11:19]},
                        {'edit.notes': 'Artifax BFI Southbank scheduling system - automated record update'}])
    qna_title_dct = []
    manifestation_dct = []
    work_date_dct = []
    work_desc_dct = []
    season_match = False

    seasons_dct = dct['seasons']
    for season in seasons_dct:
        season_id_dct = season['season_id']
        if str(season_id_dct) == str(season_id):
            season_match = True
    if not season_match:
        LOGGER.warning("work_extraction(): Season's don't match. No data extracted")
        return None

    # Title dictionary data extraction
    try:
        title1 = dct['titles'][0]['title']
        language1 = dct['titles'][0]['language_code']
    except (KeyError, IndexError):
        title1 = ''
        language1 = ''
        LOGGER.warning("Unable to extract title1 and language code1")
    try:
        title2 = dct['titles'][1]['title']
        language2 = dct['titles'][1]['language_code']
    except (KeyError, IndexError):
        title2 = ''
        language2 = ''
        LOGGER.warning("Unable to extract title2 and language code2")

    title1 = title1.rstrip('\n')
    title2 = title2.rstrip('\n')

    # Convert ISO country code to full country title
    country1 = get_country(language1)
    try:
        country2 = get_country(language2)
    except (KeyError, IndexError):
        LOGGER.info("No second title to enable country code retrieval")

    # Create title articles if available
    title_data1 = []
    title_data2 = []
    title_data1 = title_article.splitter(title1, language1)
    title_data2 = title_article.splitter(title2, language2)

    # Append to manifestation_dct
    if (len(title_data2[0]) > 0 and language2.lower() == 'en'):
        manifestation_dct.append({'title': f'{title_data2[0]}'})
        if len(title_data2[1]) > 0:
            manifestation_dct.append({'title.article': title_data2[1]})
        else:
            manifestation_dct.append({'title.article': ''})
        manifestation_dct.append({'title.type': '05_MAIN'})
        manifestation_dct.append({'title.language': country2})
    else:
        # Make title1 main title
        manifestation_dct.append({'title': f'{title_data1[0]}'})
        if len(title_data1[1]) > 0:
            manifestation_dct.append({'title.article': title_data1[1]})
        else:
            manifestation_dct.append({'title.article': ''})
        manifestation_dct.append({'title.type': '05_MAIN'})
        manifestation_dct.append({'title.language': country1})

    try:
        certification = dct['certificates'][0]['name']
    except (KeyError, IndexError):
        certification = ''
    if len(certification) > 0:
        manifestation_dct.append({'utb.fieldname': 'BBFC and BFI festival certifications'})
        manifestation_dct.append({'utb.content': certification})

    # Append to qna_title_dct
    if (len(title_data2[0]) > 0 and language2.lower() == 'en'):
        qna_title_dct.append({'title': f'{title_data2[0]} Q&A'})
        if len(title_data2[1]) > 0:
            qna_title_dct.append({'title.article': title_data2[1]})
        else:
            qna_title_dct.append({'title.article': ''})
        qna_title_dct.append({'title.type': '05_MAIN'})
        qna_title_dct.append({'title.language': country2})
    else:
        # Make title1 main title
        qna_title_dct.append({'title': f'{title_data1[0]} Q&A'})
        if len(title_data1[1]) > 0:
            qna_title_dct.append({'title.article': title_data1[1]})
        else:
            qna_title_dct.append({'title.article': ''})
        qna_title_dct.append({'title.type': '05_MAIN'})
        qna_title_dct.append({'title.language': country1})

    # Append to work_append_dct
    try:
        work_type = dct['type']
        work_append_dct.append({'production.notes': work_type})
    except (KeyError, IndexError):
        work_type = ''
    if 'television' in work_type.lower():
        work_append_dct.append({'work_type': 'T'})
    else:
        work_append_dct.append({'work_type': 'F'})
    try:
        prod_date = dct['dates'][0]['date']
        prod_date = str(prod_date)
        if len(prod_date) > 2:
            work_date_dct = ([{'title_date_start': prod_date},
                              {'title_date.type': '02_P'}])
    except (KeyError, IndexError):
        prod_date = ''
        LOGGER.warning("Unable to extract prod_date")
    try:
        description_raw = dct['short_description']
        if len(description_raw) > 1:
            description = description_raw.replace('\r\n', ' ')
            work_desc_dct = ([{'description': description},
                              {'description.type': 'Synopsis'},
                              {'description.date': TODAY}])
    except (KeyError, IndexError):
        description = ''
        LOGGER.warning("Unable to extract short_description from Artifax")
    work_append_dct.extend(work_date_dct)
    work_append_dct.extend(work_desc_dct)
    try:
        genre = dct['genres'][0]['genre_name']
    except (KeyError, IndexError):
        genre = ''
    try:
        subgenre = dct['genres'][0]['subgenre_name']
    except (KeyError, IndexError):
        subgenre = ''
    if len(genre) > 1:
        work_append_dct.append({'content.genre': genre})
    if len(subgenre) > 1:
        work_append_dct.append({'content.genre': subgenre})

    # Countries pass to function for iteration, return countries dictionary
    country_dct = []
    country_dct = country_check(dct['countries'])
    work_append_dct.extend(country_dct)
    print("----------- work extraction manifestation_dct -------------")
    print(manifestation_dct)
    return qna_title_dct, manifestation_dct, work_append_dct


def country_check(dct=None):
    '''
    Takes complete dct['country'] group and splits into separate entries
    formatted for CID work record appending
    '''
    work_country_dct = []
    if dct is None:
        dct = []
        LOGGER.warning("No dictionary data passed through to country_check()")
    for country in dct:
        try:
            country_name = country['country_name']
            work_country_dct.append({'production_country': country_name})
        except (KeyError, IndexError):
            country_name = ''

    return work_country_dct


def season_check(season_num, dct=None):
    '''
    Receives single dct entry from dct['season'] and checks if it matches
    current season_num from main() dct. Returns if yes, otherwise returns None
    '''
    if dct is None:
        dct = []
        LOGGER.warning("season_check(): No dictionary data passed through to season_check()")
    try:
        season_id = dct['season_id']
    except (KeyError, IndexError):
        LOGGER.warning("season_check(): Unable to obtain season_id")
        season_id = ''
    if int(season_num) == int(season_id):
        try:
            accepted = dct['work_on_season_status_name']
        except (KeyError, IndexError):
            LOGGER.warning("season_check(): Unable to obtain accepted value for season appearance")
            accepted = ''
        return (season_id, accepted)


def main():
    '''
    Open season works downloaded in artifax record creation, processes work_ids one by one
    Appends data to existing CID work record, accessed via priref from Artifax work record
    Where Q&A custom field active, CID Work record and new manifestation created for Q&A Internet
    Downloads work_copy and where copies exist with 'Internet' or 'Festival' makes CID manifestations
    '''
    if not utils.check_control('pause_scripts'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    # Opening log statement
    LOGGER.info('========== Python3 start: document_artifax_record_population ==========')

    # Download all json filepaths to files variable
    files = os.listdir(STORAGE_PATH)
    for file in files:
        if not file.endswith('.json') and not file.startswith(('LFF20', 'FL20', 'FFF20')):
            continue
        if (
            'Artifax_seasons' in str(file)
            or 'work_copy' in str(file)
            or 'work_season' in str(file)
        ):
            continue
        filename = os.path.basename(file)
        LOGGER.info("JSON file found to process: %s", filename)
        current_festival = ''
        festival_grouping = ''
        season_code = ''
        season_id = ''
        season_code, season_id = json_name_split(file)
        # Check if season_code corresponds to FESTIVAL dictionary (ie, qualifies for record creation)
        for key, value in FESTIVALS.items():
            if season_code in key:
                # Obtain current festival data
                current_festival = value[1]
                festival_grouping = value[2]
        LOGGER.info("== Current festival: %s. Festival grouping: %s", current_festival, festival_grouping)
        if festival_grouping != '399153':
            continue
        # Open json and load all contents to dcts, then iterate through with dct (per work_id)
        filepath = os.path.join(STORAGE_PATH, filename)
        with open(filepath, 'r') as inf:
            dcts = json.load(inf)
            for dct in dcts:
                # Artifax quick check
                if not isinstance(dct, dict):
                    LOGGER.info("SKIPPING: Dictionary missing, only string: %s", dct)
                    continue
                check_data = work_quick_check(dct)
                work_id = check_data[0]
                priref = check_data[1]
                cid_import = check_data[2]
                art_form = check_data[3]
                print(f"Work ID: {work_id}, Priref: {priref}, CID Import: {cid_import}, Art form: {art_form}")
                if 'visual' in art_form.lower():
                    LOGGER.info("Visual performance found. Skipping!")
                    continue
                # Download and retrieve necessary decision making data from work_season
                try:
                    work_season_fname = fetch_work_season(work_id)
                except FileNotFoundError:
                    LOGGER.warning("Skipping as work_season.json not available for work: %s", work_id)
                    continue
                if not os.path.exists(work_season_fname):
                    LOGGER.warning("Skipping as work_season.json not present for work_id %s", work_id)
                    continue
                # Retrieve needed data
                print(f"Work season: {work_season_fname}")
                check_work_season = work_season_retrieve(work_season_fname, season_id)
                if check_work_season is None:
                    LOGGER.warning("SKIPPING WORK ID: %s Work Season returned None. Check work season data exists: %s", work_id, work_season_fname)
                    continue
                advanced_confirm = str(check_work_season[0])
                qanda_confirm = str(check_work_season[1])
                qanda_date = str(check_work_season[2])
                cid_import_date = str(check_work_season[3])
                nfa_category = str(check_work_season[4])
                start_date = str(check_work_season[5])
                work_season_id = str(check_work_season[6])

                # Check if Priref present (passed first stage) and advanced_confirmed (cleared for second stage)
                if (len(priref) > 0 and 'yes' not in advanced_confirm.lower()):
                    LOGGER.info("SKIPPING Work ID: %s is not confirmed for advanced record population", work_id)
                    continue
                if len(priref) == 0:
                    LOGGER.info("SKIPPING Work ID: %s has no priref present in data.", work_id)
                    continue

                LOGGER.info("-------------- STAGE ONE: WORK FOUND FOR PROCESSING --------------")
                LOGGER.info("Artifax work record found for processing priref %s", priref)

                # Retrieve CID data, UTB (if needed are 0 and 1), ob_num and grouping
                search = f'priref="{priref}"'
                edit_name = ''
                try:
                    cid_data = cid_retrieve(search)
                    groupings = cid_data[0]
                    # grouping 2 should always be for BFI player and not required
                    cid_title = cid_data[1]
                    edit_name = cid_data[2]
                    LOGGER.info("CID grouping and title data retrieved for priref %s: %s", priref, cid_title)
                except Exception as err:
                    LOGGER.warning("Unable to access grouping for priref %s", priref, err)
                    groupings = []
                    cid_title = ''
                    edit_name = ''
                # Extract work metadata lists from work_id
                work_data = []
                work_data = work_extraction(season_id, dct)
                if not work_data:
                    LOGGER.warning("Work extraction failed, exiting")
                    continue
                qna_title_dct = work_data[0]
                manifestation_dct = work_data[1]
                work_append_dct = work_data[2]
                manifestation_dct.append({'grouping.lref': festival_grouping})

                # Assess what actions to enact on Work record found
                set_zero = set_one = set_two = set_three = False
                update_work = make_man = make_qna = append_new_grouping = push_lock = False

                # If 'CID import' field is 'checked' work is historical CID work (ie Film Fund/Treasure)
                # Be careful with cid_import check as it returns unchecked as '0' or checked as '1'.
                if cid_import_date != '':
                    LOGGER.info("SKIPPING FURTHER STAGES: CID Import date populated and manifestations created already for Festival: %s", festival_grouping)
                    continue
                elif str(cid_import) == '1':
                    set_zero = True
                # If edit_name has 'datadigipres' and grouping in CID does not match festival:
                elif ('datadigipres' in str(edit_name) and festival_grouping not in groupings[0]):
                    if len(groupings) >= 3:
                        if festival_grouping in groupings[2]:
                            LOGGER.info("SKIPPING FURTHER STAGES: Manifestations created already for Festival: %s", festival_grouping)
                            continue
                    set_one = True
                # Work record data appended. Make manifestations if import date == 0, otherwise skip.
                elif ('datadigipres' in str(edit_name) and festival_grouping in groupings[0]):
                    set_two = True
                # Work found unedited, begin appending/creation of new whole record
                elif 'datadigipres' not in str(edit_name):
                    if festival_grouping not in groupings[0]:
                        LOGGER.info("SKIPPING FURTHER STAGES: Festival groupings do not match for this record %s and %s", festival_grouping, groupings[0])
                        continue
                    set_three = True
                else:
                    LOGGER.warning("SKIPPING FURTHER STAGES: Unable to identify how to process Work ID: %s", work_id)
                    continue

                # Choose which set of actions to trigger
                if set_zero:
                    LOGGER.info("----------- SKIPPING STAGE TWO - %s EXISTING WORK NOT TO BE OVERWRITTEN ---------", cid_title)
                    LOGGER.info("Existing CID record has a new Festival appearance! Creating new Manifestation")
                    make_man = True
                    if 'yes' in qanda_confirm.lower():
                        make_qna = True
                    append_new_grouping = True
                    push_lock = True

                if set_one:
                    LOGGER.info("----------- SKIPPING STAGE TWO - %s WORK RECORD FROM PREVIOUS FESTIVAL ---------", cid_title)
                    LOGGER.info("Completed record found with new Festival appearance! Creating new Manifestation")
                    make_man = True
                    if 'yes' in qanda_confirm.lower():
                        make_qna = True
                    append_new_grouping = True
                    push_lock = True

                if set_two:
                    LOGGER.info("----------- SKIPPING STAGE TWO - %s WORK RECORD DATA APPENDED ALREADY ---------", cid_title)
                    LOGGER.info("New manifestations will still be created and linked to CID Work %s. Work ID %s", priref, work_id)
                    LOGGER.warning("This may result in duplicate Manifestations/Q&A records if date lock push failed previously")
                    make_man = True
                    if 'yes' in qanda_confirm.lower():
                        make_qna = True
                    push_lock = True

                if set_three:
                    LOGGER.info("CID Work %s being updated, no date lock and festival groupings match", priref)
                    update_work = True
                    make_man = True
                    if 'yes' in qanda_confirm.lower():
                        make_qna = True
                    push_lock = True

                # ============ Append work_append_dct to Work Record ============ #
                if update_work:
                    LOGGER.info("-------------- STAGE TWO: APPEND WORK DATA TO CID %s --------------", cid_title)
                    # Prepare NFA_category data from work_season and append to manifsation_dct
                    if nfa_category != '':
                        if nfa_category.lower() == 'non fiction':
                            work_append_dct.append({'nfa_category': 'D'})
                        elif nfa_category.lower() == 'fiction':
                            work_append_dct.append({'nfa_category': 'F'})

                    # Extract credits, make people records where needed and return prirefs to append to Work
                    credit_priref_dct = credit_check(nfa_category, dct['credits'])
                    cast_dct = []
                    credit_dct = []
                    for person in credit_priref_dct:
                        if '1' in person[0]:
                            cast_dct.append({'cast.credit_credited_name': person[4]})
                            cast_dct.append({'cast.credit_type': 'Cast'})
                            cast_dct.append({'cast.sequence': person[3]})
                            cast_dct.append({'cast.section': '[normal cast]'})
                        else:
                            credit_dct.append({'credit.credited_name': person[4]})
                            credit_dct.append({'credit.type': person[1]})
                            credit_dct.append({'credit.sequence': person[3]})
                            credit_dct.append({'credit.sequence.sort': person[2]})
                            credit_dct.append({'credit.section': '[normal credit]'})

                    # Append cast/credit blocks to work_append_dct
                    work_append_dct.extend(cast_dct)
                    work_append_dct.extend(credit_dct)
                    LOGGER.info("Appending data to work record now...")
                    work_update(priref, work_append_dct)
                    LOGGER.info("Checking work_append_dct written to CID Work record")
                    edit_data = cid_retrieve(f'priref="{priref}"')
                    if 'datadigipres' in str(edit_data[2]):
                        LOGGER.info("** Successfully appended additional Artifax data to work record %s", priref)
                    else:
                        LOGGER.warning("Work update failed for CID Work %s and Artifax work %s. Exiting this record for retry later", priref, work_id)
                        LOGGER.info("PRIREF: %s\n%s", priref, work_append_dct)
                        continue


                # ============ Create Manifestations ============ #
                if make_man:
                    LOGGER.info("-------------- STAGE THREE: CREATE MANIFESTATION %s --------------", cid_title)
                    work_copy_fname = fetch_work_copy(work_id)
                    print(f"******* {work_copy_fname} ********")
                    if len(work_copy_fname) > 0:
                        print(f"make_manifestations({work_copy_fname}, {current_festival}, {start_date}, {priref}, {manifestation_dct})")
                        success_man = make_manifestations(work_copy_fname, current_festival, start_date, priref, manifestation_dct)
                        if success_man == 'No results':
                            LOGGER.critical("Error creating manifestation for %s using work copy: %s", priref, work_copy_fname)
                            LOGGER.warning("The work copy downloaded is returning 'No Results'")
                            continue
                        if success_man:
                            LOGGER.info("** Manifestations created for CID work %s", priref)
                        else:
                            LOGGER.critical("Error creating manifestation for %s using work copy: %s", priref, work_copy_fname)
                            LOGGER.warning("Skipping this record's completion. Script to pick up in next pass")
                            continue
                    else:
                        LOGGER.info("Work copy filename was not returned from fetch_work_copy(). Exiting for retry later.")
                        continue

                # ============ Create Q&A Work & Manifestation ============ #
                if make_qna:
                    LOGGER.info("-------------- STAGE FOUR: CREATE Q&A %s --------------", cid_title)
                    success_qna = make_qanda(qanda_date, priref, groupings[0], qna_title_dct, manifestation_dct)
                    if success_qna:
                        LOGGER.info("Q&A Work and Manifestation records created for Priref %s", priref)
                    else:
                        LOGGER.critical("Creation of Q&A Work and Manifestation failed for CID record %s and Artifax work %s", priref, work_id)
                        continue
                else:
                    LOGGER.info("SKIPPING STAGE FOUR: No Q&A data present")

                # ============ Append new grouping to Work record ============ #
                if append_new_grouping:
                    LOGGER.info("-------------- STAGE FIVE: APPENDING GROUP TO WORK RECORD %s --------------", cid_title)
                    LOGGER.info("Pushing new grouping %s to Work record %s", festival_grouping, priref)
                    grouping_dct = {'grouping.lref': festival_grouping}
                    success_group = work_update(priref, grouping_dct)
                    if success_group:
                        LOGGER.info("New Festival grouping appended to CID work %s", priref)
                    else:
                        LOGGER.critical("Failed to update grouping %s to CID Work record priref %s", festival_grouping, priref)
                        continue

                # ============ Push date lock to Artifax ============ #
                if push_lock:
                    LOGGER.info("-------------- STAGE FIVE: PUSH DATE LOCK TO ARTIFAX %s --------------", cid_title)
                    LOGGER.info("Pushing date lock to Artifax work season form with ID: %s", work_season_id)
                    push_check = push_date_artifax(work_season_id)
                    if push_check:
                        LOGGER.info("Pushed date lock to Artifax CID Import Date field")
                    else:
                        LOGGER.critical("FAILED TO WRITE ARTIFAX LOCK TO WORK ID: %s - Script exiting and manual attention will be required", work_id)
                        continue
                LOGGER.info("-------------- ALL STAGES COMPLETED SUCCESSFULLY FOR %s --------------", cid_title)

        # When all extraction completed, move json to completed folder for next script interaction
        inf.close()
        move_file(filepath)

    LOGGER.info("================ Script completed ===================================\n")


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_manifestations(work_copy_fname: str, current_festival: str, start_date: str, priref: str, manifestation_dct=None) -> bool:
    '''
    Function to form manifestations manifestations and their creation
    '''
    man_int_priref: str = ''
    man_fest_priref: str = ''
    if manifestation_dct is None:
        print("*** WORK COPY DATA NONE ***")
        manifestation_dct = {}
    work_copy_data = work_copy_extraction(work_copy_fname, current_festival)
    print("--------------- Work copy data ------------------")
    print(work_copy_data)

    if work_copy_data == 'No results':
        return 'No results'
    try:
        manifestation_internet_dct = work_copy_data[0]
        manifestation_festival_dct = work_copy_data[1]
    except (KeyError, IndexError):
        manifestation_internet_dct = []
        manifestation_festival_dct = []
        LOGGER.exception("make_manifestations(): Unable to retrieve work_copy_id or file_format_name")

    # Create Manifestation records
    if len(manifestation_internet_dct) > 0:
        print("Attempting to make Manifestation for BFI Player")
        man_int_priref = manifestation_create(start_date, 'internet', priref, manifestation_dct, manifestation_internet_dct)
        LOGGER.info("make_manifestations(): Creation of Internet manifestation record successful. Priref %s", man_int_priref)
    else:
        print("No Internet manifestation at this time, skipping")
    if len(manifestation_festival_dct) > 0:
        print("Attempting to make Manifestation for Festival showing")
        man_fest_priref = manifestation_create(start_date, 'festival', priref, manifestation_dct, manifestation_festival_dct)
        LOGGER.info("make_manifestations(): Creation of Festival manifestation record successful. Priref %s", man_fest_priref)
    else:
        print("No Festival manifestation at the this time, skipping")

    # Check if priref's exist for either and return True
    if (len(man_int_priref) > 5 or len(man_fest_priref) > 5):
        return True

    LOGGER.info("Failed to create manifestation: \n%s\n\n%s\n\n%s", manifestation_dct, manifestation_internet_dct, manifestation_festival_dct)
    return False


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def make_qanda(qanda_date, priref, grouping, qna_title_dct=None, manifestation_dct=None) -> Optional[bool]:
    '''
    Creates new Q&A Work record with tenacity retry
    '''
    man_qna_priref: str = ''
    qna_title_dct = {} if qna_title_dct is None else qna_title_dct.copy()
    manifestation_dct = {} if manifestation_dct is None else manifestation_dct.copy()

    qna_priref = create_qna_work(qanda_date, priref, grouping, qna_title_dct)
    if len(qna_priref) > 0:
        print(f"** Q&A Work record created with Priref {qna_priref}.")
        LOGGER.info("make_qanda(): ** Q&A Work record created, priref %s", qna_priref)

        # Creates new Q&A Manifestation record with tenacity retry
        man_qna_priref = manifestation_create(qanda_date, 'qanda', qna_priref, manifestation_dct, None)
        if len(man_qna_priref) > 0:
            print(f"** Q&A Manifestation record created with Priref {man_qna_priref}.")
            LOGGER.info("** Q&A Manifestation record created, priref %s", man_qna_priref)
            return True


def credit_check(nfa_cat, dct=None):
    '''
    Receives credits dictionary from main(), iterates over entries and checks if CID person record exists
    Passes current credit dictionary to make_person_dct to form credit_dct for person record creation
    Returns a completed priref dct to main() to be written to film work records
    '''
    if dct is None:
        dct = []
        LOGGER.warning("credit_check(): Empty credit dictionary passed")

    credit_priref_dct = []

    for dictionary in dct:
        try:
            credit_name = dictionary['credit_entity_full_name']
            credit_type = dictionary['credit_type_name']
            credit_order = dictionary['order']
            credit_order = str(credit_order)
            credit_order_pad = credit_order.zfill(4)
            credit_ordering = credit_order.zfill(2)
        except (KeyError, IndexError):
            LOGGER.warning("credit_check(): Unable to obtain credit_entity_id")
            credit_type = ''
            credit_name = ''
            credit_order = ''
        credit_name = str(credit_name)

        # Does the dictionary have a name entry? If yes, append to list
        if len(credit_name) > 0:
            if 'actor' in credit_type.lower():
                name = firstname_split(credit_name)
                credit_priref_dct.append(['1', 'Cast', 'None', f'{credit_ordering}', f'{name}'])
                continue
            if 'rights holder' in credit_type.lower():
                credit_priref_dct.append(['3', '©', f'850{credit_order_pad}', f'{credit_ordering}', f'{credit_name}'])
                continue
            if ('scriptwriter' in credit_type.lower() and nfa_cat.lower() == 'fiction'):
                name = firstname_split(credit_name)
                credit_priref_dct.append(['6', 'Screenplay', f'15000{credit_order_pad}', f'{credit_ordering}', f'{name}'])
                continue
            if ('scriptwriter' in credit_type.lower() and 'non fiction' in nfa_cat.lower()):
                name = firstname_split(credit_name)
                credit_priref_dct.append(['7', 'Script', f'15500{credit_order_pad}', f'{credit_ordering}', f'{name}'])
                continue
            if 'scriptwriter' in credit_type.lower():
                name = firstname_split(credit_name)
                credit_priref_dct.append(['6', 'Screenplay', f'15000{credit_order_pad}', f'{credit_ordering}', f'{name}'])
                continue
            if 'director' in credit_type.lower():
                name = firstname_split(credit_name)
                credit_priref_dct.append(['2', 'Director', f'500{credit_order_pad}', f'{credit_ordering}', f'{name}'])
                continue
            if 'producer' in credit_type.lower():
                name = firstname_split(credit_name)
                credit_priref_dct.append(['5', 'Producer', f'3010{credit_order_pad}', f'{credit_ordering}', f'{name}'])
                continue
            if 'production company' in credit_type.lower():
                credit_priref_dct.append(['4', 'Production Company', f'1000{credit_order_pad}', f'{credit_ordering}', f'{credit_name}'])
                continue
            else:
                LOGGER.warning("%s - credit_type_name unsupported at present (review may be required)", credit_type)
        else:
            LOGGER.info("credit_check(): Credit_name not present, exiting.")

    # Sort and return for write to CID Work
    credit_priref_dct.sort()
    return credit_priref_dct


def work_update(priref, work_dct=None):
    '''
    Items passed in work_dct for amending to Work record
    '''
    if work_dct is None:
        return False

    work_dct_xml = adlib.create_record_data(CID_API, 'works', priref, work_dct)
    print(f"WORK UPDATE: {priref} {work_dct_xml}")
    try:
        record = adlib.post(CID_API, work_dct_xml, 'works', 'updaterecord')
        LOGGER.info("Result of Adlib Work update: %s", record)
        return True
    except Exception as err:
        print("Unable to append work data to CID work record", err)
        return False


def manifestation_create(start_date, event_type, priref, manifestation_dct=None, manifestation_type_dct=None):
    '''
    Receive dictionarties from main() with argument for q&a, internet or festival manifestation creation
    Separate priref input to allow for Q&A manifestation to link to new Work record for Q&A
    '''
    if manifestation_dct is None:
        manifestation_dct = []
    if manifestation_type_dct is None:
        manifestation_type_dct = []

    # Append manifestaion data to manifestation_values
    application_restriction_date = str(datetime.date.today() + datetime.timedelta(120))
    manifestation_values = ([{'input.name': 'datadigipres'},
                             {'input.date': TODAY},
                             {'input.time': str(datetime.datetime.now())[11:19]},
                             {'input.notes': 'Artifax BFI Southbank scheduling system - automated record creation'},
                             {'record_type': 'MANIFESTATION'},
                             {'record_access.user': 'BFIiispublic'},
                             {'record_access.rights': '0'},
                             {'record_access.reason': 'SENSITIVE_LEGAL'},
                             {'record_access.date': TODAY},
                             {'record_access.duration': 'TEMP'},
                             {'record_access.review_date': application_restriction_date},
                             #{'record_access.user': '$REST'},
                             #{'record_access.rights': '1'},
                             #{'record_access.reason': 'SENSITIVE_LEGAL'},
                             #{'record_access.date': TODAY},
                             #{'record_access.duration': 'TEMP'},
                             #{'record_access.review_date': application_restriction_date},
                             {'record_access.user': 'vickr'},
                             {'record_access.rights': '2'},
                             {'record_access.owner': 'Festivals'},
                             {'country_manifestation': 'United Kingdom'},
                             {'part_of_reference.lref': priref}])

    manifestation_values.extend(manifestation_dct)

    if str(event_type) == 'qanda':
        print("**** CREATING Q&A MANIFESTATION DEFAULTS")
        manifestation_qanda_defaults = ([{'manifestationlevel_type': 'INTERNET'},
                                         {'format_high_level': 'Video - Digital'},
                                         {'colour_manifestation': 'C'},
                                         {'sound_manifestation': 'SOUN'},
                                         {'transmission_date': start_date},
                                         {'transmission_coverage': 'Streaming region'},
                                         {'vod_service_type.lref': '399063'},
                                         {'broadcast_company.lref': '999702457'},
                                         {'notes': 'Represents the BFIplayer publication of the Work on web platform / mobile app streamed during the Festival.'},
                                         {'aspect_ratio': '16:9'}])
        manifestation_values.extend(manifestation_qanda_defaults)

    if str(event_type) == 'internet':
        print("**** CREATING INTERNET MANIFESTATION DEFAULTS")
        manifestation_player_defaults = ([{'manifestationlevel_type': 'INTERNET'},
                                          {'format_high_level': 'Video - Digital'},
                                          {'transmission_date': start_date},
                                          {'transmission_coverage': 'Streaming region'},
                                          {'vod_service_type.lref': '399063'},
                                          {'broadcast_company.lref': '999702457'},
                                          {'notes': 'Represents the BFIplayer publication of the Work on web platform / mobile app streamed during the Festival.'}])
        manifestation_values.extend(manifestation_player_defaults)
        manifestation_values.extend(manifestation_type_dct)

    if str(event_type) == 'festival':
        print("**** CREATING FESTIVAL MANIFESTATION DEFAULTS")
        manifestation_southbank_defaults = ([{'manifestationlevel_type': 'FESTIVAL'},
                                             {'format_high_level': 'Video - Digital'},
                                             {'release_date': start_date},
                                             {'notes': 'Represents the Festival appearance of the Work.'}])
        manifestation_values.extend(manifestation_southbank_defaults)
        manifestation_values.extend(manifestation_type_dct)

    man_priref = ''
    # Create CID record for Manifestation
    man_values_xml = adlib.create_record_data(CID_API, 'manifestations', '', manifestation_values)
    print("---------------------------")
    print(man_values_xml)
    print("---------------------------")
    try:
        record = adlib.post(CID_API, man_values_xml, 'manifestations', 'insertrecord')
        print(record)
        if record:
            try:
                man_priref = adlib.retrieve_field_name(record, 'priref')[0]
                print(f'** Manifestation record created with Priref {man_priref}')
                LOGGER.info('Manifestation record created with priref %s', man_priref)
            except Exception as err:
                print('* Unable to create Manifestation record', err)
                LOGGER.critical('Unable to create Manifestation record %s', err)

    except Exception as err:
        print("Unable to write manifestation record - error:", err)
        LOGGER.critical("Unable to write manifestation record %s", err)

    return man_priref


def create_qna_work(qna_date: str, film_priref: str, grouping: str, qna_title_dct=None):
    '''
    Uses qna_title_dct, work_default and work_restricted_defaults to generate Q&A Work record in CID
    '''
    qna_priref = ''
    qna_title_dct = {} if qna_title_dct is None else qna_title_dct.copy()

    # Work record defaults, basic and retrieve priref/object_number for Artifax push
    application_restriction_date: str = str(datetime.date.today() + datetime.timedelta(120))
    application_restriction_date_8yr: str = str(datetime.date.today() + datetime.timedelta(2922))
    work_default: list[dict[str, str]] = []
    work_default = ({'record_type': 'WORK'},
                    {'input.name': 'datadigipres'},
                    {'input.date': TODAY},
                    {'input.time': str(datetime.datetime.now())[11:19]},
                    {'input.notes': 'Artifax BFI Southbank scheduling system - automated record creation'},
                    {'grouping.lref': '132071'},
                    {'work_type': 'I'},
                    {'nfa_category': 'D'},
                    {'production_country.lref': '73938'},
                    {'creator.lref': '1'},
                    {'title_date_start': qna_date},
                    {'title_date.type': 'Release'},
                    {'content.genre.lref': '73615'},
                    {'worklevel_type': 'MONOGRAPHIC'},
                    {'grouping.lref': grouping},
                    {'related_object.reference.lref': film_priref},
                    {'related_object.association.lref': '109657'},
                    {'related_object.notes': 'Q&A event for BFI festival film'})

    work_restricted_defaults = ({'application_restriction': 'DIGITAL_API_PUBLIC'},
                                {'application_restriction.date': TODAY},
                                {'application_restriction.reason': 'STRATEGIC'},
                                {'application_restriction.duration': 'TEMP'},
                                {'application_restriction.review_date': application_restriction_date},
                                {'application_restriction.authoriser': 'mcconnachies'},
                                {'application_restriction.notes': 'BFI Festivals record, restricted until after Festival launch.'},
                                {'application_restriction': 'MEDIATHEQUE'},
                                {'application_restriction.date': TODAY},
                                {'application_restriction.reason': 'STRATEGIC'},
                                {'application_restriction.duration': 'TEMP'},
                                {'application_restriction.review_date': application_restriction_date_8yr},
                                {'application_restriction.authoriser': 'mcconnachies'},
                                {'application_restriction.notes': 'BFI Festivals record, Mediatheque restriction review date 8 years.'},
                                {'record_access.user': 'BFIiispublic'},
                                {'record_access.rights': '0'},
                                {'record_access.reason': 'SENSITIVE_LEGAL'},
                                {'record_access.date': TODAY},
                                {'record_access.duration': 'TEMP'},
                                {'record_access.review_date': application_restriction_date},
                                #{'record_access.user': '$REST'},
                                #{'record_access.rights': '1'},
                                #{'record_access.reason': 'SENSITIVE_LEGAL'},
                                #{'record_access.date': TODAY},
                                #{'record_access.duration': 'TEMP'},
                                #{'record_access.review_date': application_restriction_date},
                                {'record_access.user': 'vickr'},
                                {'record_access.rights': '2'},
                                {'record_access.owner': 'Festivals'})
    work_values = []
    work_values.extend(qna_title_dct)
    work_values.extend(work_restricted_defaults)
    work_values.extend(work_default)
    print(work_values)
    # Create basic work record
    work_values_xml = adlib.create_record_data(CID_API, 'works', '', work_values)
    try:
        record = adlib.post(CID_API, work_values_xml, 'works', 'insertrecord')
        if record:
            try:
                qna_priref = adlib.retrieve_field_name(record, 'priref')[0]
                print(f'* Work record created with Priref {qna_priref}')
                LOGGER.info('create_work(): Work record created with priref %s', qna_priref)
            except Exception as err:
                LOGGER.warning("CID work id is not present - error: %s", err)
                raise
            return qna_priref
    except Exception as err:
        print(f'* Unable to create Work Q&A child record for <{film_priref}>')
        LOGGER.critical('create_work():Unable to create Work Q&A child record for <%s> \n%s', film_priref, err)


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def push_date_artifax(object_id):
    '''
    Script to push back date lock after all records amended/created
    Receives object_id which is work_season_id
    '''
    dct = []
    data = {'object_id': f"{object_id}",
            'object_type_id': '81',
            'custom_form_element_id': '1176',
            'custom_form_assignment_id': '29600',
            'custom_form_data_value': f"{TODAY}"}

    dct = requests.request('PUT', CUSTOM_URL, headers=HEADERS, data=data)
    print(dct.text)
    dct.raise_for_status()

    if 'custom_forms_data_id' in str(dct.text):
        return True


if __name__ == '__main__':
    main()
