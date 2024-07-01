#!/usr/bin/env python3

'''
PYTHON3.7 + ONLY

Fetch metadata from Artifax
1. Fetch season json, and check within for:
   - Checks end date within this or next year
   - Where this is the case, generates date_range of previous 90 days
   - Checks if today's date falls within this date_range
   - Outputs to season_list and passes to Festival season_code check against FESTIVALS dct
2. Fetch all work_id record from season_id that passes Festival season_code check
   - Dump all to single JSON
   - Iterate through JSON list. Where priref or art_form 'Visual' present, skip
   - Where any work status differs from 'Invited - Accepted' skip.
   - title_article.splitter iterates through dct looking for articles to remove, pass back
     to title_1/2 title_art_1/2
   - For remaining films create Work CID records (add work_id in alternative_number,
     season_code priref to thesaurus)
   - DIGITAL_API_PUBLIC added to application_restriction, BFI Player grouping necessary
     (for manifestations for Player, or all Works?)
3. Push CID priref back to Artifax using Tenacity retry instead of testing if API online
   (to be tested with Artifax sandbox)
4. Delete JSON files in completed/ folder if over 2 days since last modification

NOTE: Updated to Adlib V3

Joanna White
2021
'''

# Public packages
import os
import sys
import json
import yaml
import logging
import datetime
import requests
import tenacity
import itertools
from datetime import timedelta

# Local packages
import title_article
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Local date vars for script comparison/activation
TODAY_TIME = str(datetime.datetime.now())
TIME = TODAY_TIME[11:19]
DATE = datetime.date.today()
TODAY = str(DATE)
THIS_YEAR = TODAY[:4]
NEXT_YEAR = str(int(THIS_YEAR) + 1)

# Global variables
STORAGE_PATH = os.environ['SEASONS_PATH']
JSON_DELETE_PATH = os.environ['COMPLETED_PATH']
LANGUAGE_MAP = os.environ['LANGUAGE_YAML']
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API4']

# Setup logging (running from bk-qnap-video)
logger = logging.getLogger('document_artifax_record_creation')
hdlr = logging.FileHandler(os.path.join(LOG_PATH, 'document_artifax_record_creation.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Artifax url paths and header data (BFI specific, limited API)
WORK_URL = os.environ['ARTIFAX_WORK']
SEASON_URL = os.environ['ARTIFAX_SEASON']
CUSTOM_API = os.environ['ARTIFAX_CUSTOM']
HEADERS = {
    "X-API-KEY": os.environ['ARTIFAX_API_KEY']
}

# Data for CID Festival/Artifax season thesaurus look up
FESTIVALS = {
    'FL2021': ["119", "Flare 2021", "399108"],
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


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            logger.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def check_cid():
    '''
    Check CID is online
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        logger.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def date_gen(date_str):
    '''
    Attributed to Ayman Hourieh, Stackoverflow question 993358
    Python 3.7+ only for this function - fromisoformat()
    '''
    from_date = datetime.date.fromisoformat(date_str)
    while True:
        yield from_date
        from_date = from_date - datetime.timedelta(days=1)


def get_country(code):
    '''
    Use language.yaml to extract Country name from ISO code
    '''
    country = ''
    code = code.lower()
    with open(LANGUAGE_MAP, 'r') as files:
        data = (yaml.load(files, Loader=yaml.FullLoader))
        for _ in data.items():
            if str(code) in data['languages']:
                country = data['languages'][f'{code}']
    return country


def fetch_season():
    '''
    Retrieve season json from Artifax for season_id selection
    '''
    dct = requests.request("GET", SEASON_URL, headers=HEADERS)
    fname = os.path.join(STORAGE_PATH, "Artifax_seasons.json")
    # Outputs response files to storage_path
    with open(fname, 'w') as file:
        json.dump(dct.json(), file, indent=4)
    return fname


def fetch_works(season_code, season_id):
    '''
    Retrieve work_id data from Artifax for CID record generation
    '''
    params = {"season_id": season_id,
              "season_programming_status_id": 1}
    dct = requests.request("GET", WORK_URL, headers=HEADERS, params=params)
    fname = os.path.join(STORAGE_PATH, f"{season_code}_{season_id}_{TODAY}.json")
    # Outputs response files to storage_path, named as series_id
    with open(fname, 'w') as file:
        json.dump(dct.json(), file, indent=4)
    return fname


def retrieve_seasons(json_path):
    '''
    Open Season json, extract relevant end_dates
    Check if any return 3 month range that include today's date
    Append to season_list, and return
    '''
    with open(json_path, 'r') as inf:
        dcts = json.load(inf)
        season_list = []
        for dct in dcts:
            extract = dct['end_date']
            if THIS_YEAR in extract or NEXT_YEAR in extract:
                period = []
                period = itertools.islice(date_gen(extract), 90)
                date_range = str(list(period))
                date_check = itertools.islice(date_gen(TODAY), 1)
                check = str(list(date_check))
                if check[1:-1] in date_range:
                    season_id = dct['season_id']
                    season_code = dct['season_code']
                    season_list.append({'season_id': season_id,
                                        'season_code': season_code})
        return season_list


def festival_check(season_list=None):
    '''
    Check season is the one supplied in FESTIVALS dictionary
    '''
    if season_list is None:
        season_list = []
        logger.warning("festival_check(): No season_list passed to function.")

    json_works = []
    for season in season_list:
        season_code = season['season_code']
        for key in FESTIVALS.keys():
            if season_code in key:
                key_season = FESTIVALS[key]
                key_season_id = key_season[0]
                season_id = season['season_id']
                if str(key_season_id) == str(season_id):
                    print(f"festival_check(): Match {key_season_id} and {season_id}")
                    # Retrieve all season_id works for all current seasons
                    work_path = fetch_works(season_code, season_id)
                    logger.info("festival_check(): CURRENT SEASON %s correct and will be selected for record generation", season_code)
                    json_works.append({'json_works': work_path})
                else:
                    logger.info("festival_check(): Skipping season record creation: %s Does not match this season: %s", season_code, key_season_id)
    return json_works


def main():
    '''
    Create season json from Artifax API, find current and relevant Festival season then extract these work_ids
    Iterate through each work_id to assess suitability to make new CID work record
    Extract data from relevant work_ids and pass into CID with defaults for CID works from Artifax Festivals
    '''
    check_cid()
    check_control()

    # Opening log statement
    logger.info('========== Python3 start: Fetching Artifax JSON data and creating CID record =======')

    json_path = fetch_season()
    # Pass json_path to season retrieval (this could contain multiple season's works)
    season_list = retrieve_seasons(json_path)
    # From season_list compare to FESTIVAL dictionary and return only those matches
    json_works = festival_check(season_list)
    logger.info("Collected JSON path: %s\nCollected season_list: %s\nCollected current festival list: %s", json_path, season_list, json_works)

    # Check work_ids in json_works paths for suitability to make Work records
    for dictionary in json_works:
        for val in dictionary.values():
            # Extract work_id, title, and other data
            with open(val, 'r') as inf:
                dcts = json.load(inf)
                if str(dcts).startswith('No results'):
                    logger.warning("Skipping: File does not contain dictionary: %s", val)
                    continue
                season_list = []
                season_json = os.path.basename(val)
                logger.info("Opening and extracting data from: %s", season_json)

                # split season_json name to usable variables:
                season_data = season_json.split('_')
                season_code = season_data[0]
                season_num = season_data[1]
                logger.info("=== Processing %s, season_code: %s, season_id: %s ===", season_json, season_code, season_num)
                for dct in dcts:
                    # Refresh variablies
                    priref = ''
                    title = ''
                    language = ''
                    title2 = ''
                    language2 = ''
                    work_id = ''
                    accepted = ''
                    art_form = ''
                    # Extract data from each dct
                    work_data = work_extraction(season_num, dct)
                    title = str(work_data[0])
                    title2 = str(work_data[1])
                    language = str(work_data[2])
                    language2 = str(work_data[3])
                    work_id = str(work_data[4])
                    priref = str(work_data[5])
                    accepted = str(work_data[6])
                    art_form = str(work_data[7])
                    print(f"Accepted: {accepted} Priref: {priref} Art Form: {art_form}")
                    print(f"Assessing data from work_id dictionary: {work_id} Title: {title}")

                    if len(priref) > 0:
                        logger.info("Priref found: %s Skipping work %s", priref, title)
                        continue

                    print(art_form.lower())
                    if not ('film' in art_form.lower() or 'television' in art_form.lower()):
                        logger.info("Skipping this work as it is not Film or Television: %s", title)

                    # is status "Invited - Accepted"
                    print(accepted)
                    if ('Invited - Accepted' in accepted and 'xr' not in accepted.lower()):
                        logger.info("** Title: %s to be processed", title)
                        print(f"Processing: {work_id} Title: {title}")
                        logger.info("Season performance confirmed for %s - %s. Okay to proceed making new Work record", title, work_id)
                        # Begin creating work_data_dct for CID record creation
                        work_data_dct = []
                        # Convert ISO country code to full country title
                        country1 = get_country(language)
                        print(f"Country1: {country1}")
                        try:
                            country2 = get_country(language2)
                            print(f"Country2: {country2}")
                        except (KeyError, IndexError):
                            logger.info("No second title to enable country code retrieval")
                        logger.info("Work ID being written to alternative_number")

                        # Title formatting
                        title_data1 = title_article.splitter(title, language)
                        try:
                            title_data2 = title_article.splitter(title2, language2)
                        except Exception:
                            logger.exception("No Title 2 data available")
                        if len(title_data2[0]) > 0:
                            logger.info("Title 2 present: %s. Checking if it is not English", title_data2[0])
                            if language2.lower() != 'en':
                                # Make title2 main title where foreign language
                                logger.info("Title 2 language is not English, writing as main title")
                                work_data_dct.append({'title': f'{title_data2[0]}'})
                                if len(title_data2[1]) > 0:
                                    work_data_dct.append({'title.article': title_data2[1]})
                                    print(title_data2[1])
                                else:
                                    work_data_dct.append({'title.article': ''})
                                work_data_dct.append({'title.type': '05_MAIN'})
                                work_data_dct.append({'title.language': country2})
                                work_data_dct.append({'title': f'{title_data1[0]}'})
                                if len(title_data1[1]) > 0:
                                    work_data_dct.append({'title.article': title_data1[1]})
                                    print(title_data1[1])
                                else:
                                    work_data_dct.append({'title.article': ''})
                                work_data_dct.append({'title.type': '35_ALTERNATIVE'})
                                work_data_dct.append({'title.language': country1})
                            else:
                                # Title1 set as main as title2 is English
                                work_data_dct.append({'title': f'{title_data1[0]}'})
                                if len(title_data1[1]) > 0:
                                    work_data_dct.append({'title.article': title_data1[1]})
                                    print(title_data1[1])
                                work_data_dct.append({'title.type': '05_MAIN'})
                                work_data_dct.append({'title.language': country1})
                                # Title2 also English, setting as second title alternative
                                work_data_dct.append({'title': f'{title_data2[0]}'})
                                if len(title_data2[1]) > 0:
                                    work_data_dct.append({'title.article': title_data2[1]})
                                    print(title_data2[1])
                                else:
                                    work_data_dct.append({'title.article': ''})
                                work_data_dct.append({'title.type': '35_ALTERNATIVE'})
                                work_data_dct.append({'title.language': country2})
                        else:
                            # Title1 set as main as title2 unavailable
                            work_data_dct.append({'title': f'{title_data1[0]}'})
                            if len(title_data1[1]) > 0:
                                work_data_dct.append({'title.article': title_data1[1]})
                                print(title_data1[1])
                            work_data_dct.append({'title.type': '05_MAIN'})
                            work_data_dct.append({'title.language': country1})

                        # Append Alternative number data work_id
                        work_data_dct.append({'alternative_number': work_id})
                        work_data_dct.append({'alternative_number.type': 'Artifax work_id'})

                        # Obtain season_code priref
                        logger.info("Obtaining Grouping priref from Festivals dictionary")
                        for key in FESTIVALS.keys():
                            if season_code in str(key):
                                season_data = FESTIVALS[season_code]
                                season_priref = season_data[2]
                                work_data_dct.append({'grouping.lref': season_priref})
                        print("Dictionary contents:")
                        print(work_data_dct)

                        # Create CID record and extract priref/object_number
                        cid_data = create_work(work_data_dct)
                        print(f"CID data returned from create_work: {cid_data}")
                        cid_priref = cid_data[0]
                        cid_object_number = cid_data[1]
                        # Push back if priref to Artifax
                        confirm_priref = push_priref_artifax(work_id, cid_priref)
                        if confirm_priref:
                            logger.info("Priref successfully written to Artifax: %s", cid_priref)
                        else:
                            logger.warning("ERROR WITH WRITING TO ARTIFAX")
                        # Push back of object number to Artifax
                        confirm_obnum = push_ob_num_artifax(work_id, cid_object_number)
                        if confirm_obnum is True:
                            logger.info("Object number successfully written to Artifax: %s", cid_object_number)
                        else:
                            logger.warning("Artifax push confirmation not received for %s", cid_object_number)

    #remove_json(JSON_DELETE_PATH)
    logger.info('========== Python3 end - script completed =========\n')


def work_extraction(season_num, dct=None):
    '''
    Extract work data from dct, compare with season_num
    '''
    if dct is None:
        dct = []
        logger.warning("work_extraction(): No dictionary data passed through to work_extraction()")

    # Extract season acceptance status, work_id, title, and other data
    try:
        title = dct['titles'][0]['title']
        language = dct['titles'][0]['language_code']
    except (IndexError, KeyError, TypeError):
        title = ''
        language = ''
    if not title:
        try:
            title = dct[0]['title']
            language = dct[0]['language_code']
        except (IndexError, KeyError, TypeError):
            logger.warning("work_extraction():Unable to extract title and language code")
    try:
        title2 = dct['titles'][1]['title']
        language2 = dct['titles'][1]['language_code']
    except (IndexError, KeyError, TypeError):
        title2 = ''
        language2 = ''
        logger.info("work_extraction():Unable to extract title2, it may not exist")
    try:
        work_id = dct['work_id']
    except (IndexError, KeyError, TypeError):
        work_id = ''
        logger.warning("work_extraction():Unable to extract work_id")
    priref = ''
    for custom_dct in dct['custom_forms'][0]['custom_form_sections'][0]['custom_form_elements']:
        if custom_dct['custom_form_element_id'] == 1004:
            priref = custom_dct['custom_form_data_value']
    try:
        art_form = dct['art_form']
    except (IndexError, KeyError, TypeError):
        art_form = ''
        logger.warning("work_extraction(): Unable to retrieve art_form")

    # Iterate through dct['seasons'] for season match to json
    accepted = ''
    seasons_dict = []
    try:
        seasons_dict = dct['seasons']
    except (IndexError, KeyError, TypeError):
        seasons_dict = ''

    for dictionary in seasons_dict:
        try:
            season_id = dictionary['season_id']
        except IndexError:
            logger.warning("work_extraction():Unable to obtain season_id")
            season_id = ''
        if int(season_num) == int(season_id):
            try:
                accepted = dictionary['work_on_season_status_name']
            except IndexError:
                logger.warning("work_extraction(): Unable to obtain accepted value for season appearance")
        else:
            logger.info("work_extraction(): Skipping season_id %s does not match this season: %s", season_id, season_num)

    return (title, title2, language, language2, work_id, priref, accepted, art_form)


def create_work(work_data_dct=None):
    '''
    Uses work_data_dct, work_default and work_restricted_defaults to generate Work record in CID
    '''
    cid_priref = ''
    cid_object_number = ''

    if work_data_dct is None:
        work_data_dct = {}
        logger.warning("create_work(): Work data dictionary failed to send from main()")

    # Work record defaults, basic and retrieve priref/object_numberfor Artifax push
    title = work_data_dct[0]['title']
    print(title)
    application_restriction_date = str(datetime.date.today() + datetime.timedelta(120))
    application_restriction_date_8yr = str(datetime.date.today() + datetime.timedelta(2922))
    work_default = []
    work_default = ({'input.name': 'datadigipres'},
                    {'input.date': TODAY},
                    {'input.time': str(datetime.datetime.now())[11:19]},
                    {'input.notes': 'Artifax BFI Southbank scheduling system - automated record creation'},
                    {'record_type': 'WORK'},
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
                    {'record_access.review_date': application_restriction_date},
                    {'record_access.user': 'vickr'},
                    {'record_access.rights': '2'},
                    {'record_access.owner': 'Festivals'},
                    {'grouping.lref': '132071'},
                    {'worklevel_type': 'MONOGRAPHIC'})

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
                                {'application_restriction.notes': 'BFI Festivals record, Mediatheque restriction review date 8 years.'})
    work_values = []
    work_values.extend(work_data_dct)
    work_values.extend(work_restricted_defaults)
    work_values.extend(work_default)

    # Create basic work record
    work_values_xml = adlib.create_record_data('', work_values)
    if work_values_xml is None:
        return None
    print("***************************")
    print(work_values_xml)

    try:
        logger.info("Attempting to create Work record for item %s", title)
        record = adlib.post(CID_API, work_values_xml, 'works', 'insertrecord')
        if record:
            cid_priref = adlib.retrieve_field_name(record, 'priref')[0]
            cid_object_number = adlib.retrieve_field_name(record, 'object_number')[0]
            print(f'* Work record created with Priref {cid_priref} Object number {cid_object_number}')
            logger.info('create_work(): Work record created with priref %s', cid_priref)
        else:
            logger.warning("CID priref/object_number is not present after creating CID record")
            print("Creation of record failed using adlib.post()")
    except Exception as err:
        print('* Unable to create Work record')
        logger.critical('create_work():Unable to create Work record', err)

    return (cid_priref, cid_object_number)


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def push_priref_artifax(object_id, priref):
    '''
    Script to push back priref to Artifax
    '''
    dct = []
    data = {'object_id': object_id,
            'object_type_id': '69',
            'custom_form_element_id': '1004',
            'custom_form_assignment_id': '25493',
            'custom_form_data_value': priref}

    dct = requests.request('PUT', CUSTOM_API, headers=HEADERS, data=data)
    dct.raise_for_status()
    if 'custom_forms_data_id' in str(dct.text):
        return True


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def push_ob_num_artifax(object_id, object_number):
    '''
    Write object number data to Artifax
    '''
    dct = []
    data = {'object_id': object_id,
            'object_type_id': '69',
            'custom_form_element_id': '1003',
            'custom_form_assignment_id': '25493',
            'custom_form_data_value': object_number}

    dct = requests.request('PUT', CUSTOM_API, headers=HEADERS, data=data)
    dct.raise_for_status()
    if 'custom_forms_data_id' in str(dct.text):
        return True


def remove_json(completed_path):
    '''
    Clear files moved into completed/ folder to prevent congestion
    When over 48 hours/2 days old. utcnow() depracated Py3.12
    '''
    for root, _, files in os.walk(completed_path):
        for file in files:
            filepath = os.path.join(root, file)
            delta = timedelta(seconds=864000)
            mtime = datetime.datetime.utcfromtimestamp(os.path.getmtime(filepath))
            check_time = datetime.datetime.utcnow() - delta
            # If modification time less that now minus 10 days - delete
            if mtime < check_time:
                print(f"Deleting filepath: {filepath}")
                os.remove(filepath)


if __name__ == '__main__':
    main()
