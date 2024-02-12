#!/usr/bin/env python3

'''
Fetch JSON from augmented EPG metadata API, for use by document_netflix_augmented.py

main():
1. Set date window from today backward, and within this look for new
   shows, seasons, series, episodes that have been made available
2. Once a day call the API for date range catalogue assets, using fetch():
3. If first fetch fails, fetch(): will retry three times pausing up to ten minutes between each.
4. When downloaded iterates the catalogue assets looking for those with ['meta'][0]['season'] or ['episode']
   skip any that do not have this, as they are not considered TV shows.
5. Extract asset_id from ['link'][0]['href'][0], slicing the last part of the web address
6. From this you can build the folder structures for series, seasons (numbered), and episodes (numbered).
7. Extract series, season, episode, catalogue and contributors data to JSON dictionary.
7. Place relevant JSON dumped metadata into each folder when not already in place.

Joanna White
2022
'''

# Public packages
import os
import sys
import json
import logging
import datetime
import requests
import tenacity

# Global variables
STORAGE = os.environ['NETFLIX_PATH']
CAT_ID = os.environ['PA_NETFLIX']
LOG_PATH = os.environ['LOG_PATH']
STORA_CODE = os.environ['CODE']
TODAY = datetime.date.today()
TWO_WEEKS = TODAY - datetime.timedelta(days=140)
START = f"{TWO_WEEKS.strftime('%Y-%m-%d')}T00:00:00"
END = f"{TODAY.strftime('%Y-%m-%d')}T23:59:00"
#START = '2021-07-01T00:00:00'
#END = '2023-07-26T23:59:00'
TITLE_DATA = ''
#TITLE_DATA = 'title=After%20Life&'
UPDATE_AFTER = '2019-07-01T00:00:00'

# Setup logging
LOGGER = logging.getLogger('fetch_netflix_augmented')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'fetch_netflix_augmented.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# PATV API details including unique identifiers for Netflix catalogue
URL = os.path.join(os.environ['PATV_NETFLIX_URL'], f'catalogue/{CAT_ID}/')
URL2 = os.path.join(os.environ['PATV_NETFLIX_URL'], 'asset/')
HEADERS = {
    "accept": "application/json",
    "apikey": os.environ['PATV_KEY']
}


def check_control():
    '''
    Check control JSON for downtime request
    '''
    with open(os.path.join(STORA_CODE, 'stora_control.json')) as control:
        j = json.load(control)
        if not j['netflix']:
            LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")


@tenacity.retry(wait=tenacity.wait_random(min=50, max=60))
def check_api():
    '''
    Run standard check with test Programme ID
    '''
    params = {"asset": "9016da57-c59a-5aa6-97b8-41b74bc9442f", "aliases": "True"}
    req = requests.request("GET", os.path.join(URL, 'asset'), headers=HEADERS, params=params)
    if req.status_code == 200:
        return True
    else:
        LOGGER.info("PATV API return status code: %s", req.status_code)
        raise tenacity.TryAgain


@tenacity.retry(wait=tenacity.wait_random(min=50, max=60))
def fetch(search_type, search_id):
    '''
    Fetch data from PATV URL
    '''

    if search_type == 'catalogue':
        try:
            url_all = os.path.join(URL, f"asset?{TITLE_DATA}start={START}&end={END}&updatedAfter={UPDATE_AFTER}&aliases=true")
            print(url_all)
            req = requests.get(url_all, headers=HEADERS)
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical('**** PROBLEM: Cannot fetch EPG metadata. **** \n%s', err)
            raise tenacity.TryAgain
    elif search_type == 'cat_asset':
        try:
            req = requests.get(os.path.join(URL, f"asset/{search_id}"), headers=HEADERS)
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical('**** PROBLEM: Cannot fetch EPG metadata. **** \n%s', err)
            raise tenacity.TryAgain
    elif search_type == 'asset':
        try:
            req = requests.get(os.path.join(URL2, f'{search_id}'), headers=HEADERS)
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical('**** PROBLEM: Cannot fetch EPG metadata. **** \n%s', err)
            raise tenacity.TryAgain
    elif search_type == 'contributors':
        try:
            req = requests.get(os.path.join(URL2, f'{search_id}/contributor'), headers=HEADERS)
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical('**** PROBLEM: Cannot fetch EPG metadata. **** \n%s', err)
            raise tenacity.TryAgain


def json_dump(json_path, dct=None):
    '''
    Take a catalogue dictionary
    and output to file for read/processing
    '''
    if dct is None:
        dct = {}

    with open(json_path, 'w') as file:
        json.dump(dct, file, indent=4)
        file.close()


def get_cat_assets(asset=None):
    '''
    Retrieve asset information for logs/processing
    '''
    if asset is None:
        asset = {}
    try:
        episode_title = asset['title']
    except (IndexError, KeyError):
        episode_title = ''
    try:
        cat_id = asset['id']
    except (IndexError, KeyError):
        cat_id = ''
    try:
        ep_num = asset['meta']['episode']
    except (IndexError, KeyError):
        ep_num = None
    try:
        linked_content = asset['link'][0]['href']
    except (IndexError, KeyError):
        linked_content = ''
    try:
        episode_asset_id = linked_content.split('/')[-1]
    except Exception:
        episode_asset_id = ''

    return(episode_title, cat_id, ep_num, episode_asset_id)


def get_series_title(asset=None):
    '''
    Get series title data
    '''
    if asset is None:
        asset = {}
    try:
        title = asset['title']
        title = title.replace('/', '').replace("'", "").replace('&', 'and').replace('(', '').replace(')', '')
        return title.replace(' ', '_')
    except (IndexError, KeyError):
        return None


def get_folder_list(pth):
    '''
    Get full folderpath and retrieve all directory
    names within
    '''
    folder_list = []
    for _, dirs, _ in os.walk(pth):
        for directory in dirs:
            folder_list.append(directory)
    return folder_list


def main():
    '''
    Grab last two weeks catalogue items, output to JSON for storage
    Iterate list and build asset_dict of TV items, then process
    any new items placing in programme led folder structures
    '''
    check_control()
    check_api()
    LOGGER.info('========== Fetch augmented metadata script STARTED ======================')

    # If metadata cannot be retrieved the script exits
    LOGGER.info("Requests will now attempt to retrieve the EPG channel metadata from start=%s to end=%s", START, END)
    json_dct = fetch('catalogue', CAT_ID)
    if json_dct:
        LOGGER.info("Fetched JSON data successfully.")
        catalogue_path = os.path.join(STORAGE, f"catalogue/{START.replace(':', '-')}_{END.replace(':', '-')}_catalogue.json")
        json_dump(catalogue_path, json_dct)
        print("Downloaded catalogue info...")

    # Iterate all data retrieved from catalogue for date range
    asset_dict = {}
    items = json_dct['item']
    for asset in items:
        episode_title, catalogue_id, num, episode_asset_id = get_cat_assets(asset)
        print(episode_title, catalogue_id, episode_asset_id)
        if not num:
            print(f"Skipping asset {episode_title}, no 'episode' data in 'meta'")
            LOGGER.info("Skipping asset %s %s, as does not have series/season data", episode_title, episode_asset_id)
            continue
        asset_dict[episode_asset_id.strip()] = f'{catalogue_id.strip()}, {num}'
        print(f"Added {episode_asset_id} and {catalogue_id} to dict")

    # Clean up and check for valid entries
    json_dct = None
    if len(asset_dict) == 0:
        LOGGER.warning("No items retrieved from JSON catalogue. Script exiting.")
        LOGGER.info('========== Fetch augmented metadata script ENDED ========================')
        sys.exit()

    # Iterate asset_dict, using show_asset_id to identify season/series data
    LOGGER.info("%s new assets found from metadata JSON retrieval: %s", len(asset_dict), catalogue_path)
    folder_list = get_folder_list(STORAGE)
    for ep_asset_id, cat_details in asset_dict.items():
        ep_cat_id, ep_num = cat_details.split(',')

        # Fetch all assetIDs to build folders
        episode_dct = fetch('asset', ep_asset_id)
        episode_cat_dct = fetch('cat_asset', ep_cat_id)
        season_asset_id, series_asset_id = retrieve_dct_data(episode_dct)
        episode_folder = f"episode_{ep_num.strip()}_{ep_asset_id}"
        print(f"************ EPISODE FOLDER: {episode_folder}")
        print(f"SERIES ID: {series_asset_id}, SEASON_ID: {season_asset_id}")
        print(episode_dct)
        print("********************")
        print(episode_cat_dct)

        # Series data
        if not series_asset_id:
            LOGGER.warning("Skipping: Series ID absent for episode asset %s", ep_asset_id)
            continue
        series_dct = fetch('asset', series_asset_id)
        series_title = get_series_title(series_dct)
        if not series_title:
            series_title = episode_dct['related'][1]['title']
        series_folder = f"{series_title}_{series_asset_id}"
        print(f"SERIES TITLE: {series_title}, SERIES_FOLDER: {series_folder}")

        # Season data
        if not season_asset_id:
            LOGGER.warning("Skipping: Season ID absent for episode asset %s", ep_asset_id)
            continue
        season_dct = fetch('asset', season_asset_id)
        season_num = season_dct['number']
        season_folder = f"season_{season_num}_{season_asset_id}"
        print(f"SEASON_FOLDER: {season_folder}")

        # Create path to new episode
        series_path = os.path.join(STORAGE, series_folder)
        season_path = os.path.join(series_path, season_folder)
        episode_path = os.path.join(season_path, episode_folder)
        if not os.path.exists(episode_path):
            LOGGER.info("* New episode to be added: %s", episode_path)
            os.makedirs(episode_path,mode=0o777, exist_ok=True)

        # Check for all JSON contents
        series_json = os.path.join(series_path, f'series_{series_asset_id}.json')
        if not os.path.exists(series_json):
            LOGGER.info("New Series JSON: %s", f'series_{series_asset_id}.json')
            json_dump(series_json, series_dct)
        season_json = os.path.join(season_path, f'season_{season_asset_id}.json')
        if not os.path.exists(season_json):
            LOGGER.info("New Season JSON: %s", f'season_{season_asset_id}.json')
            json_dump(season_json, season_dct)
        episode_json = os.path.join(episode_path, f'episode_{ep_asset_id}.json')
        if not os.path.exists(episode_json):
            LOGGER.info("New Episode AssetID JSON: %s", f'episode_{ep_asset_id}.json')
            json_dump(episode_json, episode_dct)
        episode_cat_json = os.path.join(episode_path, f'episode_catalogue_{ep_cat_id}.json')
        if not os.path.exists(episode_cat_json):
            LOGGER.info("New Episode catalogue JSON: %s", f'episode_catalogue_{ep_cat_id}.json')
            json_dump(episode_cat_json, episode_cat_dct)
        contributors_json = os.path.join(episode_path, f'contributors_{ep_asset_id}.json')
        if not os.path.exists(contributors_json):
            contributors_dct = fetch('contributors', ep_asset_id)
            if len(contributors_dct['item']) >= 1:
                LOGGER.info("New Contributors JSON: %s", contributors_json)
                json_dump(contributors_json, contributors_dct)

    LOGGER.info('========== Fetch augmented metadata script ENDED ================================================')


def retrieve_dct_data(dct=None):
    '''
    Check if DCT data is None, if not retrieve season/series IDs
    '''
    if dct is None:
        dct = {}
    try:
        first_type = dct['related'][0]['type']
        season_id = dct['related'][0]['id']
    except (TypeError, IndexError, KeyError):
        first_type = ''
        season_id = ''
    try:
        second_type = dct['related'][1]['type']
        series_id = dct['related'][1]['id']
    except (TypeError, IndexError, KeyError):
        second_type = ''
        series_id = ''
    if 'season' in first_type and 'series' in second_type:
        return season_id, series_id
    elif 'season' in second_type and 'series' in first_type:
        return series_id, season_id
    else:
        return None, None


if __name__ == '__main__':
    main()
