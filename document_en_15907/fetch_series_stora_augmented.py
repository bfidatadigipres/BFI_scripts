#!/usr/bin/env python3

'''
To run after PATV EPG metadata retrieval each day.
This script retrieves the series metadata for a programme (if it exists)
and populates a series_cache that informs series_work_defaults

main():
1. Make new API enquiry with series id number for each channel
2. Where present download the JSON to cache folder named as the series id
3. Save this file in series_cache/ folder

Joanna White
2020
'''

# Python packages
import os
import sys
import json
import logging
import datetime
import requests

# Setup logging
logger = logging.getLogger('fetch_series_stora_augmented')
hdlr = logging.FileHandler(os.path.join(os.environ['LOG_PATH'], 'fetch_series_stora_augmented.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Some date variables for use in API calls etc
TODAY = datetime.date.today()
YESTERDAY = TODAY - datetime.timedelta(days=3)
YESTERDAY_CLEAN = YESTERDAY.strftime('%Y-%m-%d')
START = '{}T00:00:00'.format(YESTERDAY_CLEAN)
END = '{}T23:59:00'.format(YESTERDAY_CLEAN)
# IF other date is required
#START = '2023-01-18T00:00:00'
#END = '2023-01-18T23:59:00'

# API variables to access Press Association metadata
URL = os.environ['PATV_URL']
QUERYSTRING = {"aliases": "true"}
HEADERS = {
    "Accept": "application/json",
    "apikey": os.environ['PATV_KEY']}

# Global variables
JSON_PATH = os.environ['STORA_PATH']
STORAGE_PATH = os.path.join(os.environ['STORA_PATH'], 'series_cache/')
DATE_PATH = START[0:4] + "/" + START[5:7] + "/" + START[8:10]
PATHS = os.path.join(JSON_PATH, DATE_PATH)
CODEPTH = os.environ['CODE']


def check_control():
    '''
    Check control JSON for downtime request
    '''
    with open(os.path.join(CODEPTH, 'stora_control.json')) as control:
        j = json.load(control)
        if not j['stora_qnap04']:
            logger.info("Script run prevented by downtime_control.json. Script exiting.")
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")


def main():
    '''
    Iterate date path in /media/data for yesterday
    fetching series information from PATV if found
    missing from series cache folder QNAP-04
    '''

    check_control()
    logger.info('========== Series Cache fetch metadata script STARTED ===============================================')

    for root, _, files in os.walk(PATHS):
        for file in files:
            print(root, file)
            fullpath = os.path.join(root, file)
            filename, ext = os.path.splitext(file)
            if ext == '.json':
                with open(fullpath, 'r') as inf:
                    dct = json.load(inf)
                    for subdct in dct['item'][0]["asset"]["related"]:
                        for key, value in subdct.items():
                            types = subdct["type"]
                            if types == "series":
                                try:
                                    series_id = subdct["id"]
                                    d = {" ": "_", ";": "-", "/": "-", ":": "-", "&": "and", "'": "", "!": "", "?": ""}
                                    title = subdct["title"]
                                    for k, v in d.items():
                                        title = title.replace(k, v)
                                    logger.info("Getting asset details from series_id: %s - %s", series_id, title)
                                    get_asset(series_id, title)
                                except:
                                    pass

    logger.info('========== Fetch series augmented metadata script ENDED ================================================')


def get_asset(series_id, title):
    '''
    Get series data from PATV
    using requests library
    '''

    series_url = URL + series_id
    dct = requests.request("GET", series_url, headers=HEADERS, params=QUERYSTRING)
    try:
        # Outputs response files to storage_path, named as series_id
        fname = os.path.join(STORAGE_PATH, f"{series_id}_{title}.json")
        with open(fname, 'w+') as f:
            json.dump(dct.json(), f, indent=4)
    except Exception as err:
        logger.warning("** WARNING: Exporting series data has failed to output to %s\n%s", fname, err)
    else:
        logger.info("Export of asset series metadata has been successful. Saving to top folder %s", STORAGE_PATH)


if __name__ == '__main__':
    main()

