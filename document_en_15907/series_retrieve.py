#!/usr/bin/env python3

'''
To run on demand from document_augmented_stora.py.
This script retrieves the series metadata for a programme where series data is available but not yet cached
and populates a series_cache that informs series_work_defaults in document_augmented_stora.py

series_retrieve():
1. Make new API enquiry with series id number for each channel
2. Where present download the JSON to cache folder named as the series id
3. Save this file into relevant folder, SERIES_CACHE_PATH

2023
'''

# Public packages
import os
import time
import json
import logging
import requests

# Global variables
STORAGE = os.environ['STORA_PATH']
SERIES_CACHE_PATH = os.path.join(STORAGE, 'series_cache')
LOG_PATH = os.environ['LOG_PATH']

# Setup logging
LOGGER = logging.getLogger('series_retrieve')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'series_retrieve.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# API variables to access Press Association metadata
URL = os.environ['PATV_FETCH']
QUERY = {"aliases": "true"}
HEADERS = {
    "Accept": "application/json",
    "apikey": os.environ['PATV_KEY']
}


def retrieve(fullpath):
    # Opening log statement
    LOGGER.info('========== Fetch series_retrieve.py script STARTED ===============================================')
    with open(fullpath, 'r') as inf:
        dct = json.load(inf)
        for subdct in dct['item'][0]["asset"]["related"]:
            for key, value in subdct.items():
                type = subdct["type"]
                if type == "series":
                    try:
                        series_id = subdct["id"]
                        print(f"retrieve(): Series ID: {series_id}")
                        d = { " ": "_", ";": "-", "/": "-", ":": "-", "&": "and", "'": "", "!": "", "?": "" }
                        title = subdct["title"]
                        for k, v in d.items():
                            title = title.replace(k, v)
                        LOGGER.info("Getting asset details from series_id: %s - %s", series_id, title)
                        print(f"retrieve(): Title: {title}")
                        get_asset(series_id, title)
                    except:
                        pass
    LOGGER.info('========== Fetch series_retrieve.py script ENDED ================================================')


def get_asset(series_id, title):
    series_url = os.path.join(URL, series_id)
    dct = requests.request("GET", series_url, headers=HEADERS, params=QUERY, timeout=1200)
    try:
        # Outputs response files to STORA/series_cache/, named as series_id
        fname = os.path.join(SERIES_CACHE_PATH, f"{series_id}_{title}.json")
        with open(fname, 'w') as f:
            json.dump(dct.json(), f, indent=4)
    except Exception as err:
        LOGGER.warning(("** WARNING: Exporting series data has failed to output to %s", fname), err)
    else:
        LOGGER.info("Export of asset series metadata has been successful. Saving to top folder %s", SERIES_CACHE_PATH)
        print(f"Created new series data JSON {fname}")
    time.sleep(10)

