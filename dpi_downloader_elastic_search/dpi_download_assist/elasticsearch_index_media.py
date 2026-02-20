#!/usr/bin/env python3

"""
Elastic search scirpt to manage ingest
of item record data to DPI browser allowing
new MP4 proxy copies to move through to the
video viewer.
"""

# Imports
from json import dumps, loads
from xmljson import parker
import xml.etree.ElementTree as ET
from elasticsearch import Elasticsearch
import requests
import os
import sys
import subprocess
import csv
from datetime import timedelta
import logging

sys.path.append(os.environ.get("CODE"))
import utils

API = os.environ.get("CID_API4")
ES_PATH = os.environ.get("ES_SEARCH_PATH")
LOG_PATH = os.environ.get("LOG_PATH")
LOG = os.path.join(LOG_PATH, "elastic_search_media_indexing.log")
CODE = os.environ.get("CODE")
TXT_DUMP = os.path.join(CODE, "dpi_downloader_elastic_search/dpi_download_assist/media_prirefs.txt")
logging.basicConfig(filename=LOG, level=logging.INFO, format="%(asctime)s %(message)s", filemode="w")


def call_cid_for_data():
    """
    CID calls to get data and build txt lists
    """
    search = "(object.object_number->Df=item) and (imagen.media.original_filename=* and modification>today-2)"

    try:
        response = requests.get(f"{API}?database=prirefmediaraw&search={search}&limit=0")
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)

    with open(TXT_DUMP, 'w+') as txtfile:
        txtfile.write(response.text)

    print(f"Data written to {TXT_DUMP}")


def main():
    """
    Launch media priref CID call
    then push priref list through to
    DPI browser
    """
    if not utils.check_control("pause_scripts"):
        logging.info(
            "Script run prevented by downtime_control.json. Script exiting."
        )
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(API):
        logging.warning("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    call_cid_for_data()
    es = Elasticsearch(ES_PATH)

    with open(TXT_DUMP) as txt_file:
        for count, line in enumerate(txt_file, 1):
            xml_text = ""
            status = ""
            priref = line.strip()
            print(priref)
            if count % 100 == 0:
                print('{} media prirefs processed'.format(count))

            search = f"priref={priref}"
            try:
                xml = requests.get(f"{API}?database=elasticsearchmedia&search={search}")
                xml_text = xml.text
            except requests.exceptions.RequestException as err:
                logging.error("%s - could not fetch xml from CID API: %s", priref, err)
                continue

            if '<media>' in xml_text:
                status = 'error-free'
            else:
                status = 'error'
                logging.error("%s - invalid xml (no <media> element) returned from CID API", priref)

            if status == 'error-free':
                try:
                    xmltree = (ET.fromstring(xml_text))
                except Exception as err:
                    logging.error("%s - could not convert to xml using xmltree:\n%s", priref, err)
                    continue

                json_out = dumps(parker.data(xmltree))

                try:
                    index = es.index(index="dpi_media", id=priref, document=json_out)
                    print(index['result'])
                except Exception as err:
                    logging.error("%s - could not update or create document in elasticsearch index:\n%s", priref, err)
                    continue
            else:
                logging.error("%s - could not fetch xml from CID API", priref)
                continue


if __name__ == "__main__":
    main()
