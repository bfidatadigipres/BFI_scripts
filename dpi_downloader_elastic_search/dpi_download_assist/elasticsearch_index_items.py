#!/usr/bin/env python3

"""
Elastic search scirpt to manage ingest
of item record data to DPI browser allowing
new MP4 proxy copies to move through to the
video viewer.
"""

# Imports
import os
import sys
import logging
from json import dumps
from xmljson import parker
import xml.etree.ElementTree as ET
from elasticsearch import Elasticsearch
import requests

sys.path.append(os.environ.get("CODE"))
import utils

API = os.environ.get("CID_API1")
ES_PATH = os.environ.get("ES_SEARCH_PATH")
LOG_PATH = os.environ.get("LOG_PATH")
LOG = os.path.join(LOG_PATH, "elasticsearch_item_index.log")
ADMIN = os.environ.get("CODE")
TXT_DUMP = os.path.join(ADMIN, "dpi_downloader_elastic_search/dpi_download_assist/item_prirefs.txt")
logging.basicConfig(filename=LOG, level=logging.INFO, format="%(asctime)s %(message)s", filemode="w")


def cid_call_txt_dump():
    """
    Fetch URL ingests
    """
    search = "(Df=item and reproduction.reference->imagen.media.original_filename=* and modification>today-2)"
    # Alternative searches for clean up work:
    # search = "(priref=158847299,159129143,159151027)"
    # search = "(Df=item and reproduction.reference->imagen.media.original_filename=* and (modification>='2026-02-15' and modification<='2026-02-19')"

    logging.info("Downloading prirefs with search: %s", search)
    try:
        url_ingests = requests.get(f"{API}?database=prirefcollectraw&search={search}&limit=0", timeout=300)
    except requests.exceptions.Timeout:
        print("Timed out at 30 seconds")
    except (requests.exceptions.RequestException) as err:
        raise SystemExit(err) from err

    if not url_ingests.text:
        logging.warning("No URL ingests found!")
        sys.exit("No URLs found for ingest")

    logging.info("Retrieve prirefs:\n%s", ", ".join(url_ingests.text.split("\r\n")))
    with open(TXT_DUMP, 'w') as txtfile:
        txtfile.write(url_ingests.text + '\n')
    logging.info("Ingest prirefs written to %s", TXT_DUMP)

    # Fetch Item records based on edits in the grandparent Work record for ingested Items
    search2 = "(Df=item and reproduction.reference->imagen.media.original_filename=*) and (part_of_reference->part_of_reference->edit.date>today-2)"
    # Alternative search for clean up work:
    # search2 = "(Df=item and reproduction.reference->imagen.media.original_filename=*) and (part_of_reference->part_of_reference->(modification>='2026-02-15' and modification<='2026-02-19'))"

    logging.info("Downloading second batch of prirefs with search: %s", search)
    try:
        response = requests.get(f"{API}?database=prirefcollectraw&search={search2}&limit=0", timeout=300)
        print(response)
    except requests.exceptions.Timeout as err:
        print("Timed out at 30 seconds")
        raise SystemExit(err) from err
    except requests.exceptions.RequestException as err:
        raise SystemExit(err) from err

    with open(TXT_DUMP, 'a') as txtfile:
        txtfile.write(response.text)
    logging.info("Work change prirefs written to %s", TXT_DUMP)


def main():
    """
    Trigger retrieval of CID item data
    and push into elastic search index
    """
    if not utils.check_control("pause_scripts"):
        logging.info(
            "Script run prevented by downtime_control.json. Script exiting."
        )
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    logging.info("Elasticsearch Index Items start ================================")
    cid_call_txt_dump()
    es = Elasticsearch(ES_PATH)

    # Use with open for the .txt file and read line
    logging.info("Opening file: %s", TXT_DUMP)
    with open(TXT_DUMP) as txt_file:
        for count, line in enumerate(txt_file, 1):
            xml_text = ""
            status = ""
            priref = line.strip()
            if count % 100 == 0:
                print(f"{count} item prirefs processed")

            search = f"priref={priref}"
            try:
                xml = requests.get(f"{API}?database=elasticsearchitems&search={search}", timeout=30)
                xml_text = xml.text
            except requests.exceptions.Timeout:
                logging.warning("Timed out at 30 seconds")
                continue
            except requests.exceptions.RequestException as err:
                logging.error("%s - could not fetch xml from CID API:\n%s", priref, err)
                continue

            # Check for errors
            if '<item>' in xml_text:
                status = 'error-free'
            else:
                status = 'error'
                logging.error("%s - invalid xml (no <item> element) returned from CID API", priref)

            if status == 'error-free':
                try:
                    xmltree = ET.fromstring(xml_text)
                except Exception as err:
                    logging.error("%s - could not convert to xml using xmltree:\n%s", priref, err)
                    continue

                # Convert XML to json for elasticsearch
                json_out = dumps(parker.data(xmltree))

                # Create document in items index, passing the item priref and json
                try:
                    index = es.index(index="dpi_items", id=priref, document=json_out)
                    print(index['result'])
                except Exception as err:
                    logging.error("%s - could not update or create document in elasticsearch index:\n%s", priref, err)
                    continue
            else:
                logging.error("%s - could not fetch xml from CID API", priref)
                continue

    logging.info("Elasticsearch Index Items end ==================================")


if __name__ == "__main__":
    main()
