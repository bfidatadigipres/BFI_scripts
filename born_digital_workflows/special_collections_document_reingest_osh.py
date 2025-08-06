#!/usr/bin/env python3

"""
WIP
Special Collections Document tranfsers for OSH
Moving renamed folders to SFTP / Archivematica

Script stages:
MUST BE SUPPLIED WITH SYS.ARGV[1] AT SUB-FOND LEVEL PATH
LAUNCHES FROM CRONTAB ONCE EACH NIGHT TO CHECK FOR NEW "OPEN/CLOSED" RECS

Iterate through CID records from sub-fond level down:
1. Look for CID record flag to indicate if an Item can be reingested
   as an 'OpenRecords' file visible to AtoM
2. Locate matching file in BP Nas curatorial/special_collections
   share, and build path to CSV file metadata.csv
3. Extract specific metadata from the CID item record
4. Append metadata into this CSV document to enrich DIP for AtoM display
5. Collect AIP data from CID item record and use this to initiate a reingest
   FULL, supplying processing_config='OpenRecords', asset_config_id='slug'
   and new metadata.csv for DIP creation
6. When reingest completed, check for transfer status
7. Capture progress to CID item record, and to logs.
8. JMW: Possibility to save URL for AtoM to CID item record?

NOTES:
Some assumptions in code
1. That we can write the slug into a reingest
2. That reingest via the v2 api allows package reingesting
3. That metadata can be updated in this fashing in a 'FULL' reingest

2025
"""

# Public packages
import datetime
import logging
import os
import sys
from time import sleep

# Private packages
import archivematica_sip_utils as am_utils
import tenacity

sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils

LOG = os.path.join(
    os.environ.get("LOG_PATH"), "special_collections_document_reingest_osh.log"
)
CID_API = os.environ.get("CID_API4")
# CID_API = utils.get_current_api()

LOGGER = logging.getLogger("sc_document_reingest_osh")
HDLR = logging.FileHandler(LOG)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

LEVEL = [
    "_fonds_",
    "_sub-fonds_",
    "_series_",
    "_sub-series_",
    "_sub-sub-series_",
    "_sub-sub-sub-series_",
    "_file_",
]


def main():
    """
    WIP
    """
    base_dir = ""
    if not utils.check_storage(base_dir):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    pass


@tenacity.retry(tenacity.stop_after_attempt(10))
def check_transfer_status(uuid, directory):
    """
    Check status of transfer up to 10
    times, or until retrieved
    """
    trans_dict = am_utils.get_transfer_status(uuid)

    if trans_dict.get("status") == "COMPLETE":
        LOGGER.info(
            "Transfer of package completed: %s", trans_dict.get("directory", directory)
        )
        return trans_dict
    else:
        sleep(60)
        raise


if __name__ == "__main__":
    main()
