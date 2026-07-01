import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.environ["CODE"])
import utils
import adlib_v3 as adlib
import adlib_v3_sess as adlib_sess
import logging
import shutil

logger = logging.getLogger(__name__)

CID_API = os.environ['CID_API3']
SUBTITLE_FOLDER = os.path.join(os.environ.get("ADMIN"), "off_air_tv/subtitles_not_in_cid")

class MalformedRecordError(Exception):
    """Raised when a CID record is missing or incomplete."""
    print(Exception)

def get_item_priref(object_number: str) -> str | None:
    """Look up item priref by object number."""
    search = f"object_number='{object_number}'"
    _, item_record = adlib.retrieve_record(CID_API, "items", search, "1", fields=None)
    if item_record is None:
        logger.warning("No item record found for %s", object_number)
        return None, None
    prirefs = adlib.retrieve_field_name(item_record[0], "priref")
    return prirefs[0], item_record if prirefs else None

def get_manifestation_record(item_priref):
    

def process_subtitle_file(file: str) -> None:
    """Look up CID record for a subtitle file and post the subtitle data."""
    object_number = utils.get_object_number(file)

    item_priref, item_record = get_item_priref(object_number)
    #print(f"item_priref: {item_priref}")
    if item_priref is None:
        raise MalformedRecordError(f"No item record for {object_number}")

    mani_record = get_manifestation_record(item_priref, item_record)
    if mani_record is None:
        raise MalformedRecordError(f"No manifestation for priref {item_priref}")

    print(f"manifestation record: {mani_record}")

def main():
    object_number_list = []
    list_files=os.listdir(SUBTITLE_FOLDER)[1:3]
    for file in list_files:
        process_subtitle_file(file)


if __name__ == "__main__":
    main()
