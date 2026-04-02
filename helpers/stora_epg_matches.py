import json
from datetime import datetime, timezone, timedelta
import sys
import os
import csv
from pathlib import Path
from tenacity import retry, wait_fixed
import logging

#LOGGING
LOG = sys.argv[1].split('/')[-1] + '_matches.log'
logger = logging.getLogger("flask_logger")
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)



sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
from document_en_15907 import title_article
import utils

import glob
import pandas as pd

BASE_DIR = Path(sys.argv[1])
BATCH_SIZE = 4
OUTPUT_DIR = Path("/mnt/qnap_03/test/historical_redux_metadata/matches")
OUTPUT_DIR.mkdir(exist_ok=True)

"""
The script should iterate through the JSONs in the folder above
- Extract date of broadcast (yyyy-mm-dd)
- Extract start time of broadcast (HH:MM:SS)
- Extract the channel from the path, need a dictionary to convert this channel into the broadcast_channel *done*
- Extract title of the progamme from the JSON (there maybe two examples)
"""

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
    "bbctwo": [
        "BBC Two HD",
        "This is BBC Two",
        "Highlights of programmes BBC Two. [HD]",
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


def get_stora_data(fullpath: str):
    # get channel name + broadcast_channel
    channel_data = fullpath.split('/')[-2]
    print(channel_data)
    for key, val in CHANNELS.items():
        if f"/{key}/" in fullpath:
            try:
                channel = val[0]
                print(f"Broadcast channel is {channel}")
                print(channel)
            except (IndexError, TypeError, KeyError) as err:
                print(err)

    with open(fullpath, 'r') as file:
        info_json = json.load(file)
        date_time = info_json.get('item')[0]['dateTime']
        print(f"date_time: {date_time}")
        dates = datetime.fromisoformat(date_time[:-1]).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Dates: {dates}")
        date, time_str = utils.check_bst_adjustment(dates)
        title = info_json.get('item')[0].get('title')
        duration =  info_json.get('item')[0].get('duration')
        if duration is None:
            duration = 0
        asset_title = info_json.get('item')[0].get('asset').get('title')
        if asset_title is None:
             asset_title = ''
        asset_id = info_json.get('item')[0].get('asset').get('id')
        certification = info_json["item"][0].get("certification").get("bbfc")
        if certification is None:
          certification = ''
        group = info_json["item"][0].get("meta").get("group")
        if group is None:
           group = ''
        group = str(group)
        attribute = info_json["item"][0].get("attribute")
        asset_attribute = info_json["item"][0].get("asset").get("attribute")
        if asset_attribute is None:
           asset_attribute = []
        list_attributes = attribute + asset_attribute + [group] + [certification]

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
    return date, time_str, title, asset_title, channel, asset_id, duration, certification, list_attributes, broadcast_company

@retry(wait = wait_fixed(10))
def adlib_search(channel: str, date: str, time: str, search: str, database: str = "manifestations", limit: str = "1"):
    search =  f'grouping.lref="398775" and broadcast_channel = "{channel}" and transmission_date = "{date}" and transmission_start_time = "{time}"'
    #print(search)
    hit, record = adlib.retrieve_record(os.environ.get("CID_API4"), database, search, limit)
    return hit, record


if __name__ == "__main__":
    list_path = sys.argv[1]
    count = 0
    for batch_start in range(1, 13, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE - 1, 12)
        output_files = OUTPUT_DIR / f"results_2021_{batch_start:02d}_{batch_end:02d}.csv"

        row_written = 0
        writer = None

        with open(output_files, 'a+', newline="") as csvfile:
            for month in range(batch_start, batch_end + 1):
                month_dir = BASE_DIR / f"{month:02d}"
                list_of_files = glob.glob(str(month_dir / "*/*/*.json"))
                for path in list_of_files:
                    print(f"Processing row {path}")
                    logger.info(f"Processing row {path}")
                    date, time, json_title, asset_title, channel, asset_id, duration, certification, list_attributes, broadcast_company = get_stora_data(path)
                    print(f"Date: {date}")
                    print(f"time: {time}")
                    print(f"certs :  {certification}")
                    if asset_title.title().startswith("Generic"):
                        print(asset_title)
                        title_for_split = json_title
                        generic = True
                    elif asset_title is None:
                        title_for_split = json_title
                    else:
                        title_bare = "".join(str for str in asset_title if str.isalnum())
                        print(title_bare)
                        title_for_split = json_title

                    title_split, title_article_split = title_article.splitter(title_for_split, 'en')
                    title_split = title_split.replace("\xe2\x80\x99", "'")
                    search =  f'grouping.lref="398775" and broadcast_channel = "{channel}" and transmission_date = "{date}" and transmission_start_time = "{time}"'
                    logger.info(f"Adlib search: {search}")
                    #print(search)
                    hit, record = adlib_search(channel, date, time, search, database= "manifestations", limit = "1")
                    logger.info(f"Adlib hits: {hit}")
                    logger.info(f"Adlib record: {record}")
                    if record is None:
                        print("orginal search failed, trying new search with different title")
                        logger.info("orginal search failed, trying new search with different title")
                        continue
                    priref = adlib.retrieve_field_name(record[0], "priref")
                    title_record = adlib.retrieve_field_name(record[0], "title")
                    logger.info(f"Adlib title record: {title_record}")
                    cid_title_split, cid_title_article_split = title_article.splitter(title_record[0], 'en')
                    alternative_number = adlib.retrieve_field_name(record[0], "alternative_number")
                    print(record[0])
                    arts_title = adlib.retrieve_field_name(record[0], "title.article")
                    utb_content = f"{','.join(str(x) for x in list_attributes if len(x) > 0)}"
                    if hit >= 1:
                        count+=1
                        row = {
                            "filepath": path,
                            "priref": priref[0],
                            "title.article": title_article_split,
                            "title": title_split,
                            "title.language": "English",
                            "title.type": "05_MAIN",
                            "title.article_cid": arts_title[0],
                            "cid_title_article": cid_title_split,
                            "title_cid": cid_title_article_split,
                            "title.language_cid": "English",
                            "title.type_cid": "35_ALTERNATIVE",
                            "alternative_number.type": "PATV asset id",
                            "alternative_number": asset_id,
                            "utb.fieldname": "EPG attributes",
                            "utb.content": utb_content
                            }
                        logger.info(f"Matched row: {row}")

                        print(count)
                        if writer is None:
                            writer = csv.DictWriter(csvfile, fieldnames=row.keys())
                            writer.writeheader()

                        writer.writerow(row)
                        row_written += 1

        print(f"Finished batch -> {output_files} ({row_written} rows)")
