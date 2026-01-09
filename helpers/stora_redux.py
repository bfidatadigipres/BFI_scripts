import json
from datetime import datetime, timezone
import sys
import os

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

import glob
import pandas as pd

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


def get_stora_data(filepath: str):
    # get channel name + broadcast_channel
    channel_data = filepath.split("/")[-2]
    print(channel_data)
    for key, val in CHANNELS.items():
        if f"/{key}/" in filepath:
            try:
                channel = val[0]
                print(f"Broadcast channel is {channel}")
                print(channel)
            except (IndexError, TypeError, KeyError) as err:
                print(err)
    with open(filepath, "r") as file:
        info_json = json.load(file)
        # print(info_json.get('item')[0].keys())
        date_time = info_json.get("item")[0]["dateTime"]
        date = datetime.fromisoformat(date_time[:-1]).strftime("%Y-%m-%d")
        time = datetime.fromisoformat(date_time[:-1]).strftime("%H:%M:%S")
        title = info_json.get("item")[0]["title"]
        asset_title = info_json.get("item")[0]["asset"].get("title")
        if asset_title is None:
            asset_title = ""
        asset_id = info_json.get("item")[0]["asset"]["id"]
    return date, time, title, asset_title, channel, asset_id


if __name__ == "__main__":
    full_match_results = []
    list_of_file_with_full_match = []
    list_of_no_matches = []
    list_path = os.path.join(os.environ.get("HISTORICAL_PATH"), "2015/12/*/*/*.json")
    list_of_files = glob.glob(list_path)
    for path in list_of_files:
        print(f"Processing path: {path}")
        date, time, title, asset_title, channel_name, asset_id = get_stora_data(path)
        print(date, time, title, asset_title, channel_name, asset_id)
        continue
        search = f'transmission_date = "{date}" and transmission_start_time = "{time}" and title = "{title}" and broadcast_channel = "{channel_name}"'
        hit, record = adlib.retrieve_record(
            os.environ.get("CID_API4"),
            "manifestations",
            search,
            "1",
        )
        if record is None:
            print("orginal search failed, trying new search with different title")
            new_search = f'transmission_date = "{date}" and transmission_start_time = "{time}" and broadcast_channel = "{channel_name}" and title = "{asset_title}"'
            hit, new_record = adlib.retrieve_record(
                os.environ.get("CID_API4"),
                "manifestations",
                new_search,
                "1",
            )
            if (new_record is None) or (record is None):
                list_of_no_matches.append(path)
                continue
            new_priref = adlib.retrieve_field_name(new_record[0], "priref")
            new_alternative_number = adlib.retrieve_field_name(
                new_record[0], "alternative_number"
            )
        # if (record is None) or (new_record is None):
        # continue
        priref = adlib.retrieve_field_name(record[0], "priref")
        alternative_number = adlib.retrieve_field_name(record[0], "alternative_number")
        alternative_type = "PATV historical asset id"
        if alternative_number == [None] or new_alternative_number == [None]:
            alternative_number = asset_id
            alternative_number += "***"
        if hit == 1:
            full_match_results.append(
                {
                    "priref": priref[0] or new_priref[0],
                    "alternative_number": alternative_number or alternative_number[0],
                    "alternative_number.type": "PATV historical asset id",
                }
            )
            list_of_file_with_full_match.append(path)
    print(full_match_results)
    print(f"len of files: {len(list_of_files)}")
    df = pd.DataFrame(full_match_results)
    df.to_csv("/mnt/qnap_11/full_match_results_jan.csv", index=False)
    print(list_of_file_with_full_match)
    print(len(list_of_file_with_full_match))
    print(list_of_no_matches)
    print(len(list_of_no_matches))
    # print(f"Date: {date}")
    # print(f"time: {time}")
    # print(f"title: {title}")
    # print(f"asset_title: {asset_title}")
    # print(f"channel name: {channel_name}")
