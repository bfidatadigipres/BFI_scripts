import json
from datetime import datetime, timezone, timedelta
import sys
import os

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib

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

def split_title(title_article):
    """
    An exception needs adding for "Die " as German language content
    This list is not comprehensive.
    """
    if title_article.startswith(
        (
            "A ",
            "An ",
            "Am ",
            "Al-",
            "As ",
            "Az ",
            "Bir ",
            "Das ",
            "De ",
            "Dei ",
            "Den ",
            "Der ",
            "Det ",
            "Di ",
            "Dos ",
            "Een ",
            "Eene",
            "Ei ",
            "Ein ",
            "Eine",
            "Eit ",
            "El ",
            "el-",
            "En ",
            "Et ",
            "Ett ",
            "Het ",
            "Il ",
            "Na ",
            "A'",
            "L'",
            "La ",
            "Le ",
            "Les ",
            "Los ",
            "The ",
            "Un ",
            "Une ",
            "Uno ",
            "Y ",
            "Yr ",
        )
    ):
        title_split = title_article.split()
        ttl = title_split[1:]
        title = " ".join(ttl)
        title_art = title_split[0]
        return title, title_art

    return title_article, ""

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
        #print(info_json.get('item')[0].keys())
        date_time = info_json.get('item')[0]['dateTime']
        date = datetime.fromisoformat(date_time[:-1]).strftime('%Y-%m-%d')
        time = datetime.fromisoformat(date_time[:-1]) + timedelta(hours=1)
        time_str = time.strftime("%H:%M:%S")
        title = info_json.get('item')[0]['title']
        duration =  info_json.get('item')[0]['duration']
        asset_title = info_json.get('item')[0]['asset'].get('title')
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
            broadcast_company = "454"
            print(f"Broadcast company set to BBC in {fullpath}")
        elif "itv" in fullpath:
            broadcast_company = "20425"
            print(f"Broadcast company set to ITV in {fullpath}")
        elif "more4" in fullpath or "film4" in fullpath or "/e4/" in fullpath:
            broadcast_company = "73319"
            print(f"Broadcast company set to Channel4 in {fullpath}")
        elif "channel4" in fullpath:
            broadcast_company = "73319"
            print(f"Broadcast company set to Channel4 in {fullpath}")
        elif "5star" in fullpath or "five" in fullpath:
            broadcast_company = "24404"
            print(f"Broadcast company set to Five in {fullpath}")
        elif "sky_news" in fullpath:
            broadcast_company = "78200"
            print(f"Broadcast company set to Sky News in {fullpath}")
        elif "skyarts" in fullpath:
            broadcast_company = "150001"
            print(f"Broadcast company set to Sky Arts in {fullpath}")
        elif "skymixhd" in fullpath:
            broadcast_company = "999939366"
            print(f"Broadcast company set to Sky Mix in {fullpath}")
        elif "al_jazeera" in fullpath:
            broadcast_company = "125338"
            print(f"Broadcast company set to Al Jazeera in {fullpath}")
        elif "gb_news" in fullpath:
            broadcast_company = "999831694"
            print(f"Broadcast company set to GB News in {fullpath}")
        elif "talk_tv" in fullpath:
            broadcast_company = "999883795"
            print(f"Broadcast company set to Talk TV in {fullpath}")
        elif "/u_dave" in fullpath:
            broadcast_company = "999929397"
            print(f"Broadcast company set to U&Dave in {fullpath}")
        elif "/u_drama" in fullpath:
            broadcast_company = "999929393"
            print(f"Broadcast company set to U&Drama in {fullpath}")
        elif "/u_yesterday" in fullpath:
            broadcast_company = "999929396"
            print(f"Broadcast company set to U&Yesterday in {fullpath}")
        elif "qvc" in fullpath:
            broadcast_company = "999939374"
            print(f"Broadcast company set to QVC UK in {fullpath}")
        elif "togethertv" in fullpath:
            broadcast_company = "999939362"
            print(f"Broadcast company set to Together TV in {fullpath}")
        else:
            broadcast_company = None
    return date, time_str, title, asset_title, channel, asset_id, duration, certification, list_attributes, broadcast_company



if __name__ == "__main__":
    #list_path = sys.argv[1]
    #list_of_files = glob.glob(list_path)
    count = 0
    full_match_results = []
    path = ""
    #for path in list_of_files:
    print(f"Processing row {path}")
    date, time, json_title, asset_title, channel, asset_id, duration, certification, list_attributes, broadcast_company = get_stora_data(path)
    print(f"Date: {date}")
    print(f"time: {time}")
    print(f"certs :  {certification}")
    if asset_title.title().startswith("Generic"):
            print(asset_title)
            title_for_split = json_title
            generic = True
    elif asset_title == "":
            title_for_split = json_title
    else:
         title_bare = "".join(str for str in asset_title if str.isalnum())
         if title_bare.isnumeric():
             title_for_split = json_title
         else:
             title_for_split = asset_title
    title, title_article = split_title(title_for_split)
    title = title.replace("\xe2\x80\x99", "'").replace("\xe2'\x80\x93", "-")

    if title == "Close":
            print(f"Title has 'Close' as name: {path}")
            for key, val in CHANNELS.items():
                if f"/{key}/" in path:
                    print(f"Key that's in fullpath: {key}")
                    title = val[1]
                    print(f"Replacement title for 'Close': {title}")
#
    search =  f'grouping.lref="398775" and broadcast_channel = "{channel}" and transmission_date = "{date}" and transmission_start_time = "{time}"'
    print(search)
    hit, record = adlib.retrieve_record(os.environ.get("CID_API4"), "manifestations", search, "1")
    if record is None:
        print("orginal search failed, trying new search with different title")
    priref = adlib.retrieve_field_name(record[0], "priref")
    title_record = adlib.retrieve_field_name(record[0], "title")
    alternative_number = adlib.retrieve_field_name(record[0], "alternative_number")
    print(record[0])
    arts_title = adlib.retrieve_field_name(record[0], "title.article")
    print(f"title.article: {arts_title}")
    duration_secs = str(int(duration) * 60)
    if hit >= 1:
           count+=1
           full_match_results.append(
                {
                  "priref": priref[0],
                  "title.article": title_article,
                  "title": json_title,
                  "title.language": "English",
                  "title.type": '"05_MAIN"',
                  "title.article": f'{arts_title}',
                  "title": title_record,
                  "title.language": '"English"',
                  "title.type": '"35_ALTERNATIVE"',
                  "alternative_number.type": '"PATV asset id"',
                  "alternative_number": asset_id,
                  "utb.fieldname": '"EPG attributes"',
                  "ubt.content": ", ".join(str(x) for x in list_attributes if len(x) > 0)
                }
            )
    print(count)
    #print(f"Total files processed: {len(list_of_files)}")
    #print(f"file processed percentage: {count}/ {len(list_of_files)} --------> {count / len(list_of_files)}")
    #print(f"miss rate: {len(list_of_files) - count}/{len(list_of_files)}  ----->  {(len(list_of_files) - count)/ len(list_of_files)}")
    df = pd.DataFrame(full_match_results)
    print(df)



 



