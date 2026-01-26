#!/usr/bin/env python3

"""
Take feed from nominated pointer file and process into Workflow jobs
for next Multi Machine Environment in F47 (first was DigiBeta)

Dependencies:
1. Pointer file number ??? where Items are added for processing
2. LOGS/tvam_selecta.log
3. tvam/selections.csv
4. tvam/submitta.py
"""

# Public imports
import csv
import datetime
import os
import sys
import uuid
from tenacity import retry, stop_after_attempt

# Local imports
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

sys.path.append(os.environ["WORKFLOW"])
import selections
import tape_model

LOGS = os.environ["LOG_PATH"]
CID_API = utils.get_current_api()
NOW = datetime.datetime.now()
DT_STR = NOW.strftime("%d/%m/%Y %H:%M:%S")
SELECTIONS = os.path.join(os.environ["WORKFLOW"], "tvam/selections.csv")


def get_candidates():
    """
    Retrieve items from pointer file ???
    """
    q = {
        "command": "getpointerfile",
        "database": "items",
        "number": 454,  ???
        "output": "jsonv1",
    }

    try:
        result = adlib.get(CID_API, q)
        candidates = result["adlibJSON"]["recordList"]["record"][0]["hitlist"]
    except Exception as exc:
        print(result["adlibJSON"]["diagnostic"])
        raise Exception("Cannot getpointerfile") from exc

    write_to_log(f"Total candidates: {len(candidates)}")
    return candidates


@retry(stop=stop_after_attempt(10))
def get_object_number(priref):
    """
    Retrieve object number using priref
    """
    search = f"priref={priref}"
    record = adlib.retrieve_record(CID_API, "items", search, "1", ["object_number"])[1]
    if record is None:
        raise Exception
    object_number = adlib.retrieve_field_name(record[0], "object_number")[0]
    return object_number


def main():
    """
    Selections script, write to CSV
    """
    if not utils.check_control("pause_scripts"):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        sys.exit()
    if not utils.check_storage(SELECTIONS):
        print("Script run prevented by Storage Control document. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    write_to_log(f"=== Processing Items in TVAM 1inch Pointer File === {DT_STR}\n")
    write_to_log(
        "Fetching csv data, building selected items list and fetching candidates.\n"
    )
    tvam_select_csv = os.path.join(os.environ["WORKFLOW"], "tvam/selections.csv")
    selects = selections.Selections(input_file=tvam_select_csv)
    selected_items = selects.list_items()
    candidates = get_candidates()

    # Process candidate selections in pointer
    dupe_check = []
    for priref in candidates:
        write_to_log(f"Candidate number: {candidates.index(priref)} ")
        obj = get_object_number(priref)

        # Ignore already selected items
        matched = False
        for sel in selected_items:
            if obj == sel:
                write_to_log(
                    f"* Item already in tvam/selections.csv: {priref} {obj} - Matched to {sel}\n"
                )
                matched = True
                break
        if matched is True:
            continue

        # Model tape carrier
        write_to_log(f"Modelling tape carrier for item {priref} {obj}\n")
        try:
            t = tape_model.Tape(obj)
        except Exception:
            write_to_log(f"Could not model tape from object: {obj}")
            continue

        # Get data
        fmt = t.format()
        d = t.identifiers
        d["format"] = fmt
        d["duration"] = t.duration()
        dates = t.content_dates()
        if dates:
            d["content_dates"] = ",".join([str(i) for i in dates])

        item_ids = [i["object_number"] for i in t.get_identifiers()]
        d["items"] = ",".join(item_ids)
        d["item_count"] = len(item_ids)
        d["location"] = t.location()
        d["uid"] = str(uuid.uuid4())

        write_to_log("This tape will be added to tvam/selections.csv:\n")
        write_to_log(f"{str(d)}\n")

        # Add tape to dthree/selections.csv if unique
        print(f"add: {str(d)}")
        str_check = str(d).split("uid")[0]
        if str_check in dupe_check:
            write_to_log(
                f"Skipping write to CSV, exact match already written to CSV: {str_check}"
            )
            continue
        dupe_check.append(str_check)
        # selections.add(**d) DEPRECATED
        result = selections_add(d)
        if result is None:
            write_to_log("Failed to write data to selections.csv")
            sys.exit("Failed to write data to selections.csv")

    write_to_log(f"=== Items in TVAM 1inch Pointer File completed === {DT_STR}\n")


def selections_add(data):
    """
    Write list to new row in CSV
    Replacing broken selection.add()
    Temporary, for refactoring
    """
    data_list = []
    if not isinstance(data, dict):
        return None

    if not "can_ID" in data:
        data_list.append("")
    else:
        data_list.append(data["can_ID"])
    if not "package_number" in data:
        data_list.append("")
    else:
        data_list.append(data["package_number"])
    if not "uid" in data:
        data_list.append("")
    else:
        data_list.append(data["uid"])
    if not "location" in data:
        data_list.append("")
    else:
        data_list.append(data["location"])
    if not "duration" in data:
        data_list.append("")
    else:
        data_list.append(data["duration"])
    if not "format" in data:
        data_list.append("")
    else:
        data_list.append(data["format"])
    if not "item_count" in data:
        data_list.append("")
    else:
        data_list.append(data["item_count"])
    if not "content_dates" in data:
        data_list.append("")
    else:
        data_list.append(data["content_dates"])
    if not "items" in data:
        data_list.append("")
    else:
        data_list.append(data["items"])

    try:
        print(f"Adding amended data list: {data_list}")
        with open(SELECTIONS, "a") as file:
            writer = csv.writer(file)
            writer.writerow(data_list)
            return True
    except Exception:
        write_to_log("Failed to write data to selections.csv")
        return None


def write_to_log(message):
    """
    Write to tvam selecta log
    """
    with open(os.path.join(LOGS, "tvam_selecta.log"), "a") as file:
        file.write(message)
        file.close()


if __name__ == "__main__":
    main()
