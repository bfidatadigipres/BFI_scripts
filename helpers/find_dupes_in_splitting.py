#!/usr/bin/env python3

import os
import csv
import sys

LOG = os.path.join(os.environ.get("LOG_PATH"), "delete_post_split_qnap01.log")
CSV_CAPTURE = os.path.join(os.environ.get("LOG_PATH"), "duplicate_splitting_record.csv")


def yield_lines():
    """
    Open and read log, yield
    one line at a time
    """
    with open(LOG, "r") as logs:
        for row in logs.readlines():
            yield row


def write_to_csv(datepath, ob_nums, filenames, row):
    """
    Capture instances where recs > requirement
    """
    with open(CSV_CAPTURE, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([datepath, ob_nums, filenames, row])


def main():
    """
    Iterate all logs, build
    and refresh dict of processed
    splits, posting to CSV if
    meets search criteria
    """
    ob_nums = []
    files = []
    for row in yield_lines():
        if "CID Item record found, with object number" in row:
            ob_nums.append(row.split(" ")[-1].strip())
        if "CID Media record has reference number" in row:
            files.append(row.split(" ")[-1].strip())
        if "Preserved objects: " in str(row):
            date_path, d = row.split("Preserved objects: ")
            num_found = int(d.split(" ", 1)[0].strip())
            num_count = int(d.split(" ")[-1].strip())
            if num_found > num_count:
                print(f"*** PROBLEM ***\n{row}")
                write_to_csv(date_path, ",".join(ob_nums), ",".join(files), row)
            else:
                print(f"Normal amount of records found:\n{row}")
        if (
            "Ignored because not all Items are persisted" in str(row)
            or "Moved multi-item tape file" in str(row)
            or "Moved single item tape file" in str(row)
        ):
            ob_nums = []
            files = []


if __name__ == "__main__":
    main()
