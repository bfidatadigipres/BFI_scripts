#!/usr/bin/env python3

"""
Parse `global.log` and report on files with outstanding
WARNING alerts issued for the given day.

2022
"""
# Python library imports
import csv
import datetime
# Python library imports
import os
import shutil
import sys
from typing import Final

csv.field_size_limit(10000000)

# Local imports
sys.path.append(os.environ["CODE"])
import utils

# Date variable for use in ordering error outputs
TODAY: Final = datetime.date.today()
YEST: Final = TODAY - datetime.timedelta(days=1)
YEST2: Final = TODAY - datetime.timedelta(days=2)
DATE_VAR: Final = YEST.strftime("%Y-%m-%d")
DATE_VAR2: Final = YEST2.strftime("%Y-%m-%d")
LOGS: Final = os.environ["LOG_PATH"]
CONTROL_JSON: Final = os.path.join(LOGS, "downtime_control.json")
GLOBAL_LOG: Final = os.path.join(LOGS, "autoingest/global.log")
CURRENT_ERROR_FOLD: Final = os.environ["CURRENT_ERRORS"]
CURRENT_ERRORS: Final = os.path.join(CURRENT_ERROR_FOLD, "current_errors.csv")
CURRENT_ERRORS_NEW: Final = os.path.join(CURRENT_ERROR_FOLD, "current_errors_new.csv")

FILEPATHS = [
    "AUTOINGEST_QNAP01",
    "AUTOINGEST_QNAP02",
    "AUTOINGEST_QNAP03",
    "AUTOINGEST_QNAP04",
    "AUTOINGEST_QNAP05",
    "AUTOINGEST_QNAP06",
    "AUTOINGEST_QNAP07",
    "AUTOINGEST_QNAP08",
    "AUTOINGEST_QNAP09",
    "AUTOINGEST_QNAP10",
    "AUTOINGEST_QNAP11",
    "AUTOINGEST_QNAP08_OSH",
    "BP_VIDEO",
    "BP_AUDIO",
    "BP_DIGITAL",
    "BP_SC",
    "BP_FILM1",
    "BP_FILM2",
    "BP_FILM3",
    "BP_FILM4",
    "BP_FILM5",
    "BP_FILM6",
    "AUTOINGEST_EDITSHARE",
]


def main():
    """
    For standalone use of log_parser
    not, launched from autoingest
    """
    if not utils.check_control("autoingest"):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    create_current_errors_logs()


def create_current_errors_logs() -> None:
    """
    Parse global.log entries
    """
    data: dict = {}
    with open(GLOBAL_LOG, "r") as file:
        rows = csv.reader(file, delimiter="\n")
        for row in rows:
            row = row[0].split("\t")
            print(row)
            # Temp addition to reduce current_errors.csv
            if "MD5 checksum does not yet exist for this file." in str(row):
                continue
            try:
                timedate = row[0]
                local_p = row[2]
                remote_p = row[3]
                status = row[1]
                file_ = row[4]
                message = row[5]
            except (IndexError, KeyError):
                continue
            print(timedate, status, local_p, remote_p, file_, message)
            if ".tmp" in file_ or ".ini" in file_ or ".DS_Store" in file_:
                continue

            # Add items from today only that have WARNING status file still in path
            if timedate.startswith(DATE_VAR) and "WARNING" in status:
                print(
                    f"File exists in date range with 'WARNING', adding to dictionary: {file}"
                )
                # Aggregate all messages for select files.
                if file_ in data:
                    data[file_][timedate] = (status, message, local_p, remote_p)
                else:
                    data[file_] = {timedate: (status, message, local_p, remote_p)}
            elif timedate.startswith(DATE_VAR2) and "WARNING" in status:
                print(
                    f"File exists in date range with 'WARNING', adding to dictionary: {file}"
                )
                # Aggregate all messages for select files.
                if file_ in data:
                    data[file_][timedate] = (status, message, local_p, remote_p)
                else:
                    data[file_] = {timedate: (status, message, local_p, remote_p)}

    print(data)
    append_rows: list = []
    for file_ in data.items():
        # This section removes duplicates entries, writing just last entry to csv
        latest_timedate = sorted(data[file_[0]].keys())[-1]
        latest_message = data[file_[0]][latest_timedate]
        (status, message, local_p, remote_p) = latest_message

        # Remove non-files, like .tmp, .ini and .DS_Store
        if ".tmp" in file_ or ".ini" in file_ or ".DS_Store" in file_:
            pass
        else:
            print(f"* Adding {local_p} to error log")
            local_p2 = local_p.replace("/", " | ")
            local_p2 = local_p2.lstrip(" | ")
            append_rows.append((latest_timedate[:16], local_p2, file_[0], message))

    if append_rows:
        append_rows.sort(reverse=True)
        print("* Creating CSV file current_errors.csv in current_errors folder...")
        with open(CURRENT_ERRORS, "w") as of:
            writer = csv.writer(of)
            writer.writerow(["timedate", "path", "file", "message"])
            for ar in append_rows:
                writer.writerow(ar)
    else:
        print(
            "* No files still exist where status = WARNING, so nothing to add to error log this time..."
        )
        with open(CURRENT_ERRORS_NEW, "w+") as of:
            of.write(
                "No files where status = WARNING and still_exists = True, so no error logs to report this time..."
            )
        shutil.move(CURRENT_ERRORS_NEW, CURRENT_ERRORS)

    print("* Creating versions of error log in all in-scope autoingest NAS shares")

    for autoingest_key in FILEPATHS:
        autoingest_path = os.environ.get(autoingest_key)
        if not utils.check_storage(autoingest_path):
            print(
                "Skipping path - storage_control.json returned ‘False’ for path {autoingest_path}"
            )
            continue

        if os.path.exists(autoingest_path):
            shutil.copy(
                CURRENT_ERRORS,
                os.path.join(autoingest_path, "current_errors/current_errors.csv"),
            )


if __name__ == "__main__":
    main()
