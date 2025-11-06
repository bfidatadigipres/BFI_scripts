#!/usr/bin/env python3

"""
Aggregate Flask supplied selections and submit Workflow jobs
for all KLC workflow requests

Dependencies:
1. Logs/workflow_requests_record_creator.log
2. app.py and workflow_requests.db
3. workflow_requests.py - remodelling from ../workflow.py
4. ../records.py

Web app form to supply list:
0   username TEXT NOT NULL,
1   email TEXT NOT NULL,
2   first_name TEXT NOT NULL,
3   last_name TEXT NOT NULL,
4   client_category TEXT NOT NULL,
5   jobid INTEGER PRIMARY KEY AUTOINCREMENT,
6   items_list TEXT NOT NULL,
7   activity_code TEXT NOT NULL,
8   request_type TEXT NOT NULL,
9   request_outcome TEXT NOT NULL,
10  description TEXT NOT NULL,
11  delivery_date TEXT NOT NULL,
12  destination TEXT NOT NULL,
13  instructions TEXT,
14  client_name TEXT,
15  contact_details TEXT,
16  department TEXT NOT NULL,
17  status TEXT NOT NULL,
18  date TEXT NOT NULL

Written up, but needs testing of data supply
into workflow_requests, and that records are
created and have correct data.
"""

# Public imports
import sqlite3
import os
import sys
import logging
import itertools
from datetime import datetime, timedelta
from typing import Final, Optional

# Local imports
import workflow_requests as workflow

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Global variables
LOG_PATH = os.environ["LOG_PATH"]
DATABASE = os.path.join(
    os.environ["WORKFLOW"], "flask_app/workflow_requests.db"
)  # Table to be called REQUESTS
NOW = datetime.now()
DT_STR = NOW.strftime("%d/%m/%Y %H:%M:%S")

# Set up logging
LOGGER = logging.getLogger("workflow_requests_record_creator")
HDLR = logging.FileHandler(
    os.path.join(LOG_PATH, "workflow_requests_record_creator.log")
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)
EMAIL_SENDER: Final = os.environ["EMAIL_SEND"]
EMAIL_PSWD: Final = os.environ["EMAIL_PASS"]
CID_API = os.environ.get("CID_API3")
REQUEST_TYPE = {
    "AUDIOSUITETRANSFER": "Audio suite transfer",
    "BOOKING": "Booking",
    "DATAMIGRATION": "Data migration",
    "ENCODING": "Encoding",
    "EXAMINATION": "Examination",
    "FASTBOOKING": "Fast booking",
    "FILMDUPLICATIONEXTERNAL": "Film duplication external",
    "FILMPRINTINGEXTERNAL": "Film printing external",
    "FILMPRINTINGINTERNAL": "Film printing internal",
    "HDSCANNINGEXTERNAL": "HD scanning external",
    "HDSCANNINGINTERNAL": "HD scanning internal",
    "HIGHDEFEXTERNAL": "High def transfer external",
    "INFORMATION": "Information",
    "INHOUSEAUDIODIGITISATION": "Inhouse audio digitisation",
    "INSPECTION": "Inspection",
    "NEWTITLECREATION": "New title creation",
    "NUMBERING": "Numbering",
    "OFFAIRRECORDING": "Off air recording",
    "OTHER": "Other",
    "QUALITYCONTROL": "Quality control",
    "SDEXTERNAL": "SD transfer external",
    "SDSCANNINGEXTERNAL": "SD scanning external",
    "SDSCANNINGINTERNAL": "SD scanning internal",
    "TECHNICALSELECTION": "Technical selection",
    "TELECINEEXTERNAL": "Telecine transfer external",
    "TELECINEINTERNAL": "Telecine transfer internal",
    "TRANSCODING": "Transcoding",
    "VIDEOCOPY": "Video copy",
    "VIDEOCOPYNOTSUPPORTED": "Video copy formats not supported within video lab",
    "VIDEOTRANSFEREXTERNAL": "Video transfer external",
    "VIDEOUNITINHOUSE": "Video unit inhouse",
}


def retrieve_requested() -> list[str]:
    """
    Access workflow.db and retrieve
    recently requested vaults picks
    """
    requested_data = []
    try:
        sqlite_connection = sqlite3.connect(DATABASE)
        cursor = sqlite_connection.cursor()
        print("Database connected successfully")

        cursor.execute('''SELECT * FROM REQUESTS WHERE status = "Requested"''')
        data = cursor.fetchall()
        print(data)
        for row in data:
            print(type(row), row)
            if row[17] == "Requested":
                requested_data.append(row)
        cursor.close()
    except sqlite3.Error as err:
        LOGGER.warning("%s", err)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("Database connection closed")

    # Sort for unique tuples only in list
    sorted_data = remove_duplicates(requested_data)
    return sorted_data


def remove_duplicates(list_data: list[str]) -> list[str]:
    """
    Sort and remove duplicatesdef remove_duplicates(list_data: list[str]) -> list[str]:
    """

    list_data.sort()
    grouped = itertools.groupby(list_data)
    unique = [key for key, _ in grouped]
    if len(unique) > 1:
        unique.sort(key=lambda a: a[18])
    return unique


def get_prirefs(pointer: str) -> Optional[list[str]]:
    """
    User pointer number and look up
    for list of prirefs in CID
    """
    query = {
        "command": "getpointerfile",
        "database": "items",
        "number": pointer,
        "output": "jsonv1",
    }

    try:
        result = adlib.get(CID_API, query)
        print(f"CID getpointerfile search:\n{result}")
    except Exception as exc:
        LOGGER.exception(
            "get_prirefs(): Unable to get pointer file %s\n%s", pointer, exc
        )
        result = None

    if not result["adlibJSON"]["recordList"]["record"][0]["hitlist"]:
        return None
    prirefs = result["adlibJSON"]["recordList"]["record"][0]["hitlist"]
    LOGGER.info("Prirefs retrieved: %s", prirefs)

    return prirefs


def main():
    """
    Process all items found returned from Flask app
    """
    if not utils.check_control("pause_scripts"):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    LOGGER.info("=== Workflow requests record creation start %s ===", str(datetime.now())[:18])

    requested_jobs = retrieve_requested()
    print(requested_jobs)
    if len(requested_jobs) == 0:
        LOGGER.info("No jobs found this pass. Script exiting")
        sys.exit()

    LOGGER.info("Requested jobs found: %s", len(requested_jobs))

    # Iterate jobs
    for job in requested_jobs:
        LOGGER.info("** New job:\n%s", job)
        job_id = job[5]
        saved_search = job[6]
        uname = job[0].strip()
        email = job[1].strip()
        destination = "PBK06B03000000" # Temp hard coding - needs reviewing
        purpose = job[9].strip()
        firstname = job[2].strip()
        lastname = job[3].strip()

        batch_items = get_prirefs(saved_search)
        if not batch_items:
            LOGGER.warning("No items prirefs found in saved search.")
            update_table(job_id, "Error with Saved Search")
            # Send notification email
            send_email_update(email, firstname, "Workflow request failed: Error with Saved Search number", job)
            continue
        if len(batch_items) > 50:
            LOGGER.warning("More than 50 item prirefs found in saved search.")
            update_table(job_id, "Too many items in Saved Search")
            # Send notification email
            send_email_update(email, firstname, "Workflow request failed: Too many items in Saved Search", job)
            continue

        LOGGER.info("Batch items from saved search: %s", batch_items)
        update_table(job_id, "Started")

        # Make job metadata for Batch creation
        job_metadata = {}
        deadline = (datetime.today() + timedelta(days=10)).strftime("%Y-%m-%d")
        request_date = job[18].strip()[:10]
        job_metadata["activity.code"] = job[7].strip()
        job_metadata["client.name"] = job[14].strip()
        job_metadata["client.details"] = job[15].strip()
        job_metadata["client.category"] = job[4].strip()
        job_metadata["request_type"] = job[8].strip()
        job_metadata["description"] = f"{job[10].strip()} / {str(datetime.today())[:19]}"
        job_metadata["completion.date"] = job[11].strip()
        job_metadata["final_destination"] = job[12].strip()
        job_metadata["request.details"] = job[13].strip()
        # job_metadata["request.from.department"] = job[16].strip()
        job_metadata["request.from.email"] = email
        job_metadata["request.from.name"] = f"{firstname} {lastname}"
        job_metadata["request.date.received"] = request_date
        job_metadata["negotiatedDeadline"] = deadline
        job_metadata["input.name"] = uname
        LOGGER.info("Job metadata build: %s", job_metadata)

        # Create Workflow records
        print("* Creating Workflow records in CID...")
        LOGGER.info("* Creating Workflow records in CID...")
        batch = workflow.BatchBuild(destination, purpose, uname, items=batch_items, **job_metadata)
        if not batch.successfully_completed:
            print(batch_items, batch)
            LOGGER.warning("Batch record creation failed:\n%s\n%s", batch_items, batch)
            update_table(job_id, "Error creating workflow batch")
            send_email_update(email, firstname, "Workflow request failed: Error creating workflow batch", job)
            continue
        if len(job_metadata["client.name"]) > 0:
            LOGGER.info("Client.name populated - making P&I record for %s", job_metadata["client.name"])
            p_priref = create_people_record(job_metadata["client.name"])
            if not p_priref:
                LOGGER.warning("Person record failed to create: %s", job_metadata["client.name"])

        update_table(job_id, "Completed")
        send_email_update(email, firstname, "Workflow request completed", job)

    LOGGER.info("=== Workflow requests record creation completed %s ===", str(datetime.now())[:18])


def update_table(job_id: str, new_status: str) -> None:
    """
    Update specific row with new
    data, for fname match
    """
    try:
        sqlite_connection = sqlite3.connect(DATABASE)
        cursor = sqlite_connection.cursor()
        # Update row with new status
        sql_query = """UPDATE REQUESTS SET status = ? WHERE jobid = ?"""
        data = (new_status, job_id)
        cursor.execute(sql_query, data)
        sqlite_connection.commit()
        LOGGER.info("Record updated with new status '%s'", new_status)
        cursor.close()
    except sqlite3.Error as err:
        LOGGER.warning("Failed to update database: %s", err)
    finally:
        if sqlite_connection:
            sqlite_connection.close()


def send_email_update(client_email: str, firstname: str, status: str, job: list) -> None:
    """
    Update user that their item has been
    requested and confirm their request
    data to them
    """

    if status == "Workflow request completed":
        message = f"You workflow request completed successfully at {str(datetime.now())}."
    else:
        message = f"I'm sorry but some / all of your workflow job request failed at {str(datetime.now())}.\nReport: {status}."

    subject = "CID Workflow request notification"
    body = f"""
Hello {firstname.title()},

{message}

Your original request details:
    Saved search: {job[6]}
    Activity code: {job[7]}
    Request type: {REQUEST_TYPE.get(job[8])}
    Request outcome: {job[9]}
    Job description: {job[10]}
    Delivery date: {job[11]}
    Final destination: {job[12]}
    Specific instructions: {job[13]}
    Client category: {job[4]}
    Client name: {job[14]}
    Contact details: {job[15]}

If there are problems with the request(s), please raise an issue in the BFI Collections Systems Service Desk:
https://bficollectionssystems.atlassian.net/servicedesk/customer/portal/1

This is an automated notification, please do not reply to this email.

Thank you,
Collections Systems team"""

    success, error = utils.send_email(client_email, subject, body, "")
    if success:
        LOGGER.info("Email notification sent to %s",client_email)
    else:
        LOGGER.warning("Email notification failed in sending: %s", client_email)
        LOGGER.warning("Error: %s", error)


def create_people_record(client_name):
    """
    Where client.name is populated create
    P&I record for the individual
    """

    credit_dct = []
    credit_dct.append({"name": client_name})
    credit_dct.append({"name.type": "CASTCREDIT"})
    credit_dct.append({"name.type": "PERSON"})
    credit_dct.append({"name.status": "5"})
    credit_dct.append({"record_access.user": "BFIiispublic"})
    credit_dct.append({"record_access.rights": "0"})
    credit_dct.append({"record_access.reason": "SENSITIVE_LEGAL"})
    credit_dct.append({"input.name": "datadigipres"})
    credit_dct.append({"input.date": str(datetime.now())[:10]})
    credit_dct.append({"input.time": str(datetime.now())[11:19]})

    # Convert dict to xml using adlib
    credit_xml = adlib.create_record_data(CID_API, "people", "", credit_dct)
    if not credit_xml:
        LOGGER.warning("Credit data failed to create XML: %s", credit_dct)
        return None

    # Create basic person record
    record = adlib.post(CID_API, credit_xml, "people", "insertrecord")
    if not record:
        print(f"*** Unable to create People record: {credit_xml}")
        LOGGER.critical("make_person_record():Unable to create People record\n%s", err)
    try:
        credit_priref = adlib.retrieve_field_name(record, "priref")[0]
        return credit_priref
    except Exception as err:
        print(f"*** Unable to create People record: {err}")
        LOGGER.critical("make_person_record():Unable to create People record\n%s", err)
        raise


if __name__ == "__main__":
    main()
