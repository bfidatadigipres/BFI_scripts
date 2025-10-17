#!/usr/bin/env python3

"""
Aggregate Flask supplied selections and submit Workflow jobs
for all KLC workflow requests

Dependencies:
1. Logs/workflow_requests_record_creator.log
2. app.py and workflow.db
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
    os.environ["WORKFLOW"], f"flask_app/workflow.db"
)  # Table to be called WORKFLOW_REQUESTS
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
            if row[-1] == "Requested":
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

    LOGGER.info("=== Processing items from workflow requests app ===")

    requested_jobs = retrieve_requested()
    if len(requested_jobs) == 0:
        LOGGER.info("No jobs found this pass. Script exiting")
        sys.exit()

    LOGGER.info("Requested jobs found: %s", len(requested_jobs))

    # Iterate jobs
    for job in requested_jobs:
        job_id = job[0].strip()
        items_list = job[1].strip()  # Saved search number
        cname = job[10].strip()
        cemail = job[11].strip()
        destination = job[7].strip()

        batch_items = get_prirefs(item_list)
        if not priref_items:
            update_table(job_id, status, "Error with Saved Search")
            # Send notification email
            send_email_update(cemail, cname, "Workflow request failed", job)
            continue

        update_table(job_id, status, "Started")

        # Make job metadata for Batch creation - do we need deadline?
        job_metadata = {}
        job_metadata["activity.code"] = job[2].strip()
        job_metadata["assigned_to"] = job[12].strip()  # Would this be contact details?
        job_metadata["request_type"] = job[3].strip()
        job_metadata["request_outcome"] = job[4].strip()
        job_metadata["description"] = f"{job[5].strip()} / {str(datetime.today())[:19]}"
        job_metadata["completion.date"] = job[6].strip()
        job_metadata["request.details"] = job[
            8
        ].strip()  # Maps to specific information?
        job_metadata["request.from.department"] = job[9].strip()
        job_metadata["request.from.email"] = cemail
        job_metadata["request.from.name"] = cname
        job_metadata["request.from.telephone.number"] = ""

        deadline = (datetime.today() + timedelta(days=10)).strftime("%Y-%m-%d")
        job_metadata["negotiatedDeadline"] = deadline

        # Create Workflow records
        print("* Creating Workflow records in CID...")
        batch = workflow.BatchBuild(destination, items=batch_items, **job_metadata)
        if not batch.successfully_completed:
            print(batch_items, batch)
            update_table(job_id, status, "Error creating workflow batch")
            send_email_update(cemail, cname, "Workflow request failed", job)
            continue

        update_table(job_id, status, "Completed workflow record creation")
        send_email_update(cemail, cname, "Workflow request completed", job)


def update_table(job_id: str, new_status: str) -> None:
    """
    Update specific row with new
    data, for fname match
    """
    try:
        sqlite_connection = sqlite3.connect(DATABASE)
        cursor = sqlite_connection.cursor()
        # Update row with new status
        sql_query = """UPDATE WORKLOAD_REQUESTS SET status = ? WHERE job_id = ?"""
        data = (new_status, job_id)
        cursor.execute(sql_query, data)
        sqlite_connection.commit()
        LOGGER.info("Record updated with new status %s", new_status)
        cursor.close()
    except sqlite3.Error as err:
        LOGGER.warning("Failed to update database: %s", err)
    finally:
        if sqlite_connection:
            sqlite_connection.close()


def send_email_update(client_email: str, client: str, status: str, job: list) -> None:
    """
    Update user that their item has been
    requested and confirm their request
    data to them
    """
    import smtplib
    import ssl
    from email.message import EmailMessage

    if status == "Workflow request completed":
        message = f"You workflow request completed successfully at {str(datetime.datetime.now())}."
    elif status == "Workflow request failed":
        message = f"I'm sorry but some / all of your workflow job request failed at {str(datetime.datetime.now())}."

    name_extracted = email.split(".")[0]
    subject = "DPI file download request completed"
    body = f"""
Hello {client.title()},

{message}

Your original request details:
    Items list: {job[1]}
    Activity code: {job[2]}
    Request type: {job[3]}
    Request outcome: {job[4]}
    Job description: {job[5]}
    Delivery date: {job[6]}
    Final destination: {job[7]}
    Specific instructions: {job[8]}
    Client category: {job[9]}
    Client name: {job[10]}
    Client email: {job[11]}
    Contact details: {job[12]}

If there are problems with the request(s), please raise an issue in the BFI Collections Systems Service Desk:
https://bficollectionssystems.atlassian.net/servicedesk/customer/portal/1

This is an automated notification, please do not reply to this email.

Thank you,
Digital Preservation team"""

    send_mail = EmailMessage()
    send_mail["From"] = EMAIL_SENDER
    send_mail["To"] = email
    send_mail["Subject"] = subject
    send_mail.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        try:
            smtp.login(EMAIL_SENDER, EMAIL_PSWD)
            smtp.sendmail(EMAIL_SENDER, email, send_mail.as_string())
            LOGGER.info("Email notification sent to %s", email)
        except Exception as exc:
            LOGGER.warning("Email notification failed in sending: %s\n%s", email, exc)


if __name__ == "__main__":
    main()
