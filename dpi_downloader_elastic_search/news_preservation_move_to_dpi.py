#!/usr/bin/env python3

'''
Looks to database.db for preservation date and
news channel information retrieving rows with status
'Requested'. Stores username, email, date, channel, status.

2023
'''

# Python packages
import os
import sys
import sqlite3
import logging
import itertools
import subprocess
import datetime

# GLOBAL VARIABLES
STORA = os.environ['STORA']
STORA_BACKUP = os.environ['STORA_BACKUP']
LOG_PATH = os.environ['LOG_PATH']
RSYNC_LOG = os.path.join(LOG_PATH, "news_preservation_move_to_dpi.log")
DATABASE = os.environ['DATABASE_NEWS_PRESERVATION']
EMAIL_SENDER=os.environ['EMAIL_SEND']
EMAIL_PSWD=os.environ['EMAIL_PASS']
FMT = '%Y-%m-%d %H:%M:%s'
TODAY = str(datetime.date.today())
YESTERDAY = datetime.date.today() - datetime.timedelta(days=1)
YESTERDAY2 = datetime.date.today() - datetime.timedelta(days=2)
YEST = str(YESTERDAY)
YEST2 = str(YESTERDAY2)

# Set up logging
LOGGER = logging.getLogger('news_preservation_move_to_dpi')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'news_preservation_move_to_dpi.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['power_off_all']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def date_gen(date_str):
    '''
    Generate date for checks if date
    in correct 14 day window
    '''
    from_date = datetime.date.fromisoformat(date_str)
    while True:
        yield from_date
        from_date = from_date - datetime.timedelta(days=1)


def check_date_range(preservation_date):
    '''
    Check date range is correct
    and that the file is not today
    '''
    if preservation_date in (TODAY, YEST):
        return False

    date_range = []
    period = itertools.islice(date_gen(TODAY), 14)
    for dt in period:
        date_range.append(dt.strftime('%Y-%m-%d'))

    daterange = ', '.join(date_range)
    print(f"Target range for DPI moves: {daterange}")
    if preservation_date in date_range:
        return True

    return False


def retrieve_requested():
    '''
    Access database.db and retrieve
    recently requested downloads
    '''
    requested_data = []
    try:
        sqlite_connection = sqlite3.connect(DATABASE)
        cursor = sqlite_connection.cursor()
        print("Database connected successfully")

        cursor.execute("SELECT * FROM DOWNLOADS WHERE status = 'Requested'")
        data = cursor.fetchall()
        print(data)
        for row in data:
            print(type(row), row)
            if row[-2] == 'Requested':
                requested_data.append(row)
        cursor.close()
    except sqlite3.Error as err:
        LOGGER.warning("%s", err)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("Database connection closed")

    # Sort for unique tuples only in list
    print(requested_data)
    sorted_data = remove_duplicates(requested_data)
    return sorted_data


def remove_duplicates(list_data):
    '''
    Sort and remove duplicates
    using itertools
    '''
    list_data.sort()
    grouped = itertools.groupby(list_data)
    unique = [key for key,_ in grouped]
    return unique


def update_table(preservation_date, channel, new_status):
    '''
    Update specific row with new
    data, for fname match
    '''
    try:
        sqlite_connection = sqlite3.connect(DATABASE)
        cursor = sqlite_connection.cursor()
        # Update row with new status
        sql_query = '''UPDATE DOWNLOADS SET status = ? WHERE preservation_date = ? AND channel = ?'''
        data = (new_status, preservation_date, channel)
        cursor.execute(sql_query, data)
        sqlite_connection.commit()
        print(f"Record updated with new status {new_status}")
        cursor.close()
    except sqlite3.Error as err:
        LOGGER.warning("Failed to update database: %s", err)
    finally:
        if sqlite_connection:
            sqlite_connection.close()


def main():
    '''
    Retrieve 'Requested' rows from database.db as list of
    tuples and process one at a time.
    '''
    data = retrieve_requested()
    if len(data) == 0:
        sys.exit('No data found in DOWNLOADS database')

    check_control()
    LOGGER.info("================ DPI NEWS PRESERVATION REQUESTS RETRIEVED: %s. Date: %s =================", len(data), datetime.datetime.now().strftime(FMT)[:19])
    for row in data:
        username = row[0].strip()
        email = row[1].strip()
        preservation_date = row[2].strip()
        channel = row[3].strip()
        LOGGER.info("** User %s, email %s. Preservation date requested %s for channel %s", username, email, preservation_date, channel)

        # Check date is not today/last three days or older than 14 days
        success = check_date_range(preservation_date)
        if not success:
            LOGGER.info("Skipping these items as they are not within the movable date range. If from the last three days files may not have moved to QNAP-04 yet.")
            continue

        # Make paths
        date_path = preservation_date.replace('-', '/')
        source_path = os.path.join(STORA_BACKUP, date_path, channel)

        if not os.path.exists(source_path):
            LOGGER.warning("Skipping: Error with source path: %s", source_path)
            continue

        status = move_folder(channel, date_path)
        if not status:
            message = f"Warning: Not all files confirmed as moved to DPI path for {channel} on {preservation_date}."
            send_email_update(email, preservation_date, channel, message)
            update_table(preservation_date, channel, "Move error")
            continue

        # Send notification email
        message = f"All files confirmed as moved to DPI path: {channel} {preservation_date}."
        send_email_update(email, preservation_date, channel, message)
        update_table(preservation_date, channel, "Preserved to DPI")

    LOGGER.info("================ DPI NEWS PRESERVATION REQUESTS COMPLETED. Date: %s =================\n", datetime.datetime.now().strftime(FMT)[:19])


def rsync_move(source_path, dest_path):
    '''
    DEPRECATED
    Copy files from QNAP-04
    STORA_backup/yyyy/mm/dd/channel to
    STORA/yyyy/mm/dd/channel
    '''
    source_path = source_path.rstrip('/')
    dest_path = dest_path.rstrip('/')

    if not os.path.exists(dest_path):
        os.makedirs(dest_path, mode=0o777, exist_ok=True)

    rsync_cmd = [
        'rsync',
        '--info=FLIST2,COPY2,PROGRESS2,NAME2,BACKUP2,STATS2',
        '-acvh', '--remove-source-files',
        '--no-o', '--no-g',
        source_path, dest_path,
        f'--log-file={RSYNC_LOG}'
    ]

    try:
        subprocess.call(rsync_cmd)
        LOGGER.info("Files moved to DPI path: %s", dest_path)
        return True
    except Exception as err:
        LOGGER.warning("Files failed moved to DPI path: %s", dest_path)
        LOGGER.warning(err)
        return False


def move_folder(channel, date_pth):
    '''
    Use Linux mv to shift folder from
    STORA_backup to STORA path
    '''

    from_path = os.path.join(STORA_BACKUP, date_pth, channel)
    to_path = os.path.join(STORA, date_pth, channel)

    cmd = [
        'mv',
        from_path,
        to_path
    ]

    try:
        subprocess.call(cmd)
    except Exception as err:
        LOGGER.warning("Files failed move to DPI path: %s", to_path)
        LOGGER.warning(err)
        return False

    if not os.path.exists(from_path) and os.path.exists(to_path):
        return True


def send_email_update(email, preservation_date, channel, message):
    '''
    Update user that their item has been
    downloaded, with path, folder and
    filename of downloaded file
    '''
    from email.message import EmailMessage
    import ssl
    import smtplib

    name_extracted = email.split('.')[0]
    subject = 'DPI News preservation move completed'
    body = f'''
Hello {name_extracted.title()},

Your DPI preservation request has completed for channel and date:
{channel} {preservation_date}

Confirmation:
{message}

If these items do not appear in CID within 5 days, or there is a confirmation warning issued above then please raise an issue in the BFI Collections Systems Service Desk ASAP:
https://bficollectionssystems.atlassian.net/servicedesk/customer/portal/1

This is an automated notification, please do not reply to this email.

Thank you,
Digital Preservation team'''

    send_mail = EmailMessage()
    send_mail['From'] = EMAIL_SENDER
    send_mail['To'] = email
    send_mail['Subject'] = subject
    send_mail.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        try:
            smtp.login(EMAIL_SENDER, EMAIL_PSWD)
            smtp.sendmail(EMAIL_SENDER, email, send_mail.as_string())
            LOGGER.info("Email notification sent to %s", email)
        except Exception as exc:
            LOGGER.warning("Email notification failed in sending: %s\n%s", email, exc)


if __name__ == '__main__':
    main()
