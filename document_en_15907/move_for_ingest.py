#!/usr/bin/ python3

'''
Move folders of donor file tiffs into autoingest scope, sequentially
And move ingested folders out, after checking status

Updated for Adlib_V3 and Python3

Joanna White
2024
'''

# Python imports
import os
import sys
import json
import shutil
import logging
import datetime

# Local imports
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global variables
CID_API = os.environ['CID_API3']
MOVE_FOLDER = False
INPUT_PATH = os.path.join(os.environ['IS_MEDIA'], 'donor_files/renamed_ready_for_ingest/')
INGEST_PATH = os.path.join(os.environ['AUTOINGEST_IS_ING'], 'ingest/proxy/image/')
OUTPUT_PATH = os.path.join(os.environ['IS_MEDIA'], 'donor_files/partly_ingested/')
LOGS = os.environ['LOG_PATH']
GLOBAL_LOG = os.path.join(LOGS, 'autoingest/global.log')
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
NOW = (str(datetime.datetime.now()))[0:19]
NOW_MINUS_30 = str(datetime.datetime.now() - datetime.timedelta(minutes=30))
NOW_MINUS_60 = str(datetime.datetime.now() - datetime.timedelta(minutes=60))
QUERY_30 = NOW_MINUS_30[0:16]
QUERY_60 = NOW_MINUS_60[0:16]

# Setup logging
LOGGER = logging.getLogger('folder_moves')
HDLR = logging.FileHandler(os.path.join(os.environ['IS_MEDIA'], 'donor_files/test_folder_move.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check for downtime control
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            LOGGER.info("Script run prevented by downtime_control.json. Script exiting")
            sys.exit("Script run prevented by downtime_control.json. Script exiting")


def cid_check():
    '''
    Test if CID API online
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def check_media_records(folder_search, time):
    '''
    Check number of successful CID media records created from folder
    '''
    if time is None:
        search = f'(object.object_number=CA* and imagen.media.original_filename=*{folder_search}*)'
        hits = adlib.retrieve_record(CID_API, 'media', search, '-1')[0]
    else:
        search = f'(object.object_number=CA* and imagen.media.original_filename=*{folder_search}*) and (creation>"{time}")'
        hits = adlib.retrieve_record(CID_API, 'media', search, '-1')[0]
    if hits is None:
        raise Exception(f"CID API could not be reached with Media search:\n{search}")
    return int(hits)


def main():
    '''
    Move folders where conditions allow
    '''

    LOGGER.info('==== Move for ingest script running at %s ===============', str(datetime.datetime.now())[0:19])
    for _, dirs, _ in os.walk(INGEST_PATH):
        for foldername in dirs:
            check_control()
            cid_check()
            move_folder = False
            folder_search = f'_{foldername}of'
            LOGGER.info('* Current folder is %s - searching for %s', foldername, folder_search)

            # Check number of successful ingest jobs from folder
            file = open(GLOBAL_LOG, 'r')
            ingest_jobs = len([x for x in file if folder_search in x and 'Moved ingest-ready file to BlackPearl ingest' in x])
            LOGGER.info('* Number of successful ingest jobs from folder %s: %s', foldername, ingest_jobs)

            hits = check_media_records(folder_search, None)
            # If zero CID records created for folder, exit and wait until next time
            if hits == 0:
                LOGGER.info('* No CID records created for folder, waiting until next time...')
                continue

            gap = ingest_jobs - hits
            LOGGER.info('* Number of successful CID Media records from folder %s: %s', foldername, hits)
            LOGGER.info('* Gap = %s', gap)

            if gap == 0:
                # Check whether in-scope CID Media records created in last 30 mins
                g_hits = check_media_records(folder_search, QUERY_30)
                LOGGER.info('* Number of CID Media records created from folder %s in last 30 mins: %s', foldername, g_hits)
                if g_hits == 0:
                    LOGGER.info('* Ready to move - gap is zero and no Media records created for folder %s in last 30 mins', foldername)
                    move_folder = True
                else:
                    LOGGER.info('* NOT ready to move - gap is zero, but %s Media records created for folder %s in last 30 mins - waiting a while...', g_hits, foldername)
                    move_folder = False
            else:
                # Check whether in-scope CID Media records created in last 60 mins
                g_hits = check_media_records(folder_search, QUERY_60)
                LOGGER.info('* Number of CID Media records created from folder %s in last 60 mins: %s', foldername, g_hits)
                if g_hits == 0:
                    if gap < 100:
                        LOGGER.info('* Ready to move - gap is less than 100 (%s) and no new Media records created from folder %s in last 60 mins', gap, foldername)
                        move_folder = True
                    else:
                        LOGGER.info('* NOT ready to move - gap is greater than 100 (%s) - investigate gap between ingest jobs and CID Media records', gap)
                        move_folder = False
                else:
                    LOGGER.info('* NOT ready to move - gap is less than 100 (%s) - but %s Media records created in last hour - waiting a while...', gap, g_hits)
                    move_folder = False

        if move_folder == True:
            # Move ingested folder back into donor files folder
            src = os.path.join(INGEST_PATH, foldername)
            dst = os.path.join(OUTPUT_PATH, foldername)
            print(f'* Moving {src} to {dst}')
            LOGGER.info("Moving %s to %s", src, dst)

            try:
                # shutil.move(src, dst)
                LOGGER.info('** PAUSED: Moved %s to %s', src, dst)
            except Exception as err:
                LOGGER.warning("Failed to move %s to %s", src, dst)
                print(err)
                continue

            # Move next folder in sequence into autoingest
            next_folder_integer = int(foldername) + 1
            next_folder = str(next_folder_integer).zfill(3)
            print(next_folder)
            src = os.path.join(INPUT_PATH, next_folder)
            dst = os.path.join(INGEST_PATH, next_folder)
            print(f'* Moving {src} to {dst}')
            LOGGER.info("Moving next folder %s to %s", src, dst)

            try:
                # shutil.move(src, dst)
                LOGGER.info('** PAUSED: Moved %s to %s', src, dst)
            except Exception as err:
                LOGGER.warning("Failed to move %s to %s", src, dst)
                print(err)
                continue

    LOGGER.info('==== Move for ingest script completed at %s ===============', str(datetime.datetime.now())[0:19])


if __name__ == '__main__':
    main()
