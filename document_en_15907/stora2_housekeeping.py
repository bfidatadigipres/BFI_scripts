#!/usr/bin/env python3

'''
Script to handle clean up of the emptied folders in STORAGE_PATH/*year*
*** Must run from stora2_housekeeping_start.sh which generates current file list ***
1. Extract date range for one week, ending day before yesterday and only target these paths
2. Examine each folder for presence of file 'stream.mpeg2.ts', where found skipped
3. Where not found the folder assumed finished with and .ts file moved to QNAP storage
4. The folder's contents are deleted
5. Checks for recording.log, restart or schedule files and deletes
6. Clean up of folders at day, month (if last day of month) and year level (if last day of year).

Joanna White
Python 3.7 +
2023
'''

# Python packages
import os
import logging
import datetime
import itertools

# Global paths
STORAGE_PATH = os.environ['STORAGE_PATH']
TEXT_PATH = os.path.join(os.environ['CODE_BFI'], "document_en_15907/stora2_dump_text.txt")
LOG_PATH = os.environ['LOG_PATH']

# Setup logging
logger = logging.getLogger('stora2_housekeeping')
hdlr = logging.FileHandler(os.path.join(LOG_PATH, 'stora2_housekeeping.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# Date variables for use in API calls (new year safe)
TODAY_DATE = datetime.date.today()
TODAY = str(TODAY_DATE)
YESTERDAY_DATE = TODAY_DATE - datetime.timedelta(days=4)
YESTERDAY = YESTERDAY_DATE.strftime('%Y-%m-%d')
MONTH = f"{YESTERDAY[0:4]}/{YESTERDAY[5:7]}"
STORA_PATH_MONTH = os.path.join(STORAGE_PATH, MONTH)
STORA_PATH_YEAR = os.path.join(STORAGE_PATH, MONTH[0:4])
# Alternatives if needed
# STORA_PATH_MONTH = os.path.join(STORAGE_PATH, '12')
# STORA_PATH_YEAR = os.path.join(STORAGE_PATH, '2023')


def clear_folders(path):
    '''
    Remove root folders that contain no directories or files
    '''
    for root, dirs, files in os.walk(path):
        if not (files or dirs):
            print(f"*** Folder empty {root}: REMOVE ***")
            logger.info("*** FOLDER IS EMPTY: %s -- DELETING FOLDER ***", root)
            os.rmdir(root)
        else:
            logger.info("SKIPPING FOLDER %s -- THIS FOLDER IS NOT EMPTY", root)
            print(f"FOLDER {root} NOT EMPTY - this will not be deleted")


def clear_limited_folders(path):
    '''
    Remove empty folders at one depth only
    protecting new folders ahead of recordings
    '''
    folders = [ x for x in os.listdir(path) if os.path.isdir(os.path.join(path, x)) ]
    for folder in folders:
        fpath = os.path.join(path, folder)
        if len(os.listdir(fpath)) == 0:
            print(f"*** Folder empty {folder}: REMOVE ***")
            logger.info("*** FOLDER IS EMPTY: %s -- DELETING FOLDER ***", fpath)
            os.rmdir(fpath)
        else:
            logger.info("SKIPPING FOLDER %s -- THIS FOLDER IS NOT EMPTY", fpath)
            print(f"FOLDER {folder} NOT EMPTY - this will not be deleted")


def date_gen(date_str):
    '''
    Attributed to Ayman Hourieh, Stackoverflow question 993358
    Python 3.7+ only for this function - fromisoformat()
    '''
    from_date = datetime.date.fromisoformat(date_str)
    while True:
        yield from_date
        from_date = from_date - datetime.timedelta(days=1)


def main():
    '''
    Build date range (120 days prior to day before yesterday)
    Only move/clean up folders in target date range, protecting
    empty folders created ahead of today for future recordings
    '''

    logger.info("=========== stora2_housekeeping.py START ===========")
    period = []
    date_range = []
    period = itertools.islice(date_gen(YESTERDAY), 120)
    for date in period:
        date_range.append(date.strftime('%Y/%m/%d'))

    with open(TEXT_PATH, 'r') as path:
        paths = path.readlines()
        for line in paths:
            line = line.rstrip('\n')
            if any(dt in line for dt in date_range):
                logger.info("Folder in date range to process: %s", line)

                # Skip immediately if stream found
                files = os.listdir(line)
                if 'stream.mpeg2.ts' in files:
                    print(f"*** SKIPPING {line} as Stream.mpeg2.ts file here ***")
                    logger.warning("SKIPPING: stream.mpeg2.ts found %s", line)
                    continue
                elif len(files) > 0:
                    print(f"DELETING folder contents - Stream.mpeg2.ts NOT found: {line}")
                    logger.info("DELETING FOLDER CONTENTS: No STREAM found %s", line)
                    for file in files:
                        fpath = os.path.join(line, file)
                        try:
                            os.remove(fpath)
                            print(f"Deleted: {fpath}")
                        except OSError as error:
                            print(f"Unable to delete directory content {fpath}")
                            logger.warning("Unable to delete directory content: %s\n %s", fpath, error)
                            continue

                # New block to move top level recording.log etc only if move above completes
                pth_split_old = os.path.split(line)[0]
                top_files = [x for x in os.listdir(pth_split_old) if os.path.isfile(os.path.join(pth_split_old, x))]
                for fname in top_files:
                    if fname.startswith('recording'):
                        logger.info("** Deleting recording.log: %s", os.path.join(pth_split_old, fname))
                        os.remove(os.path.join(pth_split_old, fname))
                    elif fname.startswith('restart'):
                        logger.info("** Deleting restart texts: %s", os.path.join(pth_split_old, fname))
                        os.remove(os.path.join(pth_split_old, fname))
                    elif fname.startswith('epgrecording'):
                        logger.info("** Deleting epg restart texts: %s", os.path.join(pth_split_old, fname))
                        os.remove(os.path.join(pth_split_old, fname))
                    elif fname.startswith('schedule'):
                        logger.info("** Deleting schedule JSON: %s", os.path.join(pth_split_old, fname))
                        os.remove(os.path.join(pth_split_old, fname))

            else:
                logger.info("SKIPPING OUT OF RANGE FOLDER: %s", line)
                continue

    # Clear channel/programme folders in date range
    for date in date_range:
        clear_path = os.path.join(STORAGE_PATH, date)
        clear_folders(clear_path)

    # Clear month/year level folder, only if empty
    clear_limited_folders(STORA_PATH_MONTH)
    clear_limited_folders(STORA_PATH_YEAR)

    logger.info("=========== stora2_housekeeping.py ENDS ============")


if __name__ == '__main__':
    main()
