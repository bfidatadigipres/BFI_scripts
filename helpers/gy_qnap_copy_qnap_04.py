#!/usr/bin/env python3

'''
Relocates off-air recordings from Gaydon QNAP
to QNAP-04 as and when requested. Date to be
set manually before each run.

Copies CHANNELS to STORA/ path
Copies NEWS to STORA_backup/ path for
selection by TV curatorial teams

main():
1. Iterates list of CHANNELS / NEWS, creates
   fpath then generates list of folders
   contained within (programme folders).
2. Checks if correct date path for day before
   yesterday is in designated storage path,
   if not creates new path
3. Creates new programme folder path variable,
   strips training '/' for rsync command
4. Initiates copy from local storage to
   designated storage of programme folder and
   all contents. Deletes all files from local
   storage.
5. Uses multiprocessing Pool to run parallel
   rsync encodings at once -selected in var at top

2025
'''

import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from multiprocessing import Pool
import subprocess

sys.path.append(os.environ.get("CODE"))
import utils

# Global paths
MAX_PARALLEL = 3
QNAP = os.environ.get("GY_QNAP_01")
STORA = os.environ.get("STORA")
STORA_BACKUP = os.environ.get("STORA_BACKUP")
LOG_PATH = os.environ.get("LOG_PATH")
LOG = os.path.join(LOG_PATH, 'stora1_gy_qnap_copy_qnap_04.log')

# THIS DATE PATHS TO BE EDITED MANUALLY DEPENDING ON DATE NEEDED
TARGET_DATE = "2025/12/17"

# Setup logging
logging.basicConfig(filename=LOG, filemode='a', \
                    format='%(asctime)s\t%(levelname)s\t%(message)s', level=logging.INFO)

CHANNELS = [
    'bbconehd',
    'bbctwohd',
    'bbcthree',
    'bbcfourhd',
    'bbcnewshd',
    'cbbchd',
    'cbeebieshd',
    'channel4',
    'film4',
    'five',
    '5star',
    'itv1',
    'itv2',
    'itv3',
    'itv4',
    'e4',
    'more4'
]

NEWS = [
    'al_jazeera',
    'gb_news',
    'sky_news',
    'qvc',
    'skyarts',
    'skymixhd',
    'togethertv',
    'u_dave',
    'u_drama',
    'u_yesterday'
]



def main():
    '''
    Iterate list of CHANNEL folders for yesterday
    Copy to QNAP-04/<OPTIONAL>/YYYY/MM/DD path with delete of original
    '''

    if not utils.check_storage(STORAGE):
        logger.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not utils.check_storage(QNAP):
        logger.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    for chnl in CHANNELS:
        source = os.path.join(QNAP, "STORA", TARGET_DATE, chnl)
        destintation = os.path.join(STORA, TARGET_DATE, chnl)

        if not os.path.exists(source):
            logging.warning("SKIPPING: Fault with source path: %s", source)
            continue

        folders = [os.path.join(source, d) for d in os.listdir(source) if os.path.isdir(os.path.join(source, d))]

        logging.info("START MOVE_CONTENT.PY =============== %s", source)
        print(f"Moving to destination: {source}")

        task_args = [
            (folder.rstrip("/"), destination.rstrip())
            for folder in folders
        ]
        tic = time.perf_counter()
        with Pool(processes=MAX_PARALLEL) as p:
            p.starmap(rsync, task_args)
        tac = time.perf_counter()
        time_copy = (tac - tic) // 60
        logging.info("* Rsync copy for channel %s was %s minutes", chnl, time_copy)

    for chnl in NEWS:
        source = os.path.join(QNAP, "STORA", TARGET_DATE, chnl)
        destintation = os.path.join(STORA_BACKUP, TARGET_DATE, chnl)

        if not os.path.exists(source):
            logging.warning("SKIPPING: Fault with source path: %s", source)
            continue

        folders = [os.path.join(source, d) for d in os.listdir(source) if os.path.isdir(os.path.join(source, d))]

        logging.info("START MOVE_CONTENT.PY =============== %s", source)
        print(f"Moving folders to destination: {source}")

        task_args = [
            (folder.rstrip("/"), destination.rstrip())
            for folder in folders
        ]
        tic = time.perf_counter()
        with Pool(processes=MAX_PARALLEL) as p:
            p.starmap(rsync, task_args)
        tac = time.perf_counter()
        time_copy = (tac - tic) // 60
        logging.info("* Rsync copy for channel %s was %s minutes", chnl, time_copy)


    logging.info("END MOVE_CONTENT.PY ============================================")


def rsync(fpath1, fpath2):
    '''
    Move Folders using rsync
    With archive and additional checksum
    Output moves to logs and remove source
    files from STORA path
    '''
    logging.info("Targeting folder path: %s", fpath1)
    if not os.path.exists(qnap_dest):
        os.makedirs(fpath2, mode=0o777, exist_ok=True)
        logging.info("Creating new folder paths in QNAP-04: %s", fpath2)
    folder = os.path.split(fpath1)[-1]
    new_log = Path(os.path.join(fpath2, f"{folder}_move.log"))
    new_log.touch(exist_ok=True)

    rsync_cmd = [
        'rsync', '-arvvh',
        '--info=FLIST2,COPY2,PROGRESS2,NAME2,BACKUP2,STATS2',
        '--perms', '--chmod=a+rwx',
        '--no-owner', '--no-group', '--ignore-existing',
        fpath1, qnap_dest.rstrip("/"),
        f'--log-file={new_log}'
    ]

    try:
        logging.info("rsync(): Beginning rsync move")
        subprocess.call(rsync_cmd)
    except Exception as err:
        logging.error("rsync(): Move command failure! %s", err, exc_info=True)


if __name__ == "__main__":
    main()
