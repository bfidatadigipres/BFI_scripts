# Move folders of donor file tiffs into autoingest scope, sequentially
# And move ingested folders out, after checking status

# Python imports
import os
import re
import sys
import shutil
import datetime
from shutil import copyfile

# Local application imports
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

CID_API = os.environ['CID_API4']
MOVE_FOLDER = False
INPUT_PATH = os.path.join(os.environ['IS_MEDIA'], 'donor_files/renamed_ready_for_ingest/')
INGEST_PATH = os.path.join(os.environ['AUTOINGEST_IS_ING'], 'ingest/proxy/image/')
OUTPUT_PATH = os.path.join(os.environ['IS_MEDIA'], 'donor_files/partly_ingested/')
GLOBAL_LOG = os.path.join(os.environ['LOG_PATH'], 'autoingest/global.log')

NOW = (str(datetime.datetime.now()))[0:19]
NOW_MINUS_30 = str(datetime.datetime.now() - datetime.timedelta(minutes=30))
NOW_MINUS_60 = str(datetime.datetime.now() - datetime.timedelta(minutes=60))
QUERY_30 = now_minus_30[0:16]
QUERY_60 = now_minus_60[0:16]

print '==== Move4ingest script running at {} ==============='.format(now)

for root, dirs, files in os.walk(ingest_path):
    for foldername in dirs:
        folder_search = '_{}of'.format(foldername)
        print '* Current folder is {} - searching for {}'.format(foldername, folder_search)

        # Check number of successful ingest jobs from folder
        file = open(GLOBAL_LOG, 'r')
        for line in file:
            global ingest_jobs
            ingest_jobs = sum([1 for line in file if folder_search in line and 'successfully queued for ingest' in line])
            print '* Number of successful ingest jobs from folder {}: {}'.format(foldername, ingest_jobs)

        # Check number of successful CID media records created from folder
        q = '(object.object_number=CA* and imagen.media.original_filename=*{}*)'.format(folder_search)
        d = {'database': 'media',
             'search': q,
             'limit': '-1'}

        result = cid.get(d)
        # If zero CID records created for folder, exit and wait until next time
        if int(result.hits) == 0:
            print '* No CID records created for folder, waiting until next time...'
            continue

        global gap
        gap = int(ingest_jobs) - int(result.hits)

        print '* Number of successful CID Media records from folder {}: {}'.format(foldername, result.hits)
        print '* Gap = {}'.format(gap)

        if gap == 0:
            # Check whether in-scope CID Media records created in last 30 mins
            q = '(object.object_number=CA* and imagen.media.original_filename=*{}*) and (creation>"{}")'.format(folder_search, query_30)
            d = {'database': 'media',
                 'search': q,
                 'limit': '-1'}

            result = cid.get(d)
            print '* Number of CID Media records created from folder {} in last 30 mins: {}'.format(foldername, result.hits)

            if result.hits == 0:
                print '* Ready to move - gap is zero and no Media records created for folder {} in last 30 mins'.format(foldername)
                move_folder = True
            else:
                print '* NOT ready to move - gap is zero, but {} Media records created for folder {} in last 30 mins - waiting a while...'.format(result.hits, foldername)
                move_folder = False
        else:
            # Check whether in-scope CID Media records created in last 60 mins
            q = '(object.object_number=CA* and imagen.media.original_filename=*{}*) and (creation>"{}")'.format(folder_search, query_60)
            d = {'database': 'media',
                 'search': q,
                 'limit': '-1'}

            result = cid.get(d)
            print '* Number of CID Media records created from folder {} in last 60 mins: {}'.format(foldername, result.hits)

            if result.hits == 0:
                if gap < 100:
                   print '* Ready to move - gap is less than 100 ({}) and no new Media records created from folder {} in last 60 mins'.format(gap, foldername)
                   move_folder = True

                   #print '* Suspending autoingest briefly, using autoingest_control.json'
                   #copyfile(control_off, control)
                   #src = '{}{}'.format(ingest_path, foldername)
                   #dst = '{}{}'.format(output_path, foldername)
                   #print '* Moving {} to {}'.format(src, dst)

                   #try:
                   #    shutil.move(src, dst)
                   #    print 'Moved {} to {}'.format(src, dst)
                   #except Exception as e:
                   #    print(e)
                   #    continue

                else:
                   print '* NOT ready to move - gap is greater than 100 ({}) - investigate gap between ingest jobs and CID Media records'.format(gap)
                   move_folder = False

            else:
                print '* NOT ready to move - gap is less than 100 ({}) - but {} Media records created in last hour - waiting a while...'.format(gap, result.hits)
                move_folder = False

    if move_folder == True:
        # Move ingested folder back into donor files folder 
        src = '{}{}'.format(ingest_path, foldername)
        dst = '{}{}'.format(output_path, foldername)
        print '* Moving {} to {}'.format(src, dst)

        try:
            shutil.move(src, dst)
            print '** Moved {} to {}'.format(src, dst)
        except Exception as e:
            print(e)
            continue

        # Move next folder in sequence into autoingest
        next_folder_integer = int(foldername) + 1
        if next_folder_integer < 10:
            next_folder = '00{}'.format(str(next_folder_integer))
        if next_folder_integer > 9 and next_folder_integer < 100:
            next_folder = '0{}'.format(str(next_folder_integer))
        if next_folder_integer > 99:
            next_folder = '{}'.format(str(next_folder_integer))
        src = '{}{}'.format(input_path, next_folder)
        dst = '{}{}'.format(ingest_path, next_folder)
        print '* Moving {} to {}'.format(src, dst)

        try:
            shutil.move(src, dst)
            print '** Moved {} to {}'.format(src, dst)
        except Exception as e:
            print(e)
            continue

now = (str(datetime.datetime.now()))[0:19]
print '==== Move4ingest script completed at {} ==============='.format(now)
