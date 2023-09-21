#!/usr/bin/env python3

'''
 Move multi-item whole-tape digitisations if
 the carried content has been documented and ingested.
 Moves them into deletion folder, separate shell script deletes them.

 Note: this script requires the BlackPearl creds loaded as
       as environment variables and the version of the
       SDK as installing /home/appdeploy/code/autoingest/v/

       From the current directory:
           source ../autoingest/creds.rc
           source ../autoingest/v/bin/activate
           python backup.py

Refactored for Python3
Joanna White
2023
'''

# Public packages
import os
import sys
import json
import shutil
import logging
from ds3 import ds3

# Private packages
sys.path.append(os.environ['CODE'])
import adlib
import models

# Global variables
LOG_PATH = os.environ.get('LOG_PATH')
LOG = os.path.join(LOG_PATH, 'delete_post_split_qnap01.log')
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)
CLIENT = ds3.createClientFromEnv()

TARGETS = [f"{os.environ['QNAP_VID']}/processing/"]

# Setup logging, overwrite each time
logger = logging.getLogger('delete_post_split_qnap01')
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['black_pearl'] or not j['split_control_delete']:
            logger.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def cid_check():
    '''
    Tests if CID active before all other operations commence
    '''
    try:
        logger.info('* Initialising CID session... Script will exit if CID off line')
        CUR = adlib.Cursor(CID)
        logger.info("* CID online, script will proceed %s", CUR)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        logger.critical('Cannot establish CID session, exiting script')
        sys.exit()


def get_object_list(fname):
    '''
    Build a DS3 object list for some SDK queries
    '''
    file_list = [fname]
    return [ds3.Ds3GetObject(name=x) for x in file_list]


def bp_physical_placement(fname, bucket):
    '''
    Retrieve the physical placement with object_list
    '''
    object_list = get_object_list(fname)
    query = ds3.GetPhysicalPlacementForObjectsSpectraS3Request(bucket, object_list)
    result = CLIENT.get_physical_placement_for_objects_spectra_s3(query)
    data = result.result

    if not data['TapeList']:
        return False
    try:
        persisted = data['TapeList'][0]['AssignedToStorageDomain']
    except (IndexError, KeyError):
        return False

    if 'true' in str(persisted):
        return True


def main():
    '''
    Iterate media_targets looking for files
    to process.
    '''
    for media_target in TARGETS:
#        check_control()
        cid_check()

        # Path to source media
        root = os.path.join(media_target, 'source')
        logger.info('%s\t**** Processing files in \t%s', root, root)

        # List video files in recursive sub-directories
        files = []
        for directory, _, filenames in os.walk(root):
            for filename in [f for f in filenames if f.endswith(('.mov', '.mxf', 'mkv', '.MOV', '.MKV', '.MXF'))]:
                files.append(os.path.join(directory, filename))

        # Track tapes processed total
        logger.info(files)

        # Process digitised tape files sequentially
        for filepath in files:
            f = os.path.split(filepath)[1]
            print(f'Current file: {filepath}\t{f}')

            # Expect can_ID or package_number filenames
            id_ = f.split('.')[0]

            # Model carrier
            # Label
            try:
                label = models.PhysicalIdentifier(id_)
                label_type = label.type
            except Exception as err:
                message = f'{filepath}\tUnable to determine label type\t{err}'
                print(message)
                logger.warning('%s\tUnable to determine label type', filepath)
                continue

            # Carrier
            try:
                d = {label_type: id_}
                t = models.Carrier(**d)
            except Exception as err:
                message = f'{filepath}\tUnable to model carrier\t{err}'
                print(message)
                logger.warning('%s\tUnable to model carrier\t%s', filepath, str(err))
                continue

            # Parse items carried, sort in logical order
            try:
                items = t.items
            except Exception:
                continue

            # Track BlackPearl-preserved objects
            preserved_objects = {}

            # Single- or multi-item
            whole = t.partwhole[1]
            if whole == 1:
                total_objects_expected = len(items)
            else:
                total_objects_expected = whole

            # Process each item on tape
            for item in items:
                object_number = item['object_number'][0]

                # Check expected number of media records have been created for correct grouping
                if '/qnap_h22/' in filepath or '/qnap_10/' in filepath:
                    grouping = '398385'
                else:
                    grouping = '397987'

                result = get_results(filepath, grouping, object_number, 'datadigipres')
                if not result:
                    result = get_results(filepath, grouping, object_number, 'collectionssystems')
                    if not result:
                        logger.warning('%s\tNo CID record found for object_number %s and grouping %s', object_number, grouping)
                        continue

                # Check that each media record umid has been preserved to tape by BlackPearl
                for r in result.records:
                    bp_umid = r['reference_number'][0]
                    try:
                        bucket = r['preservation_bucket'][0]
                    except (IndexError, TypeError, KeyError):
                        bucket = 'imagen'
                    original_filename = r['imagen.media.original_filename'][0]
                    print(f'* CID Media record has reference number {bp_umid} and original filename {original_filename}')
                    logger.info('%s\tCID Media record has reference number %s and original filename %s', filepath, bp_umid, original_filename)

                    # Check BlackPearl physical placement
                    if len(bucket) < 3:
                        bucket = 'imagen'
                    placement = bp_physical_placement(bp_umid, bucket)
                    if placement:
                        logger.info('%s\tPersisted\t%s\t%s\tBucket: %s', filepath, object_number, bp_umid, bucket)
                        print(f'Persisted: {f}\t{object_number}\t{bp_umid}\t{bucket}')
                        preserved_objects[original_filename] = bp_umid
                        print(f'* Preserved objects: {preserved_objects[original_filename]}')
                        print(f'* Len(preserved_objects) = {len(preserved_objects)}')
                        print(f'* Total objects expected = {total_objects_expected}')

            deleteable = len(preserved_objects) >= total_objects_expected
            print(f'{f}\tDeletable={deleteable}\t{len(preserved_objects)}/{total_objects_expected}')

            # Set move destination
            dst = os.path.join(media_target, f'delete/{f}')

            if deleteable and total_objects_expected > 0:
                # Delete single-item tapes
                if total_objects_expected == 1:
                    print(f'* Moving single item tape file to delete folder: {filepath}')
                    try:
#                        shutil.move(filepath, dst)
                        logger.info('%s\tMoved single item tape file to delete folder', filepath)
                    except Exception as err:
                        logger.warning('%s\tUnable to move file to delete folder', filepath)
                        print(f'* Unable to move file to delete folder: {filepath}')
                        print(err)
                        raise

                # Delete multi-item tapes:
                elif total_objects_expected >= 2:
                    print(f'Moving multi-item tape file to delete folder: {filepath}')
                    try:
#                        shutil.move(filepath, dst)
                        logger.info('%s\tMoved multi-item tape file to delete folder', filepath)
                    except Exception as err:
                        print(f'* Unable to move file to delete folder: {filepath}\t{err}')
                        logger.warning('%s\tUnable to move file to delete folder', filepath)
                        raise

            else:
                print(f'* Ignoring because not all Items are persisted: {filepath}, {len(preserved_objects)} persisted, {total_objects_expected} expected')
                logger.warning('%s\tIgnored because not all Items are persisted: %s persisted, %s expected', filepath,len(preserved_objects), total_objects_expected)


def get_results(filepath, grouping, object_number, input_name):
    '''
    Checks for cross-over period between 'datadigipres'
    and 'collectionssystems' in CID media record
    '''
    query = f'''(object.object_number->
                    ((grouping.lref="{grouping}")
                        and input.name="{input_name}"
                        and (source_item->
                         (object_number="{object_number}"))))'''

    q = {'database': 'media',
         'search': query,
         'fields': 'reference_number,imagen.media.original_filename, preservation_bucket',
         'output': 'json',
         'limit': '0'}

    try:
        result = CID.get(q)
        print(f'* Querying for ingest status of CID Item record {object_number}')
        logger.info('%s\tCID Item record found, with object number %s', filepath, object_number)
        return result
    except Exception as err:
        print(f'* CID query failed to obtain result using input.name {input_name}')
        print(err)
        logger.warning('%s\tCID query failed to obtain result with input.name %s', filepath, input_name)
        return None


if __name__ == '__main__':
    main()
