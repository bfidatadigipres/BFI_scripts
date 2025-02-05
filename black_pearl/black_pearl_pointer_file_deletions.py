#!/usr/bin/python3

'''
Script for manual control of automated
deletion of BP objects as indicated
via saved search filtered by DigiOps.

1. Intercepts saved searches with deletion
   requests, adds pre-defined note to <notes> field
   signalling the files are approved for deletion.
2. Manually launches this script supplying saved
   search number as sys.argv[1], one at a time.
3. Automation of deletion begins:
   i/ Iterates all prirefs listed in saved search
   ii/ CID media record data retrieved including
       reference_number, input.date, MP4 path and
       notes.
   iii/ Collates all priref media record date to
        dictionary and outputs data to screen for
        confirmation to proceed 'y/n'.
   iv/ Iterates through new dictionary checking
       if deletion 'Confirmed for deletion' in notes.
       If yes, confirms deletion in log and screen.
   v/  Using the extracted reference_number
       data proceeds to launch Python SDK command
       to retrieve BP object 'VersionID' as variable
       then delete the object from the tape library.
   vi/ Also pulls out MP4 data and deletes associated
       MP4 access proxy video. Does not delete thumb
       or large image.
   vii/ Updates completion of deletion to CID media
       record and to screen/log.
4. Closes up comms and script exits.

NOTE: Accompanying 'undelete' script to be written
      for use should an incorrect priref be placed
      into deletions schedule.
      Updated for Adlib V3

Joanna White
2023
'''

import os
import sys
import time
import logging
import tenacity

# Private package
import bp_utils as bp
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils


# Global links / set up ds3 and adlib
MP4_ACCESS1 = os.environ['MP4_ACCESS_REDIRECT']
MP4_ACCESS2 = os.environ['MP4_ACCESS2']
LOGS = os.environ['LOG_PATH']
CID_API = os.environ['CID_API4']

# Logging config
LOGGER = logging.getLogger('bp_pointer_file_deletions')
HDLR = logging.FileHandler(os.path.join(LOGS, 'bp_pointer_file_deletions.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


@tenacity.retry(wait=tenacity.wait_fixed(15))
def get_prirefs(pointer):
    '''
    User pointer number and look up
    for list of prirefs in CID
    '''
    query = {'command': 'getpointerfile',
             'database': 'media',
             'number': f'{pointer}',
             'output': 'jsonv1'}

    try:
        result = adlib.get(CID_API, query)
    except Exception as exc:
        LOGGER.exception('get_prirefs(): Unable to get pointer file %s\n%s', pointer, exc)
        result = None

    if not result['adlibJSON']['recordList']['record'][0]['hitlist']:
        return None
    return result['adlibJSON']['recordList']['record'][0]['hitlist']


def get_dictionary(priref_list):
    '''
    Iterate list of prirefs and
    collate data
    '''

    data_dict = {}
    for priref in priref_list:
        data = get_media_record_data(priref)
        data_dict[priref] = data

    return data_dict


@tenacity.retry(wait=tenacity.wait_fixed(15))
def get_media_record_data(priref):
    '''
    Get CID media record details
    '''
    search = f'priref="{priref}"'
    fields = [
        'imagen.media.original_filename',
        'access_rendition.mp4',
        'reference_number',
        'input.date',
        'notes',
        'preservation_bucket'
    ]

    try:
        record = adlib.retrieve_record(CID_API, 'media', search, '1', fields)[1]
    except Exception as exc:
        LOGGER.exception('get_media_record_data(): Unable to access Media data for %s', priref)
        raise Exception from exc

    if not record:
        return None
    try:
        ref_num = adlib.retrieve_field_name(record[0], 'reference_number')[0]
    except (IndexError, KeyError, TypeError):
        ref_num = ''
    if ref_num is None:
        ref_num = ''
    try:
        access_mp4 = adlib.retrieve_field_name(record[0], 'access_rendition.mp4')[0]
    except (IndexError, KeyError, TypeError):
        access_mp4 = ''
    if access_mp4 is None:
        access_mp4 = ''
    try:
        input_date = adlib.retrieve_field_name(record[0], 'input.date')[0]
    except (IndexError, KeyError, TypeError):
        input_date = ''
    if input_date is None:
        input_date = ''
    try:
        approved = adlib.retrieve_field_name(record[0], 'notes')[0]
    except (IndexError, KeyError, TypeError):
        approved = ''
    if approved is None:
        approved = ''
    try:
        filename = adlib.retrieve_field_name(record[0], 'imagen.media.original_filename')[0]
    except (IndexError, KeyError, TypeError):
        filename = ''
    if filename is None:
        filename = ''
    try:
        bucket = adlib.retrieve_field_name(record[0], 'preservation_bucket')[0]
    except (IndexError, KeyError, TypeError):
        bucket = ''
    if bucket is None:
        bucket = ''

    return [ref_num, access_mp4, input_date, approved, filename, bucket]


def main():
    '''
    Load pointer file, obtain all prirefs
    and interate code to make BP deletions
    '''
    if not sys.argv[1]:
        LOGGER.warning("Exiting. No pointer file supplied at script launch.")
        sys.exit('No pointer file supplied at script launch. Please try launching again.')
    if not utils.check_control('black_pearl'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    LOGGER.info("----------- Black Pearl pointer file deletions script START ------------")
    priref_list = []
    pointer = sys.argv[1]
    LOGGER.info("Pointer file received: %s", pointer)

    # Retrieve list of prirefs
    priref_list = get_prirefs(pointer)
    deletion_dictionary = get_dictionary(priref_list)
    if not priref_list:
        LOGGER.info("No data retrieved from Pointer file: %s", pointer)
        sys.exit(f"No Priref data retrieved from Pointer file: {pointer}. Script exiting.")
    if not deletion_dictionary:
        LOGGER.info("Failed to retrieve data from Pointer file prirefs: %s", pointer)
        sys.exit(f"Failed to retrieve CID media record data from Pointer file prirefs: {pointer}. Script exiting.")

    # Format data and communicate
    print(f"\nHi there! Thanks for submitting saved search file number {pointer}.\n")
    print(f"There are *{len(priref_list)}* priref(s) to be processed:\n")
    for key, val in deletion_dictionary.items():
        print(f"Priref '{key}'. File reference number '{val[0]}'.")
        print(f"{val[4]}: Access MP4 '{val[1]}'. Input date '{val[2]}'. Approval status '{val[3]}'. Bucket location in BP: '{val[5]}'.")
        print("---------------------------------------------------------------------")

    time.sleep(5)
    cont = input("\nWould you like to proceed with deletion of these assets? (y/n) ")
    if cont.lower() != 'y':
        sys.exit("\nYou've not selected 'y' so the script will exit. See you next time!")

    print("\nConfirmation to proceed received, deletion of approved assets will now begin.\n")
    deleted = []
    for key, val in deletion_dictionary.items():
        priref = key
        ref_num = val[0]
        access_mp4 = val[1]
        input_date = val[2]
        approved = val[3]
        fname = val[4]
        bucket = val[5]
        print(f"Assessing {fname}: Priref {key}, Reference {ref_num}")
        LOGGER.info("Assessing %s: Priref %s. Filename %s", ref_num, priref, fname)

        confirmation = []
        confirmation.append(f'<notes>{approved}</notes>')

        if 'Confirmed for deletion' in str(approved) and len(ref_num) >= 7:
            print(f"Confirmed for deletion: {key}, {ref_num}, {approved}")
            LOGGER.info("Confirmed for deletion: %s - %s. Priref %s", ref_num, fname, priref)
            LOGGER.info("Fetching version_id using reference number")
            version_id = bp.get_version_id(ref_num)
            if not version_id:
                LOGGER.warning("Deletion of file %s not possible, unable to retreive version_id", ref_num)
                print(f"WARNING: Deletion impossible, version_id not found for file {ref_num}")
                confirmation.append("<notes>Black Pearl file was not deleted - version_id not found</notes>")
                succ = cid_media_append(priref, confirmation)
                if succ:
                    print("CID media record notes field updated")
                continue
            print(f"Version id retrieved from Black Pearl: {version_id}")

            success = bp.delete_black_pearl_object(ref_num, version_id, bucket)
            if not success:
                LOGGER.warning("Deletion of asset failed: %s. Priref %s. Version id %s", ref_num, priref, version_id)
                print(f"WARNING: Deletion of asset failed: {ref_num}.\n")
                confirmation.append("<notes>Black Pearl file was not deleted</notes>")
                succ = cid_media_append(priref, confirmation)
                if succ:
                    print("CID media record notes field updated")
                continue

            # Check for head object of deleted asset for confirmation
            delete_check = bp.etag_deletion_confirmation(ref_num, bucket)
            if delete_check == 'Deleted':
                print(f"** DELETED FROM BLACK PEARL: {ref_num} - from bucket {bucket}")
                LOGGER.info("** DELETED FROM BLACK PEARL: %s - from bucket %s", ref_num, bucket)
                deleted.append(f"{key} {ref_num}")
                confirmation.append("<notes>Black Pearl asset deleted</notes>")
                succ = cid_media_append(priref, confirmation)
                if succ:
                    print("CID media record notes field updated")

            else:
                print(f"** FILE NOT DELETED FROM BLACK PEARL. ETAG {delete_check}")
                LOGGER.warning("** FILE NOT DELETED FROM BLACK PEARL: %s", ref_num)
                confirmation.append("<notes>Black Pearl asset was not deleted</notes>")
                succ = cid_media_append(priref, confirmation)
                if succ:
                    print("CID media record notes field updated")
                continue

            # Make MP4 paths and delete
            if len(access_mp4) > 1:
                mp4_path1, mp4_path2 = get_mp4_paths(input_date, access_mp4)
                if os.path.exists(mp4_path1):
                    LOGGER.info("** DELETED: Associated MP4 found: %s", mp4_path1)
                    print(f"Associated MP4 found, deleting now: {mp4_path1}.\n")
                    os.remove(mp4_path1)
                elif os.path.exists(mp4_path2):
                    LOGGER.info("** DELETED: Associated MP4 found: %s", mp4_path2)
                    print(f"Associated MP4 found, deleting now: {mp4_path2}.\n")
                    os.remove(mp4_path2)
                else:
                    LOGGER.warning("No associated MP4 found for file: %s %s", fname, input_date)
                    print(f"Access MP4 file {access_mp4} not found in either paths.\n")
            else:
                print("No Access MP4 data retrieved from CID media record for this file")

        elif 'Confirmed for deletion' in str(approved) and len(ref_num) < 7:
            LOGGER.warning("Skipping deletion. Reference number incomplete: %s", ref_num)
            print(f"Skipping deletion: Reference number {ref_num} seems incomplete.\n")
            confirmation.append("<notes>Deletion skipped. Incomplete reference number.</notes>")
            succ = cid_media_append(priref, confirmation)
            if succ:
                print("CID media record notes field updated")
            continue
        else:
            LOGGER.warning("Skipping deletion. Approval absent from CID media record: %s", priref)
            print(f"Skipping deletion: Confirmation not supplied to Priref {key} for filename {ref_num}.\n")
            confirmation.append("<notes>Deletion skipped. Confirmation not present in notes field.</notes>")
            succ = cid_media_append(priref, confirmation)
            if succ:
                print("CID media record notes field updated")
            continue

    print(f"Completed deletion of *{len(deleted)}* assets:")
    if len(deleted) == 0:
        print("No assets for deletion list")
    for d in deleted:
        print(f"\t{d}")

    print("\nScript completed and exiting. See you next time!\n")
    LOGGER.info("----------- Black Pearl pointer file deletions script END --------------\n")


def get_mp4_paths(input_date, access_mp4):
    '''
    Create two possible MP4 paths for deletion
    of associated MP4 asset
    '''
    year, month = input_date.split('-')[:2]
    mp4_path1 = os.path.join(MP4_ACCESS1, f"{year}{month}/", access_mp4)
    mp4_path2 = os.path.join(MP4_ACCESS2, f"{year}{month}/", access_mp4)
    return mp4_path1, mp4_path2


def cid_media_append(priref, data):
    '''
    Receive data and priref and append to CID media record
    '''
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_mid = ''.join(data)
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_mid + payload_end
  
    record = adlib.post(CID_API, payload, 'media', 'updaterecord')
    if not record:
        LOGGER.warning("cid_media_append(): Post of data failed: %s - %s", priref, payload)
        return False
    LOGGER.info("cid_media_append(): Write of access_rendition data appear successful for Priref %s", priref)
    return True


if __name__ == '__main__':
    main()
