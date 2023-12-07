#!/usr/bin/env python3

'''
wav_folder_record_creation_rename.py

Script functions:

1. Pick up folders (named after their source Item record)
2. Check in CID that the source item record has sound_item = 'Sound'
3. Check all WAVs in folder - mediaconch policy check against POLICY_WAV
   - If any fail then pause the process and exit with warning log
4. New item record is created and linked to source item's manifestation
   and to source_item. Extract ob_number and convert to new filename
   with part_whole 01of01 (or carried over from supplied folder).
5. Add names of all files into the quality comments field, and populate quality date field.
6. Folder renamed and moved to automation_wav/for_tar_wrap for TAR wrapping
7. All actions logged human readable for Mike, and placed in audio ops
   folder, at top level.

NOTES: No changes implemented yet.
       Waiting on questions from Steph.

Joanna White
2023
'''

# Public packages
import os
import re
import sys
import json
import time
import shutil
import logging
import datetime
import subprocess

# Private packages
sys.path.append(os.environ['CODE'])
import adlib

# Global paths/vars
AUTO_WAV_PATH = os.environ['AUTOMATION_WAV']
WAV_RENAME_PATH = os.path.join(AUTO_WAV_PATH, 'record_create_folder_rename/')
FOR_TAR_WRAP = os.path.join(AUTO_WAV_PATH, 'for_tar_wrap/')
WAV_POLICY = os.environ['POLICY_WAV']
FAILED_PATH = os.path.join(AUTO_RENAME_PATH, 'failed_rename/')
LOCAL_LOG = os.path.join(AUTO_WAV_PATH, 'record_create_folder_rename.log')
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API3']
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]

# Setup logging
LOGGER = logging.getLogger('tar_folder_record_create_rename')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'wav_folder_record_create_rename.log'))
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
        if not j['pause_scripts']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def cid_check():
    '''
    Tests if CID active before all other operations commence
    '''
    try:
        LOGGER.info('* Initialising CID session... Script will exit if CID off line')
        CUR = adlib.Cursor(CID)
        LOGGER.info("* CID online, script will proceed")
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical('Cannot establish CID session, exiting script')
        sys.exit()


def remove_whitespace(title):
    '''
    Remove excess whitespace from badly formed names
    '''
    new_title = re.sub(' +', ' ', title)
    print(new_title)
    return new_title


def conformance_check(filepath):
    '''
    Checks mediaconch policy against WAV files
    '''
    mediaconch_cmd = [
        'mediaconch', '--force',
        '-p', WAV_POLICY,
        filepath
    ]

    result = subprocess.check_output(mediaconch_cmd)
    result = str(result)

    if 'N/A!' in result or 'pass!' not in result:
        return f"FAIL! '{filepath}'\n{result}"
    else:
        return "PASS!"


def make_object_number(folder):
    '''
    Convert file or directory to CID object_number
    '''
    filename_split = folder.split('_')
    object_num = '-'.join(filename_split[:-1])

    return object_num, filename_split[-1]


def cid_query(database, search, object_number):
    '''
    Format CID query for cid_data_retrieval()
    '''
    query = {'database': database,
             'search': search,
             'limit': '0',
             'output': 'json',
             'fields': 'priref, title, title.article, title.language, object_number, source_item, derived_item, sound_item'}
    try:
        query_result = CID.get(query)
    except Exception:
        print(f"cid_query(): Unable to retrieve data for {object_number}")
        LOGGER.exception("cid_query(): Unable to retrieve data for %s", object_number)
        query_result = None

    print(query_result.records)

    try:
        priref = query_result.records[0]['priref'][0]
        print(priref)
    except (KeyError, IndexError) as err:
        priref = ""
        print(err)
    try:
        title = query_result.records[0]['Title'][0]['title'][0]
    except (KeyError, IndexError) as err:
        title = ""
        print(err)
    try:
        title_article = query_result.records[0]['Title'][0]['title.article'][0]
    except (KeyError, IndexError) as err:
        print(err)
        title_article = ""
    try:
        title_language = query_result.records[0]['Title'][0]['title.language'][0]
    except (KeyError, IndexError) as err:
        title_language = ""
        print(err)
    try:
        ob_num = query_result.records[0]['object_number'][0]
    except (KeyError, IndexError) as err:
        ob_num = ""
        print(err)
    try:
        source_item = query_result.records[0]['Source_item'][0]['source_item'][0]
    except (KeyError, IndexError) as err:
        source_item = ""
        print(err)
    try:
        derived_item = query_result.records[0]['Derived_item'][0]['derived_item'][0]
    except (KeyError, IndexError) as err:
        derived_item = ""
        print(err)
    try:
        sound_item = query_result.records[0]['sound_item'][0]['value'][1]
    except (KeyError, IndexError) as err:
        sound_item = ""
        print(err)

    new_title = remove_whitespace(title)

    return (priref, title, title_article, ob_num, derived_item, source_item, title_language, sound_item)


def cid_data_retrieval(ob_num):
    '''
    Retrieve source data from CID
    to link to for new Item record
    '''
    cid_data = []
    search = f'(object_number="{ob_num}")'
    priref = cid_query('Items', search, ob_num)[0]

    if priref:
        print("Priref retrieved, checking for title.language...")
        parent_search = f'(parts_reference->priref="{priref}")'
    else:
        parent_search = f'(parts_reference->object_number="{ob_num}")'

    LOGGER.info("Retrieving CID parent manifestation data using query: %s", parent_search)
    parent_data = cid_query('Manifestations', parent_search, ob_num)

    try:
        cid_data.extend(parent_data)
    except Exception:
        cid_data.extend('', '', '', '', '', '', '', '')
        LOGGER.exception("The parent data retrieval was not successful:")

    if priref:
        source_search = f'(priref="{priref}")'
    else:
        source_search = f'(object_number="{ob_num}")'
    LOGGER.info("Retrieving CID source item data using query: %s", source_search)
    source_data = cid_query('Items', source_search, ob_num)
    try:
        cid_data.extend(source_data)
    except Exception:
        cid_data.extend('', '', '', '', '', '', '', '')

    return cid_data


def main():
    '''
    Looks through WAV_ARCHIVE_PATH for files ending .wav/.WAV
    If part whole is greater than 01, extract all parts to list and check all present
    Find 01of* and check against policy, before CID actions to generate Item record
    extract object number and make new filename. Apply filename to all parts (mediaconch check)
    and move to autoingest path in audio isilon share.
    '''

    LOGGER.info("========== wav folder record creation rename START ============")
    check_control()
    cid_check()

    directory_list = {}
    for root, dirs, _ in os.walk(WAV_RENAME_PATH):
        for directory in dirs:
            if directory == 'failed_rename':
                continue
            dirpath = os.path.join(root, directory)
            dirlist = os.listdir(dirpath)
            wav_files = [ x for x in dirlist if x.endswith(('.wav', '.WAV', '.mp3', '.MP3')) ]
            if len(wav_files) != len(dirlist):
                LOGGER.info("Non audio files found in directory %s", directory)
            if len(wav_files) > 0:
                directory_list[dirpath] = wav_files.sort()
            else:
                LOGGER.info("Skipping: No audio files in folder %s", directory)
                continue
    '''
    directory_list = {
        '/mnt/isilon/audio_ops/folder/C_625940_01of01': ['File One1.wav', 'File Two2.wav'],
        '/mnt/isilon/audio_ops/folder/C_626169_01of01': ['Sound One1.wav', 'Sound Two2.wav']
    }
    '''
    if not directory_list:
        LOGGER.info("No items found this time. Script exiting")
        sys.exit()

    for key, value in directory_list.items():
        LOGGER.info("======== Folder path being processed %s ========", key)
        LOGGER.info("Contents of folder being processed:")
        LOGGER.info("%s", ', '.join(value))
        fpath, folder = os.path.split(key)

        # Mediaconch policy assessment
        quality_comments = []
        mediaconch_assess = []
        for file in value:
            if file.endswith(('.wav', '.WAV')):
                filepath = os.path.join(key, file)
                success = conformance_check(filepath)
                LOGGER.info("Conformance check results for %s: %s", file, success)
                quality_comments.append(file)
                if 'PASS!' not in success:
                    mediaconch_assess.append('FAIL')
                    local_log(f"File failed Mediaconch policy: {file}\n{success}")

        if 'FAIL' in mediaconch_assess:
            LOGGER.warning("Skipping: One or more WAV file in folder %s has failed the policy", folder)
            continue
        source_ob_num, part_whole = make_object_number(folder)
        if len(source_ob_num) == 0:
            local_log(f"Skipping: Unable to retrieve Source object number from folder: {folder}")
            continue

        print(source_ob_num, part_whole)
        local_log(f"Source object number retrieved from folder {folder}: {source_ob_num}")
        cid_data = cid_data_retrieval(source_ob_num)
        print(cid_data)

        # Check source Item source_item field
        if cid_data[15] != 'Sound':
            LOGGER.info("Skipping: Supplied CID item record source does not have 'sound_item': 'Sound'")
            local_log(f"WARNING: Could not find 'Sound' in sound_item record for source item {source_ob_num}")
            continue
        # Check source Item priref
        if len(cid_data[8]) == 0:
            LOGGER.info("Skipping: No priref retrieved for folder %s", folder)
            local_log(f"WARNING: Could not find priref for source item {source_ob_num}")
            continue
        # Compile list of enclosed files
        qual_comm = f"TAR file contains: {'; '.join(quality_comments)}."

        # Make CID record with Title of source item
        if len(cid_data[9]) > 0:
            LOGGER.info("Making new CID item record for WAV using parent title %s", cid_data[9])
            local_log(f"Creating new CID item record using parent title: {cid_data[9]}")
            wav_ob_num, wav_priref = create_wav_record(cid_data[0], cid_data[9], cid_data[10], cid_data[14], cid_data[8], qual_comm)
        # Else use Title of manifestation parent
        elif len(cid_data[1]) > 0:
            LOGGER.info("Making new CID item record for WAV using grandparent title %s", cid_data[1])
            local_log(f"Creating new CID item record using grandparent title: {cid_data[1]}")
            wav_ob_num, wav_priref = create_wav_record(cid_data[0], cid_data[1], cid_data[2], cid_data[6], cid_data[8], qual_comm)
        else:
            local_log(f"Unable to retrieve CID data for: {source_ob_num}. Moving file to failed_rename folder.")
            LOGGER.warning("Title information absent from CID data retrieval, skipping record creation")

            # Move file to failed_rename/ folder
            fail_path = os.path.join(FAILED_PATH, folder)
            print(f"Moving {key} to {fail_path}")
            shutil.move(key, fail_path)
            continue

        # Check wav_ob_num present following Item creation
        if wav_ob_num:
            local_log(f"Creation of new WAV item record successful: {wav_ob_num}")
            LOGGER.info("Creation of new WAV item record successful: %s", wav_ob_num)
        else:
            LOGGER.warning("No WAV object number obtained - failed record creation")
            local_log(f"FAILED: Creation of new WAV Item record failed for {file}. Leaving to retry")
            continue

        # Rename file and move
        success = rename(folder, wav_ob_num)
        if success is not None:
            local_log(f"File {folder} renamed {success[1]}")
            LOGGER.info("Folder %s renamed successfully to %s", folder, success[1])

            # Move to automation_wav/for_tar_wrap/
            local_log(f"Moving to for_tar_wrap/ - folder {success[0]}")
            move_success = ingest_move(success[0], success[1])
            if move_success:
                local_log(f"{success[1]} moved to {move_success}\n")
                LOGGER.info("Moved to automation_dpx/for_tar_wrap/ path: %s.\n", move_success)
            else:
                LOGGER.warning("FAILED MOVE: %s did not move to autoingest.\n", success[1])
                local_log(f"Failed to move {success[1]} to automation_dpx/for_tar_wrap/ - please complete this manually\n")
        else:
            local_log(f"FAIL: Folder {folder} renaming failed, moving to failed_rename folder")
            local_log(f"FAIL: Please rename '{folder}' manually to '{success[1]}' and move to for_tar_wrap.\n")
            LOGGER.warning("FAILED ATTEMPT AT RENAMING: Updated log for manual renaming. Moving to failed_rename folder.\n")
            # Move folder to failed_rename/ folder
            fail_path = os.path.join(FAILED_PATH, folder)
            print(f"Moving {filepath} to {fail_path}")
            shutil.move(fpath, fail_path)
            continue

    LOGGER.info("================ END WAV folder record creation rename END =================")


def create_wav_record(gp_priref, title, title_article, title_language, source_priref, qual_comm):
    '''
    Item record creation for WAV file
    TO DO: Needs reviewing with Lucy
    '''
    print(gp_priref, title, title_article)
    record_defaults = []
    item_defaults = []

    record_defaults = ([{'input.name': 'datadigipres'},
                        {'input.date': str(datetime.datetime.now())[:10]},
                        {'input.time': str(datetime.datetime.now())[11:19]},
                        {'input.notes': 'Created by automation to aid ingest of legacy projects and workflows.'}, #
                        {'record_access.owner': 'Acquisitions Full'}]) #

    item_defaults = ([{'record_type': 'ITEM'}, #
                      {'code_type': 'Uncompressed'}, # Unknown
                      {'bit_depth.type': 'AUDIO'}, # Unknown
                      {'bit_depth': '24'}, # Unknown
                      {'sample_rate': '96kHz'}, # Unknown
                      {'grouping.lref': ''}, # Unknown
                      {'acquisition.method': 'Created by'}, #
                      {'acquisition.source': 'BFI National Archive'}, #
                      {'acquisition.source.lref': '999570701'}, #
                      {'acquisition.reason': 'Digital deliverable from BFI'}, # Unsure
                      {'copy_status': 'M'}, #
                      {'copy_usage': 'Restricted access to preserved digital file'}, #
                      {'copy_usage.lref': '131560'}, #
                      {'creator': 'BFI National Archive'}, #
                      {'creator.lref': '999570701'}, #
                      {'creator.role.lref': '392405'}, #
                      {'description.date': str(datetime.datetime.now())[:10]},
                      {'file_type': 'WAV'}, #
                      {'item_type': 'DIGITAL'}, #
                      {'quality_comments': qual_comm}, #
                      {'quality_comments.date': str(datetime.datetime.now())[:10]}, #
                      {'source_item.lref': source_priref}, #
                      {'source_item.content': 'SOUND'}]) #

    wav_ob_num = ""
    item_values = []
    item_values.extend(record_defaults)
    item_values.extend(item_defaults)

    # Appended data
    item_values.append({'part_of_reference.lref': gp_priref}) #
    item_values.append({'title': title}) #
    if len(title_article) > 0:
        item_values.append({'title.article': title_article}) #
    if len(title_language) > 0:
        item_values.append({'title.language': title_language}) #
    else:
        item_values.append({'title.language': 'English'}) #
    item_values.append({'title.type': '05_MAIN'}) #
    print(item_values)

    try:
        i = CUR.create_record(database='items',
                              data=item_values,
                              output='json',
                              write=True)
        print(i)
        print(i.records)
        if i.records:
            try:
                wav_priref = i.records[0]['priref'][0]
                wav_ob_num = i.records[0]['object_number'][0]
                print(f'** WAV Item record created with Priref {wav_priref}')
                print(f'** WAV Item record created with object number {wav_ob_num}')
                LOGGER.info('WAV Item record created with priref %s', wav_priref)
                return wav_ob_num, wav_priref
            except Exception:
                LOGGER.exception("WAV Item record failed to retrieve object number")
                return None
        else:
            print(f"\nUnable to create CID WAV item record for {title}")
            LOGGER.exception("Unable to create WAV item record!")
            return None
    except Exception:
        print(f"\nUnable to create CID WAV item record for {title}")
        LOGGER.exception("Unable to create WAV item record!")
        return None


def append_source(source_ob_num, priref, ob_num, comment, date):
    '''
    Where source_item can't be written with record creation
    appended after record created. Check source_item field
    after push is only way to verify if successful.
    '''
    source = {
         'source_item': source_ob_num,
         'quality_comments': comment,
         'quality_comments.date': date
    }
    try:
        result = CUR.create_occurrences(database='items',
                                        priref=priref,
                                        data=source,
                                        output='json')
        print(result)
    except Exception as err:
        LOGGER.warning("Unable to append work data to CID work record: %s", err)

    # Attempt retrieval of source_item, only means to check if populated
    time.sleep(10)
    search = f'(priref="{priref}")'
    data = cid_query('items', search, ob_num)
    print(f"CHECK FOR SOURCE_ITEM: {data}")

    if str(data[5]) == str(source_ob_num):
        print(f"Retrieved source_item field from Item record: {data[5]} match {source_ob_num}")
        return True
    else:
        print(f"Retrieved source_item field from Item. No match for {source_ob_num}")
        return False


def rename(folder, ob_num):
    '''
    Receive original folder path and rename
    based on object number, return new filepath, filename
    '''
    new_name = ob_num.replace('-', '_')
    folderpath = os.path.join(WAV_RENAME_PATH, folder)
    name_split = folder.split('_')
    new_fname = f"{new_name}_{name_split[-1]}"
    print(f"Renaming {folder} to {new_fname}")
    new_fpath = os.path.join(WAV_RENAME_PATH, new_fname)

    try:
        os.rename(folderpath, new_fpath)
        return (new_fpath, new_fname)

    except OSError:
        LOGGER.warning("There was an error renaming %s to %s", folder, new_fname)
        return None


def ingest_move(filepath, new_filename):
    '''
    Take file path and check move to autoingest
    '''
    tar_wrap_path = os.path.join(FOR_TAR_WRAP, new_filename)
    try:
        shutil.move(filepath, tar_wrap_path)
        return tar_wrap_path
    except Exception as err:
        LOGGER.warning("ingest_move(): Failed to move %s to Autoingest %s", new_filename, err)
        return None


def log_pprint(dct):
    '''
    Make neat string variable from dct
    '''
    data_store = ''
    for file, data in dct.items():
        data = (f"{file}: {data}\n")
        data_store = f"{data_store}" + f"{data}"
    return data_store


def local_log(data):
    '''
    Write collected data actions list of items
    to local log in audio_operations
    '''
    if len(data) > 0:
        with open(LOCAL_LOG, 'a+') as log:
            log.write(f"{data}\n")
            log.close()


if __name__ == '__main__':
    main()
