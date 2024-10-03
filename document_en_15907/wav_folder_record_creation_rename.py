#!/usr/bin/env python3

'''
Script functions:

1. Pick up folders (named after their source Item record)
2. Check in CID that the source item record has sound_item = 'Sound'
3. Check all WAVs in folder - mediaconch policy check against POLICY_WAV
   - If any fail then pause the process and exit with warning log
4. New item record is created and linked to source item's manifestation
   and to source_item. Extract ob_number and convert to new filename
   with part_whole carried over from supplied folder.
5. Add names of all files into the quality comments field, and populate quality date field.
6. Folder renamed and moved to automation_wav/for_tar_wrap for TAR wrapping
7. All actions logged human readable for Mike, and placed in audio ops
   folder, at top level.

NOTE: Updated for Adlib V3

2023
'''

# Public packages
import os
import re
import sys
import json
import shutil
import logging
import datetime
import subprocess

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global paths/vars
AUTO_WAV_PATH = os.environ['AUTOMATION_WAV']
WAV_RENAME_PATH = os.path.join(AUTO_WAV_PATH, 'record_create_folder_rename/')
FOR_TAR_WRAP = os.path.join(AUTO_WAV_PATH, 'for_tar_wrap/')
WAV_POLICY = os.environ['POLICY_WAV']
FAILED_PATH = os.path.join(WAV_RENAME_PATH, 'failed_rename/')
LOCAL_LOG = os.path.join(WAV_RENAME_PATH, 'record_create_folder_rename.log')
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API4']
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
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
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
    fields = [
        'priref',
        'title',
        'title.article',
        'title.language',
        'object_number',
        'source_item',
        'derived_item',
        'sound_item'
    ]
    record = adlib.retrieve_record(CID_API, database, search, '0', fields)[1]
    if not record:
        print(f"cid_query(): Unable to retrieve data for {object_number}")
        LOGGER.exception("cid_query(): Unable to retrieve data for %s", object_number)
        return None

    print(record[0])
    if 'priref' in str(record[0]):
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    else:
        priref = ""
    if 'title' in str(record[0]):
        title = adlib.retrieve_field_name(record[0], 'title')[0]
    else:
        title = ""
    if 'title.article' in str(record[0]):
        title_article = adlib.retrieve_field_name(record[0], 'title.article')[0]
    else:
        title_article = ""
    if 'title.language' in str(record[0]):
        title_language = adlib.retrieve_field_name(record[0], 'title.language')[0]
    else:
        title_language = ""
    if 'object_number' in str(record[0]):
        ob_num = adlib.retrieve_field_name(record[0], 'object_number')[0]
    else:
        ob_num = ""
    if 'source_item' in str(record[0]):
        source_item = adlib.retrieve_field_name(record[0], 'source_item')[0]
    else:
        source_item = ""
    if 'derived_item' in str(record[0]):
        derived_item = adlib.retrieve_field_name(record[0], 'derived_item')[0]
    else:
        derived_item = ""
    if 'sound_item' in str(record[0]):
        sound_item = adlib.retrieve_field_name(record[0], 'sound_item')[0]
    else:
        sound_item = ""

    new_title = remove_whitespace(title)

    return (priref, new_title, title_article, ob_num, derived_item, source_item, title_language, sound_item)


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
    Looks through WAV_RENAME_PATH for files ending .wav/.WAV
    If part whole is greater than 01, extract all parts to list and check all present
    Find 01of* and check against policy, before CID actions to generate Item record
    extract object number and make new filename. Apply filename to all parts (mediaconch check)
    and move to autoingest path in audio isilon share.
    '''
    check_control()
    cid_check()
    directory_list = {}
    dirs = [ x for x in os.listdir(WAV_RENAME_PATH) if os.path.isdir(os.path.join(WAV_RENAME_PATH, x)) ]
    for directory in dirs:
        if directory == 'failed_rename':
            continue
        dirpath = os.path.join(WAV_RENAME_PATH, directory)
        wav_files = []
        other_files = []
        for root, dirs, files in os.walk(dirpath):
            for file in files:
                if file.endswith(('.wav', '.WAV')):
                    wav_files.append(os.path.join(root, file))
                else:
                    other_files.append(os.path.join(root, file))
        if len(other_files) > 0:
            LOGGER.warning("Non WAV files found in %s: %s", directory, other_files)
            LOGGER.warning("Skipping processing this folder until files reviewed.")
            continue
        if len(wav_files) > 0:
            directory_list[dirpath] = wav_files
        else:
            LOGGER.info("Skipping: No audio files in folder %s", directory)
            continue
    if not directory_list:
        LOGGER.info("No items found this time. Script exiting")
        sys.exit()

    LOGGER.info("========== wav folder record creation rename START ============")
    print(directory_list)
    for key, value in directory_list.items():
        print(key, value)
        LOGGER.info("======== Folder path being processed %s ========", key)
        LOGGER.info("Contents of folder being processed:")
        join_up = ', '.join(value)
        LOGGER.info("%s", join_up)
        folder = os.path.split(key)[-1]
        local_log(f"============= NEW WAV FOLDER FOUND {folder} ============= {str(datetime.datetime.now())}")
        local_log(f"Folder contents: {', '.join(value)}")

        # Mediaconch policy assessment
        quality_comments = []
        mediaconch_assess = []
        for filepath in value:
            if filepath.endswith(('.wav', '.WAV')):
                file = os.path.split(filepath)[-1]
                success = conformance_check(filepath)
                LOGGER.info("Conformance check results for %s: %s", file, success)
                quality_comments.append(os.path.relpath(filepath, key))
                if 'PASS!' not in success:
                    mediaconch_assess.append('FAIL')
                    local_log(f"File failed Mediaconch policy: {file}\n{success}")

        if 'FAIL' in mediaconch_assess:
            LOGGER.warning("Skipping: One or more WAV file in folder %s has failed the policy", folder)
            local_log("Skipping: Problem detected with encosed WAV file\n")
            continue
        source_ob_num, part_whole = make_object_number(folder)
        if len(source_ob_num) == 0:
            local_log(f"Skipping: Unable to retrieve Source object number from folder: {folder}")
            LOGGER.warning("Skipping: Unable to retrieve source object number from folder: %s", folder)
            continue
        print(source_ob_num, part_whole)
        local_log(f"Source object number retrieved from folder {folder}: {source_ob_num}")
        cid_data = cid_data_retrieval(source_ob_num)
        print(cid_data)

        # Check source Item source_item field
        if cid_data[15] not in ('Sound', 'Combined', 'Mixed', 'Combined as Sound'):
            LOGGER.info("Skipping: Supplied CID item record source does not have 'sound_item': 'Sound'")
            local_log(f"Skipping: Could not find 'Sound' in sound_item record for source item {source_ob_num}")
            continue
        # Check source Item priref
        if len(cid_data[8]) == 0:
            LOGGER.info("Skipping: No priref retrieved for folder %s", folder)
            local_log(f"SKIPPING: Could not find priref for source item {source_ob_num}")
            continue
        # Make CID record with Title of source item
        if len(cid_data[9]) > 0:
            LOGGER.info("Making new CID item record for WAV using source item title %s", cid_data[9])
            local_log(f"Creating new CID item record using source item title: {cid_data[9]}")
            wav_ob_num, wav_priref = create_wav_record(cid_data[0], cid_data[9], cid_data[10], cid_data[14], cid_data[8])
            local_log(f"New CID item record created: {wav_ob_num} {wav_priref}")

        # Else use Title of manifestation parent
        elif len(cid_data[1]) > 0:
            LOGGER.info("Making new CID item record for WAV using manifestation parent title %s", cid_data[1])
            local_log(f"Creating new CID item record using manifestation parent title: {cid_data[1]}")
            wav_ob_num, wav_priref = create_wav_record(cid_data[0], cid_data[1], cid_data[2], cid_data[6], cid_data[8])
        else:
            local_log(f"Unable to retrieve CID data for: {source_ob_num}. Moving file to failed_rename folder.")
            LOGGER.warning("Title information absent from CID data retrieval, skipping record creation")

            # Move file to failed_rename/ folder
            fail_path = os.path.join(FAILED_PATH, folder)
            local_log(f"Moving {key} to {fail_path}")
            shutil.move(key, fail_path)
            continue
        # Check wav_ob_num present following Item creation
        if wav_ob_num:
            local_log(f"Creation of new WAV item record successful: {wav_ob_num}")
            LOGGER.info("Creation of new WAV item record successful: %s", wav_ob_num)
        else:
            LOGGER.warning("No WAV object number obtained - failed record creation")
            local_log(f"FAILED: Creation of new WAV Item record failed for {folder}. Leaving to retry")
            continue

        # Compile list of enclosed files and add to quality comments of new Item record
        qual_comm = f"TAR file contains: {'; '.join(quality_comments)}."
        append_success = adlib.add_quality_comments(CID_API, wav_priref, qual_comm)
        if not append_success:
            LOGGER.warning("Unable to add QUALITY COMMENTS data to CID Item record <%s>\n<%s>", wav_priref, qual_comm)

        # Rename file and move
        success = rename(folder, wav_ob_num)
        if success is not None:
            local_log(f"File {folder} renamed to {success[1]}")
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
            print(f"Moving {key} to {fail_path}")
            shutil.move(key, fail_path)
            continue

    LOGGER.info("================ END WAV folder record creation rename END =================")


def create_wav_record(gp_priref, title, title_article, title_language, source_priref):
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
                        {'input.notes': 'Created by automation to aid ingest of legacy projects and workflows.'},
                        {'record_access.owner': 'Acquisitions Full'}])

    item_defaults = ([{'record_type': 'ITEM'},
                      {'acquisition.method': 'Created by'},
                      {'acquisition.source': 'BFI National Archive'},
                      {'acquisition.source.lref': '999570701'},
                      {'acquisition.reason': 'Digital deliverable from BFI'},
                      {'copy_status': 'M'},
                      {'copy_usage': 'Restricted access to preserved digital file'},
                      {'copy_usage.lref': '131560'},
                      {'creator': 'BFI National Archive'},
                      {'creator.lref': '999570701'},
                      {'creator.role.lref': '392405'},
                      {'description.date': str(datetime.datetime.now())[:10]},
                      {'file_type': 'WAV'},
                      {'code_type': 'Uncompressed'},
                      {'item_type': 'DIGITAL'},
                      {'source_item.lref': source_priref},
                      {'source_item.content': 'SOUND'}])

    wav_ob_num = ""
    item_values = []
    item_values.extend(record_defaults)
    item_values.extend(item_defaults)

    # Appended data
    item_values.append({'part_of_reference.lref': gp_priref})
    item_values.append({'title': title})
    if len(title_article) > 0:
        item_values.append({'title.article': title_article})
    if len(title_language) > 0:
        item_values.append({'title.language': title_language})
    else:
        item_values.append({'title.language': 'English'})
    item_values.append({'title.type': '05_MAIN'})
    print(item_values)

    item_values_xml = adlib.create_record_data(CID_API, 'items', '', item_values)
    print(item_values_xml)

    record = adlib.post(CID_API, item_values_xml, 'items', 'insertrecord')
    if not record:
        return None
    try:
        wav_priref = adlib.retrieve_field_name(record, 'priref')[0]
        wav_ob_num = adlib.retrieve_field_name(record, 'object_number')[0]
        print(f'** WAV Item record created with Priref {wav_priref}')
        print(f'** WAV Item record created with object number {wav_ob_num}')
        LOGGER.info('WAV Item record created with priref %s', wav_priref)
        return wav_ob_num, wav_priref
    except Exception as err:
        print(f"\nUnable to create CID WAV item record for {title} {err}")
        LOGGER.exception("Unable to create WAV item record!")
        return None


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
