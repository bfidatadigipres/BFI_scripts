#!/usr/bin/env python3

'''
magnetic_rename_wav.py

Script functions:

1. Pick up files (part whole 01 search first, then create list of all
   parts where whole is > 01, and check all files are in folder
   before acting against them - skip if part missing)
2. 01of* file - mediaconch policy check against POLICY_WAV
   - If pass, look up source record in CID, extract priref and create
     new item record for WAV file(s) linked ‘source’ to the source Item
     Got to step 3
   - If not pass, move to failed/ folder and append note to log
     Script exits
3. New item record object number is obtained, converted to new filename
   with existing part_whole and extension
4. File renamed and moved to autoingest folder path
5. All actions logged human readable for Mike, and placed in audio ops
   folder, at top level.

NOTE: Updated to work with Adlib V3      

Joanna White
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
import requests
import subprocess

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global paths/vars
WAV_ARCHIVE_PATH = os.environ['WAV_ARCHIVE_MAG']
WAV_POLICY = os.environ['POLICY_WAV']
FAILED_PATH = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename/')
AUTOINGEST = os.path.join(os.environ['AUDIO_OPS_FIN'], os.environ['AUTOINGEST_AUD'])
LOCAL_LOG = os.path.join(WAV_ARCHIVE_PATH, 'magnetic_preservation_renaming.log')
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API4']

# Setup logging
LOGGER = logging.getLogger('magnetic_rename_wav')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'magnetic_rename_wav.log'))
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


def fname_split(filename):
    '''
    Receive filename, extract part whole
    and return items split up
    '''
    fname, ext = os.path.splitext(filename)
    file_split = fname.split('_')
    if len(file_split) == 4:
        part_whole = str(file_split[3])
        part, whole = part_whole.split('of')
        return (f"{file_split[0]}_{file_split[1]}_{file_split[2]}_", part, whole, ext)
    elif len(file_split) == 3:
        part_whole = str(file_split[2])
        part, whole = part_whole.split('of')
        return (f"{file_split[0]}_{file_split[1]}_", part, whole, ext)
    else:
        return None, None, None, None


def remove_whitespace(title):
    '''
    Remove excess whitespace from badly formed names
    '''
    new_title = re.sub(' +', ' ', title)
    print(new_title)
    return new_title


def return_range(filename):
    '''
    Receive filename for WAV, extract part whole data
    create all fnames for range and return as list
    '''
    fname, part, whole, ext = fname_split(filename)
    if not fname:
        return []
    part = int(part)
    whole = int(whole)
    range_list = []

    for count in range(1, whole + 1):
        name = f"{fname}" + str(count).zfill(2) + 'of' + str(whole).zfill(2) + f"{ext}"
        range_list.append(name)
    return range_list


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


def check_range(range_list):
    '''
    Checks for all present in path and returns False boolean
    if any one is missing, or returns whole sequence path list
    '''
    file_paths = []
    for item in range_list:
        filepath = os.path.join(WAV_ARCHIVE_PATH, item)
        if os.path.isfile(filepath):
            file_paths.append(filepath)
        else:
            LOGGER.info("check_range(): WAV FILE %s missing from %s folder", item, WAV_ARCHIVE_PATH)
            return None
    return file_paths


def make_object_number(file_path):
    '''
    Convert file or directory to CID object_number
    '''
    if os.path.isfile(file_path):
        path_split = os.path.split(file_path)
        fname = str(path_split[1])
        filename = os.path.splitext(fname)[0]
        filename = filename.replace('_', '-')
        object_number = filename[:-7]
        return object_number


def cid_query(database, search, object_number):
    '''
    Format CID query for cid_data_retrieval()
    '''
    fields = [
        'priref',
        'title',
        'title.article',
        'object_number',
        'derived_item',
        'source_item',
        'title.language',
        'sound_system_item'
    ]
    record = adlib.retrieve_record(CID_API, database, search, '0', fields)[1]
    if not record:
        print(f"cid_query(): Unable to retrieve data for {object_number}")
        LOGGER.exception("cid_query(): Unable to retrieve data for %s", object_number)
        return None
    print(record)

    cid_data = {}
    for fname in fields:
        key = fname.replace('.', '_')
        if fname in str(record[0]):
            cid_data[key] = adlib.retrieve_field_name(record[0], fname)[0]
        else:
            cid_data[key] = ""

    return cid_data


def cid_data_retrieval(ob_num):
    '''
    Retrieve source data from CID
    to link to for new Item record
    '''
    cid_data = []
    search = f'(object_number="{ob_num}")'
    cid_data = cid_query('Items', search, ob_num)
    priref = cid_data['priref']
    if priref:
        print("Priref retrieved, checking for title.language...")
        parent_search = f'(parts_reference.lref="{priref}")'
    else:
        return None
    
    LOGGER.info("Retrieving CID data using query: %s", parent_search)
    parent_data = cid_query('Manifestations', parent_search, ob_num)
    for _, v in parent_data.items():
        cid_data.append(v)
    
    if priref:
        source_search = f'(priref="{priref}")'
    else:
        source_search = f'(object_number="{ob_num}")'

    source_data = cid_query('Items', source_search, ob_num)
    for _, v in source_data.items():
        cid_data.append(v)

    return cid_data


def main():
    '''
    Looks through WAV_ARCHIVE_PATH for files ending .wav/.WAV
    If part whole is greater than 01, extract all parts to list and check all present
    Find 01of* and check against policy, before CID actions to generate Item record
    extract object number and make new filename. Apply filename to all parts (mediaconch check)
    and move to autoingest path in audio isilon share.
    '''
    LOGGER.info("========== magnetic_rename_wav.py START ============")
    check_control()
    cid_check()

    wav_files = [f for f in os.listdir(WAV_ARCHIVE_PATH) if f.endswith(('.wav', '.WAV'))]
    if len(wav_files) == 0:
        LOGGER.info("No WAV files in archive path. Script exiting.")
        sys.exit("No WAV files in archive path. Exiting.")

    for file in wav_files:
        singleton = multipart = False
        filepath = os.path.join(WAV_ARCHIVE_PATH, file)

        # Check singleton or multipart
        if file.endswith(('01of01.wav', '01of01.WAV')):
            singleton = True
            print(f"{file} is a single item")
            local_log(f"============= NEW WAV FILE FOUND ============= {str(datetime.datetime.now())}")
            local_log(f"File found for processing: {file}")

        else:
            multipart = True
            local_log(f"============= NEW WAV FILE GROUP FOUND ============= {str(datetime.datetime.now())}")
            if re.match(".+01of*", file):
                print(f"{file} is multipart")
                range_list = return_range(file)
                if not range_list:
                    local_log(f"Skipping: Range extraction failed for multipart filename {file}")
                    LOGGER.warning("Range could not be extracted from multipart filename: %s", file)
                    continue
                local_log(f"First item in multi-reel group found for processing: {file}")
                files_present = check_range(range_list)
                if files_present is None:
                    local_log("File(s) missing in sequence, leaving this group until all present.\n")
                    LOGGER.info("%s range incomplete, leaving until all parts present.\n", file)
                    continue
                else:
                    local_log(f"All files within range present:\n{range_list}")
            else:
                LOGGER.info("%s multipart file not first part, skipping.\n", file)
                continue

        success_check = {}
        # Mediaconch policy assessment
        if singleton:
            success = conformance_check(filepath)
            print(success)
            local_log(f"Conformance check results for {file}: {success}")
        if multipart:
            for item in files_present:
                success_multi = conformance_check(item)
                success_check[item] = success_multi
                print(f"{item} success: {success_multi}")
            success_check_string = log_pprint(success_check)
            local_log(f"Conformance check results for {file} and all parts:\n{success_check_string}")

        # SINGLETON PROCESS START ====================
        if singleton and 'PASS!' in str(success):
            source_ob_num = make_object_number(filepath)
            print(source_ob_num)
            local_log(f"Source object number created from filename: {source_ob_num}")
            # Retrieve source item priref
            if len(source_ob_num) > 0:
                cid_data = cid_data_retrieval(source_ob_num)
                if not cid_data:
                    continue
                print(cid_data)
                if cid_data[11] != source_ob_num:
                    LOGGER.warning("Source object number <%s> is not a match to filename converted object_number <%s>", cid_data[11], source_ob_num)
                    continue
            # Check that source item record is Magnetic
            if not 'Magnetic (sound)' in str(cid_data[15]):
                LOGGER.info("Source item record is not Magnetic (sound) in sound_system_item: %s", str(cid_data[15]))
                local_log("Skipping: 'Magnetic (sound)' not found in sound_system_item")
                continue
            # Make CID record using source_item title / manifestation parent title
            if len(cid_data[9]) > 0:
                LOGGER.info("Making new CID item record for WAV using parent title %s", cid_data[9])
                local_log(f"Creating new CID item record using parent title: {cid_data[9]}")
                wav_data = create_wav_record(cid_data[0], cid_data[9], cid_data[10], cid_data[14], cid_data[11])
            elif len(cid_data[1]) > 0:
                LOGGER.info("Making new CID item record for WAV using parent title %s", cid_data[1])
                local_log(f"Creating new CID item record using grandparent title: {cid_data[1]}")
                wav_data = create_wav_record(cid_data[0], cid_data[1], cid_data[2], cid_data[6], cid_data[11])
            else:
                local_log(f"Unable to retrieve CID data for: {source_ob_num}. Moving file to failed_rename folder.")
                LOGGER.warning("Title information absent from CID data retrieval, skipping record creation")
                # Move file to failed_rename/ folder
                fail_path = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename', file)
                print(f"Moving {filepath} to {fail_path}")
                shutil.move(filepath, fail_path)
                continue

            # Check wav_ob_num present following Item creation
            if wav_data is None:
                LOGGER.warning("No WAV object number obtained - failed record creation")
                local_log(f"FAILED: Creation of new WAV Item record failed for {file}. Leaving to retry")
                continue

            # Rename file and move
            success = rename(file, wav_data[1])
            if success is not None:
                local_log(f"File {file} renamed {success[1]}")
                LOGGER.info("File %s renamed successfully to %s", file, success[1])
                # Move to autoingest
                local_log(f"Moving to autoingest - file {success[0]}")
                move_success = ingest_move(success[0], success[1])
                if move_success:
                    local_log(f"{success[1]} moved to {move_success}\n")
                    LOGGER.info("Moved to autoingest path: %s.\n", move_success)
                else:
                    LOGGER.warning("FAILED MOVE: %s did not move to autoingest.\n", success[1])
                    local_log(f"Failed to move {success[1]} to autoingest - please complete this manually\n")
            else:
                local_log(f"FAIL: File {file} renaming failed, moving to failed_rename folder")
                local_log(f"FAIL: Please rename '{file}' manually to '{success[1]}' and move to autoingest.\n")
                LOGGER.warning("FAILED ATTEMPT AT RENAMING: Updated log for manual renaming. Moving to failed_rename folder.\n")
                # Move file to failed_rename/ folder
                fail_path = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename', file)
                print(f"Moving {filepath} to {fail_path}")
                shutil.move(filepath, fail_path)
                continue

        elif singleton and 'FAIL!' in str(success):
            # Output failure message
            local_log(f"FAILED POLICY: {file} - Moving file to failed_rename folder for inspection.")
            LOGGER.warning("%s failed Mediaconch policy check, skipping record creation and moving to failed_rename folder", file)
            # Move to failed_rename
            fail_path = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename', file)
            print(f"Moving {filepath} to {fail_path}")
            shutil.move(filepath, fail_path)
            continue

        # MULTIPART PROCESS START ====================
        if multipart and 'PASS!' in success_check_string:
            source_ob_num = make_object_number(filepath)
            print(source_ob_num)
            local_log(f"Source object number created from file: {source_ob_num}")
            # Retrieve source item priref
            if len(source_ob_num) > 0:
                cid_data = cid_data_retrieval(source_ob_num)
                if not cid_data:
                    continue
                print(cid_data)
            # Check that source item record is Magnetic
            if not 'Magnetic (sound)' in str(cid_data[15]):
                LOGGER.info("Source item record is not Magnetic (sound) in sound_system_item: %s", str(cid_data[15]))
                local_log("Skipping: 'Magnetic (sound)' not found in sound_system_item")
                continue
            # Make record, retrieve object number
            if len(cid_data[9]) > 0:
                LOGGER.info("Making new CID item record for WAV using parent title %s", cid_data[9])
                local_log(f"Creating new CID item record using parent title: {cid_data[9]}")
                wav_data = create_wav_record(cid_data[0], cid_data[9], cid_data[10], cid_data[14], cid_data[11])
            elif len(cid_data[1]) > 0:
                LOGGER.info("Making new CID item record for WAV using grandparent title %s", cid_data[1])
                local_log(f"Creating new CID item record using grandparent title: {cid_data[1]}")
                wav_data = create_wav_record(cid_data[0], cid_data[1], cid_data[2], cid_data[6], cid_data[11])
            else:
                local_log(f"Unable to retrieve CID data for: {source_ob_num}. Moving files to failed_rename folder.")
                LOGGER.warning("Title information absent from CID data retrieval, skipping record creation")
                # Iterate range_list and move file to failed_rename/ folder
                for local_file in range_list:
                    local_filepath = os.path.join(WAV_ARCHIVE_PATH, local_file)
                    local_fail_path = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename', local_file)
                    print(f"Moving {local_filepath} to {local_fail_path}")
                    local_log(f"Moving {local_file} to {local_fail_path}")
                    LOGGER.info("Moving %s to fail path: %s", local_file, local_fail_path)
                    shutil.move(local_filepath, local_fail_path)
                continue

            # Check wav_ob_num present following Item creation
            if wav_data is None:
                LOGGER.warning("No WAV object number obtained - failed record creation")
                local_log(f"FAILED: Creation of new WAV Item record failed for {file}. Leaving to retry")
                continue

            # Remove any items that fail policy before rename/move
            for key, val, in success_check.items():
                if 'FAIL!' in str(val):
                    filename = os.path.basename(key)
                    local_fail_path = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename', filename)
                    print(f"Moving {key} to {local_fail_path}")
                    local_log(f"FAILED MEDIACONCH POLICY: Moving {filename} to {local_fail_path}")
                    local_log(f"Please resupply and conformance check this file manually. Object number: {wav_data[1]}")
                    LOGGER.info("Moving %s to fail path: %s", filename, local_fail_path)
                    shutil.move(key, local_fail_path)

            # Rename all files and move to autoingest
            for local_file in range_list:
                success = rename(local_file, wav_data[1])
                if success is not None:
                    local_log(f"File {local_file} renamed {success[1]}")
                    LOGGER.info("File %s renamed successfully to %s", local_file, success[1])
                    # Move to autoingest
                    local_log(f"Moving to autoingest - file {success[0]}")
                    move_success = ingest_move(success[0], success[1])
                    if move_success:
                        local_log(f"{success[1]} moved to {move_success}\n")
                        LOGGER.info("Moved to autoingest path: %s", move_success)
                    else:
                        LOGGER.warning("FAILED MOVE: %s did not move to autoingest.\n", success[1])
                        local_log(f"Failed to move {success[1]} to autoingest - please complete this manually\n")
                else:
                    local_log(f"FAIL: File {local_file} renaming failed, moving to failed_rename folder")
                    local_log(f"FAIL: Please rename '{local_file}' manually after wav object number {wav_data[0]}) and move to autoingest.\n")
                    LOGGER.warning("FAILED ATTEMPT AT RENAMING: Updated log for manual renaming. Moving to failed_rename folder.\n")
                    # Move file to failed_rename/ folder
                    local_filepath = os.path.join(WAV_ARCHIVE_PATH, local_file)
                    local_fail_path = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename', local_file)
                    print(f"Moving {local_filepath} to {local_fail_path}")
                    shutil.move(local_filepath, local_fail_path)

        elif multipart and 'FAIL!' in success_check_string:
            # Output failure message
            local_log(f"FAILED POLICY: {file} and all parts - moving file to failed_rename folder for inspection.")
            LOGGER.warning("%s and all parts failed Mediaconch policy check, skipping record creation and moving to failed_rename folder", file)
            # Iterate range_list and move file to failed_rename/ folder
            for local_file in range_list:
                local_filepath = os.path.join(WAV_ARCHIVE_PATH, local_file)
                local_fail_path = os.path.join(WAV_ARCHIVE_PATH, 'failed_rename', local_file)
                print(f"Moving {local_filepath} to {local_fail_path}")
                local_log(f"Moving {local_file} to {local_fail_path}")
                LOGGER.info("Moving %s to fail path: %s.", local_file, local_fail_path)
                shutil.move(local_filepath, local_fail_path)
            local_log("All failed moves completed.\n")
            LOGGER.info("All failed moves completed.\n")

    LOGGER.info("================ END magnetic_rename_wav.py END =================")


def create_wav_record(gp_priref, title, title_article, title_language, source_ob_num):
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
                        {'input.notes': 'Magnetic preservation WAV automated record generation.'},
                        {'record_access.owner': '$REST'}])

    item_defaults = ([{'record_type': 'ITEM'},
                      {'code_type': 'Uncompressed'},
                      {'bit_depth.type': 'AUDIO'},
                      {'bit_depth': '24'},
                      {'sample_rate': '96kHz'},
                      {'transfer_speed.lref': '99839'},
                      {'acquisition.method': 'Created by'},
                      {'acquisition.source': 'BFI National Archive'},
                      {'acquisition.source.lref': '999570701'},
                      {'acquisition.reason': 'Collections care'},
                      {'copy_status': 'M'},
                      {'copy_usage': 'Restricted access to preserved digital file'},
                      {'copy_usage.lref': '131560'},
                      {'creator': 'BFI National Archive'},
                      {'creator.lref': '999570701'},
                      {'creator.role.lref': '392405'},
                      {'description.date': str(datetime.datetime.now())[:10]},
                      {'file_type': 'WAV'},
                      {'item_type': 'DIGITAL'},
                      {'source_item': source_ob_num},
                      {'source_item.content': 'SOUND'},
                      {'production.reason': 'Magnetic preservation WAV digitisation project'},
                      {'production.notes': 'WAV file'}])

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
    item_values_xml = adlib.create_record_data(CID_API, 'items', '', data=item_values)
    print(item_values_xml)

    record = adlib.post(CID_API, item_values_xml, 'items', 'insertrecord')
    if not record:
        print(f"\nUnable to create CID WAV item record for {title}")
        LOGGER.exception("Unable to create WAV item record!")
        return None
    try:        
        wav_priref = adlib.retrieve_field_name(record, 'priref')[0]
        wav_ob_num = adlib.retrieve_field_name(record, 'object_number')[0]
        print(f'** WAV Item record created with Priref {wav_priref}')
        print(f'** WAV Item record created with object number {wav_ob_num}')
        LOGGER.info('WAV Item record created with priref %s', wav_priref)
        return wav_priref, wav_ob_num
    except Exception:
        print(f"\nUnable to retrieve priref/object number from new item record for {title}")
        LOGGER.exception("Unable to retrieve priref and object number from new item record!")
        return None


def rename(file, ob_num):
    '''
    Receive original file path and rename filename
    based on object number, return new filepath, filename
    '''
    new_name = ob_num.replace('-', '_')
    filepath = os.path.join(WAV_ARCHIVE_PATH, file)
    name_split = file.split('_')
    new_filename = f"{new_name}_{name_split[-1]}"
    print(f"Renaming {file} to {new_filename}")
    new_filepath = os.path.join(WAV_ARCHIVE_PATH, new_filename)

    try:
        os.rename(filepath, new_filepath)
        return (new_filepath, new_filename)

    except OSError:
        LOGGER.warning("There was an error renaming %s to %s", file, new_filename)
        return None


def ingest_move(filepath, new_filename):
    '''
    Take file path and check move to autoingest
    '''
    autoingest_path = os.path.join(AUTOINGEST, new_filename)
    try:
        shutil.move(filepath, autoingest_path)
        return autoingest_path
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
