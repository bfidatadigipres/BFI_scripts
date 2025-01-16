"""
pre_autoingest_checksum_checks script checks if the files MD5 and supplied
MD5s by suppliers - like Amazon / Netflix or DMS Teracopy transfer - match.
Documents all actions and writes to ingest_check.log. Full, partial and
non-matching media are moved to ingest_check subfolders accordingly.

2025
"""

# Imports
import os
import sys
import shutil
import logging
import datetime

# Local import
sys.path.append(os.environ['CODE'])
import utils

# Global vars
LOG_PATH = os.environ["LOG_PATH"]

# Set up logging
LOGGER = logging.getLogger("pre_autoingest_checksum_checks_backup")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "ingest_check_script_backup.log"))
FORMATTER = logging.Formatter("%(asctime)s \t %(levelname)s \t %(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

IGNORE_FOLDERS = {
    "checksum_folder",
    "ingest_match",
    "ingest_failed",
    "ingest_partial",
}


def read_doc_for_match(filepath, hash_number) -> list:
    """
    Opens and reads checksum filepath supplied, uses hash to read
    all lines checking for match forced to anycase. Where found
    return list with full checksum line, checksum file path, True bool
    """

    match_list = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f.readlines():
            normalised_line = line.replace("\\", "/").rstrip('\n')
            if hash_number in normalised_line.upper():
                print(hash_number, f" match found: {normalised_line}")
                match_list.append(f"{normalised_line}, {os.path.relpath(filepath, sys.path[1])}")

    return match_list


def pygrep(checksum_folder, hash_value) -> list:
    """
    Read all checksum documents for match to hash value,
    iterate checksum documents available and feed to read_doc_for_match
    """
    list_of_files = []
    checksum_list = os.listdir(checksum_folder)
    for checksum_doc in checksum_list:
        try:
            match = read_doc_for_match(os.path.join(checksum_folder, checksum_doc), hash_value)
            if len(match) > 0:
                list_of_files += match
        except Exception as err:
            print(err)

    return list_of_files


def local_log(full_path, data) -> None:
    """
    Writes to local logs found in ingest_check folder
    """

    local_log_path = os.path.join(full_path, "ingest_check.log")
    timestamp = str(datetime.datetime.now())

    with open(local_log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp[0:19]} - {data}\n")
        log_file.close()


def make_folder_inventory(dpath, filepath) -> dict:
    '''
    Build dict key filename and value local checksum
    plus any matching line in checksum matches
    '''
    inventory = {}
    local_log(filepath, f"Processing files within folder {os.path.relpath(dpath, filepath)}:")
    LOGGER.info("Processing files in folder %s:", os.path.relpath(dpath, filepath)
    for root, _, files in os.walk(dpath):
        for file in files:
            fpath = os.path.join(root, file)
            file_relpath = os.path.relpath(fpath, filepath)
            hash_number = utils.create_md5_65536(fpath)
            matching = pygrep(os.path.join(filepath, 'checksum_folder'), hash_number.upper())
            inventory[f"{file_relpath} - {hash_number}"] = matching
            LOGGER.info("%s:\n - Local MD5: %s\n - Source checksum match: %s", file_relpath, hash_number, ' / '.join(matching))
            local_log(filepath, f"- {file_relpath}\n   - Local MD5: {hash_number}\n   - Source checksum matches: {' / '.join(matching)}")

    return inventory


def main() -> None:
    """
    Iterate over files/directories to build logs for DMS team
    confirming checksums, and matches where found
    """

    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit("Shell script failed to pass argument to Python script")
    if not utils.check_control('power_off_all'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    # the file path should refer to the ingest_check folder
    filepath = os.path.join(sys.argv[1], "Acquisitions/ingest_check")

    LOGGER.info("Starting checksum validation process")
    print(filepath)

    file_list = [ x for x in os.listdir(filepath) if os.path.isfile(os.path.join(filepath, x)) and x != 'ingest_check.log' ]
    dir_list = [ x for x in os.listdir(filepath) if os.path.isdir(os.path.join(filepath, x)) and x not in IGNORE_FOLDERS ]

    if not file_list and not dir_list:
        sys.exit("No files/folders found for processing at this time.")

    file_relpath = ''
    # Process files first, complex directories after
    LOGGER.info("======================Starting pre autoingest checks========================")
    local_log(filepath, "======================Starting pre autoingest checks===========================")
    for file in file_list:
        fpath = os.path.join(filepath, file)
        file_relpath = os.path.relpath(fpath, filepath)
        if '.dstore' in file.lower():
            continue
        local_log(filepath, f"*** New file being processed: {file_relpath}")
        LOGGER.info("File being processed: %s", file_relpath)

        # Make local hash for search
        hash_number = utils.create_md5_65536(fpath)
        matching = pygrep(os.path.join(filepath, 'checksum_folder'), hash_number.upper())
        print(f"{len(matching)} matched: {matching}")

        # Assess file responses and move files accordingly
        if len(matching) == 0:
            LOGGER.warning("No checksum match made: %s. Moving to ingest_failed/", file)
            local_log(filepath, "\tNo matching checksum found across checksum documentation")
            local_log(filepath, f"\tLocal checksum generated: {hash_number.upper()}")
            local_log(filepath, "Moving to ingest_failed/ folder")
            local_log(filepath, '--------------------------------------')
            shutil.move(fpath, os.path.join(filepath, f'ingest_failed/{file}'))
        elif len(matching) == 1:
            LOGGER.info("Checksum match made for %s:\n%s", file, match)
            for match in matching:
                checksum_line, checksum_match_path = match.split(',')
                if file.upper() in checksum_line.upper():
                    LOGGER.info("Checksum full match found in file: %s", checksum_match_path)
                    local_log(filepath, f"Match found in file: {checksum_match_path}\n - Source checksum file entry: {checksum_line}\n - Local checksum generated: {hash_number.upper()}")
                    move_path = 'ingest_match'
                else:
                    LOGGER.info("Checksum partial match found in file: %s", checksum_match_path)
                    local_log(filepath, f"Partial match found in file: {checksum_match_path}\n - Source checksum file entry: {checksum_line}\n - Local checksum generated: {hash_number.upper()}")
                    move_path = 'ingest_partial'
                local_log(filepath, f"Moving to {move_path}/ folder")
                local_log(filepath, '--------------------------------------')
                shutil.move(fpath, os.path.join(filepath, move_path, file))
        else:
            LOGGER.info("Multiple checksum matches made for %s:\n%s", file, matching)
            match_str = []
            for match in matching:
                checksum_line, checksum_match_path = match.split(',')
                if file.upper() in checksum_line.upper():
                    match_str.append('full match')
                else:
                    match_str.append('partial match')
                LOGGER.info("Checksum %s found in file: %s", match_str, checksum_match_path)
                local_log(filepath, f"{match_str.title()} found in file: {checksum_match_path}\n - Checksum file entry: {checksum_line}\n - Local checksum generated: {hash_number.upper()}")

            if 'full match' in str(match_str):
                local_log(filepath, "Filename matched in a checksum document. Moving to ingest_match/ folder")
                local_log(filepath, "** Please investigate why this file has two checksum matches!")
                local_log(filepath, '--------------------------------------')
                LOGGER.info("Checksum match, filename match. Moving to ingest_match/")
                shutil.move(fpath, os.path.join(filepath, f'ingest_match/{file}'))
            else:
                local_log(filepath, "Filename not matched in checksum document. Moving to partial_match/ folder")
                local_log(filepath, "** Please investigate why this file has two checksum matches!")
                local_log(filepath, '--------------------------------------')
                LOGGER.info("Checksum match, filename NO match. Moving to ingest_partial/")
                shutil.move(fpath, os.path.join(filepath, f'ingest_partial/{file}'))
        continue

    for directory in dir_list:
        dpath = os.path.join(filepath, directory)
        dir_relpath = os.path.relpath(dpath, filepath)

        local_log(filepath, f"** New complex folder being processed: {dir_relpath}")
        LOGGER.info("*** New folder being processed: %s", file_relpath)

        # Make local hash for all files and update log
        hash_dict = make_folder_inventory(dpath, filepath)
        checksum_file_list = []

        # Look for match types to decide where whole folder is moved:
        full_match = partial_match = no_match = False
        for key, value in hash_dict.items():
            file_relpath = key.split(' - ')[0]
            fname = os.path.basename(file_relpath)
            if len(value) == 0:
                no_match = True
            for match in value:
                checksum_file_list.append(match.split(', ')[-1])
                if fname.lower() in match.lower():
                    full_match = True
                else:
                    partial_match = True
        # Check if all checksum matched to one checksum document
        checksum_file_list = list(set(checksum_file_list))
        if len(checksum_file_list) > 1:
            LOGGER.warning("More than one checksum document matched to directory:\n%s", ', '.join(checksum_file_list))
            local_log(filepath, f"More than one checksum document matched to directory contents:\n {', '.join(checksum_file_list)}")
            partial_match = True

        # Assess file responses and move folders accordingly
        if no_match is True:
            LOGGER.warning("One or more files in folder does not have checksum match made: %s", dir_relpath)
            LOGGER.info("File moved to: %s", os.path.join(filepath, 'ingest_failed'))
            local_log(filepath, f"Please see notes above. Not all files have matching checksums for {dir_relpath}")
            local_log(filepath, "Moving whole folder to ingest_failed/ folder for manual review")
            local_log(filepath, '--------------------------------------')
            shutil.move(dpath, os.path.join(filepath, f'ingest_failed/{directory}'))
        elif partial_match is True:
            LOGGER.warning("One or more files in folder does not have clean match: %s", dir_relpath)
            LOGGER.info("File moved to: %s", os.path.join(filepath, 'ingest_partial'))
            local_log(filepath, f"Please see notes above. Not all files have cleanly matching checksums for {dir_relpath}")
            local_log(filepath, "Moving whole folder to ingest_partial/ folder for manual review")
            local_log(filepath, '--------------------------------------')
            shutil.move(dpath, os.path.join(filepath, f'ingest_partial/{directory}'))
        elif full_match is True:
            LOGGER.info("All files in folder have checksum and filename matched: %s", dir_relpath)
            LOGGER.info("File moved to: %s", os.path.join(filepath, 'ingest_match'))
            local_log(filepath, f"All files have matching checksums and filenames for {dir_relpath}")
            local_log(filepath, "Moving whole folder to ingest_match/")
            local_log(filepath, '--------------------------------------')
            shutil.move(dpath, os.path.join(filepath, f'ingest_match/{directory}'))
        continue

    LOGGER.info(
        "======================pre autoingest checks End===================================="
    )
    local_log(
        filepath,
        "======================pre autoingest checks End====================================",
    )


if __name__ == "__main__":
    main()
