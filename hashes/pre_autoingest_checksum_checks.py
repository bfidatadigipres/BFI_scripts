"""
pre_autoingest_checsum_checks that checks if the supplied checksum provided 
by the suppliers (Amazon, Netflix and more) or their own tercopy transfer.
for a media file is the same as the local generated checksum. 
Documents/ all actions are stored in ingest_check.log stored inside ingest_check.

This script is ran before running autoingest.py.

"""

import os
import re
import sys
import shutil
import logging
import datetime
from pathlib import Path


sys.path.append(os.environ["CODE"])
import utils

LOG_PATH = os.environ["LOG_PATH"]
CHECKSUM_PATH = os.path.join(LOG_PATH, "checksum_md5")

LOGGER = logging.getLogger("pre_autoingest_checksum_checks")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "pre_autoingest_checksum_checks.log"))
FORMATTER = logging.Formatter("%(asctime)s \t %(levelname)s \t %(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

LOGGED_MESSAGES = set()

IGNORED_EXTENSION = {".ini", ".DS_Store", ".mhl", ".tmp", ".dpx", ".DPX", ".log"}
IGNORE_FOLDERS = {"checksum_folder", "ingest_match", "ingest_failed", "ingest_partial"}


def normalised_file(path):
    """replace the path that has \\ to / in order for the file to be processed."""

    return path.replace("\\", "/")


def process_file_for_match(filepath, hash_number, file_name):
    """
    Based on the filename or checksum, this function will find either inside
    the checksum folder and returns a tuple containing the line in
    checksum file, the checkusm file path and bool

    Returns
    -------
       lists_of_files: list
          contain the list of tuples containing matches found inside checksum file
    """

    lists_of_files = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                # print(line)
                normalised_line = normalised_file(line)
                if (
                    re.search(hash_number.lower(), normalised_line, re.IGNORECASE)
                    or file_name in normalised_line
                ):
                    # print(hash_number, " found")
                    lists_of_files.append(f"{normalised_line}, {filepath}")
    except (FileNotFoundError, IOError) as e:
        LOGGER.error(f"Error reading {filepath}: {e}")
    except Exception as e:
        LOGGER.error(f"Unexpected error processing {filepath}: {e}")
    return lists_of_files


def pygrep(checksum_folder, hash_value, file_name):
    """
    This function represent the python version of the linux command 'grep'
    Return
    --------
    lists_of_files: list
        return a list of file matches

    """
    lists_of_files = []
    for checksum_file in os.listdir(checksum_folder):
        if not checksum_file.endswith(tuple(IGNORED_EXTENSION)):
            # print(checksum_file)
            filepath = os.path.join(checksum_folder, checksum_file)
            # print(filepath)
            lists_of_files.extend(
                process_file_for_match(filepath, hash_value, file_name)
            )
            # print(lists_of_files)
    return lists_of_files


def handle_different_file_matches(match, root, file, hash_number, filepath, file_dict):
    """Handles logic if local checkusm matches with supplied checksum values"""
    doc_location = str(match).split(",")[-1]
    supplied_checksum = str(match).split(",")[0].split(" ")[0]

    if hash_number in str(match) and file not in str(match):

        file_dict[os.path.join(root, file)] = match
        local_log(
            filepath,
            f"Checksum match found. Filename doesn't match in in checksum document for file: {file}!",
        )
        local_log(
            filepath,
            f"Supplied checksum: {supplied_checksum}, local checksum: {hash_number} the file found in checksum file: {doc_location}",
        )
        local_log(filepath, f"Found in document: {doc_location}")
        local_log(filepath, "--------------------------------------")

    elif file in str(match) and hash_number not in str(match).lower():

        local_log(
            filepath,
            f"The filename matches but the local and suppiled checksum does not match for file: {file}!",
        )
        local_log(
            filepath,
            f"Supplied checksum: {supplied_checksum} found in document: {doc_location}, local checksum: {hash_number}",
        )
        local_log(filepath, f"Found in document: {doc_location}")

        file_dict[os.path.join(root, file)] = match
        local_log(filepath, "--------------------------------------")
    elif file in str(match) and hash_number in str(match).lower():
        # print(f"hash: {hash_number in str(match).lower()}")
        # print(f"file: {file in str(match)}")
        # print(f"line: {str(match)}")
        doc_location = str(match).split(",")[-1]
        supplied_checksum = str(match).split(",")[0].split(" ")[0]
        local_log(filepath, f"Checksum matches for file: {file}")
        local_log(filepath, f"Local checksum: {hash_number}")
        local_log(
            filepath,
            f"Supplied checksum: {supplied_checksum}, Found in document: {doc_location}",
        )
        local_log(filepath, "--------------------------------------")

        file_dict[os.path.join(root, file)] = match

    else:
        # print("no match")
        # print(f"hash: {hash_number in str(match).lower()}")
        # print(f"file: {file in str(match)}")
        # print(f"line: {str(match)}")
        local_log(
            filepath,
            f"Checksum is not found for file: {file}",
        )
        local_log(filepath, f"Local checksum: {hash_number}")
        local_log(filepath, "--------------------------------------")
        file_dict[os.path.join(root, file)] = match

    return file_dict


def move_file_based_on_outcome(
    filepath, dpath, dir, no_match, partial_match, full_match
):
    """
    Moves file to specific directory based on the outcome of their checksum and
    file validation results.

    Return:
    -------
    str: status of the move
    """

    if dir:
        if no_match is True:
            LOGGER.warning(
                "One or more files in folder does not have checksum match made: %s",
                dpath,
            )
            LOGGER.info("File moved to: %s", os.path.join(filepath, "ingest_failed"))
            local_log(
                filepath,
                f"Please see notes above. Not all files have matching checksums for {dpath}",
            )
            local_log(
                filepath,
                "Moving whole folder to ingest_failed/ folder for manual review",
            )
            local_log(filepath, "--------------------------------------")
            shutil.move(dpath, os.path.join(filepath, f"ingest_failed/{dir}"))
        elif partial_match is True:
            LOGGER.warning(
                "One or more files in folder does not have clean match: %s", dpath
            )
            LOGGER.info("File moved to: %s", os.path.join(filepath, "ingest_partial"))
            local_log(
                filepath,
                f"Please see notes above. Not all files have cleanly matching checksums for {dpath}",
            )
            local_log(
                filepath,
                "Moving whole folder to ingest_partial/ folder for manual review",
            )
            local_log(filepath, "--------------------------------------")
            shutil.move(dpath, os.path.join(filepath, f"ingest_partial/{dir}"))
        elif full_match is True:
            LOGGER.info(
                "All files in folder have checksum and filename matched: %s", dpath
            )
            LOGGER.info("File moved to: %s", os.path.join(filepath, "ingest_match"))
            local_log(
                filepath, f"All files have matching checksums and filenames for {dpath}"
            )
            local_log(filepath, "Moving whole folder to ingest_match/")
            local_log(filepath, "--------------------------------------")
            shutil.move(dpath, os.path.join(filepath, f"ingest_match/{dir}"))

        return "Directory has been moved"
    else:
        if no_match is True:
            LOGGER.warning("This file doesn't have a checksum match: %s", dpath)
            LOGGER.info("File moved to: %s", os.path.join(filepath, "ingest_failed"))
            local_log(
                filepath,
                f"Please see notes above. Not all files have matching checksums for {dpath}",
            )
            local_log(
                filepath,
                "Moving file to ingest_failed/ folder for manual review",
            )
            local_log(filepath, "--------------------------------------")
            shutil.move(dpath, os.path.join(filepath, f"ingest_failed/"))

        elif partial_match is True:
            LOGGER.warning("This file  does not have clean match: %s", dpath)
            LOGGER.info("File moved to: %s", os.path.join(filepath, "ingest_partial"))
            local_log(
                filepath,
                f"Please see notes above. This file does not have have matching checksums for {dpath}",
            )
            local_log(
                filepath,
                "Moving file to ingest_partial/ folder for manual review",
            )
            local_log(filepath, "--------------------------------------")
            shutil.move(dpath, os.path.join(filepath, f"ingest_partial/"))
        else:
            LOGGER.info("The file have matching checksum and filename: %s", dpath)
            LOGGER.info("File moved to: %s", os.path.join(filepath, "ingest_match"))
            local_log(
                filepath, f"This file have matching checksums and filenames for {dpath}"
            )
            local_log(filepath, "Moving file to ingest_match/")
            local_log(filepath, "--------------------------------------")
            shutil.move(dpath, os.path.join(filepath, f"ingest_match/"))
        return "File has been moved"


def file_to_checksum_retrival(filepath, dir, ignore_folders):
    """This script goes through each file in ingest_check folder (ignoring the other folder) and
    find the checksum for each file and return s dictinary containing the line corresponding
    to the checksum file or false as a value and the file as the key"""

    file_dict = {}
    if dir is None:
        dir = filepath

    for root, dirs, files in os.walk(dir):
        # for each file, generate a local checksum, checks if the local checksum value is found in the 'checksum folder'
        # using the function pygrep

        dirs[:] = [d for d in dirs if d not in ignore_folders]
        # print(dirs)

        for file in files:
            if os.path.isfile(os.path.join(root, file)) and not file.endswith(
                (".log", ".txt", ".md5", ".swp")
            ):
                # print(file)
                local_log(
                    filepath,
                    f"New file/folder being processed: {os.path.join(root, file)}",
                )
                file_dict[os.path.join(root, file)] = False
                hash_number = utils.create_md5_65536(os.path.join(root, file))
                print(hash_number)

                # checksum_path = os.path.join(CHECKSUM_PATH, f"{file}.md5")
                # print(hash_number.upper(), os.path.join(root, file))

                # utils.checksum_write(checksum_path, hash_number, filepath, file)

                matching = pygrep(
                    f"{filepath}/checksum_folder",
                    hash_number.upper(),
                    os.path.join(root, file),
                )
                if bool(matching) is False:
                    local_log(filepath, f"Checksum is not found for file: {file}")
                    local_log(filepath, f"Local checksum: {hash_number}")
                    local_log(filepath, "--------------------------------------")

                # for each outcome, amend the key value in file_dict

                for match in matching:
                    print(match)
                    #
                    file_dict = handle_different_file_matches(
                        match, root, file, hash_number, filepath, file_dict
                    )

    return file_dict


def local_log(full_path, data):
    """
    Writes to local logs found in ingest_check folder, ensures that no duplicate messages are logged within the same session

    Parameters:
    ----------
      fullpath: str
         fullpath to local logs (ingest_check.log)

      data: str
         the message to be logged

      unique_message: set() | None
         A set of unique messages that have already been logged in current session
         if not, a new set os initalised.

    Returns:
    -------
    None
    """

    local_log_path = os.path.join(full_path, "ingest_check.log")
    timestamp = str(datetime.datetime.now())

    if not os.path.isfile(local_log_path):
        with open(local_log_path, "x", encoding="utf-8") as log_file:
            log_file.close()

    with open(local_log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{data} - {timestamp[0:19]}\n")
        log_file.close()

    return unique_message


def main():
    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit("Shell script failed to pass argument to Python script")

    # if not utils.check_control('power_off_all'):
    #    # LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
    #     sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    # the file path should refer to the ingest_check folder found in any qnap folders
    filepath = os.path.join(sys.argv[1], "test/Acquisitions/ingest_check")
    file_dict = {}

    # seperate file from folder, dont process folders,full matrch partial match and no match
    LOGGER.info(
        "======================Starting pre autoingest checks===================================="
    )
    local_log(
        filepath,
        "======================Starting pre autoingest checks====================================",
    )

    LOGGER.info("Generating local hash file, starting checksum validation process")
    print(filepath)
    ignore_folders = {
        "checksum_folder",
        "ingest_match",
        "ingest_failed",
        "ingest_partial",
    }

    file_list = [
        x
        for x in os.listdir(filepath)
        if os.path.isfile(os.path.join(filepath, x))
        and not x.endswith((".log", ".txt", ".md5", ".swp"))
    ]
    dir_list = [
        x
        for x in os.listdir(filepath)
        if os.path.isdir(os.path.join(filepath, x)) and x not in ignore_folders
    ]

    # print(file_list)
    # print(dir_list)

    for dir in dir_list:
        # print(dir)
        full_dir_path_to_examine = os.path.relpath(os.path.join(filepath, dir))
        # print(full_dir_path_to_examine)
        local_log(filepath, f"processing file: {full_dir_path_to_examine}")
        file_dict = file_to_checksum_retrival(
            filepath, os.path.join(filepath, dir), ignore_folders
        )
        print(file_dict)
        result = []
        partial_match = full_match = no_match = False
        for key, value in file_dict.items():
            # get file path
            file = key.split("ingest_check/")[-1]
            hash_number = utils.create_md5_65536(key)
            print(hash_number)
            if value is False:
                local_log(filepath, f"{key}: {value}")
                no_match = True

            else:
                result = value.split(",")[0]
                checksum_found = value.split(",")[-1]
                print(f"result: {result}, checksum_found")

            if (file in result and hash_number not in result) or (
                hash_number in result and file not in result
            ):
                partial_match = True

            else:
                full_match = True

        move_file_based_on_outcome(
            filepath, full_dir_path_to_examine, dir, no_match, partial_match, full_match
        )

    for file in file_list:
        dir = None
        full_file_path_to_examine = os.path.relpath(os.path.join(filepath, file))
        local_log(filepath, f"processing file: {full_file_path_to_examine}")
        file_dict = file_to_checksum_retrival(filepath, dir, ignore_folders)
        print(file_dict)
        result = []
        partial_match = full_match = no_match = False
        for key, value in file_dict.items():
            # get file path
            file = key.split("ingest_check/")[-1]
            hash_number = utils.create_md5_65536(key)
            print(hash_number)
            if value is False:
                no_match = True

            else:
                result = value.split(",")[0]
                checksum_found = value.split(",")[-1]
                print(result, checksum_found)

            if (file in result and hash_number not in result) or (
                hash_number in result and file not in result
            ):
                partial_match = True

            else:
                full_match = True

        move_file_based_on_outcome(
            filepath,
            full_file_path_to_examine,
            dir,
            no_match,
            partial_match,
            full_match,
        )


"""
                # generate a dictonary containing the file structure as well as the file and the tuple containing the the hash value,
                # location of the checksum value and if its a mismatch, full match or no match
                file_result = finding_file_structure(file_dict)
                print(file_result)
                move_file_based_on_outcome(filepath, file_result)
"""


if __name__ == "__main__":
    main()
