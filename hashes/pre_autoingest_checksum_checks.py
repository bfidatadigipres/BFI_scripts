"""
pre_autoingest_checsum_checks that checks if the supplied checksum provided 
by the suppliers (Amazon, Netflix and more) or their own tercopy transfer.
for a media file is the same as the local generated checksum. 
Documents/ all actions are stored in ingest_check.log stored inside ingest_check.

This script is ran before running autoingest.py.

main()

1. checks if the correct amount of arguments is passed into the command line/crontab
2. check if any of the system are down by looking into downtime_control.json
3. goes through the ingest check folder
  a. ignore all other folder such as checksum_folder and anything starting 
     with ingest_xxxxx
  b. goes through all the media files supplied in the ingest_check folder 
     and checks if it doesnt start with .md5, .txt or .log
  c. creates a dictonary(file_dict) where all the values are set to False 
     and the key acts as the filepath to the media file,
  d. use functon 'pygrep' that goes through the checksum folder to find 
     any matching checksum or filename and get and store the line 
     corresponding to the match as a tuple
  e. if there's a mismatch between the locally generated and the supplier's 
     checksum value/hash_number or the checksum is the same but doesnt have the correct file name, 
     the values are amended to reflect the change
  f. creates a new dictonary(file_results) where it would store the key as the 
     path to the folders inside ingest_check and the value containing the file inside the folder 
     to maintain the file structure. This is done using the function finding_file_structure
  g. goes through each value inside file_results
     a. if the first element in the tuple is set to False, move the file into the ingest_failed
     b. if the tuple contains 'missmatch.....', the file is  move to ingest_partial
     c. if a tuple contain a true, the file is moved to ingest_match
     note: if these files resides in the folder, then it creates a folder 
     (inside folder starting with ingest_xxxx) with the same name 
     and input the file into that folder, 
     you will have folders with the same name into different ingest folders. 
     This is dealt with later. the folders inside ingest_check are then removed.
4. the script then checks if there's mutiple folders with the same name in
   multiple ingest_xxxxx folders. 
   If found the results are stored into a set based on these scenatios:
  a. folders with the same name can be found in ingest_match and ingest_failed
  b. folders with the same name can be found in ingest_match and ingest_partial
  c. folders with the same name can be found in ingest_partial and ingest_failed
5. if the scenario falls into 4a or 4c, then the entire folder is moved to ingest_failed, 
   the folder inside the source folder is removed
6. if the scenario falls into 4b, then the entire folder is moved into ingest_partial. 
   The folder inside the source folder is removed

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

IGNORED_EXTENSION = {".ini", ".DS_Store", ".mhl", ".tmp", ".dpx", ".DPX", ".log"}


def move_files(from_file, to_file):
    """
    move files to folder

    """
    try:
        if not os.path.exists(from_file):
            LOGGER.info(
                "Potential error: Source file does not exists / the file has already been moved"
            )
            return ""

        destination_dir = os.path.dirname(to_file)

        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

        shutil.move(from_file, destination_dir)
        LOGGER.info(
            f"File successfully moved from source: {from_file} to destination: {to_file}"
        )
        return f"File successfully moved from source: {from_file} to destination: {to_file}"

    except (FileNotFoundError, IOError) as e:
        LOGGER.error(f"Error reading {from_file}: {e}")

    except Exception as e:
        LOGGER.warning(f"Error: {e}")
        return "file doesnt exists"


def move_file_based_on_file_structure(filepath, file, move_to_files):

    path = Path(file).relative_to(filepath)
    destination_file = ""

    if len(path.parts) <= 1:
        destination_file = Path(move_to_files) / path.name

    else:
        LOGGER.info(f"this file is a subdirectory: {file}")
        sub_dir = Path(f"{move_to_files}") / path.parent
        if not sub_dir.exists():
            sub_dir.mkdir(parents=True, exist_ok=True)

        destination_file = os.path.join(sub_dir, path.name)

    move_files(file, destination_file)
    clean_up_empty_directory(filepath, path)


def clean_up_empty_directory(filepath, path):

    current_path = Path(filepath) / path.parent
    while current_path != Path(filepath):
        if not current_path.exists():
            break

        if not os.listdir(current_path):
            LOGGER.info(f"removing folder: {current_path}")
            shutil.rmtree(current_path)
        else:
            break

        current_path = current_path.parent


def finding_file_structure(file_dict):
    """
    This function returns a dictonary containing the folder as well as the files inside the folders , retaining the file structure
    """
    folder_file_struct = {}
    for file, value in file_dict.items():
        folder_path = os.path.dirname(file)
        # check if the folder path is either checksum_folder, ingest_full_match, ingest... and etc ingest_check.log
        if (
            "checksum_folder" in folder_path
            or "ingest_full_match" in folder_path
            or "ingest_failed" in folder_path
            or "ingest_partial" in folder_path
        ):
            continue
        if folder_path not in folder_file_struct:
            folder_file_struct[folder_path] = []
        folder_file_struct[folder_path].append((file, value))

    return folder_file_struct


def normalised_file(path):

    return path.replace("\\", "/")


def process_file_for_match(filepath, hash_number, file_name):
    lists_of_files = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                # print(line)
                normalised_path = normalised_file(line)
                if (
                    re.search(hash_number, normalised_path, re.IGNORECASE)
                    or file_name in normalised_path
                ):
                    print(hash_number, " found")
                    lists_of_files.append(
                        (
                            normalised_path,
                            filepath,
                            True,
                        )
                    )

    except (FileNotFoundError, IOError) as e:
        LOGGER.error(f"Error reading {filepath}: {e}")
    except Exception as e:
        LOGGER.error(f"Unexpected error processing {filepath}: {e}")

    return lists_of_files


def pygrep(folder_name, hash_value, file_name):
    """
    This function represent the python version of the linux command 'grep'

    Parameters:
    -----------

    folder_name: string
        the folder path to the checksum value.

    hash_value: string
        the hash value from the hash function

    Returns:
    --------
    checker: (bool, result)
        return true if the hash value is in the file with the corresponding line in the file

    """
    lists_of_files = []
    for root, _, files in os.walk(folder_name):
        for file in files:
            if not file.endswith(tuple(IGNORED_EXTENSION)):
                filepath = os.path.join(root, file)
                print(filepath)
                lists_of_files.extend(
                    process_file_for_match(filepath, hash_value, file_name)
                )
            # print(lists_of_files)

    return lists_of_files


def handle_different_file_matches(match, root, file, hash_number, filepath, file_dict):
    """Handles logic if local checkusm matches with supplied checksum values"""

    if hash_number in str(match[0]) and file not in str(match[0]):

        file_dict[os.path.join(root, file)] = (
            match[0],
            match[1],
            "Miss match, same checksum not the same file",
        )
        local_log(
            filepath,
            f"Checksum match found. Filename doesn't match in in checksum document for file: {file}!",
        )
        local_log(
            filepath,
            f"Supplied checksum: {str(match[0]).split()[0]}, local checksum: {hash_number} the file found in checksum file: {str(match[0].split()[-1])}",
        )
        local_log(filepath, f"Found in document: {(match[1])}")

    elif file in str(match[0]) and hash_number not in str(match[0]).lower():

        local_log(
            filepath,
            f"The filename matches but the local and suppiled checksum does not match for file: {file}!",
        )
        local_log(
            filepath,
            f"Supplied checksum: {str(match[0]).split()[0]} found in document: {str(match[0]).split()[-1]}, local checksum: {hash_number}",
        )
        local_log(filepath, f"Found in document: {(match[1])}")

        file_dict[os.path.join(root, file)] = (
            match[0],
            match[1],
            "Miss match, same file but not the same checksum",
        )

    else:

        if hash_number in str(match[0]) and file in str(match[0]):
            local_log(filepath, f"Checksum matches for file: {file}")
            local_log(filepath, f"Local checksum: {hash_number}")
            local_log(
                filepath,
                f"Supplied checksum: {str(match[0]).split()[0]}, Found in document: {match[1]}",
            )
        else:
            local_log(
                filepath,
                f"Checksum is not found for file: {file}",
            )
            local_log(filepath, f"Local checksum: {hash_number}")

        file_dict[os.path.join(root, file)] = match

    return file_dict


def move_file_based_on_outcome(filepath, file_result):
    for _, results in file_result.items():
        for result in results:
            if result[-1] is False:
                move_file_based_on_file_structure(
                    filepath, result[0], f"{filepath}/ingest_failed"
                )
                local_log(
                    filepath,
                    "Checks completed, Moving file: {result} to ingest_failed",
                )
                local_log(
                    filepath,
                    "------------------------------------------------------------------",
                )
            elif result[1][-1] == "Miss match, same file but not the same checksum":
                LOGGER.info(
                    f"==== theres a missmatch between the file name, two or more file has the same checksum value: {result}"
                )
                local_log(
                    filepath,
                    f"Checks completed, moving file: {result[0]} to ingest_partial",
                )
                move_file_based_on_file_structure(
                    filepath, result[0], f"{filepath}/ingest_partial/"
                )
                local_log(
                    filepath,
                    "------------------------------------------------------------------",
                )

            elif result[1][-1] == "Miss match, same checksum not the same file":
                LOGGER.info(
                    f"==== theres a missmatch, same checksum not the same file: {result}"
                )
                local_log(
                    filepath,
                    f"Checks completed, moving file: {result[0]} to ingest_partial",
                )
                move_file_based_on_file_structure(
                    filepath, result[0], f"{filepath}/ingest_partial/"
                )
                local_log(
                    filepath,
                    "------------------------------------------------------------------",
                )
            else:
                LOGGER.info(
                    "=======local and supplied md5 file are the same============"
                )
                move_file_based_on_file_structure(
                    filepath, result[0], f"{filepath}/ingest_match/"
                )
                local_log(
                    filepath,
                    f"Checks completed, Moving file: {result[0]} to ingest_match, perserving the file structure",
                )
                local_log(
                    filepath,
                    "------------------------------------------------------------------",
                )


def local_log(full_path, data, unique_message=None):

    unique_message = unique_message or set()

    if data in unique_message:
        return unique_messages

    unique_message.add(data)

    local_log_path = os.path.join(full_path, "ingest_check.log")
    timestamp = str(datetime.datetime.now())

    if not os.path.isfile(local_log_path):
        with open(local_log_path, "x", encoding="utf-8") as log_file:
            log_file.close()

    with open(local_log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{data} - {timestamp[0:19]}\n")
        log_file.close()

    # return unique_message | {data}


def rearrange_common_folders(common_folder, src_path, des_path):

    LOGGER.info(
        f"moving folder {src_path} to {des_path} as there's a common folder in both source and destination path"
    )
    for folder in common_folder:
        shutil.copytree(
            f"{src_path}/{folder}", f"{des_path}/{folder}", dirs_exist_ok=True
        )
        shutil.rmtree(f"{src_path}/{folder}")
    LOGGER.info(
        f"moving folder {src_path} to {des_path}, moving the folder is now complete"
    )


def main():
    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit("Shell script failed to pass argument to Python script")

    # if not utils.check_control('power_off_all'):
    #    # LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
    #     sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    # the file path should refer to the ingest_check folder found in any qnap folders
    filepath = os.path.join(sys.argv[1], "Acquisitions/ingest_check")
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

    for root, dirs, files in os.walk(filepath):
        # for each file, generate a local checksum, checks if the local checksum value is found in the 'checksum folder'
        # using the function pygrep

        dirs[:] = [d for d in dirs if d not in ignore_folders]
        print(dirs)

        for file in files:
            if os.path.isfile(os.path.join(root, file)) and not file.endswith(
                (".log", ".txt", ".md5", ".swp")
            ):
                local_log(
                    filepath,
                    f"New file/folder being processed: {os.path.join(root, file)}",
                )
                file_dict[os.path.join(root, file)] = False
                hash_number = utils.create_md5_65536(os.path.join(root, file))

                checksum_path = os.path.join(CHECKSUM_PATH, f"{file}.md5")
                print(hash_number.upper(), os.path.join(root, file))

                utils.checksum_write(checksum_path, hash_number, filepath, file)

                matching = pygrep(
                    f"{filepath}/checksum_folder",
                    hash_number.upper(),
                    os.path.join(root, file),
                )
                # print(matching)

                # for each outcome, amend the key value in file_dict
                for match in matching:

                    file_dict = handle_different_file_matches(
                        match, root, file, hash_number, filepath, file_dict
                    )

                print(file_dict)

                # generate a dictonary containing the file structure as well as the file and the tuple containing the the hash value,
                # location of the checksum value and if its a mismatch, full match or no match
                file_result = finding_file_structure(file_dict)
                print(file_result)
                move_file_based_on_outcome(filepath, file_result)

    local_log(
        filepath,
        "Checking if the same folder can be found in multiple ingest folders",
    )
    list_of_failed_dirs = os.listdir(f"{filepath}/ingest_failed")
    list_of_partial_dirs = os.listdir(f"{filepath}/ingest_partial")
    list_of_match_dirs = os.listdir(f"{filepath}/ingest_match")

    common_folder_in_failed_partial = set(list_of_failed_dirs).intersection(
        list_of_partial_dirs
    )
    common_folder_in_failed_match = set(list_of_failed_dirs).intersection(
        list_of_match_dirs
    )
    common_folder_in_partial_match = set(list_of_partial_dirs).intersection(
        list_of_match_dirs
    )

    LOGGER.info(common_folder_in_failed_partial)
    LOGGER.info(common_folder_in_failed_match)
    LOGGER.info(common_folder_in_partial_match)

    if common_folder_in_failed_partial:
        rearrange_common_folders(
            common_folder_in_failed_partial,
            f"{filepath}/ingest_partial",
            f"{filepath}/ingest_failed",
        )
        local_log(
            filepath,
            f"Common folders: {common_folder_in_failed_partial} found!!! Moving folder contents to ingest_failed, keeping the file structure",
        )

    if common_folder_in_failed_match:
        rearrange_common_folders(
            common_folder_in_failed_match,
            f"{filepath}/ingest_match",
            f"{filepath}/ingest_failed",
        )
        local_log(
            filepath,
            f"Common folders: {common_folder_in_failed_match} found!!! Moving folder contents to ingest_failed, keeping the file structure",
        )

    if common_folder_in_partial_match:
        rearrange_common_folders(
            common_folder_in_partial_match,
            f"{filepath}/ingest_match",
            f"{filepath}/ingest_partial",
        )
        local_log(
            filepath,
            f"Common folders: {common_folder_in_partial_match} found!!! Moving folder contents to ingest_partial, keeping the file structure",
        )

    LOGGER.info(
        "======================pre autoingest checks End===================================="
    )
    local_log(
        filepath,
        "======================pre autoingest checks End====================================",
    )


if __name__ == "__main__":
    main()
