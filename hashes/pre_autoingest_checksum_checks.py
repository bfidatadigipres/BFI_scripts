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

IGNORED_EXTENSION = {".ini", ".DS_Store", ".mhl", ".tmp", ".dpx", ".DPX", ".log"}
LOGGED_MESSAGES = set()


def move_files(from_file, to_file):
    """
    move files to folder from source to destination directory

    Parameters:
    -----------
        from_file: string
            source of the file

        to_file: string
            destination of the file

    Raises:
    -------
        FileNotFoundError: if file does not exists
        IOError: general input/output errors during file operations

    Returns:
    --------
    str: a string is outputted if the file has been moved or not
    """
    try:
        if not os.path.exists(from_file):
            LOGGER.info(
                "Potential error: Source file does not exists / the file has already been moved"
            )
            return "Potential error: Source file does not exists / the file has already been moved"

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

    return "process complete"


def move_file_based_on_file_structure(filepath, file, move_to_files):
    """
    moves the files to the destination directory, perserving the relative file structure.

    if the file  is nested in subdirectories, this function ensures that
    the destination directory maintain the same subdirectory structure as the source
    and remove the empty folders

    Parameters:
    -----------
       filepath: str
         the full path to the media file

      file: str
        media file e.g: N_1234_567.mkv or spirited_away.mkv

      move_to_files: str
          destination directory to move folder/file to
    Returns:
    --------
    None
    """
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
    """
    This function remove all empty directory after the file has been moved

    Parameters:
    -----------
       filepath: str
         the full path to the media file

       path: str
         the media file path


    Returns:
    --------
    None

    """

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
    This function returns a dictonary containing the folder as well as
    the files inside the folders , retaining the file structure


    Parameters:
    ----------
       file_dict: dictonary
          the dictonary containing the media's full file path and tuple containing
          if the checksum matches or not, with the corresponding line in the
          checksum file (in checksum folder) and the checksum file full path


    Returns:
    --------
       folder_file_struct: dict
           dictonary containing the directory name of the file
           as the key and all the files inside that directory
           as the value


    """
    folder_file_struct = {}
    for file, value in file_dict.items():
        folder_path = os.path.dirname(file)
        # check if the folder path is either checksum_folder,
        # ingest_full_match, ingest... and etc ingest_check.log
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
    """
    replace the path that has \\ to / in order for the file to be processed.


    Parameter:
    ---------
      path: str
         path containing \\ to represent directory?

    Return:
    -------
      path: str
        the new path
    """

    return path.replace("\\", "/")


def process_file_for_match(filepath, hash_number, file_name):
    """
    Based on the filename or checksum, this function will find either inside 
    the checksum folder and returns a tuple containing the line in 
    checksum file, the checkusm file path and bool


    Parameters:
    ----------
     filepath: str
       the checksum filepath (in the checksum folder)

     hash_number: str
       the checksum value

     file_name: str
       the media filepath


    Raises:
    -------
     FileNotFoundError: if the file is not found
     IOError: general input/output errors during file operation
     Exception : for general exception


    Returns:
    -------
       lists_of_files: list
          contain the list of tuples containing matches found inside checksum file
    """

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

    file_name: string
         the media filename to match

     Returns:
     --------
     lists_of_files: list
         return a list of file matches

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

    return lists_of_files


def handle_different_file_matches(
    match, root, file, hash_number, filepath, file_dict
):
    """
    Handles logic to compare local checkusm matches with supplied checksum values

    Parameters:
    -----------

       match: list
            lists of file matches based on file_name or hash value/checksum value

       root: str
           root directory

       hash_number: str
             checksum value of the media file

       filepath: str
               path to the ingest_check.log

       file_dict: dict
             empty dict to store the results

     Return:
     -------

       file_dict: dict
             resulting dict storing the comparison outcome with the file path as the key
             and the tuple containing the supplied checksum, document location and
             status as the value

    """

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

        if hash_number in str(match[0]).lower() and file in str(match[0]):
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


def move_file_based_on_outcome(filepath, file_result, unique_message):
    """
    Moves file to specific directory based on the outcome of their checksum and
    file validation results.

    Parameters:
    -----------
      filepath: str
          root directory (i.e ingest_check)


      file_result: dict
            dictonary containing the comparison outcome with the file path as the key
            and the tuple containing the supplied checksum, document location and
            status as the value

    Return:
    -------
    None
    """
    processed_files = set()
    for results in file_result.values():
        for result in results:
            file_path, checksum_val = result[0], result[1]

            if file_path in processed_files:
                continue
            #print("\n")
            #print(len(result))
            #print(result[0][1])
            #sys.exit()

            if checksum_val[-1] is False:
                move_file_based_on_file_structure(
                    filepath, file_path, f"{filepath}/ingest_failed"
                )
                local_log(
                    filepath,
                    "Checks completed, Moving file: {file_path} to ingest_failed", 
                )
                local_log(
                    filepath,
                    "------------------------------------------------------------------",
                )
            elif checksum_val[-1] == "Miss match, same file but not the same checksum":
                LOGGER.info(
                    f"==== theres a missmatch between the file name, two or more file has the same checksum value: {file_path, checksum_val}"
                )
                unique_message = local_log(
                    filepath,
                    f"Checks completed, moving file: {file_path} to ingest_partial", 
                )
                move_file_based_on_file_structure(
                    filepath, file_path, f"{filepath}/ingest_partial/"
                )
                local_log(
                    filepath,
                    "------------------------------------------------------------------",
                )

            elif checksum_val[-1] == "Miss match, same checksum not the same file":
                LOGGER.info(
                    f"==== theres a missmatch, same checksum not the same file: {file_path, checksum_val}"
                )
                unique_message = local_log(
                    filepath,
                    f"Checks completed, moving file: {file_path} to ingest_partial", 
                )
                move_file_based_on_file_structure(
                    filepath, file_path, f"{filepath}/ingest_partial/"
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
                    filepath, file_path, f"{filepath}/ingest_match/"
                )
                unique_message = local_log(
                    filepath,
                    f"Checks completed, Moving file: {file_path} to ingest_match, perserving the file structure",
                    
                )
                local_log(
                    filepath,
                    "------------------------------------------------------------------", 
                )

            processed_files.add(file_path)


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
    None | unique_message
    """

    global LOGGED_MESSAGES

    if data in LOGGED_MESSAGES or data != "------------------------------------------------------------------":
        return 

    LOGGED_MESSAGES.add(data)

    local_log_path = os.path.join(full_path, "ingest_check.log")
    timestamp = str(datetime.datetime.now())

    if not os.path.isfile(local_log_path):
        with open(local_log_path, "x", encoding="utf-8") as log_file:
            log_file.close()

    with open(local_log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{data} - {timestamp[0:19]}\n")
        log_file.close()

    #return unique_message


def rearrange_common_folders(common_folder, src_path, des_path):
    """
    Based on common folders found, this function will move all contents from source to destination directory,
    maintain the file structure and remove empty directories

    Parameters:
    ----------

      common_folder: set
         unique list of folder found in more than one ingest_xxxx location

      src_path: str
         source path

      des_path: str
        destination path


    Returns
    -------
    None
    """
    LOGGER.info(
        f"moving folder {src_path} to {des_path} as there's a common folder in both source and destination path", 
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
    unique_message = set()

    # seperate file from folder, dont process folders,full matrch partial match and no match
    LOGGER.info(
        "======================Starting pre autoingest checks===================================="
    )
    local_log(
        filepath,
        "======================Starting pre autoingest checks===================================="
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
                move_file_based_on_outcome(filepath, file_result, unique_message)

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
            f"Common folders: {common_folder_in_failed_match} found!!! Moving folder contents to ingest_failed, keeping the file structure"
        )

    if common_folder_in_partial_match:
        rearrange_common_folders(
            common_folder_in_partial_match,
            f"{filepath}/ingest_match",
            f"{filepath}/ingest_partial",
        )
        local_log(
            filepath,
            f"Common folders: {common_folder_in_partial_match} found!!! Moving folder contents to ingest_partial, keeping the file structure"
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
