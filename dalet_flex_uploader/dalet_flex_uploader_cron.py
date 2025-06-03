#!/usr/bin/python3

"""
Script to write MKV/JSON files to S3 Bucket for Flex
** To be run from launching shell script to populate sys.argv[1] **

Python functions:
1. Check os.path.exists(), and being storing log data into list
2. Receive filepath from sys, and split filepath from file name
   populate 'filepath' and 'file_name' variable for use in code
3. Launch s3 boto.client to begin copy to BUCKET
4. Capture success statement and move filepath to PA_SUCCESS/file_name
5. If copy errors, 3 standard retries made (aws config file updated)

NOTES: Script configured for test files startswith(N_) commented out
       Shell launch script directed at temporary folder also

Python 3.8
"""

import datetime
import os
import shutil
import sys
import time

import boto3
import botocore

# Global variables
H22_PLAYER_ARCHIVE = os.path.join(os.environ["QNAP_04"], "BFI_replay/")
PA_ERRORS = os.path.join(H22_PLAYER_ARCHIVE, "upload_errors")
PA_SUCCESS = os.path.join(H22_PLAYER_ARCHIVE, "upload_completed")
BUCKET = os.environ.get("DALET_BUCKET")


def dump_to_log(log_list):
    """
    Check if log exists, if not create
    Append all of move data to log in one hit
    so that concurrent runs don't interweave
    """
    log_path = os.path.join(H22_PLAYER_ARCHIVE, "dalet_flex_upload.log")
    data = "\n".join(log_list)

    if not os.path.isfile(log_path):
        with open(log_path, "x") as log:
            log.close()
    with open(log_path, "a+") as log:
        log.write(f"{data}\n\n")
        log.close()


def main():
    """
    Receive sys.argv[1] from Shell launch script
    Store up log comments into list to add to log
    in one go at close of script
    """
    log_list = []

    if len(sys.argv) < 2:
        log_list.append(f"SCRIPT EXITING: Error with shell script input:\n {sys.argv}")
        dump_to_log(log_list)
        sys.exit()

    if os.path.exists(sys.argv[1]):
        fullpath = sys.argv[1]
        path_split = os.path.split(fullpath)
        file_name = path_split[1]
        print(file_name)

        if file_name.endswith((".mov", ".mkv", ".MOV", ".MKV")):
            now = str(datetime.datetime.now())
            print(now[:19])
            log_list.append(f"--------- FOUND {file_name} - {now[:19]} ---------")
            if check_if_uploaded(file_name) is True:
                log_list.append("** Filename found in uploaded folder - skipping!")
                sys.exit()
            log_list.append(f"New file found in H22 PlayerArchive folder: {fullpath}")
            log_list.append(f"Uploading file to AWS bucket {BUCKET}")
            print("Upload start")
            tic = time.perf_counter()
            success = upload(fullpath, None, "video")
            toc = time.perf_counter()
            upload_time = (toc - tic) / 60
            print(upload_time)
            if success is True:
                move_path = os.path.join(PA_SUCCESS, file_name)
                log_list.append(f"File {file_name} uploaded successfully")
                log_list.append(
                    f"Upload to AWS took {round(upload_time, 2)} in minutes (or parts of)"
                )
                log_list.append(f"Moving to completed folder {move_path}")
                try:
                    shutil.move(fullpath, move_path)
                    if not os.path.exists(move_path):
                        log_list.append(
                            "WARNING: File did not arrive in upload_complete/ folder!"
                        )
                        raise Exception
                    else:
                        log_list.append(
                            f"MOVE SUCCESS: {file_name} moved to {move_path}"
                        )
                except Exception as error:
                    log_list.append(
                        f"MOVE FAIL: {fullpath} did not move to {PA_SUCCESS}\n{error}"
                    )
                    raise error

            else:
                log_list.append(f"Copy to AWS bucket failed: {success}")
                log_list.append(f"Failed upload to AWS took {upload_time} minutes")
                log_list.append(
                    f"Five retry attempts failed, please check file {file_name}"
                )
                log_list.append(
                    f"Moving to errors folder {os.path.join(PA_ERRORS, file_name)}"
                )
                try:
                    shutil.move(fullpath, os.path.join(PA_ERRORS, file_name))
                    log_list.append(f"MOVE SUCCESS: {fullpath} moved to {PA_ERRORS}")
                except Exception as error:
                    log_list.append(
                        f"MOVE FAIL: {fullpath} did not move to {PA_ERRORS}\n{error}"
                    )
                    raise error

            now = str(datetime.datetime.now())
            log_list.append(f"--------- COMPLETE {file_name} - {now[0:19]} ---------")

        elif file_name.endswith((".JSON", ".json")):
            vname = os.path.splitext(file_name)[0]
            video_file = ""
            match_list = [x for x in os.listdir(path_split[0]) if vname in str(x)]

            if len(match_list) == 2:
                if match_list[0] != file_name:
                    video_file = match_list[0]
                elif match_list[1] != file_name:
                    video_file = match_list[1]
                else:
                    log_list.append(
                        f"Error locating matching file to {fullpath}. Exiting."
                    )
                    dump_to_log(log_list)
                    sys.exit()
            elif len(match_list) == 1:
                log_list.append("No video file in folder yet. Script exiting.")
                dump_to_log(log_list)
                sys.exit()
            elif len(match_list) > 2:
                log_list.append("Too many files for JSON retrieved. Exiting.")
                dump_to_log(log_list)
                sys.exit()
            else:
                log_list.append(f"Error processing {fullpath}. Exiting.")
                dump_to_log(log_list)
                sys.exit()

            if not video_file:
                log_list.append("Problem retrieving video file name. Script exiting.")
                dump_to_log(log_list)
                sys.exit()

            video_fullpath = os.path.join(path_split[0], video_file)
            now = str(datetime.datetime.now())

            log_list.append(
                f"--------- FOUND {file_name} {video_file} - {now[:19]} ---------"
            )
            if check_if_uploaded(video_file) is True:
                log_list.append("** Filename found in uploaded folder - skipping!")
                sys.exit()

            log_list.append(
                f"New split files found in H22 PlayerArchive folder:\n{fullpath}\n{video_fullpath}"
            )
            log_list.append(f"Uploading files to AWS bucket {BUCKET}")
            print("Split file / JSON upload start")
            tic = time.perf_counter()
            success = upload(fullpath, video_fullpath, "json")
            toc = time.perf_counter()
            upload_time = (toc - tic) / 60
            print(upload_time)

            if success is True:
                move_path1 = os.path.join(PA_SUCCESS, file_name)
                move_path2 = os.path.join(PA_SUCCESS, video_file)
                log_list.append(f"Files {file_name} {video_file} uploaded successfully")
                log_list.append(
                    f"Upload to AWS took {round(upload_time, 2)} in minutes (or parts of)"
                )
                log_list.append(f"Moving to completed folder {move_path1} {move_path2}")
                try:
                    shutil.move(fullpath, move_path1)
                    log_list.append(f"MOVE SUCCESS: {file_name} moved to {move_path1}")
                except Exception as error:
                    log_list.append(
                        f"MOVE FAIL: {fullpath} did not move to {PA_SUCCESS}\n{error}"
                    )
                    raise error
                try:
                    shutil.move(video_fullpath, move_path2)
                    log_list.append(f"MOVE SUCCESS: {video_file} moved to {move_path2}")
                except Exception as error:
                    log_list.append(
                        f"MOVE FAIL: {video_fullpath} did not move to {PA_SUCCESS}\n{error}"
                    )
                    raise error

            else:
                log_list.append(f"Copy to AWS bucket failed: {success}")
                log_list.append(f"Failed upload to AWS took {upload_time} minutes")
                log_list.append(
                    f"Five retry attempts failed, please check file {file_name} {video_file}"
                )
                log_list.append(
                    f"Moving to errors folder {os.path.join(PA_ERRORS, file_name)}"
                )
                try:
                    shutil.move(fullpath, os.path.join(PA_ERRORS, file_name))
                    log_list.append(f"MOVE SUCCESS: {fullpath} moved to {PA_ERRORS}")
                except Exception as error:
                    log_list.append(
                        f"MOVE FAIL: {fullpath} did not move to {PA_ERRORS}\n{error}"
                    )
                    raise error
                try:
                    shutil.move(video_fullpath, os.path.join(PA_ERRORS, video_file))
                    log_list.append(
                        f"MOVE SUCCESS: {video_fullpath} moved to {PA_ERRORS}"
                    )
                except Exception as error:
                    log_list.append(
                        f"MOVE FAIL: {video_fullpath} did not move to {PA_ERRORS}\n{error}"
                    )
                    raise error

            now = str(datetime.datetime.now())
            log_list.append(
                f"--------- COMPLETE {file_name} {video_file} - {now[0:19]} ---------"
            )

        # Write to human readable log
        dump_to_log(log_list)


def check_if_uploaded(fname):
    """
    Check in upload pth for name match
    """
    uploaded_files = [
        x for x in os.listdir(PA_SUCCESS) if os.path.isfile(os.path.join(PA_SUCCESS, x))
    ]
    for file in uploaded_files:
        if fname == file:
            return True


def upload(fpath, vpath, arg):
    """
    Retrieve fullpath, split file name from path
    Upload to s3 bucket with check to ensure
    all worked okay. Return boolean so retry
    can occur if fails
    """

    # Create the client
    s3 = boto3.client("s3")

    if arg == "video":
        file_name_split = os.path.split(fpath)[1]
        file_name = f"not-platform-ready/no_split/{file_name_split}"

        # Upload returns True if successful or raises error
        try:
            s3.upload_file(fpath, BUCKET, file_name)
            return True
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "LimitExceededException":
                return "API call limit exceeded. Retrying using boto3 standard retry"
            else:
                return err
        except Exception as error:
            return error

    elif arg == "json":
        json_name = os.path.split(fpath)[1]
        video_name = os.path.split(vpath)[1]
        file_name = f"not-platform-ready/split/video/{video_name}"
        file_name_json = f"not-platform-ready/split/json/{json_name}"

        # Upload returns True if successful or raises error
        try:
            s3.upload_file(fpath, BUCKET, file_name_json)
            s3.upload_file(vpath, BUCKET, file_name)
            return True
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "LimitExceededException":
                return "API call limit exceeded. Retrying using boto3 standard retry"
            else:
                return err
        except Exception as error:
            return error


if __name__ == "__main__":
    main()
