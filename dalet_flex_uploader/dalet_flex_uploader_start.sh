#!/bin/bash -x

# ==========================================
# Launcher script for Dalet Flex Uploaded ==
# ==========================================

DATE_FULL=$(date +'%Y-%m-%d - %T')

# Paths
PLAYER_ARCHIVE="${QNAP_04}BFI_replay/"
DALET_PATH="${CODE}dalet_flex_uploader/"

PLAYER_ARCHIVE1="${PLAYER_ARCHIVE}flex_uploads/"
SPLIT_SOURCE="${PLAYER_ARCHIVE}to_be_transferred/json_split_test/"
FILE_LIST="${DALET_PATH}flex_upload_file_list.txt"
SPLIT_LIST="${DALET_PATH}flex_upload_split_file_list.txt"
LOG_PATH="${PLAYER_ARCHIVE}dalet_flex_upload.log"

# replace list to ensure clean data
rm "$FILE_LIST" "$SPLIT_LIST"
touch "$FILE_LIST" "$SPLIT_LIST"

echo " ========================= SHELL SCRIPT LAUNCH ========================== $DATE_FULL" >> "$LOG_PATH"
echo " == Start dalet_flex_uploader scripts in $PLAYER_ARCHIVE1 == " >> "$LOG_PATH"
echo " == Shell script creating text output for parallel launch of Python scripts == " >> "$LOG_PATH"

# Command to build MKV/MOV list and JSON list for Dalet upload
find "${PLAYER_ARCHIVE1}" -maxdepth 1 -mindepth 1 -iname "*.mkv" -mmin +30 >> "${FILE_LIST}"
find "${PLAYER_ARCHIVE1}" -maxdepth 1 -mindepth 1 -iname "*.mov" -mmin +30 >> "${FILE_LIST}"
find "${SPLIT_SOURCE}" -maxdepth 1 -mindepth 1 -iname "*.json" -mmin +30 >> "${SPLIT_LIST}"

echo " == Launching GNU parallel to run muliple Python3.11 scripts for Dalet uploads == " >> "${LOG_PATH}"
grep '/mnt/' "${FILE_LIST}" | parallel --jobs 5 "${PYENV311} ${DALET_PATH}dalet_flex_uploader_cron.py {}"
grep '/mnt/' "${SPLIT_LIST}" | parallel --jobs 5 "${PYENV311} ${DALET_PATH}dalet_flex_uploader_cron.py {}"

echo " ========================= SHELL SCRIPT END ========================== $DATE_FULL" >> "${LOG_PATH}"
