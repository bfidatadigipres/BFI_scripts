#!/bin/bash -x

# =====================================
# Launcher script for checksum_maker.py
# =====================================

DATE_FULL=$(date +'%Y-%m-%d - %T')

# Received data
PTH="$1"
JOBS="$2"
PATH_INSERT="${1//['/']/_}"

# Paths from environmental variables
LOG_LEAD="$LOG_PATH"
CODE_LEAD="$CODE"
PY3_LAUNCH="$PYENV311"
LOG="${LOG_LEAD}checksum_maker${PATH_INSERT}launch.log"
AUTOINGEST="${PTH}autoingest/black_pearl_ingest/"
AUTOINGEST2="${PTH}autoingest/black_pearl_netflix_ingest/"
AUTOINGEST3="${PTH}autoingest/black_pearl_amazon_ingest/"
HASHES="$HASH_PATH"
DUMP_TO="${HASHES}${PATH_INSERT}autoingest_file_list.txt"

# replace list to ensure clean data
rm "${DUMP_TO}"
touch "${DUMP_TO}"

# Directory path to run shell script
cd "${CODE_LEAD}hashes/"

echo " ========================= CHECKSUM MAKER -- PTH -- SHELL START ========================== $DATE_FULL" >> "${LOG}"
echo " == Start checksum_maker scripts in AUTOINGEST == " >> "${LOG}"
echo " == Shell script creating autoingest file list for parallel launch of Python scripts == " >> "${LOG}"

# Command to build file list from autoingest folder ingest/ contents
find "${AUTOINGEST}" -type f -mmin +10 >> "${DUMP_TO}"
find "${AUTOINGEST2}" -type f -mmin +10 >> "${DUMP_TO}"
find "${AUTOINGEST3}" -type f -mmin +10 >> "${DUMP_TO}"
list=$(cat "${DUMP_TO}" | tr " " "\n")
echo "${PTH} files to have checksum's generated *if not already created*:" >> "${LOG}"
echo "${list}" >> "${LOG}"

# Hardcoded Python3 version with local library dependency installations necessary for script (tenacity)
echo " == Launching GNU parallel to run multiple Python3 scripts for MD5 generation == " >> "${LOG}"
grep '/mnt/' "${DUMP_TO}" | parallel --jobs "$JOBS" "$PY3_LAUNCH checksum_maker_mediainfo.py {}"

DATE_CLOSE=$(date +'%Y-%m-%d - %T')
echo " ========================================== SHELL SCRIPT END ============================================= $DATE_CLOSE" >> "${LOG}"
