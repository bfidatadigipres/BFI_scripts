#!/bin/bash -x

# =========================================================
# Launcher script for metadata_clean_up.py
# =========================================================

DATE_FULL=$(date +'%Y-%m-%d - %T')

# Local variables from environmental vars
CODE_PTH="${CODE}hashes/"
LOG="${LOG_PATH}metadata_clean_up.log"

function control {
    boole=$(cat "${CONTROL_JSON}" | grep "pause_scripts" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      exit 0
    fi
}

# Control check inserted into code
control

# replace list to ensure clean data
rm "${CODE_PTH}metadata_clean_up_list.txt"
touch "${CODE_PTH}metadata_clean_up_list.txt"

echo " ========================= SHELL SCRIPT LAUNCH ========================== $DATE_FULL" >> "${LOG}"
echo " == Start list extraction for metadata folder CID_mediainfo == " >> "${LOG}"
echo " == Shell script creating metadata_clean_up_list.txt output for parallel launch of Python scripts == " >> "${LOG}"

# Command to build unique sorted list from cid_mediainfo path
find "${CID_MEDIAINFO}" -name "*_TEXT.txt" > "${CODE_PTH}metadata_clean_up_list.txt"
find "${CID_MEDIAINFO}" -name "*_EXIF.txt" >> "${CODE_PTH}metadata_clean_up_list.txt"

echo " == Launching GNU parallel to run muliple Python3 scripts for metadata_clean_up == " >> "${LOG}"
grep '/mnt/' "${CODE_PTH}metadata_clean_up_list.txt" | parallel --jobs 20 "${PYENV313} ${CODE_PTH}metadata_clean_up.py {}"

echo " ========================= SHELL SCRIPT END ========================== $DATE_FULL" >> "${LOG}"

