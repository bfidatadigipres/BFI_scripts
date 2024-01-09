#!/bin/bash -x

# ==========================================
# Launcher script for stora2_housekeeping.py
# ==========================================

code_pth="${CODE_BFI}document_en_15907/"
date_FULL=$(date +'%Y-%m-%d  - %T')
year=$(date +'%Y')
last_year=$(date -d "$(date +%Y) -1 week" +'%Y')
log="${LOG_PATH}stora2_housekeeping.log"

function control {
    boole=$(cat "${CODE}stora_control.json" | grep "stora_qnap" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
        echo "Control json requests script exit immediately" >> "$log"
        exit 0
    fi
}

# Check control
control

# Log entries ahead of python launch
echo " ========================= Start stora2_housekeeping in year folders == $date_FULL" >> "$log"
echo " == Shell script creating dump_text.txt for date path(s) == $date_FULL" >> "$log"

if [ "$year" = "$last_year" ]; then
    # Generate txt file containing all folders 5 directories deep only:
    find "${STORAGE_PATH}${year}" -maxdepth 4 -mindepth 4 -type d > "${code_pth}stora2_dump_text.txt"
else
    echo "Years differ, checking ${year} and ${last_year} paths for folders"
    # Generate txt file containing all folders 5 directories deep only:
    find "${STORAGE_PATH}${year}" -maxdepth 4 -mindepth 4 -type d > "${code_pth}stora2_dump_text.txt"
    find "${STORAGE_PATH}${last_year}" -maxdepth 4 -mindepth 4 -type d >> "${code_pth}stora2_dump_text.txt"
fi

echo " == Launching python3 script to clean up and delete empty folders from date paths == $date_FULL" >> "$log"
# Launch python3 script
"$PYENV_DDP" "${CODE_BFI}document_en_15907/stora2_housekeeping.py"

echo " ========================= Shell script finished == $date_FULL" >> "$log"
