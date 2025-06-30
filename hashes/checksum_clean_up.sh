#!/bin/bash -x

# ===========================================
# Launcher script for checksum_clean_up.py ==
# ===========================================

LOG_LEAD="$LOG_PATH"
CODE_LEAD="$CODE"
PY3_LAUNCH="$PYENV313"
LOG="${LOG_LEAD}checksum_clean_up.log"
CHECKSUM_PATH="${LOG_LEAD}checksum_md5/"
CHECKSUM_LIST="${CODE_LEAD}checksum_list.txt"

function control {
    boole=$(cat "${CONTROL_JSON}" | grep "pause_scripts" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      echo 'Control json requests script exit immediately'
      exit 0
    fi
}

# Control check inserted into code
control

# Build list of MD5 files and output to log for reference
echo " ===================== SHELL SCRIPT LAUNCH CHECKSUM_CLEAN_UP =========================== " >> "${LOG}"
find "$CHECKSUM_PATH" -name '*.md5' | sort -R |  sort -n -k10.12 > "${CHECKSUM_LIST}"
echo "List of MD5 files to be processed:" >> "$LOG"
cat "$CHECKSUM_LIST" >> "$LOG"

# Pass list to Python3 multiple jobs
grep '/mnt/' "${CHECKSUM_LIST}" | parallel --jobs 10 "$PY3_LAUNCH ${CODE_LEAD}hashes/checksum_clean_up.py {}"

echo " =============================== SHELL SCRIPT END ====================================== " >> "${LOG}"
