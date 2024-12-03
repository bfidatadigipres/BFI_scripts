#!/bin/bash -x

# ============================================
# === DPX sequence TAR preservation script ===
# ============================================

# Global paths exctracted from environmental vars
SCRIPT_LOG="${QNAP_FILM}${DPX_SCRIPT_LOG}"
DPX_PATH="${QNAP_FILM}${DPX_WRAP}"
DESTINATION="${QNAP_FILM}${DPX_TARRED}"

function control {
    boole=$(cat "${CONTROL_JSON}" | grep "power_off_all" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      echo 'Control json requests script exit immediately'
      exit 0
    fi
}

# Control check inserted into code
control

# Function to write output to log, bypass echo calls, using just log + statement.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${SCRIPT_LOG}dpx_tar_wrapping_checksum.log"

# Regenerate list of TAR files and check if populated
list=$(ls "$DPX_PATH")
echo "$list" | sort -n -k10.12 > "${QNAP_FILM}${TAR_PRES}tar_list.txt"
[ -s "${QNAP_FILM}${TAR_PRES}tar_list.txt" ]
num=$(echo "$?")

if [ $num=0 ]
  then
    # Start TAR preparations and wrap of DPX sequences
    log "===================== DPX TAR preservation workflow start ====================="
  else
    echo "No files available for TAR wrapping, script exiting"
    exit 1
fi

# ==============================
# == LAUNCH PYTHON TAR SCRIPT ==
# ==============================

grep ^N_ "${QNAP_FILM}${TAR_PRES}tar_list.txt" | sort -n -k10.12 | parallel --jobs 4 "${PY3_ENV} ${PY3_TAR_QNAP_FILM} {}"

# Refresh list of TAR files
rm "${QNAP_FILM}${TAR_PRES}tar_list.txt"

# Start TAR preparations and wrap of DPX sequences
log "===================== DPX TAR preservation workflow end ======================="
