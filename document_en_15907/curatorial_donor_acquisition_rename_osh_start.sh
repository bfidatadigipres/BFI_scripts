#!/bin/bash -x

# ==========================================================
# Launcher script for curatorial_donor_acquisition_rename.py
# ==========================================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Local variables from environmental vars
source_path="${QNAP_09_OSH}"
dump_to="${CODE}document_en_15907/"
log_path="${LOG_PATH}curatorial_donor_acquisition_rename_osh.log"

function control {
    boole=$(cat "${CONTROL_JSON}" | grep "power_off_all" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      echo 'Control json requests script exit immediately'
      exit 0
    fi
}

function pauseScript {
    boole=$(cat "${CONTROL_JSON}" | grep "pause_scripts" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      echo 'Control json requests script exit immediately'
      exit 0
    fi
}

# Control checks
control
pauseScript

# Directory path change just to run shell find commands
cd "${dump_to}"

# Clean list and repopulate
echo "" > "${dump_to}curatorial_donor_acquisition_osh.txt"
find "${source_path}" -mindepth 1 -maxdepth 3 -type d -name 'Workflow_*' >> "${dump_to}curatorial_donor_acquisition_osh.txt"

if [ -s "${dump_to}curatorial_donor_acquisition_osh.txt" ]
  then
    echo " ========================= SHELL SCRIPT LAUNCH ========================== $date_FULL" >> "${log_path}"
    echo " == Start curatorial donor acquisition renaming in $curatorial_path == " >> "${log_path}"
    echo " == Shell script creating curatorial_donor_acquisition.txt for parallel launch of Python scripts == " >> "${log_path}"

    echo " == Launching GNU parallel to run multiple Python3 scripts for renaming == " >> "${log_path}"
    grep '/mnt/' "${dump_to}curatorial_donor_acquisition_osh.txt" | parallel --jobs 1 "${PYENV311} curatorial_donor_acquisition_rename_osh.py {}"

    echo " ========================= SHELL SCRIPT END ========================== $date_FULL" >> "${log_path}"
  else
    exit 0
fi
