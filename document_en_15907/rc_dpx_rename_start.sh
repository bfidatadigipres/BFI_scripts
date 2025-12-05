#!/bin/bash -x

# ============================================
# Launcher script for qnap06_rc_dpx_rename.py
# ============================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Local variables from environmental vars
qnap_path="${QNAP_FILMOPS}${SEQ_RENUMBER}raw/"
qnap_path2="${QNAP_FILMOPS}${SEQ_RENUMBER}graded/"
dump_to="${CODE_PATH}document_en_15907/"
log_path="${LOG_PATH}qnap06_rc_dpx_rename.log"

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


# Control check inserted into code
control

# pause scripts check inserted into code
pauseScript

# Directory path change just to run shell find commands
cd "${dump_to}"

# replace list to ensure clean data
rm "${dump_to}qnap06_rc_dpx_renaming.txt"
touch "${dump_to}qnap06_rc_dpx_renaming.txt"

echo " ========================= SHELL SCRIPT LAUNCH ========================== $date_FULL" >> "${log_path}"
echo " == Start QNAP06 RC DPX renaming in $qnap_path == " >> "${log_path}"
echo " == Shell script creating qnap06_rc_dpx_renaming.txt for parallel launch of Python scripts == " >> "${log_path}"

# Return full list of paths depth 2 to dump_text
find "${qnap_path}" -maxdepth 1 -mindepth 1 -type d | sort >> "${dump_to}qnap06_rc_dpx_renaming.txt"
find "${qnap_path2}" -maxdepth 1 -mindepth 1 -type d | sort >> "${dump_to}qnap06_rc_dpx_renaming.txt"
echo "List of folder paths for renumbering:" >> "${log_path}"
grep '/mnt/' "${dump_to}qnap06_rc_dpx_renaming.txt" >> "${log_path}"

# Launch python in sudo to allow permissions access to folders
echo " == Launching GNU parallel to run multiple Python3 scripts for renaming == " >> "${log_path}"
grep '/mnt/' "${dump_to}qnap06_rc_dpx_renaming.txt" | parallel --jobs 1 "sudo ${PYENV311} qnap06_rc_dpx_rename.py {}"

echo " ========================= SHELL SCRIPT END ========================== $date_FULL" >> "${log_path}"
