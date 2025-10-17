#!/bin/bash -x

# ======================================================
# Launcher script for access copy generation MP4 / JPEG
# ======================================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Local variables from environmental vars
path="$1"
transcode_path1="${path}${LEGACY_PROXY}"
job_num="$2"
path_insert="${1//['/']/_}"
dump_to="${LOG_PATH}mp4_transcode${path_insert}files.txt"
log_path="${LOG_PATH}mp4_transcode_make_jpeg_legacy.log"
python_script="${CODE}access_copy_creation/mp4_transcode_make_jpeg_legacy.py"

function control {
    boole=$(cat "${CONTROL_JSON}" | grep "power_off_all" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      echo 'Control json requests script exit immediately'
      exit 0
    fi
}

function pauseScript {
    boole=$(cat "${CONTROL_JSON}" | grep "pause_script" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately: MP4_transcode scripts" >> "${LOG}"
      echo 'Control json requests script exit immediately: MP4_transcode scripts'
      exit 0
    fi
}

# Control check inserted into code
control

# pause scripts check inserted into code
pauseScript

# replace list to ensure clean data
echo "" > "${dump_to}"

echo " ========================= SHELL LAUNCH - $path_insert ========================== $date_FULL" >> "${log_path}"
echo " == Start MP4 transcode/JPEG creation in $transcode_path1 == " >> "${log_path}"
echo " == Shell script creating dump_text.txt output for parallel launch of Python scripts == " >> "${log_path}"

# Command to build file list to supply to Python
find "${transcode_path1}" -maxdepth 1 -mindepth 1 -type f -mmin +30 >> "${dump_to}"

echo " == Launching GNU parallel to run muliple Python3 scripts for encoding == " >> "${log_path}"
grep '/mnt/' "${dump_to}" | parallel --jobs "$job_num" "$PYENV $python_script {}"

echo " ========================= SHELL END - $path_insert ========================== $date_FULL" >> "${log_path}"
