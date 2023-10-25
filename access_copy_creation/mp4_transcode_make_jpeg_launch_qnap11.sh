#!/bin/bash -x

# ===========================================================
# Launcher script for Autoingest MP4 / JPEG transcode script
# ===========================================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Local variables from environmental vars
transcode_path="${QNAP_11}${TRANS}"
dump_to="mp4_transcode_make_jpeg_files_qnap11.txt"
dump_temp="mp4_transcode_make_jpeg_files_qnap11_temp.txt"
log_path="${LOG_PATH}mp4_transcode_make_jpeg.log"
python_script="${CODE}access_copy_creation/mp4_transcode_make_jpeg.py"

# replace list to ensure clean data
echo "" > "${dump_to}"
touch "${dump_temp}"

echo " ========================= SHELL SCRIPT LAUNCH ========================== $date_FULL" >> "${log_path}"
echo " == Start MP4 transcode/JPEG creation in $transcode_path == " >> "${log_path}"
echo " == Shell script creating dump_text.txt output for parallel launch of Python scripts == " >> "${log_path}"

# Command to build MKV list from two v210 paths containing multiple archive folders
find "${transcode_path}" -maxdepth 1 -mindepth 1 -type f -mmin +10 >> "${dump_to}"
grep '/mnt/' "${dump_to}" | shuf | head -n 200 > "${dump_temp}"

echo " == Launching GNU parallel to run muliple Python3 scripts for encoding == " >> "${log_path}"
grep '/mnt/' "${dump_temp}" | parallel --jobs 4 "sudo ${PYENV} $python_script {}"

rm "${dump_temp}"

echo " ========================= SHELL SCRIPT END ========================== $date_FULL" >> "${log_path}"
