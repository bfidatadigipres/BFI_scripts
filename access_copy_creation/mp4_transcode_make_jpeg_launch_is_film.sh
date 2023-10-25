#!/bin/bash -x

# ==========================================================
# Launcher script for Autoingest MP4 / JPEG transcode script
# ==========================================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Local variables from environmental vars
transcode_path1="${IS_FILM}${TRANS}"
dump_to="${LOG_PATH}mp4_transcode_make_jpeg_files_is_film.txt"
log_path="${LOG_PATH}mp4_transcode_make_jpeg.log"
python_script="${CODE}access_copy_creation/mp4_transcode_make_jpeg.py"

# replace list to ensure clean data
echo "" > "${dump_to}"

echo " ========================= SHELL LAUNCH - ISILON FILM ========================== $date_FULL" >> "${log_path}"
echo " == Start MP4 transcode/JPEG creation in $transcode_path1 == " >> "${log_path}"
echo " == Shell script creating dump_text.txt output for parallel launch of Python scripts == " >> "${log_path}"

# Command to build file list to supply to Python
find "${transcode_path1}" -maxdepth 1 -mindepth 1 -type f -mmin +30 >> "${dump_to}"

echo " == Launching GNU parallel to run muliple Python3 scripts for encoding == " >> "${log_path}"
grep '/mnt/' "${dump_to}" | parallel --jobs 4 "sudo ${PYENV} $python_script {}"

echo " ========================= SHELL END - ISILON FILM ========================== $date_FULL" >> "${log_path}"

