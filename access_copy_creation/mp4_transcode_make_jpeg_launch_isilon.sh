#!/bin/bash -x

# ===========================================================
# Launcher script for Autoingest MP4 / JPEG transcode script
# ===========================================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Local variables from environmental vars
transcode_path1="${IS_AUD}${TRANS}"
transcode_path2="${IS_DIG}${TRANS}"
transcode_path3="${IS_SPEC}${TRANS}"
transcode_path4="${IS_MED}${TRANS}"
dump_to="${LOG_PATH}mp4_transcode_make_jpeg_files_isilon.txt"
log_path="${LOG_PATH}mp4_transcode_make_jpeg.log"
python_script="${CODE}access_copy_creation/mp4_transcode_make_jpeg.py"

# replace list to ensure clean data
echo "" > "${dump_to}"

echo " ========================= SHELL LAUNCH - ISILON ========================== $date_FULL" >> "${log_path}"
echo " == Start MP4 transcode/JPEG creation in Isilon transcode paths, Special Collections, Ingest, Audio and Digital == " >> "${log_path}"
echo " == Shell script creating dump_text.txt output for parallel launch of Python scripts == " >> "${log_path}"

# Command to build file list to supply to Python
find "${transcode_path2}" -maxdepth 1 -mindepth 1 -type f -mmin +10 >> "${dump_to}"
find "${transcode_path1}" -maxdepth 1 -mindepth 1 -type f -mmin +10 >> "${dump_to}"
find "${transcode_path3}" -maxdepth 1 -mindepth 1 -type f -mmin +10 >> "${dump_to}"
find "${transcode_path4}" -maxdepth 1 -mindepth 1 -type f -mmin +10 >> "${dump_to}"

echo " == Launching GNU parallel to run muliple Python3 scripts for encoding == " >> "${log_path}"
grep '/mnt/' "${dump_to}" | parallel --jobs 4 "sudo ${PYENV} $python_script {}"

echo " ========================= SHELL END - ISILON ========================== $date_FULL" >> "${log_path}"
