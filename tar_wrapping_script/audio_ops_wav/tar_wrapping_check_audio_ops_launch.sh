#!/bin/bash -x

# ============================================
# Launch script for Audio Ops tar wrap script =
# ============================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Function to check for control json activity
function control {
    boole=$(cat "${LOG_PATH}downtime_control.json" | grep "rawcooked" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      log "Control json requests script exit immediately"
      log "===================== TAR WRAPPING CHECKSUM SCRIPT ENDED(rawcooked) ====================="
      exit 0
    fi
}

function pauseScript {
    boole=$(cat "${LOG_PATH}downtime_control.json" | grep "pause_scripts" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      log "Control json requests script exit immediately"
      log "===================== TAR WRAPPING CHECKSUM SCRIPT ENDED(rawcooked) ====================="
      exit 0
    fi
}

# Control check
control

# pause_script check
pauseScript

# Path to folder
FPATH="${AUTOMATION_WAV}for_tar_wrap/"
LOGS="${AUTOMATION_WAV}tar_wrapping_checksum.log"
FLIST="${AUTOMATION_WAV}temp_file_list.txt"

touch "$FLIST"

if [ -z "$(ls -A ${FPATH})" ]
  then
    echo "Folder empty, for_tar_wrap, script exiting."
    exit 1
  else
    echo " =========== TAR WRAPPING CHECKSUM SCRIPT START =========== $date_FULL" >> "$LOGS"
    echo " Looking for files or folders in $FPATH" >> "$LOGS"
    echo " Writing any files/folders found to $FLIST" >> "$LOGS"
fi

find "$FPATH" -maxdepth 1 -mindepth 1 -mmin +30 | while IFS= read -r items; do
  item=$(basename "$items")
  echo "$item"
  echo "${FPATH}${item}" >> "$FLIST"
done

cat "$FLIST" >> "$LOGS"

# Launching Python script using parallel
echo " Launching Python script to TAR wrap folders " >> "$LOGS"
grep "/mnt/" "$FLIST" | parallel --jobs 1 "${PYENV311} ${CODE}tar_wrapping_script/audio_ops_wav/tar_wrapping_check_audio_ops.py {}"
echo " =========== TAR WRAPPING CHECKSUM SCRIPT END =========== $date_FULL" >> "$LOGS"

rm "$FLIST"
