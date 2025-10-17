#!/bin/bash

# Launcher for delete_post_split.py script which moves F47 and H22 whole-tape
# digitisations where all parts have been persisted to
# a backup folder on the server for deletion by a second script
function control {
    boole=$(cat "${CONTROL_JSON}" | grep "power_off_all" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      exit 0
    fi
}

function pauseScripts {
    boole=$(cat "${CONTROL_JSON}" | grep "pause_scripts" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      exit 0
    fi
}

# Control check inserted into code
control

pauseScripts
# Log script start
echo "Start delete_post_split_qnap01.py: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"

# use virtualenv python bin
"${PYENV311}" "${CODE}splitting_scripts/delete_post_split_qnap01.py"

# Log script end
echo "Finish delete_post_split_qnap01.py: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"

# Action deletion of F47 Ofcom files in QNAP Video processing/delete folder
echo "Actioning deletion of F47 Ofcom files (QNAP Video) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"
sudo rm "${QNAP_VID}/processing/delete/*"

echo "Completed deletion of F47 Ofcom files (QNAP Video) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"
