#!/bin/bash

# Launcher for delete_post_split_memnon.py script which
# digitisations where all parts have been persisted to
# a backup folder on the server for deletion by a second script

function control {
    boole=$(cat "${CONTROL_JSON}" | grep "power_off_all" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      exit 0
    fi
}

# Control check inserted into code
control

# Log script start
echo "Start delete_post_split_memnon.py: $(date)" >> "${LOG_PATH}delete_post_split_memnon.log"

# use virtualenv python bin
"${PYENV311}" "${CODE}splitting_scripts/delete_post_split_memnon.py"

# Log script end
echo "Finish delete_post_split_memnon.py: $(date)" >> "${LOG_PATH}delete_post_split_memnon.log"

# Action deletion of Memnon D3 files in QNAP Video processing/delete folder
echo "Actioning deletion of Memnon D3 files (QNAP-08) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split_memnon.log"
sudo rm "${QNAP_08}/memnon_processing/delete/*"

echo "Completed deletion of Memnon D3 files (QNAP-08) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split_memnon.log"
