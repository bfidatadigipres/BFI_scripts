#!/bin/bash

# Launcher for delete_post_split.py script which moves F47 and H22 whole-tape
# digitisations where all parts have been persisted to
# a backup folder on the server for deletion by a second script

# Log script start
echo "Start delete_post_split_qnap01.py: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"

# use virtualenv python bin
"${PY3_ENV}" "${CODE}splitting_scripts/delete_post_split_qnap01.py"

# Log script end
echo "Finish delete_post_split_qnap01.py: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"

# Action deletion of F47 Ofcom files in QNAP Video processing/delete folder
echo "Actioning deletion of F47 Ofcom files (QNAP Video) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"
sudo rm "${QNAP_VID}/processing/delete/*"

echo "Completed deletion of F47 Ofcom files (QNAP Video) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split_qnap01.log"
