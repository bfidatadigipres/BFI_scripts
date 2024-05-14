#!/bin/bash

# Launcher for delete_post_split.py script which moves F47 and H22 whole-tape
# digitisations where all parts have been persisted to
# a backup folder on the server for deletion by a second script

# Log script start
echo "Start delete_post_split.py: $(date)" >> "${LOG_PATH}delete_post_split.log"

# use virtualenv python bin
"${PYENV311}" "${CODE}splitting_scripts/delete_post_split.py"

# Log script end
echo "Finish delete_post_split.py: $(date)" >> "${LOG_PATH}delete_post_split.log"

# Action deletion of H22 files in QNAP processing/delete folder
echo "Actioning deletion of H22 files identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"
#sudo rm "${QNAP_H22}/processing/delete/*"

# Action deletion of F47 Ofcom files in Isilon processing/delete folder
echo "Actioning deletion of F47 Ofcom files identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"
#sudo rm "${ISILON_VID}/processing/delete/*"

# Action deletion of F47 Ofcom files in QNAP Video processing/delete folder
echo "Actioning deletion of F47 Ofcom files (QNAP-08) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"
#sudo rm "${QNAP_08}/processing/delete/*"

# Action deletion of H22 files in QNAP-10 processing/delete folder
echo "Actioning deletion of H22 files in QNAP-10 identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"
#sudo rm "${QNAP_10}/processing/delete/*"

echo "Completed deletion of H22 files identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"
echo "Completed deletion of F47 Ofcom files (Isilon video) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"
echo "Completed deletion of F47 Ofcom files (QNAP Video) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"
echo "Completed deletion of F47 Ofcom files (QNAP-08) identified for deletion: $(date)" >> "${LOG_PATH}delete_post_split.log"

