#!/bin/bash -x

# Launcher for Selecta python script
# for booking Workflow jobs for in-
# scope records in a pointerfile

function pauseScripts {
    boole=$(cat "${CONTROL_JSON}" | grep "pause_scripts" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately" >> "${LOG}"
      echo 'Control json requests script exit immediately'
      exit 0
    fi
}

# Log script start
echo "Start Selecta: $(date)" >> "${LOG_PATH}tvam_selecta.log"

# pause_scripts checks
pauseScripts


# Collect selections from pointer file
"$PYENV311" "${CODE}workflow/tvam/selecta.py"

# Create Workflow jobs
"$PYENV311" "${CODE}workflow/tvam/submitta.py"

# Log script end
echo "Finish Selecta: $(date)" >> "${LOG_PATH}tvam_selecta.log"
