#!/bin/bash -x

# Launcher for Selecta python script
# for booking Workflow jobs for in-
# scope records in a pointerfile

# Log script start
echo "Start Selecta: $(date)" >> "${LOG_PATH}d3_selecta.log"

# Collect selections from pointer file
"$PYENV311" "${CODE}workflow/dthree/selecta.py"

# Create Workflow jobs
"$PYENV311" "${CODE}workflow/dthree/submitta.py"

# Log script end
echo "Finish Selecta: $(date)" >> "${LOG_PATH}d3_selecta.log"
