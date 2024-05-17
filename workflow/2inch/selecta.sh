#!/bin/bash

# Launcher for Selecta python script
# for booking Workflow jobs for in-
# scope records in a pointerfile

# Log script start
echo "Start Selecta: $(date)" >> "${CODE}workflow/2inch/selecta.log"

# Collect selections from pointer file
"$PYENV311" "${CODE}workflow/2inch/selecta.py"

# Create Workflow jobs
"$PYENV311" "${CODE}workflow/2inch/submitta.py"

# Log script end
echo "Finish Selecta: $(date)" >> "${CODE}workflow/2inch/selecta.log"
