#!/bin/bash

date_FULL=$(date +'%Y-%m-%d - %T')

# navigate to directory
cd "${CODE}black_pearl/"

# create new autoingest.log by writing current timestamp to first line
echo "==================" $date_FULL "Autoingest is running ===========================" > "${LOG_PATH}autoingest.log"

# NORMAL version outputting detailed log to autoingest.log and overwriting every time
"$PYENV311" autoingest.py | tee -a "${LOG_PATH}autoingest.log"

# Trace output version for debugging issues
# rm "${LOG_PATH}autoingest_trace.txt"
# touch "${LOG_PATH}autoingest_trace.txt"
# "$PYENV311" -m trace --trace autoingest.py | tee -a "${LOG_PATH}autoingest_trace.txt"

# Output log to date prefix autoingest.log to retain for reference
# "$PYENV311" autoingest.py | tee -a "${LOG_PATH}${date_FULL}_autoingest.log"

