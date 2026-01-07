#!/bin/bash -x

# Launcher for split.py script which models carriers
# for in-scope whole-tape digitisation files and then
# creates item-level files and documents them in CID

SOURCE_NUM="$1"
TARGET="${QNAP_H22}/processing/source/${SOURCE_NUM}"
LOG="${QNAP_H22}/processing/log/split_${SOURCE_NUM}.log"
SCRIPT="${CODE}splitting_scripts/split_fixity_h22.py"

function pauseScript {
    boole=$(cat "${CONTROL_JSON}" | grep "pause_script" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      echo "Control json requests script exit immediately: splitting scripts" >> "${LOG}"
      echo 'Control json requests script exit immediately: splitting scripts'
      exit 0
    fi
}

# pause scripts check inserted into code
pauseScript

# Log script start
echo "" >> "$LOG"
echo "Start Python 3 split_fixity_h22.py: $(date)" >> "$LOG"

# Perform splitting twice: once-through for single-item
# carriers and then once-through for multi-item tapes
"$PY3_ENV" "$SCRIPT" "${TARGET}"
"$PY3_ENV" "$SCRIPT" "${TARGET}" multi

# Log script end
echo "Finish split_fixity_h22.py: $(date)" >> "$LOG"
