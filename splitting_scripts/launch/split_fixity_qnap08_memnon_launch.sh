#!/bin/bash -x

# Launcher for split_fixity_qnap08.py script which models carriers
# for in-scope whole-tape digitisation files and then
# creates item-level files and documents them in CID

SOURCE_NUM="$1"
TARGET="${QNAP_08}/memnon_processing/source/${SOURCE_NUM}"
LOG="${QNAP_08}/memnon_processing/log/split_${SOURCE_NUM}.log"
SCRIPT="${CODE}splitting_scripts/split_fixity_qnap08_memnon.py"

# Log script start
echo "" >> "$LOG"
echo "Start Python 3 split_fixity_qnap08_memnon.py: $(date)" >> "$LOG"

# Perform splitting twice: once-through for single-item
# carriers and then once-through for multi-item tapes
"$PY3_ENV" "$SCRIPT" "${TARGET}"
"$PY3_ENV" "$SCRIPT" "${TARGET}" multi

# Log script end
echo "Finish split_fixity_qnap08_memnon.py: $(date)" >> "$LOG"
