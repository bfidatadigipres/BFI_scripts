#!/bin/bash -x

# Loop through H22 RNA deliverables in a list of directories, creating new CID records
# and renaming each one with the new <object_number>

# Date variable for use in log
DATE_NOW=$(date +%F\ %T)

# Ensure only one instance of script is running
for pid in $(pidof -x seek.sh); do
    if [ $pid != $$ ]; then
        echo "[$(date)] : document_h22_rna_files.sh : Process is already running with PID $pid"
        exit 1
    fi
done

targets=(
    "${GRACK_H22}/ATG_Exceptions/"
    "${GRACK_H22}/DC1/"
    "${GRACK_H22}/LMH/"
    "${GRACK_H22}/MX1/"
    "${GRACK_H22}/VDM/"
    "${GRACK_H22}/INN/"
    "${GRACK_H22}/CJP/"
    "${GRACK_H22}/IMES/"
    # Append more paths here as needed eg new suppliers folders
)

# Loop through list of target paths
for i in "${targets[@]}"; do
    echo "$i"
    # Search for validated MOVs (should be whitespace-safe) - move them into documeneted, for transcoding
    find "$i" -maxdepth 1 -type f -name "*.mov" -not -name "*partial*" | sort | while IFS= read -r filename; do
        "${PYENV}" "${CODE}document_h22/document_h22.py" "$filename" --destination "${GRACK_H22}/processing/documented/"
    done
    # Search for validated MKVs (should be whitespace-safe) - move them into rna_mkv, for aspet ratio triage to place in correct autoingest path
    find "$i" -maxdepth 1 -type f -name "*.mkv" -not -name "*partial*" | sort | while IFS= read -r filename; do
        "${PYENV}" "${CODE}document_h22/document_h22.py" "$filename" --destination "${GRACK_H22}/processing/rna_mkv/"
        # echo "${DATE_NOW} : ${i} has been moved into rna_mkv to be moved into autoingest" >> "${H22_POLICIES}rna_mkv_move.txt"
    done
done

