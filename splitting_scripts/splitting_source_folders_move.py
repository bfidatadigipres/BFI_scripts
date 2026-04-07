#!/usr/bin/env python3

"""
Iterates through each of list paths:
1. main(): counts total number of 'files_to_move' listed in top path only
2. Splits this list into four or seven and saves to list variables 'move1' through 'move7'
3. count(): counts total files in folders 1 upto 10, formats zfill(3)
4. Checks for matching reels in list / reels in source paths -> moves all to same folder
5. Batches up matching reels where found, and remaining files into a list of lists
5. Return list 'move_folders' comma separated, ordered into emptiest path first
6. Iterates moves file by file passing file name and 'new_path' to move():

2020
"""

import os
import shutil
import sys
from typing import Final

# Private imports
sys.path.append(os.environ["CODE"])
import utils

# List with paths, folder names for counts
PATHS: Final = [
    os.path.join(os.environ["QNAP_08"], "processing/source/"),
    os.path.join(os.environ["QNAP_08"], "memnon_processing/source/"),
    os.path.join(os.environ["QNAP_10"], "processing/source/"),
    os.path.join(os.environ["QNAP_VID"], "processing/source/"),
]


def look_for_matches(fname, pth):
    """
    Look for matching file reels in
    source folders, and return source/<num>
    """
    match = fname[:7]
    for root, _, files in os.walk(pth):
        for file in files:
            if file.startswith(match):
                if file != fname:
                    return root

    return None


def find_repeating_characters(file_list):
    """
    Group files from list into matching
    batches where first 7 characters are
    the same as neighbours
    """
    trimmed = []
    for file in file_list:
        trimmed.append(file[:7])

    groups = {}
    for uniq in set(trimmed):
        uniq_group = []
        for file in file_list:
            if file.startswith(uniq):
                uniq_group.append(file)
        if len(uniq_group) > 1:
            groups[uniq] = uniq_group

    return groups


def main():
    """
    Counts total number of files to move
    sorts into even amounts and moves to
    numbered subfolders
    """
    if not utils.check_control("power_off_all"):
        sys.exit("Exit requested by downtime_control.json")
    if not utils.check_control("pause_scripts"):
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    move_folders = []
    # Iterate through path list, searching top folder only for files to move
    for pth in PATHS:
        print("PATH BEING USED: {}".format(pth))
        if not utils.check_storage(pth):
            LOGGER.info(
                "Skipping path %s - prevented by Storage Control document.", pth
            )
            continue
        files_to_move = []
        for file in os.listdir(pth):
            if os.path.isfile(os.path.join(pth, file)):
                if file.endswith((".mkv", ".MKV")) and not file.startswith("partial"):
                    print("Extra .mkv located: {}".format(pth + file))
                    files_to_move.append(file)
                elif file.endswith((".mov", ".MOV")) and not file.startswith("partial"):
                    print("Extra .mov located: {}".format(pth + file))
                    files_to_move.append(file)

        # Before any other actions find matching reels and move to same path
        already_moved = []
        for enum, file in enumerate(files_to_move):
            move_path = look_for_matches(file, pth)
            if not move_path:
                continue
            print(f"Matched reel found, moving to {move_path}")
            old_path = os.path.join(pth, file)
            shutil.move(old_path, move_path)
            already_moved.append(enum)
        # Clean up files_to_move list
        for num in already_moved:
            files_to_move.pop(num)

        move_folders = count(pth)
        print(move_folders)
        folders_total = len(move_folders)
        print("Total amount of numbered folders in path: {}".format(folders_total))

        if folders_total < 6:
            moves = splitter(files_to_move, 4)
        elif folders_total < 8:
            moves = splitter(files_to_move, 6)
        elif folders_total <= 10:
            moves = splitter(files_to_move, 7)
        else:
            moves = splitter(files_to_move, 15)

        if len(moves) == 1:
            for lines in moves[0]:
                new_path = move_folders[0][4:]
                move(pth, lines, new_path)
        else:
            for num in range(0, len(moves)):
                for lines in moves[num]:
                    new_path = move_folders[num][4:]
                    move(pth, lines, new_path)


def move(pth: str, lines: str, new_path: str) -> None:
    """
    Build old path and move files
    """
    old_path = os.path.join(pth, lines)
    try:
        shutil.move(old_path, new_path)
        print("Moving file {} to folder {}".format(old_path, new_path))
    except:
        print("Unable to move file {} to folder {}".format(lines, new_path))


def splitter(arr: list[str], count: int) -> list[list[str]]:
    """
    Push all reel groups into first entry in
    returned list of files
    """
    priorities = []
    groups = find_repeating_characters(arr)
    if groups:
        for k, v in groups.items():
            print(f"Matched reels found starting {k}: {v}")
            if not priorities:
                priorities = v
            else:
                priorities = priorities + v

    if priorities:
        filtered = [x for x in arr if x not in priorities]
        count = count - 1
    else:
        filtered = arr
    if not filtered:
        return [priorities]

    items = [filtered[i::count] for i in range(count)]
    if priorities:
        items.append(priorities)

    return items


def count(pth: str) -> list[str]:
    """
    counts folder contents
    Writes data to new list in order of emptiest
    """
    folder_count: list[str] = []
    for num in range(1, 16):
        try:
            count = len(os.listdir(pth + f"{num}"))
            folder_count.append(str(count).zfill(3) + "," + pth + f"{num}")
        except Exception:
            pass

    # orders list value counts
    move_order = sorted(folder_count)
    return move_order


if __name__ == "__main__":
    main()
