#!/usr/bin/env python3

"""
Iterates through each of list paths:
1. main(): counts total number of 'files_to_move' listed in top path only
2. Splits this list into four or seven and saves to list variables 'move1' through 'move7'
3. count(): counts total files in folders 1 upto 10, formats zfill(3)
4. Return these in list 'move_folders' comma separated, ordered emptiest path first
5. Iterates 'move1' through 'move7' file by file passing file name and 'new_path' to move():

2020
"""

import os
import shutil
import sys
from typing import Any, Final, Optional

# Private imports
sys.path.append(os.environ["CODE"])
import utils

# List with paths, folder names for counts
PATHS: Final = [
    os.path.join(os.environ["QNAP_08"], "processing/source/"),
    os.path.join(os.environ["QNAP_08"], "memnon_processing/source/"),
    os.path.join(os.environ["QNAP_10"], "processing/source/"),
    os.path.join(os.environ["QNAP_H22"], "processing/source/"),
    os.path.join(os.environ["QNAP_VID"], "processing/source/"),
]


def main():
    """
    Counts total number of files to move
    sorts into even amounts and moves to
    numbered subfolders
    """
    if not utils.check_control("power_off_all"):
        sys.exit("Exit requested by downtime_control.json")
    move_folders = []
    # Iterate through path list, searching top folder only for files to move
    for pth in PATHS:
        print("PATH BEING USED: {}".format(pth))
        files_to_move = []
        for file in os.listdir(pth):
            if os.path.isfile(os.path.join(pth, file)):
                if file.endswith((".mkv", ".MKV")) and not file.startswith("partial"):
                    print("Extra .mkv located: {}".format(pth + file))
                    files_to_move.append(file)
                elif file.endswith((".mov", ".MOV")) and not file.startswith("partial"):
                    print("Extra .mov located: {}".format(pth + file))
                    files_to_move.append(file)

        move_folders = count(pth)
        print(move_folders)
        folders_total = len(move_folders)
        print("Total amount of numbered folders in path: {}".format(folders_total))

        if folders_total < 6:
            move1, move2, move3, move4 = splitter(files_to_move, 4)
            for lines in move1:
                new_path = move_folders[0][4:]
                move(pth, lines, new_path)
            for lines in move2:
                new_path = move_folders[1][4:]
                move(pth, lines, new_path)
            for lines in move3:
                new_path = move_folders[2][4:]
                move(pth, lines, new_path)
            for lines in move4:
                new_path = move_folders[3][4:]
                move(pth, lines, new_path)

        elif folders_total < 8:
            move1, move2, move3, move4, move5, move6 = splitter(files_to_move, 6)
            for lines in move1:
                new_path = move_folders[0][4:]
                move(pth, lines, new_path)
            for lines in move2:
                new_path = move_folders[1][4:]
                move(pth, lines, new_path)
            for lines in move3:
                new_path = move_folders[2][4:]
                move(pth, lines, new_path)
            for lines in move4:
                new_path = move_folders[3][4:]
                move(pth, lines, new_path)
            for lines in move5:
                new_path = move_folders[4][4:]
                move(pth, lines, new_path)
            for lines in move6:
                new_path = move_folders[5][4:]
                move(pth, lines, new_path)

        elif folders_total < 11:
            move1, move2, move3, move4, move5, move6, move7 = splitter(files_to_move, 7)
            for lines in move1:
                new_path = move_folders[0][4:]
                move(pth, lines, new_path)
            for lines in move2:
                new_path = move_folders[1][4:]
                move(pth, lines, new_path)
            for lines in move3:
                new_path = move_folders[2][4:]
                move(pth, lines, new_path)
            for lines in move4:
                new_path = move_folders[3][4:]
                move(pth, lines, new_path)
            for lines in move5:
                new_path = move_folders[4][4:]
                move(pth, lines, new_path)
            for lines in move6:
                new_path = move_folders[5][4:]
                move(pth, lines, new_path)
            for lines in move7:
                new_path = move_folders[6][4:]
                move(pth, lines, new_path)

        else:
            (
                move1,
                move2,
                move3,
                move4,
                move5,
                move6,
                move7,
                move8,
                move9,
                move10,
                move11,
                move12,
                move13,
                move14,
                move15,
            ) = splitter(files_to_move, 15)
            for lines in move1:
                new_path = move_folders[0][4:]
                move(pth, lines, new_path)
            for lines in move2:
                new_path = move_folders[1][4:]
                move(pth, lines, new_path)
            for lines in move3:
                new_path = move_folders[2][4:]
                move(pth, lines, new_path)
            for lines in move4:
                new_path = move_folders[3][4:]
                move(pth, lines, new_path)
            for lines in move5:
                new_path = move_folders[4][4:]
                move(pth, lines, new_path)
            for lines in move6:
                new_path = move_folders[5][4:]
                move(pth, lines, new_path)
            for lines in move7:
                new_path = move_folders[6][4:]
                move(pth, lines, new_path)
            for lines in move8:
                new_path = move_folders[7][4:]
                move(pth, lines, new_path)
            for lines in move9:
                new_path = move_folders[8][4:]
                move(pth, lines, new_path)
            for lines in move10:
                new_path = move_folders[9][4:]
                move(pth, lines, new_path)
            for lines in move11:
                new_path = move_folders[10][4:]
                move(pth, lines, new_path)
            for lines in move12:
                new_path = move_folders[11][4:]
                move(pth, lines, new_path)
            for lines in move13:
                new_path = move_folders[12][4:]
                move(pth, lines, new_path)
            for lines in move14:
                new_path = move_folders[13][4:]
                move(pth, lines, new_path)
            for lines in move15:
                new_path = move_folders[14][4:]
                move(pth, lines, new_path)


def move(pth: str, lines: str, new_path: str) -> None:
    old_path = os.path.join(pth, lines)
    try:
        shutil.move(old_path, new_path)
        print("Moving file {} to folder {}".format(old_path, new_path))
    except:
        print("Unable to move file {} to folder {}".format(lines, new_path))


def splitter(arr: list[str], count: int) -> list[list[str]]:
    return [arr[i::count] for i in range(count)]


def count(pth: str) -> list[str]:
    """
    counts folder contents
    Writes data to new list in order of emptiest
    """
    folder_count: list[str] = []
    try:
        count1 = len(os.listdir(pth + "1"))
        folder_count.append(str(count1).zfill(3) + "," + pth + "1")
        count2 = len(os.listdir(pth + "2"))
        folder_count.append(str(count2).zfill(3) + "," + pth + "2")
        count3 = len(os.listdir(pth + "3"))
        folder_count.append(str(count3).zfill(3) + "," + pth + "3")
        count4 = len(os.listdir(pth + "4"))
        folder_count.append(str(count4).zfill(3) + "," + pth + "4")
        count5 = len(os.listdir(pth + "5"))
        folder_count.append(str(count5).zfill(3) + "," + pth + "5")
        try:
            count6 = len(os.listdir(pth + "6"))
            folder_count.append(str(count6).zfill(3) + "," + pth + "6")
        except:
            pass
        try:
            count7 = len(os.listdir(pth + "7"))
            folder_count.append(str(count7).zfill(3) + "," + pth + "7")
        except:
            pass
        try:
            count8 = len(os.listdir(pth + "8"))
            folder_count.append(str(count8).zfill(3) + "," + pth + "8")
        except:
            pass
        try:
            count9 = len(os.listdir(pth + "9"))
            folder_count.append(str(count9).zfill(3) + "," + pth + "9")
        except:
            pass
        try:
            count10 = len(os.listdir(pth + "10"))
            folder_count.append(str(count10).zfill(3) + "," + pth + "10")
            count11 = len(os.listdir(pth + "11"))
            folder_count.append(str(count11).zfill(3) + "," + pth + "11")
            count12 = len(os.listdir(pth + "12"))
            folder_count.append(str(count12).zfill(3) + "," + pth + "12")
            count13 = len(os.listdir(pth + "13"))
            folder_count.append(str(count13).zfill(3) + "," + pth + "13")
            count14 = len(os.listdir(pth + "14"))
            folder_count.append(str(count14).zfill(3) + "," + pth + "14")
            count15 = len(os.listdir(pth + "15"))
            folder_count.append(str(count15).zfill(3) + "," + pth + "15")
        except:
            pass
    except Exception as error:
        print("count():  Unable to count folders:", error)
    # orders list value counts
    move_order = sorted(folder_count)
    return move_order


if __name__ == "__main__":
    main()
