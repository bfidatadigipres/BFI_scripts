import os
import sys
import logging
import shutil
import re
from pathlib import Path
# from hashes import *
import checksum_maker_mediainfo as cmm

sys.path.append(os.environ['CODE'])
import utils

LOG_PATH = os.environ['LOG_PATH']
# LOG_PATH = '/mnt/qnap_04/Admin/Logs'
CHECKSUM_PATH = os.path.join(LOG_PATH, 'checksum_md5')

LOGGER = logging.getLogger('pre_autoingest_checksum_checks')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'pre_autoingest_checksum_checks.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

LOCAL_LOGGER =  logging.getLogger('ingest_check')
LOCAL_HDLR = logging.FileHandler(os.path.join('hashes/ingest_check', 'ingest_check.log'))
LOCAL_FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
LOCAL_HDLR.setFormatter(FORMATTER)
LOCAL_LOGGER.addHandler(LOCAL_HDLR)
LOCAL_LOGGER.setLevel(logging.INFO)

# semi checks
       

def move_files(from_file, to_file):
    '''
    move files to folder

    Parameters:
    -----------
        from_file: string
            source of the file

        to_file: string
            destination of the file

    Returns:
    --------
    None
    
    '''
    try:
        if not os.path.exists(from_file):
            LOGGER.info("Error: Source file does not exists")
        destination_dir = os.path.dirname(to_file)
        
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

        shutil.move(from_file, to_file)
        LOGGER.info(f"File successfully moved from source: {from_file} to destination: {to_file}")
        return f"File successfully moved from source to destination"
    except Exception as e:
        LOGGER.warning(f"Error: {e}")
        LOCAL_LOGGER.info(f"Error: {e}")
        return 'file doesnt exists'
    
def move_file(file, move_to_files):
    
    path = Path(file).relative_to('hashes/ingest_check')

    if len(path.parts) < 1:
        LOCAL_LOGGER.info('this file is in the root directory')
        
        move_file(file, move_to_files)

    else:
        LOGGER.info(f'this file has subdirectory: {file}')
        sub_dir = Path(f'{move_to_files}')/ path.parent
        destination_file = os.path.join(sub_dir, path.name)
        
        
        move_files(file, destination_file)

        current_path = Path('hashes/ingest_check') / path.parent
        while current_path != Path('hashes/ingest_check'):

            if not os.listdir(current_path):
                LOGGER.info(f'removing folder: {current_path}')
                shutil.rmtree(current_path)
            else:
                break

            current_path = current_path.parent

        
def pygrep(folder_name, hash_value):
    '''
    This function represent the python version of the linux command 'grep'

    Parameters:
    -----------

    folder_name: string
        the folder path to the checksum value.

    hash_value: string
        the hash value from the hash function

    file_name: string
        the filename path to find if the hash_value doesnt exists

    Returns:
    --------
    checker: (bool, result)
        return true if the hash value is in the file with the corresponding line in the file

    '''
    for root, _, files in os.walk(folder_name):
        lists_of_files = []
        for file in files:
            if not file.endswith(('.ini', '.DS_Store', '.mhl', '.tmp', '.dpx', '.DPX', '.log')):
                filepath = os.path.join(root, file)
                print(filepath)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        for line in f:
                            if re.search(hash_value, line):
                                lists_of_files.append((True,line, os.path.join(root, file)))
                            elif re.search(file_name, line):
                                lists_of_files.append((True, line, os.path.join(root, file)))
                except Exception as e:
                    LOGGER.error(f"Error reading {filepath}: {e}")
    return lists_of_files

def main():
    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit('Shell script failed to pass argument to Python script')

    # if not utils.check_control('power_off_all'):
    #    # LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
    #     sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    # the file path should refer to the ingest_check folder found in any qnap folders
    filepath = sys.argv[1]
    file_dict = {}
    
    # seperate file from folder, dont process folders,full matrch partial match and no match
    LOGGER.info('======================Starting pre autoingest checks====================================')

    LOGGER.info("Generating local hash file, starting checksum validation process")
    # consider folders containing files
    # 

    for root, dirs, files in os.walk('hashes/ingest_check'):
        if root == 'hashes/ingest_check/ingest_failed' or root == 'hashes/ingest_check/ingest_approved' or root == 'hashes/ingest_check/ingest_parital' or root == 'hashes/ingest_check/checksum_folder':
            continue
        for file in files:
            if os.path.isfile(os.path.join(root, file)) and not file.endswith(('.log', '.txt', '.md5')):
               
                file_dict[os.path.join(root, file)] =  False
                hash_number = utils.create_md5_65536(os.path.join(root, file))
                
                checksum_path = os.path.join(CHECKSUM_PATH, f'{file}.md5')
                cmm.checksum_write(checksum_path, hash_number, 'hashes/ingest_check', file)
                matching = pygrep('hashes/ingest_check/checksum_folder', hash_number)


                for match in matching:

                    # if hash_number in str(match[1]) and file in str(match[1]):
                    #     file_dict[os.path.join(root, file)] = match
                    
                    if hash_number in str(match[1]) and file not in str(match[1]):
                        file_dict[os.path.join(root, file)] = ('Miss match, not the same file, same checksum', match[1], match[2])
                    
                    elif file in str(match[-2]) and hash_number not in str(match[-2]):
                        file_dict[os.path.join(root, file)] = ('Miss match, same file not the same checksum', match[1], match[2])

                    else:
                        file_dict[os.path.join(root, file)] = match
            print(file_dict)
            for files, results in file_dict.items():
                if results == False:
                    LOGGER.info('=======local md5 and supplied md5 do not match at all============')
                    LOCAL_LOGGER.info('=======local md5 and supplied md5 do not match at all============')
                    move_file(files, 'hashes/ingest_check/ingest_failed/')
                
                elif results[0] == 'Miss match, not the same file, same checksum':
                    LOGGER.info(f'==== theres a missmatch between the file name, two or more file has the same checksum value: {results}')
                    LOCAL_LOGGER.info(f'==== theres a missmatch between the file name, two or more file has the same checksum value: {results}')
                    move_file(files, 'hashes/ingest_check/ingest_partial/')

                elif results[0] == 'Miss match, same file not the same checksum':
                    LOGGER.info(f'==== theres a missmatch, same checksum not the same file: {results}')
                    LOCAL_LOGGER.info(f'==== theres a missmatch, same checksum not the same file: {results}')
                    move_file(files, 'hashes/ingest_check/ingest_partial/')

                else:
                    LOGGER.info(f'=======local and supplied md5 file are the same============')
                    LOCAL_LOGGER.info(f'=======local and supplied md5 file are the same============')
                    move_file(str(files), 'hashes/ingest_check/ingest_approved/')
    LOGGER.info('======================pre autoingest checks End====================================')

    
if __name__ == "__main__":
    # print(move_files('hashes/ingest_check/folder_1/folder_2/167.mkv', 'hashes/ingest_check/ingest_failed/folder_1/folder_2'))
    main()
    #print(move_file('hashes/ingest_check/folder_1/folder_2/126.mkv', 'hashes/ingest_check/ingest_partial/'))