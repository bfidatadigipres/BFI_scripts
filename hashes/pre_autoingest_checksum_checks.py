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
LOCAL_HDLR = logging.FileHandler(os.path.join(sys.argv[1], 'Acquisitions/ingest_check/ingest_check.log'))
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
    
def move_file(filepath,file, move_to_files):
    
    path = Path(file).relative_to(filepath)

    if len(path.parts) < 2:
        LOCAL_LOGGER.info('this file is in the root directory')
        
        move_file(file, move_to_files)

    else:
        LOGGER.info(f'this file has subdirectory: {file}')
        LOCAL_LOGGER.info(f'this file has subdirectory: {file}')
        sub_dir = Path(f'{move_to_files}')/ path.parent
        destination_file = os.path.join(sub_dir, path.name)
        
        
        move_files(file, destination_file)

        current_path = Path(filepath) / path.parent
        while current_path != Path(filepath):

            if not os.listdir(current_path):
                LOGGER.info(f'removing folder: {current_path}')
                shutil.rmtree(current_path)
            else:
                break

            current_path = current_path.parent


def finding_file_structure(file_dict):
	'''
	This function returns a dictonary containing the folder as well as the files inside the folders , retaining the file structure

	Parameters:
	----------
	
	file_dict: dict
		the dictonary containing the media file with the corresponding checksum file , 
		full file path and the supplied checksum file provided by the suppliers

	Returns:
	--------

	folder_file_stuct: dict
		the dictonary providing the folder path as its key and the media files inside the folder and the results of the checksum find as its value  
	'''
	folder_file_struct = {}
	for file, value in file_dict.items():
		folder_path = os.path.dirname(file)
		# check if the folder path is either checksum_folder, ingest_full_match, ingest... and etc
		if folder_path == os.path.join(sys.argv[1], 'Acquisitions/ingest_check/checksum_folder') or folder_path == os.path.join(sys.argv[1], 'Acquisitions/ingest_check/ingest_full_match') or  folder_path == os.path.join(sys.argv[1], 'Acquisitions/ingest_check/ingest_parital_match') or folder_path == os.path.join(sys.argv[1], 'Acquisitions/ingest_check/ingets_no_match') or folder_path == os.path.join(sys.argv[1], 'Acquisitions/ingest_check/ingest_check.log'):
			continue
		if folder_path not in folder_file_struct:
			folder_file_struct[folder_path] = []
		folder_file_struct[folder_path].append((file, value))

	return folder_file_struct
        
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
                                lists_of_files.append((line, os.path.join(root, file), True))
                            elif re.search(file_name, line):
                                lists_of_files.append((line, os.path.join(root, file), True))
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
    filepath = os.path.join(sys.argv[1], 'Acquisitions/ingest_check')
    file_dict = {}
    
    # seperate file from folder, dont process folders,full matrch partial match and no match
    LOGGER.info('======================Starting pre autoingest checks====================================')

    LOGGER.info("Generating local hash file, starting checksum validation process")
    # consider folders containing files
    # 
    print(filepath)
    
    for root, dirs, files in os.walk(filepath):
        #print(root)
       # print(dirs)
        #print(files)

        if root == f'{filepath}/ingest_no_match' or root == f'{filepath}/ingest_full_match' or root == f'{filepath}/ingest_parital' or root == f'{filepath}/checksum_folder':
            continue
            print('help')
        for file in files:
            if os.path.isfile(os.path.join(root, file)) and not file.endswith(('.log', '.txt', '.md5')):
               
                file_dict[os.path.join(root, file)] =  False
                hash_number = utils.create_md5_65536(os.path.join(root, file))
                
                checksum_path = os.path.join(CHECKSUM_PATH, f'{file}.md5')
                cmm.checksum_write(checksum_path, hash_number, 'filepath', file)
                matching = pygrep(f'{filepath}/checksum_folder', hash_number)
                print(matching)


                for match in matching:

                    # if hash_number in str(match[1]) and file in str(match[1]):
                    #     file_dict[os.path.join(root, file)] = match
                  
                    if hash_number in str(match[0]) and file not in str(match[0]):
                        file_dict[os.path.join(root, file)] = (match[0], match[1], 'Miss match, same checksum not the same file')
                    
                    elif file in str(match[0]) and hash_number not in str(match[0]):
                        file_dict[os.path.join(root, file)] = (match[0], match[1], 'Miss match, same file but not the same checksum')

                    else:
                        file_dict[os.path.join(root, file)] = match

            print(file_dict)
            result = finding_file_structure(file_dict)
            print(result)

'''            for files, results in file_dict.items():
                if results == False:
                    LOGGER.info('=======local md5 and supplied md5 do not match at all============')
                    LOCAL_LOGGER.info('=======local md5 and supplied md5 do not match at all============')
                    move_file(filepath, files, f'{filepath}/ingest_no_match/')
                
                elif results[0] == 'Miss match, not the same file, same checksum':
                    LOGGER.info(f'==== theres a missmatch between the file name, two or more file has the same checksum value: {results}')
                    LOCAL_LOGGER.info(f'==== theres a missmatch between the file name, two or more file has the same checksum value: {results}')
                    move_file(filepath, files, f'{filepath}/ingest_partial/')

                elif results[0] == 'Miss match, same file not the same checksum':
                    LOGGER.info(f'==== theres a missmatch, same checksum not the same file: {results}')
                    LOCAL_LOGGER.info(f'==== theres a missmatch, same checksum not the same file: {results}')
                    move_file(filepath, files, f'{filepath}/ingest_partial/')

                else:
                    LOGGER.info(f'=======local and supplied md5 file are the same============')
                    LOCAL_LOGGER.info(f'=======local and supplied md5 file are the same============')
                    move_file(filepath, str(files), f'{filepath}/ingest_match/')
    LOGGER.info('======================pre autoingest checks End====================================')
'''
    
if __name__ == "__main__":
    # print(move_files('hashes/ingest_check/folder_1/folder_2/167.mkv', 'hashes/ingest_check/ingest_failed/folder_1/folder_2'))
    main()
   # print(move_files('/mnt/qnap_11/digital_operations/Acquisitions/ingest_check/ingest_partial/MKV_sample.mkv', '/mnt/qnap_11/digital_operations/Acquisitions/ingest_check'))
