import os
import sys
import logging
import shutil
import re
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

LOCAL_LOGGER =  logging.getLogger('checksum_checker')
LOCAL_HDLR = logging.FileHandler(os.path.join('hashes/ingest_check', 'checksum_checker.log'))
LOCAL_FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
LOCAL_HDLR.setFormatter(FORMATTER)
LOCAL_LOGGER.addHandler(LOCAL_HDLR)
LOCAL_LOGGER.setLevel(logging.INFO)




# find a matching file with checksum file
# check folder
def find_files_and_md5(folder_path):
    '''
    This function finds the media and corresponding md5 files in 
    'ingest_check' folder.

    Parameters:
    -----------
        folder_path: string
            the folder path to ingest_check
    
    Returns:
    --------
       results: dict
            stores all the file has has a matching media and md5 files, missing
            md5 file or media file
    '''
    matched_results = []
    missing_md5 = []
    missing_media_file = []

    for root, dirs, files in os.walk(folder_path):
        file_set = set(files)
        
        for file in files:
            if file.endswith(('.ini', '.DS_Store', '.mhl', '.json', '.tmp', '.dpx', '.DPX', '.log')):
                continue
            
            # checks if there's a media file
            if not file.endswith('.md5'):
                md5_file = f'{file}.md5'
                if md5_file in file_set:
                    matched_results.append((file, md5_file))
                else:
                    missing_md5.append(file)
            else:
                media_file = file[:-4]
                if media_file not in file_set:
                    missing_media_file.append(file)

    return {
        "matches": matched_results,
        "missing_md5_files": missing_md5,
        'missing_media': missing_media_file
    }


       

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
        
        # if not os.path.exists(destination_dir):
        #     os.makedirs(destination_dir)

        shutil.move(from_file, to_file)
        LOGGER.info("File successfully moved from source to destination")
        return "File successfully moved from source to destination"
    except Exception as e:
        LOGGER.warning(f"Error: {e}")
        return 'file doesnt exists'
    

def pygrep(folder_name, hash_value, file_name):
    '''
    This function represent the python version of the linux command 'grep'

    Parameters:
    -----------

    folder_name: string
        the folder path to the checksum value.

    hash_value: string
        the hash value from the hash function

    Returns:
    --------
    checker: bool
        return true if the hash value is in the file
    '''
    for root, _, files in os.walk(folder_name):
        for file in files:
            if not file.endswith(('.ini', '.DS_Store', '.mhl', '.tmp', '.dpx', '.DPX', '.log')):
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        for line in f:
                            if re.search(hash_value, line):
                                return True, line
                            elif re.search(file_name, line):
                                return True, line.strip().split(" ")[0]
                except Exception as e:
                    LOGGER.error(f"Error reading {filepath}: {e}")
    return False, None

def main():
    if len(sys.argv) < 2:
        LOGGER.error("Shell script failed to pass argument path via GNU parallel")
        sys.exit('Shell script failed to pass argument to Python script')

    # if not utils.check_control('power_off_all'):
    #    # LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
    #     sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    # the file path should refer to the ingest_check folder found in any qnap folders
    filepath = sys.argv[1]
    print(filepath)

    LOGGER.info('======================Starting pre autoingest checks====================================')
    media_pairing = find_files_and_md5(filepath)

    if media_pairing['missing_md5_files']:
        LOCAL_LOGGER.warning(f'No matching md5 for {media_pairing["missing_md5_files"]}')
        LOGGER.warning(f'No matching md5 for {media_pairing["missing_md5_files"]}')
    
    elif media_pairing['missing_media']:
        LOCAL_LOGGER.warning(f'No matching media file for {media_pairing["missing_media"]}')
        LOGGER.warning(f'No matching media file for {media_pairing["missing_media"]}')
        

    for media_file, md5_file in media_pairing['matches']:
        LOGGER.info("Generating local hash file, starting checksum validation process")
        hash_number = utils.create_md5_65536(os.path.join(filepath, media_file))

        checksum_path = os.path.join(CHECKSUM_PATH, f'{media_file}.md5')

        cmm.checksum_write(checksum_path, hash_number, filepath, media_file)

        # open supplied md5 file
        result, value = pygrep(filepath, hash_number, media_file)

        if result == True:
            LOGGER.info('=======local md5 and supplied md5 matched============ ')
            print(os.path.join(filepath, media_file))
            move_files(os.path.join(filepath, media_file), 'hashes/ingest_approved')
            move_files(os.path.join(filepath, md5_file), "hashes/ingest_approved")
        else:
            LOGGER.info('local md5 and supplied md5 are not matched, please check the supplied md5 file and look at the local logs')
            LOCAL_LOGGER.info(f'local md5 and supplied md5 are not matched. Local md5 checksum: {hash_number}. Supplied md5 checksum: {value}')
            move_files(os.path.join(filepath, media_file), "hashes/ingest_reject/")
            move_files(os.path.join(filepath, md5_file), "hashes/ingest_reject/")


    LOGGER.info('======================pre autoingest checks End====================================')

    
if __name__ == "__main__":
    print(move_files('hashes/ingest_check/1245.mkv', 'hashes/ingest_failed/1245.mkv'))
   #main()