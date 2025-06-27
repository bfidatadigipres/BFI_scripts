#!/usr/bin/env python3
"""
Utility script for API POST/GETs from Archivematica
SFTP and Archivemtica storage service API, plus
API calls to AtoM API

To be used for born digital workflows for OSH3
2025
"""

import base64
import json
import os
import sys
import json
import base64
import paramiko
import requests

TS_UUID = os.environ.get("AM_TS_UUID")
SFTP_UUID = os.environ.get("AM_TS_SFTP")
SFTP_USR = os.environ.get("AM_SFTP_US")
SFTP_KEY = os.environ.get("AM_SFTP_PW")
REL_PATH = os.environ.get("AM_RELPATH")
ARCH_URL = os.environ.get("AM_URL")
API_NAME = os.environ.get("AM_API")
API_KEY = os.environ.get("AM_KEY")
SS_PIPE = os.environ.get("AM_SS_UUID")
SS_NAME = os.environ.get("AMSS_USR")
SS_KEY = os.environ.get("AMSS_KEY")
ATOM_URL = os.environ.get("ATOM_URL") # Upto api/
ATOM_KEY = os.environ.get("ATOM_KEY_META")
ATOM_AUTH = os.environ.get("ATOM_AUTH")
HEADER = {
    "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
    "Content-Type": "application/json"
}
ATOM_HEADER = {
    'REST-API-Key': ATOM_KEY,
    'Accept': 'application/json'
}
SS_HEADER = {
    "Authorization": f"ApiKey {SS_NAME}:{SS_KEY}",
    "Content-Type": "application/json"
}

if not ARCH_URL or not API_NAME or not API_KEY or not SFTP_UUID or not SFTP_USR or not SFTP_KEY or not REL_PATH:
    sys.exit(
        "Error: Please set AM_URL, AM_API (username), and AM_KEY (API key) environment variables."
    )


def sftp_connect():
    '''
    Make connection
    '''
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ARCH_URL.lstrip('https://'), '22', SFTP_USR, SFTP_KEY)
    return ssh_client.open_sftp()


def send_to_sftp(fpath, top_level_folder):
    '''
    Check for parent folder, if absent mkdir
    First step SFTP into Storage Service, then check
    content has made it into the folder
    '''

    relpath = fpath.split(top_level_folder)[-1]
    whole_path, file = os.path.split(relpath)
    root, container = os.path.split(whole_path)
    remote_path = os.path.join("sftp-transfer-source/API_Uploads", root)

    # Create ssh / sftp object
    sftp = sftp_connect()

    try:
        root_contents = sftp.listdir(remote_path)
    except OSError as err:
        print(f"Error attempting to retrieve path {remote_path}")
        root_contents = ''
        success = sftp_mkdir(sftp, remote_path)
        if not success:
            print(f"Failed to make new directory for {remote_path}")
            return None

    if container not in root_contents:
        success = sftp_mkdir(sftp, os.path.join(remote_path, container))
        if not success:
            print(f"Failed to make new directory for {os.path.join(remote_path, container)}")
            return None
    else:
        print(f"Folder {container} found in Archivematica already")

    print(f"Moving file {file} into Archivematica path {os.path.join(remote_path, container)}")
    response = sftp_put(sftp, fpath, os.path.join(remote_path, container, file))
    if response is False:
        print(f"Failed to send file to SFTP: {file} / {fpath}")
        return None
    else:
        print(f"File {file} successfully PUT to {os.path.join(remote_path, container, file)}")
    print("Making CSV folder...")
    m_relpath = os.path.join(remote_path, container, 'metadata/')
    mpath = os.path.join(os.path.split(fpath)[0], 'metadata/')
    metadata_fpath = os.path.join(mpath, 'metadata.csv')
    if os.path.exists(metadata_fpath):
        success = sftp_mkdir(sftp, m_relpath)
        if not success:
            print(f"Failed to make new directory for {m_relpath}")
            return None
        response = sftp_put(sftp, metadata_fpath, os.path.join(m_relpath, 'metadata.csv'))
        if response is False:
            print(f"Failed to send file to SFTP: 'metadata.csv' / {m_relpath}")
            return None
        else:
            print(f"File 'metadata.csv' successfully PUT to {m_relpath}")

    files = sftp.listdir(os.path.join(remote_path, container))
    sftp.close()
    return files


def sftp_put(sftp_object, fpath, relpath):
    '''
    Handle PUT to sftp
    '''
    print(f"PUT request received:\n{fpath}\n{relpath}")

    try:
        data = sftp_object.put(fpath, relpath)
        if 'SFTPAttributes' in str(data):
            return True
    except FileNotFoundError as err:
        print(f"File {fpath} was not found.")
        return False
    except OSError as err:
        print(f"Error attempting to PUT {fpath}")
        return False


def sftp_mkdir(sftp_object, relpath):
    '''
    Handle making directory
    '''
    try:
        sftp_object.mkdir(relpath)
    except OSError as err:
        print(f"Error attempting to MKDIR metadata/")
        return None

    relpath = relpath.rstrip('/')
    root, fold = os.path.split(relpath)
    content = sftp_object.listdir(root)
    print(content)
    if fold in str(content):
        return content

    return None


def send_as_package(fpath, atom_slug, item_priref, process_config, auto_approve_arg):
    """
    Send a package using v2beta package, subject to change
    Args: Path from top series, AToM slug, CID priref, OpenRecords or ClosedRecords, bool
    """
    # Build correct folder paths
    PACKAGE_ENDPOINT = os.path.join(ARCH_URL, "api/v2beta/package")
    folder_path = os.path.basename(fpath)
    path_str = f"{TS_UUID}:/bfi-sftp/sftp-transfer-source/API_Uploads/{fpath}"
    encoded_path = base64.b64encode(path_str.encode("utf-8")).decode("utf-8")

    # Create payload and post
    data_payload = {
        "name": folder_path,
        "path": encoded_path,
        "type": "standard",
        "processing_config": process_config,
        "accession": item_priref,
        "access_system_id": atom_slug,
        "auto_approve": auto_approve_arg,
    }
    print(json.dumps(data_payload))
    print(f"Starting transfer of {path_str}")
    try:
        response = requests.post(PACKAGE_ENDPOINT, headers=HEADER, data=json.dumps(data_payload))
        response.raise_for_status()
        print(f"Package transfer initiatied - status code {response.status_code}:")
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def get_transfer_status(uuid):
    '''
    Look for transfer status of new
    transfer/package. Returns:
    {
        "type": "transfer",
        "path": "/var/archivematica/sharedDirectory/currentlyProcessing/FILENAME5-66312695-e8af-441f-a867-aa9460436434/",
        "directory": "FILENAME5-66312695-e8af-441f-a867-aa9460436434",
        "name": "FILENAME5",
        "uuid": "66312695-e8af-441f-a867-aa9460436434",
        "microservice": "Create SIP from transfer objects",
        "status": "COMPLETE",
        "sip_uuid": "d2edd55f-9ab4--bff2-ad2d9573614d",
        "message": "Fetched status for 66312695-e8af-441f-a867-aa9460436434 successfully."
    }
    sip_uuid == aip_uuid needed for reingest
    '''
    status_endpoint = os.path.join(ARCH_URL, f"api/transfer/status/{uuid.strip()}")
    try:
        response = requests.get(status_endpoint, headers=HEADER)
        response.raise_for_status()
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def get_ingest_status(sip_uuid):
    '''
    Look for transfer status of new
    transfer/package. Returns:
    {
        "directory": "FILENAME5-66312695-e8af-441f-a867-aa9460436434",
        "message": "Fetched status for 66312695-e8af-441f-a867-aa9460436434 successfully.",
        "microservice": "Remove the processing directory",
        "name": "FILENAME5",
        "path": "/var/archivematica/sharedDirectory/currentlyProcessing/FILENAME5-66312695-e8af-441f-a867-aa9460436434/",
        "status": "COMPLETE",
        "type": "SIP",
        "uuid": "66312695-e8af-441f-a867-aa9460436434" 
    }
    uuid == aip_uuid needed for reingest
    '''
    status_endpoint = os.path.join(ARCH_URL, f"api/ingest/status/{sip_uuid.strip()}")
    try:
        response = requests.get(status_endpoint, headers=HEADER)
        response.raise_for_status()
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None

def get_transfer_list():
    """
    Calls to retrieve UUID for
    transfers already in Archivematica
    """
    COMPLETED = os.path.join(ARCH_URL, "api/transfer/completed/")
    api_key = f"{API_NAME}:{API_KEY}"
    headers = {"Accept": "*/*", "Authorization": f"ApiKey {api_key}"}

    try:
        response = requests.get(COMPLETED, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        if data and "results" in data:
            return data["results"]
        else:
            print("Error: 'results' key not found in the response.")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    return None


def get_location_uuids():
    """
    Call the v2 locations to retrieve
    UUID locations for different
    Archivematica services
    """
    SS_END = f"{ARCH_URL}:8000/api/v2/location/schema/"
    api_key = f"{SS_NAME}:{SS_KEY}"
    headers = {
        "Accept": "*/*", "Authorization": f"ApiKey {api_key}",
        "Content-type": "application/json"
    }

    try:
        respnse = requests.get(SS_END, headers=headers)
        respnse.raise_for_status()
        data = respnse.json()
        if data and "results" in data:
            return data["results"]
    except requests.exceptions.RequestException as err:
        print(err)
        return None


def get_atom_objects(skip_path):
    '''
    Return json dict containting
    objects (max 10 return, skip)
    '''
    
    try:
        response = requests.get(skip_path, auth=("bfi", ATOM_AUTH), headers=ATOM_HEADER, accept_redirect=True)
        objects = json.loads(response.text)
        print(f"Objects received: {objects}")
        if objects.get('results', None):
            return objects

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def get_all_atom_objects():
    '''
    Handle skip iteration through all available
    information objects, call get_atom_objects
    with interative skip numbers from totals
    '''

    endpoint = os.path.join(ATOM_URL, "informationobjects")
    objects = get_atom_objects(endpoint)
    if not objects:
        print("Warning, unable to find any informationobjects")
        return None

    total_obs = objects.get("total", None)
    if not isinstance(total_obs, int):
        print(f"Warning, unable to get total information objects from API: {objects}")
        return None    

    all_list = []
    for item in objects['results']:
        all_list.append(item)

    runs = total_obs // 10
    for num in range(1, runs+1):
        skip_endpoint = f"{endpoint}?skip={num}0"
        new_ob = get_atom_objects(skip_endpoint)
        if not new_ob:
            continue
        for item in new_ob['results']:
            all_list.append(item)
    if len(all_list) != total_obs:
        print(f"May not have retrieved all information objects correctly!")

    return all_list


def get_slug_match(slug_match):
    '''
    Handles retrieval of all AtoM information objects
    then builds list of slugs and attempts match
    '''
    list_of_objects = get_all_atom_objects()
    if list_of_objects is None:
        return None
    
    for item in list_of_objects:
        try:
            slug = item.get('slug', None)
        except (KeyError, TypeError) as err:
            print(err)
            continue
        if slug is None:
            continue
        elif slug == slug_match:
            return True
    return False


def delete_sip(sip_uuid):
    '''
    Remove (hide) a SIP from Archivematica
    after it's been transfered in error
    '''
    ENDPOINT = f"{ARCH_URL}/api/ingest/{sip_uuid}/delete/"
    try:
        response = requests.delete(ENDPOINT, headers=HEADER)
        response.raise_for_status()
        print(f"Package deletion success: {response.text}:")
        return response.text
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def reingest_aip(aip_uuid, type, process_config):
    '''
    Function for reingesting an AIP to create
    an open DIP for AtoM revision
    type = 'FULL', 'OBJECT', 'METADATA_ONLY'
    Full needed to supply processing_config update (Closed_to_Open)
    '''
    PACKAGE_ENDPOINT = f"{ARCH_URL}:8000/api/v2/file/{aip_uuid}/reingest/"

    # Create payload and post
    data_payload = {
        "pipeline": TS_UUID,
        "reingest_type": type,
        "processing_config": process_config
    }
    print(json.dumps(data_payload))
    print(f"Starting reingest of AIP UUID: {aip_uuid}")
    try:
        response = requests.post(PACKAGE_ENDPOINT, headers=SS_HEADER, data=json.dumps(data_payload))
        response.raise_for_status()
        print(f"Package transfer initiatied - status code {response.status_code}:")
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None

