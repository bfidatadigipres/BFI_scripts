#!/usr/bin/env python3
"""
Script for testing Archivematica writes
of SIP data
"""

import base64
import json
import os
import sys
import json
import base64
import paramiko
import requests


SFTP_UUID = os.environ.get("AM_TS_SFTP")
SFTP_USR = os.environ.get("AM_SFTP_US")
SFTP_KEY = os.environ.get("AM_SFTP_PW")
REL_PATH = os.environ.get("AM_RELPATH")
ARCH_URL = os.environ.get("AM_URL")  # Basic URL for bfi archivematica
API_NAME = os.environ.get("AM_API")  # temp user / key
API_KEY = os.environ.get("AM_KEY")
SS_NAME = os.environ.get("AMSS_USR")
SS_KEY = os.environ.get("AMSS_KEY")

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


def send_to_sftp(fpath):
    '''
    First step SFTP into Storage Service, then check
    content has made it into the folder
    '''

    relpath = fpath.split("GUR-2_sub-fonds_Born-Digital/")[-1]
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


def send_as_transfer(fpath, priref):
    """
    Receive args from test run
    convert to data payload then
    post to Archivematica TestAPI/
    folder for review
    """
    sftp = sftp_connect()
    root_contents = sftp.listdir(fpath)
    if not root_contents:
        sys.exit(f"Supplied path to SFTP object not found: {fpath}")

    # Build correct folder path
    TRANSFER_ENDPOINT = os.path.join(ARCH_URL, "api/transfer/start_transfer/")
    folder_path = os.path.basename(fpath)
    path_str = f"{SFTP_UUID}:{fpath}"
    encoded_path = base64.b64encode(path_str.encode('utf-8')).decode('utf-8')
    print(f"Changed local path {path_str}")
    print(f"to base64 {encoded_path}")

    headr = {
        "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
        "Content-Type": "application/json",
    }

    # Create payload and post
    data_payload = {
        "name": folder_path,
        "type": "standard",
        "accession": f"CID_priref_{priref}",
        "paths": [encoded_path],
        "rows_id": [""],
    }

    print(data_payload)
    print(f"Starting transfer... to Archivematica {fpath}")
    try:
        response = requests.post(TRANSFER_ENDPOINT, headers=headr, data=json.dumps(data_payload))
        print(response.raise_for_status())
        print(f"Transfer initiatied - status code {response.status_code}:")
        print(response.json())
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
        print("Response as text:\n{response.text}")


def send_as_package(fpath, access_system_id, auto_approve_arg):
    """
    Send a package using v2beta package
    with access system id to link in atom.
    Args, ab path, ATOM slug, bool auto approve true/false
    """

    if not os.path.exists(fpath):
        sys.exit(f"Path supplied cannot be found: {fpath}")

    # Build correct folder path
    PACKAGE_ENDPOINT = os.path.join(ARCH_URL, "api/v2beta/package/")
    folder_path = os.path.basename(fpath)
    path_str = f"{SFTP_UUID}:API_Tests/{folder_path}"
    encoded_path = base64.b64encode(path_str.encode("utf-8")).decode("utf-8")
    print(f"Changed local path {path_str}")
    print(f"to base64 {encoded_path}")

    headr = {
        "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
        "Content-Type": "application/json",
    }

    # Create payload and post
    data_payload = {
        "name": folder_path,
        "path": encoded_path,
        "type": "standard",
        "access_system_id": access_system_id,
        "processing_config": "automated",
        "auto_approve": auto_approve_arg,
    }

    print(f"Starting transfer... to {folder_path} {REL_PATH}")
    try:
        response = requests.post(PACKAGE_ENDPOINT, headers=headr, data=data_payload)
        response.raise_for_status()
        print(f"Package transfer initiatied - status code {response.status_code}:")
        print(response.json())
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
            return None
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
