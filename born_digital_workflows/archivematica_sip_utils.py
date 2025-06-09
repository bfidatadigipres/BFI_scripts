#!/usr/bin/env python3
"""
Script for testing Archivematica writes
of SIP data
"""

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

if not ARCH_URL or not API_NAME or not API_KEY or not SFTP_UUID or not SFTP_USR or not SFTP_KEY or not REL_PATH:
    sys.exit(
        "Error: Please set AM_URL, AM_API (username), and AM_KEY (API key) environment variables."
    )


def send_to_sftp(fpath):
    '''
    First step SFTP into Storage Service, then check
    content has made it into the folder 
    '''
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ARCH_URL.replace('https://', ''), '22', SFTP_USR, SFTP_KEY)
    sftp = ssh_client.open_sftp()

    # Send the SFTP, download directory to check present
    folder = os.path.basename(fpath)
    remote_path = os.path.join("mnt/sto_bfi_processing/sftp-transfer-source", "API_Tests/", folder)
    sftp.put(fpath, remote_path)
    directory_check = os.path.join("mnt/sto_bfi_processing/sftp-transfer-source", "API_Tests/")
    files = sftp.listdir(directory_check)
    return files


def send_as_transfer(fpath, priref):
    """
    Receive args from test run
    convert to data payload then
    post to Archivematica TestAPI/
    folder for review
    """
    if not os.path.exists(fpath):
        sys.exit(f"Path supplied cannot be found: {fpath}")

    # Build correct folder path
    TRANSFER_ENDPOINT = os.path.join(ARCH_URL, "api/transfer/start_transfer/")
    folder_path = os.path.basename(fpath)
    path_str = f"{SFTP_UUID}:API_Tests/{folder_path}"
    encoded_path = base64.b64encode(path_str.encode('utf-8')).decode('utf-8')
    print(f"Changed local path {path_str}")
    print(f"to base64 {encoded_path}")

    headr = {
        "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
        "Content-Type": "application/json"
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
        response = requests.post(TRANSFER_ENDPOINT, headers=headr, data=json.loads(data_payload))
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
    path_str = f"{LOCATION}:{fpath}"
    encoded_path = base64.b64encode(path_str.encode("utf-8")).decode("utf-8")
    print(f"Changed local path {path_str}")
    print(f"to base64 {encoded_path}")

    headr = {
        "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
        "Content-Type": "application/json",
    }

    # Create payload and post
    data_payload = {
        "name": TRANSFER_NAME, # Or folder_path?
        "path": encoded_path,
        "type": "standard",
        "access_system_id": access_system_id,
        "processing_config": "automated",
        "auto_approve": auto_approve_arg,
    }

    print(f"Starting transfer... to {TRANSFER_NAME} {rel_path}")
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
    '''
    Calls to retrieve UUID for
    transfers already in Archivematica
    '''
    COMPLETED = os.path.join(ARCH_URL, "api/transfer/completed/")
    api_key = f"{API_NAME}:{API_KEY}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"ApiKey {api_key}"
    }

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
    '''
    Call the v2 locations to retrieve
    UUID locations for different 
    Archivematica services
    '''
    SS_END = f"{ARCH_URL}:8000/api/v2/location/"
    api_key = f"{API_NAME}:{API_KEY}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"ApiKey {api_key}"
    }

    try:
        respnse = requests.get(SS_END, header=headers)
        respnse.raise_for_status()
        data =respnse.json()
        if data and "results" in data:
            return data["results"]
    except requests.exceptions.RequestException as err:
        print(err)
        return None

