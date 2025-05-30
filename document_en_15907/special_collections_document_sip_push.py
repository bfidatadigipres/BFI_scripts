'''
Script for testing Archivematica writes
of SIP data
'''

import os
import sys
import requests
import base64

LOCATION = os.environ.get('AM_TS_UUID') # Transfer source
ARCH_URL = os.environ.get('AM_URL') # Basic URL for bfi archivametica
API_NAME = os.environ.get('AM_API') # temp user / key
API_KEY = os.environ.get('AM_KEY')
TRANSFER_ENDPOINT = os.path.join(ARCH_URL, "api/transfer/start_transfer/")
PACKAGE_ENDPOINT = os.path.join(ARCH_URL, "api/v2beta/package/")
TRANSFER_NAME = 'API Tests'


def send_as_transfer(fpath):
    '''
    Receive args from test run
    convert to data payload then
    post to Archivematica TestAPI/
    folder for review
    '''
    if not os.path.exists(fpath):
        sys.exit(f"Path supplied cannot be found: {fpath}")

    # Build correct folder path
    rel_path = os.path.basename(fpath)
    path_str = f"{LOCATION}:{rel_path}"
    encoded_path = base64.b64encode(path_str.encode('utf-8')).decode('utf-8')
    print(f"Changed local path {path_str}")
    print(f"to base64 {encoded_path}")

    headr = {
        "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Create payload and post
    data_payload = {
        "name": TRANSFER_NAME,
        "type": "standard",
        "accession": f"CID_priref_{priref}",
        "paths[]": encoded_path,
        "rows_id[]": [""],
    }

    print(f"Starting transfer... to {TRANSFER_NAME} {rel_path}")
    try:
        response = requests.post(TRANSFER_ENDPOINT, headers=headr, data=data_payload)
        response.raise_for_status()
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


def send_as_package(fpath, access_system_id, arg):
    '''
    Send a package using v2beta package
    with access system id to link in atom.
    Args, ab path, ATOM slug, bool auto approve true/false
    '''

    if not os.path.exists(fpath):
        sys.exit(f"Path supplied cannot be found: {fpath}")

    # Build correct folder path
    rel_path = os.path.basename(fpath)
    path_str = f"{LOCATION}:{rel_path}"
    encoded_path = base64.b64encode(path_str.encode('utf-8')).decode('utf-8')
    print(f"Changed local path {path_str}")
    print(f"to base64 {encoded_path}")

    headr = {
        "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
        "Content-Type": "application/json"
    }

    # Create payload and post
    data_payload = {
        "name": TRANSFER_NAME,
        "path": encoded_path,
        "type": "standard",
        "access_system_id": access_system_id,
        "processing_config": "automated",
        "auto_approve": arg,
    }

    print(f"Starting transfer... to {TRANSFER_NAME} {rel_path}")
    try:
        response = requests.post(PACKAGE_ENDPOINT, headers=HEADERS, data=data_payload)
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
