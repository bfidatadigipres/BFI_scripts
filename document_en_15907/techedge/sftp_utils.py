#!/usr/bin/env python3

"""
SFTP retrieve export of today's
off-air television advertisements

2025
"""

import os
import paramiko
import tenacity
from typing import Optional

DESTINATION = os.environ.get("ADVERTS_PATH")
TE_URL = os.environ.get("TECHEDGE_FTP_URL")
SFTP_USR = os.environ.get("TE_SFTP_USR")
SFTP_KEY = os.environ.get("TE_SFTP_KEY")


def sftp_connect() -> paramiko.sftp_client.SFTPClient:
    """
    Make SFTP Client connection
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(TE_URL, "22", SFTP_USR, SFTP_KEY)

    return ssh_client.open_sftp()


@tenacity.retry(wait=tenacity.wait_fixed(10))
def get_metadata(target_day: str, sftp: paramiko.SSHClient) -> Optional[str]:
    """
    Connect using sftp_connect
    then iterate /Export folder
    to retrieve target date path
    target day YYYY-MM-DD format
    """
    sftp = sftp_connect()
    files = sftp.listdir("/Export")

    for file in files:
        if file.startswith(target_day):
            dest_path = os.path.join(DESTINATION, file)
            sftp.get(f"/Export/{file}", dest_path)
            if os.path.exists(dest_path):
                return dest_path

    return None
