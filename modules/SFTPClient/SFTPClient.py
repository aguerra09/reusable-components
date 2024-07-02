from datetime import datetime

import paramiko
from io import BytesIO
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SFTPClient:
    def __init__(self, host, port=22, username=None, password=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.transport = None

    def __enter__(self):
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.connect(username=self.username, password=self.password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            logger.info(f"Connected to SFTP: {self.host}")
            return self
        except Exception as e:
            logger.error(f"Failed to connect to SFTP: {e}")
            self.close_transport()
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        self.close_transport()
        if exc_type:
            logger.error(f"An error occurred: {exc_value}")

    def close_transport(self):
        if self.transport:
            self.transport.close()
            logger.info("SFTP connection closed.")

    def upload_dataframe(self, df, remote_path):
        buffer = BytesIO()
        try:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            self.sftp.putfo(buffer, remote_path)
            logger.info(f"DataFrame uploaded to {remote_path}")
        finally:
            buffer.close()

    def download_dataframe(self, remote_path):
        buffer = BytesIO()
        try:
            self.sftp.getfo(remote_path, buffer)
            buffer.seek(0)
            df = pd.read_csv(buffer)
            return df
        finally:
            buffer.close()

    def list_files(self, path="."):
        return self.sftp.listdir(path)

    def get_file_modification_time(self, remote_path):
        file_attr = self.sftp.stat(remote_path)
        mod_time = datetime.fromtimestamp(file_attr.st_mtime)
        logger.info(f"Last modification time of {remote_path} is {mod_time}")
        return mod_time
