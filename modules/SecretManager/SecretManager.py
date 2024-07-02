import json
import logging

import google_crc32c
from google.cloud import secretmanager


class SecretManager:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()

    def create_secret(self, secret_id: str) -> None:
        self.client.create_secret(
            request={
                "parent": f"projects/{self.project_id}",
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        logging.info(f"Created secret {secret_id}")

    def get_secret(self, secret_id: str) -> dict:
        logging.info("Pulling latest secret version...")
        name = self.client.secret_version_path(
            self.project_id,
            secret_id,
            "latest",
        )
        response = self.client.access_secret_version(request={"name": name})
        crc32c = google_crc32c.Checksum()
        crc32c.update(response.payload.data)
        if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
            logging.error("Data corruption detected.")
            raise ValueError("Data corruption detected.")

        payload = response.payload.data.decode("UTF-8")

        return json.loads(payload)

    def upload_secret(self, secret_id: str, payload: dict) -> None:
        logging.info("Pushing secret...")
        parent = self.client.secret_path(self.project_id, secret_id)
        payload_bytes = json.dumps(payload).encode("UTF-8")
        crc32c = google_crc32c.Checksum()
        crc32c.update(payload_bytes)
        self.client.add_secret_version(
            request={
                "parent": parent,
                "payload": {
                    "data": payload_bytes,
                    "data_crc32c": int(crc32c.hexdigest(), 16),
                },
            }
        )