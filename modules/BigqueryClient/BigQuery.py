import logging

import google
from google.cloud import bigquery
import pandas as pd
from google.cloud.exceptions import NotFound


def get_client(project_id, location, scopes=None):
    credentials, _ = google.auth.default(scopes=scopes)
    return bigquery.Client(
        project=project_id,
        location=location,
        credentials=credentials,
    )


class BigQuery:
    def __init__(self, project_id, location, scopes=None) -> None:
        self.project_id = project_id
        self.location = location
        self.client = get_client(project_id, location, scopes)
        self.logger = logging.getLogger(__name__)

    def query(self, query: str, location: str = None) -> pd.DataFrame:
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        job = self.client.query(
            query,
            job_config=job_config,
            location=location,
        )
        if job.errors:
            self.logger.error(f"Failed to query {query}.")
            raise Exception(job.errors)
        else:
            self.logger.info(f"Executed query {query}.")
        return job.to_dataframe()

    def table_exists(self, table_id: str) -> bool:
        try:
            self.client.get_table(f"{table_id}")
            return True
        except NotFound:
            return False

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_id: str,
        write_disposition: bigquery.WriteDisposition = "WRITE_APPEND",
    ) -> None:
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = write_disposition
        job_config.autodetect = True
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ]

        try:
            self.client.load_table_from_dataframe(
                df,
                table_id,
                job_config=job_config,
            ).result()
            self.logger.info(f"Loaded {table_id}.")
        except Exception as e:
            self.logger.error(f"Failed to load {table_id}.")
            raise e

    def get_last_load_date(self, table_id: str, date_field: str) -> pd.DataFrame:
        try:
            return self.query(
                f"""
                SELECT
                    MAX({date_field}) AS last_load_date
                FROM `{self.project_id}.{table_id}`
                """
            )
        except Exception as e:
            self.logger.error("Failed to get last load date.")
            raise e

    def delete_table(self, table_id: str) -> None:
        if self.table_exists(table_id):
            self.client.delete_table(table_id)
            self.logger.info(f"Deleted table {table_id}.")
        else:
            self.logger.info(f"Table {table_id} does not exist.")

    def create_dataset(self, dataset_id: str, location: str = None) -> None:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location or self.location
        self.client.create_dataset(dataset)
        self.logger.info(f"Created dataset {dataset_id}.")

    def create_table(self, table_id: str, schema: list[dict]) -> None:
        table = bigquery.Table(table_id, schema=schema)
        self.client.create_table(table)
        self.logger.info(f"Created table {table_id}.")