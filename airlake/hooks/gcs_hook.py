from typing import Iterable
import logging

import pandas as pd
from airflow.providers.google.common.hooks.base_google import (
    GoogleBaseHook as GoogleCloudBaseHook,
)
from airflow.providers.google.cloud.hooks.bigquery import split_tablename
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import job
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import BigQueryReadClient


class BigQueryNativeHookMixing:
    def get_client(self) -> bigquery.Client:
        credentials = self._get_credentials()
        return bigquery.Client(credentials=credentials, project=credentials.project_id)

    @property
    def bqstorage_client(self):
        return bigquery_storage.BigQueryReadClient(
            credentials=self._get_credentials(),
        )

    def get_bigquery_storage_client_v1(self) -> BigQueryReadClient:
        return BigQueryReadClient(credentials=self._get_credentials())

    def pandas_df(self, sql: str, labels: dict = None, *args, **kwargs) -> pd.DataFrame:
        job: bigquery.QueryJob = self.job(sql)
        return job.result().to_dataframe(bqstorage_client=self.bqstorage_client)

    def stream(self, sql: str) -> Iterable[dict]:
        return self.job(sql).result()

    def total_rows(self, sql: str) -> int:
        r = self.job(sql).result()
        return r.total_rows

    def job(self, sql: str, labels: dict = None) -> bigquery.QueryJob:
        logging.info("Executing `%s`", sql)
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        job_config.use_query_cache = False
        job_config.labels = labels if labels is not None else {}
        query_job = self.get_client().query(sql, job_config=job_config)
        logging.info(
            "Got job id `%s`. Destination table `%s`",
            query_job.job_id,
            query_job.destination,
        )
        return query_job

    def run_extract(
        self,
        source_project_dataset_table,
        destination_cloud_storage_uris,
        compression="NONE",
        export_format="CSV",
        field_delimiter=",",
        print_header=True,
        labels=None,
    ):
        client = self.get_client()
        source_project, source_dataset, source_table = split_tablename(
            table_input=source_project_dataset_table,
            default_project_id=client.project,
            var_name="source_project_dataset_table",
        )
        dataset_ref = client.dataset(source_dataset, project=source_project)
        table_ref = dataset_ref.table(source_table)

        logging.info("Exporting to %s", destination_cloud_storage_uris)
        conf = job.ExtractJobConfig()
        conf.destination_format = export_format
        conf.field_delimiter = field_delimiter
        conf.print_header = print_header
        conf.compression = compression

        extract_job = client.extract_table(
            table_ref,
            destination_cloud_storage_uris,
            job_config=conf,
        )
        extract_job.result()  # Wai

    def create_read_streams(
        self,
        project,
        dataset,
        table,
        data_format,
        read_options,
        max_streams=0,
    ):
        requested_session = types.ReadSession(
            table=f'projects/{project}/datasets/{dataset}/tables/{table}',
            data_format=data_format,
            read_options=read_options,
        )

        return self.get_bigquery_storage_client_v1().create_read_session(
            parent=f'projects/{project}',
            read_session=requested_session,
            max_stream_count=max_streams,
        )


class BigQueryNativeHook(GoogleCloudBaseHook, BigQueryNativeHookMixing):
    pass
