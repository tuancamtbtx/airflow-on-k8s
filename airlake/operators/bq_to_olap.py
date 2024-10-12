import json
from datetime import datetime, timedelta
from typing import List
import logging
import functools
import concurrent.futures as cfutures

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook as GoogleCloudStorageHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook as GoogleCloudBaseHook
from google.cloud import bigquery
import pytz

from airlake.hooks.druid_hook import (
    DruidHook,
    DruidIndexerBuilder,
    bq_schema_to_druid_schema
)


class BigQueryToOLAPHourOperator(BaseOperator):
    DEFAULT_MAX_PARALLEL = 16
    INTERVAL_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'
    LOCALTZ = pytz.timezone("Asia/Ho_Chi_Minh")
    template_fields = ('table', 'gcs_bucket', 'olap_destination_table')
    ui_color = '#a0169f'

    @apply_defaults
    def __init__(
            self,
            table='',
            olap_destination_table='',
            timestamp_column='',
            timestamp_format='auto',
            druid_dataset='dwh.druid',
            bigquery_conn_id='bigquery_default',
            druid_ingest_conn_id='druid_ingest_default',
            gcs_bucket='druid',
            segmenent_granularity='hour',
            sort_keys: List[str] = None,
            query_granularity=None,
            olap_metrics=None,
            olap_dimentions=None,
            exclude_dimensions=None,
            max_concurrency=8,
            *args,
            **kwargs):
        super(BigQueryToOLAPHourOperator, self).__init__(*args, **kwargs)
        if not table or not olap_destination_table or not timestamp_column:
            raise AirflowException('Missing required fields: table, olap_destination_table, timestamp_column')

        self.table = table
        self.olap_destination_table = olap_destination_table
        self.bigquery_conn_id = bigquery_conn_id
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.gcs_bucket = gcs_bucket
        self.timestamp_column = timestamp_column
        self.timestamp_format = timestamp_format
        self.segmenent_granularity = segmenent_granularity
        self.query_granularity = query_granularity
        # pre-defined dim and measures
        self.olap_metrics = olap_metrics
        self.olap_dimentions = olap_dimentions
        self.exclude_dimensions = exclude_dimensions
        self.sort_keys = sort_keys or []
        self.LOGGER = logging.getLogger(__name__)
        self.max_concurrency = min(max_concurrency or 8, self.DEFAULT_MAX_PARALLEL)

    def execute(self, context: dict):
        execution_date = context.get('execution_date')\
            .in_timezone('Asia/Ho_Chi_Minh')
        gcs_prefix = 'v3/{}/{}'.format(
            self.olap_destination_table,
            datetime.now().strftime("%Y_%m_%d_%H_%M_%S"),
        )

        hours = self._analyze_table(execution_date)

        with cfutures.ProcessPoolExecutor(max_workers=self.max_concurrency) as executor:
            logging.info('Submiting jobs ...')
            future_results = [
                executor.submit(self._sync_hour, gcs_prefix, i, execution_date)
                for i in hours
            ]
            logging.info('Wait for %s jobs: complete ...', len(future_results))
            # let the this executor crashes if any Exception occurred
            for f in cfutures.as_completed(future_results):
                f.result()

    def _analyze_table(self, execution_date: datetime):
        '''Sort the hour by total rows. So we can run the large partition hour first'''
        ds = execution_date.strftime('%Y-%m-%d')
        sql = '''
            SELECT
                EXTRACT(HOUR FROM {event_time} AT TIME ZONE '+7') AS hour,
                COUNT(*) AS total
            FROM `{table}`
            WHERE DATE({event_time}, '+7') = '{ds}'
            GROUP BY 1
            ORDER BY 2 DESC
        '''.format(
            table=self.table,
            ds=ds,
            event_time=self.timestamp_column,
        )
        result = self.client.query(sql).result()
        return [r['hour'] for r in result]

    def _sync_hour(self, gcs_prefix, hour: int, execution_date: datetime):
        ds = execution_date.strftime('%Y-%m-%d')
        ds_nodash  = execution_date.strftime('%Y%m%d')
        base_date = self.LOCALTZ.localize(datetime.strptime(ds, '%Y-%m-%d'), is_dst=None)\
            .astimezone(pytz.utc)
        start_date = base_date + timedelta(hours=hour)
        end_date = base_date + timedelta(hours=hour+1)
        # convert this date into utc time
        intervals = '{}/{}'.format(
            start_date.strftime(self.INTERVAL_FMT),
            end_date.strftime(self.INTERVAL_FMT),
        )
        gcs_prefix = '{}/{}'.format(gcs_prefix, hour)
        destination_cloud_storage_uris = [
            'gs://{}/{}/*.json.gz'.format(self.gcs_bucket, gcs_prefix)
        ]
        sql = '''
            SELECT
                * EXCEPT({event_time}),
                UNIX_MILLIS({event_time}) AS {event_time}
            FROM `{table}`
            WHERE DATE({event_time}, '+7') = '{ds}'
            AND EXTRACT(HOUR FROM {event_time} AT TIME ZONE '+7') = {hour}
        '''.format(
            table=self.table,
            ds=ds,
            event_time=self.timestamp_column,
            hour=hour
        )
        sync_table = 'dwh.druid.{}_{}_{}'.format(
            self.olap_destination_table,
            ds_nodash,
            hour,
        )
        self.LOGGER.info('Insert to %s from SQL %s', sync_table, sql)
        bq_cursor = self.bq_cursor
        bq_cursor.run_query(
            sql=sql,
            destination_dataset_table=sync_table,
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
        )
        self.LOGGER.info('Export to %s from table %s',
                         destination_cloud_storage_uris,
                         sync_table)
        bq_cursor.run_extract(
            sync_table,
            destination_cloud_storage_uris,
            compression='GZIP',
            export_format='NEWLINE_DELIMITED_JSON',
        )

        # 3. submit indexing task
        # get ingestion measures if not exist
        detected_dimentions = self.olap_dimentions
        if not detected_dimentions:
            project_id, dataset, table = sync_table.split('.')
            bq_schema = (
                bq_cursor.service.tables()
                .get(projectId=project_id, datasetId=dataset, tableId=table)
                .execute(num_retries=bq_cursor.num_retries)["schema"]
            )
            detected_dimentions = bq_schema_to_druid_schema(bq_schema)

        exclude_dimensions = (self.exclude_dimensions or []) + (self.sort_keys or [])

        def filter_dims(dim_col):
            name = dim_col['name'] if isinstance(dim_col, dict) else dim_col
            return name not in exclude_dimensions and name != self.timestamp_column

        detected_dimentions = [v for v in detected_dimentions if filter_dims(v)]
        # move sort_keys to top of dim spec
        olap_dimentions = self.sort_keys + detected_dimentions

        gcs_hook = GoogleCloudStorageHook(gcp_conn_id=self.bigquery_conn_id)
        files = gcs_hook.list(self.gcs_bucket, prefix=gcs_prefix)
        builder = self._job_builder(olap_dimentions, files, intervals)\
            .single()\
            .max_retry(4)
        payload = builder.build()

        druid_hook = DruidHook(druid_ingest_conn_id=self.druid_ingest_conn_id)
        num_try = 0
        while True:
            if num_try > 3:
                raise AirflowException('Got Max retries')

            num_try += 1
            # now sumit all jobs
            self.LOGGER.info('[Try %s] Submit task %s',
                             num_try,
                             json.dumps(payload))
            try:
                druid_hook.submit_indexing_job(payload)
                self.LOGGER.info('Indexed Success !!!')
                return
            except Exception as e:
                self.LOGGER.error(e)

    @property
    @functools.lru_cache()
    def client(self) -> bigquery.Client:
        hook = GoogleCloudBaseHook(gcp_conn_id=self.bigquery_conn_id)
        return bigquery.Client(project=hook.project_id,
                               credentials=hook._get_credentials())

    @property
    def bq_cursor(self):
        return BigQueryHook(gcp_conn_id=self.bigquery_conn_id)\
            .get_conn().cursor()

    def _job_builder(self, schema: dict, files: List[str], intervals: str):
        gcs_files = [{'bucket': self.gcs_bucket, 'path': i} for i in files]
        builder = DruidIndexerBuilder()\
            .set_data_source(self.olap_destination_table)\
            .set_dimensions(schema)\
            .set_timestamp_spec_column(self.timestamp_column)\
            .set_timestamp_spec_format(self.timestamp_format)\
            .set_segmenent_granularity(self.segmenent_granularity)\
            .set_query_granularity(self.query_granularity)\
            .set_gcs_files(gcs_files)\
            .set_parallel(self.DEFAULT_MAX_PARALLEL)\
            .set_interval_dates([intervals])

        if self.olap_metrics:
            builder = builder.set_metrics(self.olap_metrics)

        return builder
