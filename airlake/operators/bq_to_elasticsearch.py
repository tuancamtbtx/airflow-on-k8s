from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook as GoogleCloudStorageHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


from elasticsearch import Elasticsearch
from elasticsearch import helpers
import gzip
import json
import os
import io
from airlake.hooks.elasticsearch_hook import ElasticSearchHook


class BigqueryToElasticSearchOperator(BaseOperator):
    template_fields = (
        "source_project_dataset_table",
        "destination_cloud_storage_uris",
        "gcs_bucket",
        "gcs_prefix",
        "elasticsearch_conn_id",
        "es_index",
        "size_bulk",
        "labels",
    )

    @apply_defaults
    def __init__(
        self,
        sql="",
        destination_cloud_storage_uris="",
        source_project_dataset_table="",
        source_bucket="tiki-brain",
        es_index="",
        gcp_conn_id="bigquery_default",
        gcs_bucket="",
        gcs_prefix="",
        export_format="NEWLINE_DELIMITED_JSON",
        elasticsearch_conn_id="elasticsearch_default",
        compression=None,
        field_delimiter=",",
        print_header=True,
        labels=None,
        delegate_to=None,
        size_bulk=1000,
        *args,
        **kwargs
    ):
        super(BigqueryToElasticSearchOperator, self).__init__(*args, **kwargs)
        # config for input
        if not source_project_dataset_table and not destination_cloud_storage_uris:
            raise Exception(
                "Must have source_project_dataset_table or destination_cloud_storage_uris"
            )
        self.source_project_dataset_table = source_project_dataset_table
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        # init for gcp
        self.gcp_conn_id = gcp_conn_id
        # init for gcs config write file
        self.export_format = export_format
        self.compression = compression
        self.print_header = print_header
        self.field_delimiter = field_delimiter
        self.labels = labels
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        # init for elasticsearch
        self.es_index = es_index
        self.elasticsearch_conn_id = elasticsearch_conn_id
        self.es_client = None

        self.delegate_to = delegate_to
        self.size_bulk = size_bulk
        # validate input from config dag

    def execute(self, context):
        self.log.info(
            "Sync table %s to elasticsearch", self.source_project_dataset_table
        )
        self.es_hook = ElasticSearchHook(self.elasticsearch_conn_id, self.es_index)
        self.gcs_hook = GoogleCloudStorageHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )
        es_client = self.es_hook.get_client()
        self.es_client = es_client
        es_info = es_client.info()
        self.log.info("elasticsearch info: " + json.dumps(es_info))
        self.bigquery_to_gcs()
        self.es_index_alias = self.es_hook.generate_alias_index_by_time(self.es_index)
        self.mapping(self.es_index_alias)
        list_file = self.get_gcs_file_in_object()
        if len(list_file) == 0:
            raise Exception("Empty stages files, check your sql or table")
        for file_name in list_file:
            self.process_by_file(file_name)
        # swap index after sync finish
        alias_exist_arr = self.es_hook.get_alias_by_prefix(self.es_index)
        if len(alias_exist_arr) > 0:
            for alias in alias_exist_arr:
                if alias != self.es_index_alias and alias < self.es_index_alias:
                    self.es_hook.remove_index(alias)
        self.es_hook.alias_index(self.es_index_alias, self.es_index)

    def bigquery_to_gcs(self):
        self.log.info(
            "Executing extract of %s into: %s",
            self.source_project_dataset_table,
            self.destination_cloud_storage_uris,
        )
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        project_id, dataset, table = self.source_project_dataset_table.split(".")
        self.bq_schema = (
            cursor.service.tables()
            .get(projectId=project_id, datasetId=dataset, tableId=table)
            .execute(num_retries=cursor.num_retries)["schema"]
        )
        cursor.run_extract(
            source_project_dataset_table=self.source_project_dataset_table,
            destination_cloud_storage_uris=self.destination_cloud_storage_uris,
            compression=self.compression,
            export_format=self.export_format,
            field_delimiter=self.field_delimiter,
            print_header=self.print_header,
            labels=self.labels,
        )

    def mapping(self, es_index_alias):

        if self.es_client.indices.exists(index=es_index_alias):
            es_mapping = self.es_hook.build_schema_to_elasticseach_schema(
                self.bq_schema, True
            )
            self.log.info("Mapping schema -update: " + json.dumps(es_mapping))
            self.es_client.indices.put_mapping(
                index=es_index_alias, body=es_mapping, ignore=[400, 404]
            )
        else:
            es_mapping = self.es_hook.build_schema_to_elasticseach_schema(
                self.bq_schema, True
            )
            self.log.info("Mapping schema - create: " + json.dumps(es_mapping))
            es_setting = {
                "settings": {"index": {"number_of_shards": 3, "number_of_replicas": 2}}
            }
            self.es_client.create(
                index=self.es_index_alias, id="_doc", ignore=[400, 404], body=es_setting
            )
            self.es_client.indices.put_mapping(
                index=es_index_alias, body=es_mapping, ignore=[400, 404]
            )

    def get_gcs_file_in_object(self):
        if "*" in self.gcs_prefix:
            wildcard_position = self.gcs_prefix.index("*")
            self.log.info(
                "get list object from bucket: %s - prefix: %s ",
                self.gcs_bucket,
                self.gcs_prefix[:wildcard_position],
            )
            list_objects = self.gcs_hook.list(
                self.gcs_bucket,
                prefix=self.gcs_prefix[:wildcard_position],
                delimiter=self.gcs_prefix[wildcard_position + 1 :],
            )
            return list_objects
        return []

    def count_line_file(self, filename):
        num_line = 0
        with gzip.open(filename, mode="rb") as gcs_data:
            for i, _ in enumerate(gcs_data):
                num_line += 1
                pass
        return num_line + 1

    def process_by_file(self, gcs_object):
        local_file = os.path.basename(gcs_object)
        tmp_file_json = os.path.join("/tmp", local_file)
        self.log.info(
            "Executing download: %s, %s, %s",
            self.gcs_bucket,
            gcs_object,
            local_file,
        )
        self.gcs_hook.download(self.gcs_bucket, gcs_object, tmp_file_json)
        count = 0
        count_debug = 0
        list_jsons = []
        num_line = self.count_line_file(tmp_file_json)
        with gzip.open(tmp_file_json, mode="rb") as gcs_data:
            for line in gcs_data:
                if len(line.strip()) != 0:
                    json_text = json.loads(line.decode("utf-8"))
                    list_jsons.append(json_text)
                    count += 1
                    count_debug += 1
                if count == self.size_bulk:
                    self.es_hook.bulk(list_jsons, self.es_index_alias)
                    list_jsons = []
                    count = 0
                if count + 1 == num_line:
                    self.es_hook.bulk(list_jsons, self.es_index_alias)
                    self.log.info("Finished process file: %s", local_file)
        self.log.info("File total row: " + str(num_line))
        self.log.info("Process total row: " + str(count_debug))
