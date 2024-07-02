from typing import Any
from airflow.models import BaseOperator
from airflow.utils.context import Context

class BigqueryToSheetOperator(BaseOperator):
    template_fields = (
        "source_project_dataset_table",
        "destination_cloud_storage_uris",
        "gcs_bucket",
        "gcs_prefix",
        "gcp_conn_id",
        "labels",
    )
    def __init__(
        self,
        source_project_dataset_table: str = None,
        destination_cloud_storage_uris: str = None,
        compression: str = "",
        export_format: str = "CSV",
    ):
        self.source_project_dataset_table = source_project_dataset_table
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.compression = compression
        self.export_format = export_format
    
    def execute(self, context: Context) -> Any:
        return super().execute(context)