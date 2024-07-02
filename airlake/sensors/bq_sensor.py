import logging

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from airlake.hooks.gcs_hook import BigQueryNativeHook


class BigQuerySQLSensor(BaseSensorOperator):
    template_fields = ("sql",)
    ui_color = "#f0eee4"

    @apply_defaults
    def __init__(
        self,
        sql: str,
        bigquery_conn_id="bigquery_default_conn",
        delegate_to=None,
        *args,
        **kwargs
    ):
        if not sql:
            raise Exception("Must have sql")
        super(BigQuerySQLSensor, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info("Sensor checks sql:\n%s", self.sql)
        hook = BigQueryNativeHook(
            gcp_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
        )

        try:
            return hook.total_rows(self.sql) > 0
        except Exception as e:
            logging.error("Execute error %s", e)
            return False
