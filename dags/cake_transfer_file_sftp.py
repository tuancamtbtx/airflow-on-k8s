
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.dates import days_ago

from aircake.operators.sftp_download_operator import SFTPGetMultipleFilesOperator
from aircake.operators.sftp_transfer_operator import SFTPPutMultipleFilesOperator
from aircake.logger import loggerFactory

logger = loggerFactory(__name__)


with DAG("cake_transfer_file_sftp",
        schedule_interval="@daily",
        tags=["cake_assessment"],
        start_date=days_ago(2)) as dag:

    sftp_watcher = SFTPGetMultipleFilesOperator(
        task_id=f"sftp_file_watcher",
        ssh_conn_id="sftp_a_conn",
        local_directory=f'opt/airflow/tmp/local',
        remote_directory=f'upload', 
        create_intermediate_dirs=True,
        do_xcom_push=True,
    )
    sftp_transfer = SFTPPutMultipleFilesOperator(
        task_id=f"sftp_file_transfer",
        ssh_conn_id="sftp_b_conn",
        local_directory=f'opt/airflow/tmp/local',
        remote_directory=f'upload',  
        create_intermediate_dirs=True,
    )
    sftp_watcher >> sftp_transfer