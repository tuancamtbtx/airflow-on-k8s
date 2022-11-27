import unittest
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance

from aircake.operators.sftp_download_operator import SFTPGetMultipleFilesOperator

DEFAULT_DATE = datetime(2022, 11, 27)

class Test_SFTPGetMultipleFilesOperator(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.dag = DAG('test_dag', default_args={'owner': 'nguyenvantuan140397@gmail.com', 'start_date': DEFAULT_DATE})
        self.ssh_conn_id="sftp_a_conn",
        self.local_directory=f'opt/airflow/tmp/local',
        self.remote_directory=f'upload', 

    def test_download_local(self):
        task = SFTPGetMultipleFilesOperator(
            ssh_conn_id=self.ssh_conn_id,
            local_directory=self.local_directory,
            remote_directory=self.remote_directory, 
            task_id='dowload_local', 
            dag=self.dag
        )
        ti = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(ti.get_template_context())
        assert result is True