from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the function to be executed by the PythonOperator
def print_hello_world():
    print("Hello, World!")

# Define the DAG
dag = DAG(
    'print_hello_world',
    default_args=default_args,
    description='A simple DAG to print Hello, World!',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

# Define the PythonOperator
hello_world_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=print_hello_world,
    dag=dag,
)

# Set the task dependencies
hello_world_task