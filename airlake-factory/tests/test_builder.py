from airfactory.dagbuilder import AirlakeDataBuilder


dag = AirlakeDataBuilder(
    dag_name="test_dag",
    dag_config={
        "schedule_interval": "0 0 * * *",
        "timetable": {
            "start_date": "2021-01-01",
            "end_date": "2021-01-02",
        },
        "default_args": {
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
        },
        "tasks": {
            "task1": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo 1",
                "execution_timeout": 60,
                "sla": 30,
            },
            "task2": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo 2",
                "execution_timeout": 60,
                "sla": 30,
            },
        }
    },
).build()
print(dag)