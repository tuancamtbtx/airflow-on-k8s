from airfactory.render import dump_to_py

def test_render():
    print(dump_to_py(
        name="test_dag",
        conf={
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
        }
    ))

test_render()