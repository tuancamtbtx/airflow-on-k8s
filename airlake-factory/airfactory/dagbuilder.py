from typing import Optional,List, Any, Dict,Union,Tuple
from croniter import croniter
import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG, configuration
from airflow.utils.dates import cron_presets


from airfactory.core.consts import TaskFields
from airlake.utils import timestamp
from airlake.utils.logger import LoggerMixing
_TASK_FIELD_TRANSFORM = {
    "execution_timeout": timestamp.seconds_to_delta,
    "sla": timestamp.seconds_to_delta,
}

TASK_T = Union[BaseOperator, TaskGroup]
DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh'

def identify_fn(v):
    return v
class DagBuilder(LoggerMixing):
       # these are params only used in the DAG factory, not in the tasks
    SYSTEM_PARAMS: List[str] = [TaskFields.Operator, TaskFields.Dependencies]

    def __init__(self, dag_name: str, dag_config: Dict[str, DAG]):
        self.dag_config: Dict[str, Any] = dag_config
        self.dag_config["dag_id"] = dag_name

    def build(self) -> Tuple[str, Any]:
        """
        Generates a DAG from the config
        Returns:
            (dag_id, DAG instance)
        """
        dag_params: Dict[str, Any] = self.dag_config
        schedule_interval: Optional[str] = (
            dag_params.get("schedule_interval", None) or None
        )
        self.logger.info("Building DAG %s", dag_params["dag_id"])
        

    @classmethod
    def make_tasks(
        cls,
        tasks: Dict[str, Dict[str, Any]],
        dag: Optional[DAG] = None,
        task_group: Optional[TaskGroup] = None,
    ) -> Dict[str, TASK_T]:
        pass

    @classmethod
    def make_task(
        cls,
        operator: str,
        task_params: Dict[str, Any],
        dag: Optional[DAG] = None,
        parent_group: Optional[TaskGroup] = None,
    ) -> Tuple[str, TASK_T]:
        pass
    @classmethod
    def _parse_task_kwargs(cls, task_params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            k: (_TASK_FIELD_TRANSFORM.get(k, None) or identify_fn)(v)
            for k, v in task_params.items()
        }
    @classmethod
    def validate_task(cls, task_params: Dict[str, Any]):
        pass

    @classmethod
    def verify_cron(cls, schedule_interval: Optional[str]) -> bool:
        if schedule_interval == "@once" or schedule_interval is None:
            return True

        if isinstance(schedule_interval, str) and schedule_interval in cron_presets:
            _schedule_interval = cron_presets.get(schedule_interval)
        else:
            _schedule_interval = schedule_interval

        if _schedule_interval is None:
            return True

        try:
            croniter(_schedule_interval, datetime.datetime.now())
            return croniter.is_valid(_schedule_interval)
        except Exception as e:
            print(
                "Invalid schedule_interval",
                schedule_interval,
                "compiled to",
                _schedule_interval,
            )
            raise e
    
    @classmethod
    def validate_label(cls, name: str):
        # https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
        if 0 < len(name) < 63:
            return
        raise Exception(
            f"Consider change the name less than 63 chars: `{name}`. If not use please remove yaml file! Love you <3"
        )