
import datetime
import pendulum
from croniter import croniter
from typing import Optional,List, Any, Dict,Union,Tuple,Callable

from airflow import DAG, configuration
from airflow.models.baseoperator import BaseOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import cron_presets
from airflow.utils.module_loading import import_string

from airfactory.core.consts import TaskFields
from airfactory.core.consts import DefaultARGsFields, DagFields, OperatorName
from airfactory.common import timestamp
from airfactory.common.logger import LoggerMixing

_TASK_FIELD_TRANSFORM = {
	"execution_timeout": timestamp.seconds_to_delta,
	"sla": timestamp.seconds_to_delta,
}

TASK_T = Union[BaseOperator, TaskGroup]
DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh'

def identify_fn(v):
	return v


class AirlakeDataBuilder(LoggerMixing):
	"""
	A factory class for building DAG objects based on provided configurations.
	"""

	# these are params only used in the DAG factory, not in the tasks
	SYSTEM_PARAMS: List[str] = [TaskFields.Operator, TaskFields.Dependencies]

	def __init__(self, dag_name: str, dag_config: Dict[str, DAG]):
		self.dag_config: Dict[str, Any] = dag_config
		self.dag_config["dag_id"] = dag_name

	def build(self) -> Tuple[str, Any]:
		"""
		Builds and returns a DAG object based on the provided configuration.

		Returns:
			Tuple[str, Any]: A tuple containing the DAG ID and the tasks associated with the DAG.
		"""
		dag_params: Dict[str, Any] = self.dag_config
		schedule_interval: Optional[str] = (
			dag_params.get("schedule_interval", None) or None
		)
		self.verify_cron(schedule_interval)
		timetable: Optional[Dict] = (
			dag_params.get("timetable", None) or None
		)
		args = dict(
			execution_time=timestamp.seconds_to_delta(
				dag_params.get("execution_timeout", 0)
			),
			sla=timestamp.seconds_to_delta(dag_params.get("sla", 0)),
		)
		default_args = dag_params.get("default_args", {})
		default_args.update(args)
		dag_id = dag_params["dag_id"]
		self.validate_label(dag_id)
		self.logger.info(f"Building DAG {dag_id}")
		dag: DAG = DAG(
			dag_id,
			schedule_interval=schedule_interval,
			max_active_runs=dag_params.get(
				"max_active_runs",
				configuration.conf.getint("core", "max_active_runs_per_dag"),
			),
			is_paused_upon_creation=dag_params.get(
				"is_paused_upon_creation", None),
			default_args=default_args,
			catchup=False,
			start_date=default_args.get(
				DefaultARGsFields.StartDate, pendulum.today(tz=DEFAULT_TIMEZONE)),
			tags=dag_params.get("tags", []),
		)
		tasks: Dict[str, Dict[str, Any]] = dag_params[DagFields.Tasks]
		self.logger.info(f"Building tasks for DAG {dag_id} with tasks: {tasks}")
		return dag_id, self.make_tasks(tasks, dag=dag)
	

	@classmethod
	def make_tasks(
			self,
			tasks: Dict[str, Dict[str, Any]],
			dag: Optional[DAG] = None,
			task_group: Optional[TaskGroup] = None,
	) -> Dict[str, TASK_T]:
		"""
		Create and configure tasks based on the provided task configurations.

		Args:
			tasks (Dict[str, Dict[str, Any]]): A dictionary containing task configurations.
			dag (Optional[DAG]): The DAG object to which the tasks will be added. Defaults to None.
			task_group (Optional[TaskGroup]): The TaskGroup to which the tasks will be added. Defaults to None.

		Returns:
			Dict[str, TASK_T]: A dictionary mapping task IDs to their corresponding tasks.

		"""
		task_dict: Dict[str, TASK_T] = {}
		task_kwargs: Dict[str, Any] = {}

		for task_id, task_conf in tasks.items():
			self.validate_label(task_id)
			task_conf = {**task_conf, **{"task_id": task_id}, **task_kwargs}
			operator: str = task_conf.get(TaskFields.Operator)
			params: Dict[str, Any] = {
				k: v for k, v in task_conf.items() if k not in self.SYSTEM_PARAMS
			}
			task_id, task = self.make_task(
				operator=operator,
				task_params=params,
				dag=dag,
				parent_group=task_group,
			)
			task_dict[task_id] = task
		# set task dependencies after creating tasks
		for task_id, task in task_dict.items():
			self.validate_task(task)
			dependencies: List[str] = tasks.get(task_id, {}).get(
				TaskFields.Dependencies, []
			)
			for dep_id in dependencies:
				task.set_upstream(task_dict[dep_id])
		return task_dict

	@classmethod
	def make_task(
			self,
			operator: str,
			task_params: Dict[str, Any],
			dag: Optional[DAG] = None,
			parent_group: Optional[TaskGroup] = None,
	) -> Tuple[str, TASK_T]:
		"""
		Create and configure a task based on the provided operator and task parameters.

		Args:
			operator (str): The operator to be used for the task.
			task_params (Dict[str, Any]): The parameters for configuring the task.
			dag (Optional[DAG]): The DAG object to which the task will be added. Defaults to None.
			parent_group (Optional[TaskGroup]): The parent TaskGroup to which the task will be added. Defaults to None.

		Returns:
			Tuple[str, TASK_T]: A tuple containing the task ID and the configured task object.

		"""
		task_id = task_params.get(TaskFields.Id)

		if operator.endswith(OperatorName.TaskGroup):
			task_group = TaskGroup(
				group_id=task_id,
				dag=dag,
				task_group_id=task_id,
				retries=task_params.get("retries", 0),
				weight_rule=task_params.get("weight_rule", "upstream"),
				parent_group=parent_group,
			)
			self.make_tasks(
				tasks=task_params.get(TaskFields.Tasks, {}),
				dag=dag,
				task_group=task_group,
			)
			return task_id, task_group
		print(f"Building task {task_id} with operator {operator}")

		OperatorClass: Callable[..., BaseOperator] = import_string(operator)
		task_kwargs = self._parse_task_kwargs(task_params)
		task_kwargs["execution_timeout"] = datetime.timedelta(
			hours=configuration.conf.getint(
				"core", "execution_timeout_hour", fallback="6"
			)
		)
		task: BaseOperator = OperatorClass(
			dag=dag,
			task_group=parent_group,
			**task_kwargs,
		)
		return task_id, task

	@classmethod
	def _parse_task_kwargs(self, task_params: Dict[str, Any]) -> Dict[str, Any]:
		"""
		Parses the task parameters and transforms them based on predefined rules.

		Args:
			task_params (Dict[str, Any]): The task parameters to be parsed.

		Returns:
			Dict[str, Any]: The parsed task parameters.

		"""
		return {
			k: (_TASK_FIELD_TRANSFORM.get(k, None) or identify_fn)(v)
			for k, v in task_params.items()
		}

	@classmethod
	def validate_task(self, task_params: Dict[str, Any]):
		"""
		Validate the task parameters.

		Args:
			task_params (Dict[str, Any]): The task parameters to be validated.

		Returns:
			bool: True if the task parameters are valid, False otherwise.

		Raises:
			Exception: If the task ID or operator is missing.

		"""
		return True

	@classmethod
	def verify_cron(self, schedule_interval: Optional[str]) -> bool:
		"""
		Verify if the provided schedule interval is a valid cron expression.

		Args:
			schedule_interval (Optional[str]): The schedule interval to be verified.

		Returns:
			bool: True if the schedule interval is valid, False otherwise.

		Raises:
			Exception: If the schedule interval is invalid.

		"""
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
		"""
		Validate the label name.

		Args:
			name (str): The label name to be validated.

		Raises:
			Exception: If the label name is too long.

		"""
		if 0 < len(name) < 63:
			return
		raise Exception(
			f"Consider change the name less than 63 chars: `{name}`. If not use please remove yaml file! Love you <3"
		)