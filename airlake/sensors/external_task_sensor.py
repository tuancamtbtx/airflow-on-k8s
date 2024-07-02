from datetime import timedelta


from airflow.sensors.external_task import ExternalTaskSensor as AExternalTaskSensor
from airflow.models import BaseOperatorLink, DagBag, DagModel, DagRun, TaskInstance
from sqlalchemy import func


class ExecutionDateFn:
    def __init__(self, execution_delta_hour: int = None,
            execution_delta_minutes: int = None):
        self.execution_delta_hour = execution_delta_hour
        self.execution_delta_minutes = execution_delta_minutes

    def __call__(self, execution_date, context=None):
        execution_date = context['data_interval_start']
        if self.execution_delta_hour is not None:
            return execution_date - timedelta(hours=self.execution_delta_hour)
        elif self.execution_delta_minutes is not None:
            return execution_date - timedelta(minutes=self.execution_delta_minutes)
        return execution_date

class ExternalTaskSensor(AExternalTaskSensor):
    def __init__(
            self,
            execution_delta_hour: int = None,
            execution_delta_minutes: int = None,
            *args,
            **kwargs):
        kwargs.pop('execution_delta', None)
        kwargs['execution_date_fn'] = ExecutionDateFn(execution_delta_hour, execution_delta_minutes)
        kwargs['check_existence'] = True
        super(ExternalTaskSensor, self).__init__(*args, **kwargs)

    def get_count(self, dttm_filter, session, states) -> int:
        """
        Get the count of records against dttm filter and states

        :param dttm_filter: date time filter for execution date
        :type dttm_filter: list
        :param session: airflow session object
        :type session: SASession
        :param states: task or dag states
        :type states: list
        :return: count of record against the filters
        """
        TI = TaskInstance
        DR = DagRun
        if not dttm_filter:
            return 0
        if self.external_task_id:
            dag_run = (
                session.query(DR)
                .filter(
                    DR.dag_id == self.external_dag_id,
                    DR.data_interval_start == dttm_filter[0]
                )
                .first()
            )
            if not dag_run:
                return 0
            count = (
                session.query(func.count())  # .count() is inefficient
                .filter(
                    TI.dag_id == self.external_dag_id,
                    TI.task_id == self.external_task_id,
                    TI.state.in_(states),
                    TI.run_id == dag_run.run_id,
                )
                .scalar()
            )
        else:
            count = (
                session.query(func.count())
                .filter(
                    DR.dag_id == self.external_dag_id,
                    DR.state.in_(states),
                    DR.data_interval_start.in_(dttm_filter),
                )
                .scalar()
            )
        if count > 1:
            return 1
        return count
