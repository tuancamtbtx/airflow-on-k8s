from airflow.plugins_manager import AirflowPlugin
from datetime import date, datetime, timedelta
from airflow.utils.db import provide_session
from airflow.models import DagRun, DAG
from airflow.utils import timezone
import pendulum


DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh'


@provide_session
def _get_dag_run(ti, session=None):
    """Get DagRun obj of the TaskInstance ti

    Args:
        ti (TYPE): the TaskInstance object
        session (None, optional): Not in use

    Returns:
        DagRun obj: the DagRun obj of the TaskInstance ti
    """
    task = ti.task
    dag_run = None
    if hasattr(task, 'dag'):
        dag_run = (
            session.query(DagRun)
            .filter_by(
                dag_id=task.dag.dag_id,
                execution_date=ti.execution_date)
            .first()
        )
        session.expunge_all()
        session.commit()
    return dag_run


def ds_add_no_dash(ds, days):
    """
    Add or subtract days from a YYYYMMDD
    :param ds: anchor date in ``YYYYMMDD`` format to add to
    :type ds: str
    :param days: number of days to add to the ds, you can use negative values
    :type days: int
    >>> ds_add('20150101', 5)
    '20150106'
    >>> ds_add('20150106', -5)
    '20150101'
    """

    ds = datetime.strptime(ds, '%Y%m%d')
    if days:
        ds = ds + timedelta(days)
    return ds.isoformat()[:10].replace('-', '')


def execution_date(ti):
    """get the TaskInstance execution date (in DAG timezone) in pendulum obj

    Args:
        ti (TaskInstance): the TaskInstance object

    Returns:
        pendulum obj: execution_date in pendulum object (in DAG tz)
    """
    execution_date = ti.dag_run.logical_date
    if ti.dag_run.data_interval_start:
        execution_date = ti.dag_run.data_interval_start
    # execution_date_pdl = pendulum.instance(execution_date)
    # dagtz_execution_date_pdl = execution_date_pdl.in_timezone(DEFAULT_TIMEZONE)
    return timezone.coerce_datetime(execution_date).in_timezone(
        DEFAULT_TIMEZONE)


def next_execution_date(ti):
    """get the TaskInstance next execution date (in DAG timezone) in pendulum obj

    Args:
        ti (TaskInstance): the TaskInstance object

    Returns:
        pendulum obj: next execution_date in pendulum object (in DAG tz)
    """

    # For manually triggered dagruns that aren't run on a schedule, next/previous
    # schedule dates don't make sense, and should be set to execution date for
    # consistency with how execution_date is set for manually triggered tasks, i.e.
    # triggered_date == execution_date.
    # dag = ti.task.dag
    # data_interval = dag.get_run_data_interval(ti.dag_run)
    # execution_date = timezone.coerce_datetime(data_interval.start)
    execution_date = ti.dag_run.logical_date
    if ti.dag_run.data_interval_start:
        execution_date = ti.dag_run.data_interval_start
    if dag_run and dag_run.external_trigger:
        next_execution_date = execution_date
    else:
        next_execution_date = ti.task.dag.following_schedule(
            execution_date)

    next_execution_date_pdl = pendulum.instance(next_execution_date)
    dagtz_next_execution_date_pdl = next_execution_date_pdl.in_timezone(
        DEFAULT_TIMEZONE)
    return dagtz_next_execution_date_pdl


def next_ds(ti):
    """get the TaskInstance next execution date (in DAG timezone) in YYYY-MM-DD string
    """
    dagtz_next_execution_date_pdl = execution_date(ti).add(days=1)
    return dagtz_next_execution_date_pdl.strftime('%Y-%m-%d')


def next_ds_nodash(ti):
    """get the TaskInstance next execution date (in DAG timezone) in YYYYMMDD string
    """
    dagtz_next_ds_str = next_ds(ti)
    return dagtz_next_ds_str.replace('-', '')


def prev_execution_date(ti):
    """get the TaskInstance previous execution date (in DAG timezone) in pendulum obj

    Args:
        ti (TaskInstance): the TaskInstance object

    Returns:
        pendulum obj: previous execution_date in pendulum object (in DAG tz)
    """

    # For manually triggered dagruns that aren't run on a schedule, next/previous
    # schedule dates don't make sense, and should be set to execution date for
    # consistency with how execution_date is set for manually triggered tasks, i.e.
    # triggered_date == execution_date.
    # dag = ti.task.dag
    # data_interval = dag.get_run_data_interval(ti.dag_run)
    # execution_date = timezone.coerce_datetime(data_interval.start)
    execution_date = ti.dag_run.logical_date
    if ti.dag_run.data_interval_start:
        execution_date = ti.dag_run.data_interval_start
    if dag_run and dag_run.external_trigger:
        prev_execution_date = execution_date
    else:
        prev_execution_date = ti.task.dag.previous_schedule(
            execution_date)

    prev_execution_date_pdl = pendulum.instance(prev_execution_date)
    dagtz_prev_execution_date_pdl = prev_execution_date_pdl.in_timezone(
        DEFAULT_TIMEZONE)
    return dagtz_prev_execution_date_pdl


def prev_ds(ti):
    """get the TaskInstance prev execution date (in DAG timezone) in YYYY-MM-DD string
    """
    return (execution_date(ti) - timedelta(1)).strftime('%Y-%m-%d')


def prev_ds_nodash(ti):
    """get the TaskInstance prev execution date (in DAG timezone) in YYYYMMDD string
    """
    dagtz_prev_ds_str = prev_ds(ti)
    return dagtz_prev_ds_str.replace('-', '')


def ds(ti):
    return execution_date(ti).strftime('%Y-%m-%d')


def ds_nodash(ti):
    return execution_date(ti).strftime('%Y%m%d')


def ds_custom_format(ti, format='%Y-%m-%d'):
    return execution_date(ti).strftime(format)


def tomorrow_ds_nodash(ti):
    return next_ds_nodash(ti)


def tomorrow_ds(ti):
    return next_ds(ti)


def yesterday_ds(ti):
    return prev_ds(ti)


def yesterday_ds_nodash(ti):
    return prev_ds_nodash(ti)


def ts(ti):
    return execution_date(ti).to_iso8601_string()


def ds_add(ti, days=1):
    """get the TaskInstance next execution date (in DAG timezone) in YYYY-MM-DD string
    """
    if isinstance(ti, str):
        ds = datetime.strptime(ti, '%Y-%m-%d')
        ds = ds + timedelta(days)
        return ds.isoformat()[:10]
    dagtz_next_execution_date_pdl = execution_date(ti).add(days=days)
    return dagtz_next_execution_date_pdl.strftime('%Y-%m-%d')


def ds_add_nodash(ti, days=1):
    """get the TaskInstance next execution date (in DAG timezone) in YYYY-MM-DD string
    """
    if isinstance(ti, str):
        ds = datetime.strptime(ti, '%Y%m%d')
        ds = ds + timedelta(days)
        return ds.isoformat()[:10]
    dagtz_next_execution_date_pdl = execution_date(ti).add(days=days)
    return dagtz_next_execution_date_pdl.strftime('%Y%m%d')


def ds_add_custom_format(ti, days=1, format='%Y-%m-%d'):
    if isinstance(ti, str):
        ds = datetime.strptime(ti, '%Y%m%d')
        ds = ds + timedelta(days)
        return ds.strftime(format)
    dagtz_next_execution_date_pdl = execution_date(ti).add(days=days)
    return dagtz_next_execution_date_pdl.strftime(format)


def beginning_week_ds(ti):
    dow = execution_date(ti).day_of_week
    if dow == 0:
        return (execution_date(ti) - timedelta(days=6)).strftime('%Y-%m-%d')
    return (execution_date(ti) - timedelta(days=dow-1)).strftime('%Y-%m-%d')


def beginning_week_ds_nodash(ti):
    dow = execution_date(ti).day_of_week
    if dow == 0:
        return (execution_date(ti) - timedelta(days=6)).strftime('%Y%m%d')
    return (execution_date(ti) - timedelta(days=dow-1)).strftime('%Y%m%d')


def beginning_month_ds(ti):
    return execution_date(ti).replace(day=1).strftime('%Y-%m-%d')


def beginning_month_ds_nodash(ti):
    return execution_date(ti).replace(day=1).strftime('%Y%m%d')


def end_month_ds(ti):
    day = execution_date(ti)
    # this will never fail
    next_month = day.replace(day=28) + timedelta(days=4)
    return (next_month - timedelta(days=next_month.day)).strftime('%Y-%m-%d')


def end_month_ds_nodash(ti):
    day = execution_date(ti)
    # this will never fail
    next_month = day.replace(day=28) + timedelta(days=4)
    return (next_month - timedelta(days=next_month.day)).strftime('%Y%m%d')


def beginning_quarter_ds(ti):
    return execution_date(ti).first_of('quarter').strftime('%Y-%m-%d')


def beginning_quarter_ds_nodash(ti):
    return execution_date(ti).first_of('quarter').strftime('%Y%m%d')


class AirflowLocalTimeZonePlugin(AirflowPlugin):
    name = "localtz"
    macros = [
        execution_date,
        ds,
        ds_nodash,
        ds_custom_format,
        ds_add_no_dash,
        ds_add,
        ds_add_nodash,
        ds_add_custom_format,
        ts,
        next_execution_date,
        next_ds,
        next_ds_nodash,
        prev_execution_date,
        prev_ds,
        prev_ds_nodash,
        tomorrow_ds,
        tomorrow_ds_nodash,
        yesterday_ds,
        yesterday_ds_nodash,
        beginning_week_ds,
        beginning_week_ds_nodash,
        beginning_month_ds,
        beginning_month_ds_nodash,
        end_month_ds,
        end_month_ds_nodash,
        beginning_quarter_ds,
        beginning_quarter_ds_nodash,
    ]
