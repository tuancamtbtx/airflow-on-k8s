from datetime import timedelta
from typing import Optional
import json

from pendulum import UTC, Date, DateTime, Time, now
from typing import Dict, List, Optional, Any

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.exceptions import AirflowTimetableInvalid

DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh'


class TikiTimeTable(Timetable):

    def __init__(self, config: Dict[str, Any]) -> None:
        def custom_sort(value):
            if value[1] is None:
                return (value[0], [])
            return (value[0], sorted(value[1]))
        self._config = dict(map(custom_sort, config.items()))
        self.description = f"TikiTimeTable: config {json.dumps(self._config)}"

    def serialize(self) -> Dict[str, Any]:
        return {"config": self._config}

    def validate(self) -> None:
        if 1 == 2:
            raise AirflowTimetableInvalid("Invalid Timetable")
        return

    @classmethod
    def deserialize(self, value: Dict[str, Any]) -> Timetable:
        return self(value["config"])

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        return DataInterval.exact(run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if restriction.earliest is None:  # No start_date. Don't schedule.
            return None
        # There was a previous run on the regular schedule.
        if last_automated_data_interval is not None:
            last_start = last_automated_data_interval.end
        else:  # This is the first ever run on the regular schedule.
            last_start = max(restriction.earliest, now(tz=DEFAULT_TIMEZONE))

        last_start_weekday = (last_start.weekday() + 1) % 7
        last_start_hour = last_start.hour
        if last_start_hour in self._config.get(
                str(last_start_weekday), []):
            idx = self._config.get(
                str(last_start_weekday), []).index(last_start_hour)
        else:
            # Not found
            idx = 999
        next_start_date = last_start.date()
        next_start_hour = last_start_hour
        if idx < (len(self._config.get(str(last_start_weekday), [])) - 1):
            next_start_hour = self._config.get(
                str(last_start_weekday))[idx+1]
        else:
            find_next_day = True
            for each in self._config.get(str(last_start_weekday), []):
                if each > last_start_hour:
                    find_next_day = False
                    next_start_hour = each
                    break

            if find_next_day:
                i = 1
                while i <= 7:
                    next_start_date = last_start.date() + timedelta(days=i % 7)
                    next_start_weekday = next_start_date.weekday()
                    if len(self._config.get(str(next_start_weekday), [])) > 0:
                        next_start_hour = self._config.get(
                            str(next_start_weekday))[0]
                        break
                    i += 1
        next_start = DateTime.combine(
            next_start_date, Time(hour=next_start_hour)).replace(tzinfo=DEFAULT_TIMEZONE)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=last_start, end=next_start)


class TikiTimetablePlugin(AirflowPlugin):
    name = "tiki_timetable"
    timetables = [TikiTimeTable]
