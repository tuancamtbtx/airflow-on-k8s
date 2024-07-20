import re
from datetime import datetime, date, timedelta
from typing import Dict, Any, Union, Pattern, Match, AnyStr, Optional

import yaml
import pendulum


def get_start_date(date_value: Union[str, datetime, date], timezone: str = "UTC") -> datetime:
    """
    Takes value from DAG config and generates valid start_date. Defaults to
    today, if not a valid date or relative time (1 hours, 1 days, etc.)

    :param date_value: either a datetime (or date) or a relative time as string
    :type date_value: Uniont[datetime, date, str]
    :param timezone: string value representing timezone for the DAG
    :type timezone: str
    :returns: datetime for start_date
    :type: datetime.datetime
    """
    try:
        local_tz: pendulum.Timezone = pendulum.timezone(timezone)
    except Exception as e:
        raise Exception(f"Failed to create timezone; err: {e}")
    if isinstance(date_value, date):
        return datetime.combine(date=date_value, time=datetime.min.time()).replace(tzinfo=local_tz)
    if isinstance(date_value, datetime):
        return date_value.replace(tzinfo=local_tz)
    rel_delta: timedelta = get_time_delta(date_value)
    now: datetime = (
        datetime.today()
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .replace(tzinfo=local_tz)
    )
    if not rel_delta:
        return now
    return (now - rel_delta)


def get_time_delta(time_string: str) -> timedelta:
    """
    Takes a time string (1 hours, 10 days, etc.) and returns
    a python timedelta object

    :param time_string: the time value to convert to a timedelta
    :type time_string: str
    :returns: datetime.timedelta for relative time
    :type datetime.timedelta
    """
    rel_time: Pattern = re.compile(
        pattern=r"((?P<hours>\d+?)\s+hour)?((?P<minutes>\d+?)\s+minute)?((?P<seconds>\d+?)\s+second)?((?P<days>\d+?)\s+day)?",
        # noqa
        flags=re.IGNORECASE,
    )
    parts: Optional[Match[AnyStr]] = rel_time.match(string=time_string)
    if not parts:
        raise Exception(f"Invalid relative time: {time_string}")
    # https://docs.python.org/3/library/re.html#re.Match.groupdict
    parts: Dict[str, str] = parts.groupdict()
    time_params = {}
    if all(value == None for value in parts.values()):
        raise Exception(f"Invalid relative time: {time_string}")
    for time_unit, magnitude in parts.items():
        if magnitude:
            time_params[time_unit] = int(magnitude)
    return timedelta(**time_params)


class NoDatesSafeLoader(yaml.SafeLoader):
    @classmethod
    def remove_implicit_resolver(cls, tag_to_remove):
        """
        Remove implicit resolvers for a particular tag

        Takes care not to modify resolvers in super classes.

        We want to load datetimes as strings, not dates, because we
        go on to serialise as json which doesn't have the advanced types
        of yaml, and leads to incompatibilities down the track.
        """
        if not 'yaml_implicit_resolvers' in cls.__dict__:
            cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

        for first_letter, mappings in cls.yaml_implicit_resolvers.items():
            cls.yaml_implicit_resolvers[first_letter] = [(tag, regexp)
                                                         for tag, regexp in mappings
                                                         if tag != tag_to_remove]


NoDatesSafeLoader.remove_implicit_resolver('tag:yaml.org,2002:timestamp')


def safe_int(v) -> int:
    try:
        return int(v)
    except:
        return None


def read_content(path: str):
    with open(path, 'r') as f:
        return f.read()
