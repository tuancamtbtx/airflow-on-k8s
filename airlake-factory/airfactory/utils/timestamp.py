from datetime import timedelta
from typing import Optional


def seconds_to_delta(value: Optional[int]) -> Optional[timedelta]:
    if value is None:
        return None
    try:
        value = int(value)
    except:
        return None
    return timedelta(seconds=value)
