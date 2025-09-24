import datetime
from typing import Tuple
from zoneinfo import ZoneInfo


def et_to_utc(naive_et_dt: datetime.datetime) -> Tuple[datetime.datetime, datetime.datetime]:
    et_timezone = ZoneInfo("America/New_York")
    local_dt = naive_et_dt.replace(tzinfo=et_timezone)
    utc_dt = local_dt.astimezone(datetime.timezone.utc)
    return local_dt, utc_dt


if __name__ == "__main__":
    naive_et_dt = datetime.datetime(2025, 12, 19, 16, 11, 23)
    local_dt, utc_dt = et_to_utc(naive_et_dt)
    print(local_dt.isoformat())
    print(utc_dt.isoformat())
