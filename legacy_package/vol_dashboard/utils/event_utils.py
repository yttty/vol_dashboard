import datetime
from pprint import pprint
from typing import Literal

import numpy as np

from vol_dashboard.config import ADJ_MINUTES_AFTER_RELEASE, MINUTES_AFTER_RELEASE, MINUTES_BEFORE_RELEASE
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.utils.tz_utils import et_to_utc


def get_events(op: Literal["previous", "upcoming"]) -> list[tuple]:
    db_conn = VolDbConnector()
    events = db_conn.get_events()

    matched_events = []
    for event_name, date_str, time_et_str in events:
        naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
        _, utc_dt = et_to_utc(naive_et_dt)
        start_before_dt = utc_dt - datetime.timedelta(minutes=MINUTES_BEFORE_RELEASE)
        end_after_dt = utc_dt + datetime.timedelta(
            minutes=ADJ_MINUTES_AFTER_RELEASE.get(event_name, MINUTES_AFTER_RELEASE)
        )
        match op:
            case "previous":
                if end_after_dt < datetime.datetime.now(tz=datetime.timezone.utc):
                    matched_events.append((event_name, date_str, time_et_str, utc_dt))
            case "upcoming":
                if start_before_dt > datetime.datetime.now(tz=datetime.timezone.utc):
                    matched_events.append((event_name, date_str, time_et_str, utc_dt))
            case _:
                raise ValueError

    return matched_events


def get_previous_events():
    return get_events("previous")


def get_upcoming_events():
    return get_events("upcoming")


if __name__ == "__main__":
    print("Previous events")
    pprint(get_previous_events())
    print("Upcoming events")
    pprint(get_upcoming_events())
