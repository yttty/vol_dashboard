import datetime
from collections import namedtuple
from pprint import pprint
from typing import Literal

from .config import ADJ_MINUTES_AFTER_RELEASE, MINUTES_AFTER_RELEASE, MINUTES_BEFORE_RELEASE
from .tz_utils import et_to_utc

# --- 🗓️ Economic Event Schedule ---
# Structure: ('Event Name', 'YYYY-MM-DD', 'HH:MM in ET')
ECONOMIC_EVENTS = [
    # 2024
    ("NFP", "2024-10-04", "08:30"),
    ("CPI", "2024-10-10", "08:30"),
    ("PPI", "2024-10-11", "08:30"),
    ("FOMC", "2024-09-18", "14:00"),
    ("NFP", "2024-11-01", "08:30"),
    ("FOMC", "2024-11-07", "14:00"),
    ("CPI", "2024-11-13", "08:30"),
    ("PPI", "2024-11-14", "08:30"),
    ("NFP", "2024-12-06", "08:30"),
    ("CPI", "2024-12-11", "08:30"),
    ("PPI", "2024-12-12", "08:30"),
    ("FOMC", "2024-12-18", "14:00"),
    # 2025
    ("NFP", "2025-01-10", "08:30"),
    ("CPI", "2025-01-16", "08:30"),
    ("PPI", "2025-01-17", "08:30"),
    ("FOMC", "2025-01-29", "14:00"),
    ("NFP", "2025-02-07", "08:30"),
    ("CPI", "2025-02-13", "08:30"),
    ("PPI", "2025-02-14", "08:30"),
    ("NFP", "2025-03-07", "08:30"),
    ("CPI", "2025-03-13", "08:30"),
    ("PPI", "2025-03-14", "08:30"),
    ("FOMC", "2025-03-19", "14:00"),
    ("NFP", "2025-04-04", "08:30"),
    ("CPI", "2025-04-10", "08:30"),
    ("PPI", "2025-04-11", "08:30"),
    ("FOMC", "2025-04-30", "14:00"),
    ("NFP", "2025-05-02", "08:30"),
    ("CPI", "2025-05-13", "08:30"),
    ("PPI", "2025-05-14", "08:30"),
    ("NFP", "2025-06-06", "08:30"),
    ("FOMC", "2025-06-11", "14:00"),
    ("PPI", "2025-06-12", "08:30"),
    ("NFP", "2025-07-03", "08:30"),
    ("CPI", "2025-07-11", "08:30"),
    ("FOMC", "2025-07-30", "14:00"),
    ("NFP", "2025-08-01", "08:30"),
    ("CPI", "2025-08-12", "08:30"),
    ("PPI", "2025-08-13", "08:30"),
    ("NFP", "2025-09-05", "08:30"),
    ("CPI", "2025-09-11", "08:30"),
    ("PPI", "2025-09-12", "08:30"),
    ("FOMC", "2025-09-17", "14:00"),
    ("NFP", "2025-10-03", "08:30"),
    ("PPI", "2025-10-10", "08:30"),
    ("CPI", "2025-10-16", "08:30"),
    ("FOMC", "2025-10-29", "14:00"),
    ("NFP", "2025-11-07", "08:30"),
    ("CPI", "2025-11-14", "08:30"),
    ("PPI", "2025-11-14", "08:30"),
    ("NFP", "2025-12-05", "08:30"),
    ("FOMC", "2025-12-10", "14:00"),
    ("CPI", "2025-12-11", "08:30"),
    ("PPI", "2025-12-12", "08:30"),
    # 2026
    ("NFP", "2026-01-09", "08:30"),
    ("FOMC", "2026-01-28", "14:00"),
    ("NFP", "2026-02-06", "08:30"),
    ("FOMC", "2026-03-18", "14:00"),
    ("NFP", "2026-03-06", "08:30"),
    ("NFP", "2026-04-03", "08:30"),
    ("FOMC", "2026-04-29", "14:00"),
    ("NFP", "2026-05-08", "08:30"),
    ("NFP", "2026-06-05", "08:30"),
    ("FOMC", "2026-06-17", "14:00"),
    ("NFP", "2026-07-02", "08:30"),
    ("FOMC", "2026-07-29", "14:00"),
    ("NFP", "2026-08-07", "08:30"),
    ("NFP", "2026-09-04", "08:30"),
    ("FOMC", "2026-09-16", "14:00"),
    ("NFP", "2026-10-02", "08:30"),
    ("FOMC", "2026-10-28", "14:00"),
    ("NFP", "2026-11-06", "08:30"),
    ("NFP", "2026-12-04", "08:30"),
    ("FOMC", "2026-12-09", "14:00"),
]

Event = namedtuple(
    "Event",
    [
        "event_name",
        "date_str",
        "time_et_str",
        "utc_dt",
    ],
)


def get_events(op: Literal["previous", "upcoming"]) -> list[tuple]:
    matched_events = []
    for event_name, date_str, time_et_str in ECONOMIC_EVENTS:
        naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
        _, utc_dt = et_to_utc(naive_et_dt)
        start_before_dt = utc_dt - datetime.timedelta(minutes=MINUTES_BEFORE_RELEASE)
        end_after_dt = utc_dt + datetime.timedelta(
            minutes=ADJ_MINUTES_AFTER_RELEASE.get(event_name, MINUTES_AFTER_RELEASE)
        )
        match op:
            case "previous":
                if end_after_dt < datetime.datetime.now(tz=datetime.timezone.utc):
                    matched_events.append(Event(event_name, date_str, time_et_str, utc_dt))
            case "upcoming":
                if start_before_dt > datetime.datetime.now(tz=datetime.timezone.utc):
                    matched_events.append(Event(event_name, date_str, time_et_str, utc_dt))
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
