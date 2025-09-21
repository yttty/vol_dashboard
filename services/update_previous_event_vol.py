import datetime

import numpy as np
import pandas as pd
from config import INSTRUMENTS
from db_connector import VolDbConnector
from loguru import logger
from tz_utils import et_to_utc

MINUTES_BEFORE_RELEASE = 24 * 60
MINUTES_AFTER_RELEASE = 30


def calculate_realized_volatility(npa: np.array) -> float:
    """Calculates annualized realized volatility from 1-minute price data."""
    if len(npa) < 2:
        return 0.0
    # # log_returns = np.log(df["close"] / df["close"].shift(1))
    # realized_variance = np.sum(log_returns**2)
    # minutes_in_year = 365.25 * 24 * 60
    # annualized_volatility = np.sqrt(realized_variance) * np.sqrt(minutes_in_year / len(df))
    # return annualized_volatility * 100


def update_previous_event_vol():
    db_conn = VolDbConnector()
    events = db_conn.get_events()

    previous_events = []
    for event_name, date_str, time_et_str in events:
        naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
        _, utc_dt = et_to_utc(naive_et_dt)
        start_before_dt = utc_dt - datetime.timedelta(minutes=MINUTES_BEFORE_RELEASE)
        end_after_dt = utc_dt + datetime.timedelta(minutes=MINUTES_AFTER_RELEASE)
        if end_after_dt < datetime.datetime.now(tz=datetime.timezone.utc):
            previous_events.append((event_name, date_str, time_et_str))

    for event_name, date_str, time_et_str in previous_events[-1:]:
        naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
        local_dt, utc_dt = et_to_utc(naive_et_dt)
        start_before_dt = utc_dt - datetime.timedelta(minutes=MINUTES_BEFORE_RELEASE)
        end_after_dt = utc_dt + datetime.timedelta(minutes=MINUTES_AFTER_RELEASE)

        for instrument_name in INSTRUMENTS:
            event_vol_id = f"{event_name}/{date_str}/{time_et_str}/{instrument_name}"
            logger.info(f"Update event vol of {event_vol_id}")

            before_klines = db_conn.get_klines(
                symbol=instrument_name,
                interval="1m",
                from_timestamp=int(start_before_dt.timestamp()),
                to_timestamp=int(utc_dt.timestamp()),
                exchange="DERIBIT",
            )
            if len(before_klines) != MINUTES_BEFORE_RELEASE:
                logger.error(f"Not enough kline before {event_vol_id}")
                continue
            before_close = np.array(list(map(lambda kl: kl[7], before_klines)))
            after_klines = db_conn.get_klines(
                symbol=instrument_name,
                interval="1m",
                from_timestamp=int(utc_dt.timestamp()),
                to_timestamp=int(end_after_dt.timestamp()),
                exchange="DERIBIT",
            )
            if len(after_klines) != MINUTES_AFTER_RELEASE:
                logger.error(f"Not enough kline after {event_vol_id}")
                continue
            after_close = np.array(list(map(lambda kl: kl[7], after_klines)))

            vol_before = calculate_realized_volatility(before_close)
            vol_after = calculate_realized_volatility(after_close)
            event_vol = np.sqrt(max((vol_after / 100) ** 2 - (vol_before / 100) ** 2, 0)) * 100
            logger.info(f"Event vol for {event_vol_id} is {event_vol}")


if __name__ == "__main__":
    update_previous_event_vol()
