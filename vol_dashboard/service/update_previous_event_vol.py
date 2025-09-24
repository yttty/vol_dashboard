import datetime

import numpy as np
from config import INSTRUMENTS, MINUTES_AFTER_RELEASE, MINUTES_BEFORE_RELEASE
from loguru import logger

from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.utils.event_utils import get_previous_events
from vol_dashboard.utils.tz_utils import et_to_utc


def calculate_realized_volatility(prices_by_min: np.array) -> float:
    """Calculates annualized realized volatility from 1-minute price data."""
    if len(prices_by_min) < 2:
        raise ValueError
    log_prices = np.log(prices_by_min)
    log_returns = np.diff(log_prices)
    realized_variance = np.sum(log_returns**2)
    minutes_in_year = 365.25 * 24 * 60
    annualized_volatility = np.sqrt(realized_variance) * np.sqrt(minutes_in_year / len(prices_by_min))
    return float(annualized_volatility)


def update_previous_event_vol():
    db_conn = VolDbConnector()
    previous_events = get_previous_events()
    for event_name, date_str, time_et_str in previous_events:
        naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
        _, utc_dt = et_to_utc(naive_et_dt)
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
            event_vol = float(np.sqrt(max((vol_after / 100) ** 2 - (vol_before / 100) ** 2, 0)) * 100)
            logger.info(
                f"Event[{event_vol_id}] EventVol[{event_vol:.4f}] VolBefore[{vol_before:.4f}] VolAfter[{vol_after:.4f}]"
            )
            update_dt = datetime.datetime.now(tz=datetime.timezone.utc)
            db_conn.insert_event_vols(
                vol_records=[
                    (
                        event_vol_id,
                        event_name,
                        instrument_name,
                        utc_dt,
                        vol_before,
                        vol_after,
                        event_vol,
                        update_dt,
                    )
                ]
            )


if __name__ == "__main__":
    update_previous_event_vol()
