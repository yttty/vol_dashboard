import datetime
import json

import numpy as np
import pandas as pd
from loguru import logger

from vol_dashboard.config import INSTRUMENTS, MINUTES_AFTER_RELEASE, MINUTES_BEFORE_RELEASE
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.connector.redis_connector import get_redis_instance
from vol_dashboard.utils.event_utils import get_previous_events
from vol_dashboard.utils.tz_utils import et_to_utc
from vol_dashboard.utils.vol_utils import calculate_realized_volatility

DB_CONN = VolDbConnector()
RDS = get_redis_instance()


def update_previous_event_vol():
    previous_events = get_previous_events()
    for event_name, date_str, time_et_str, _ in previous_events:
        naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
        _, utc_dt = et_to_utc(naive_et_dt)
        start_before_dt = utc_dt - datetime.timedelta(minutes=MINUTES_BEFORE_RELEASE)
        end_after_dt = utc_dt + datetime.timedelta(minutes=MINUTES_AFTER_RELEASE)

        for instrument_name in INSTRUMENTS:
            event_vol_id = f"{event_name}/{date_str}/{time_et_str}/{instrument_name}"
            logger.info(f"Update event vol of {event_vol_id}")

            before_klines = DB_CONN.get_klines(
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
            after_klines = DB_CONN.get_klines(
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
            DB_CONN.insert_event_vols(
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


def estimate_event_vol() -> None:
    """
    saved value example:
    {
        "CPI|BTC": 0.48650858129208474,
        "CPI|ETH": 0.8956060885913556,
        "FOMC|BTC": 0.8341024650036615,
        "FOMC|ETH": 1.2593448799387723,
        "NFP|BTC": 0.6719360519847367,
        "NFP|ETH": 0.8433502237656211,
        "PPI|BTC": 0.20108337643044344,
        "PPI|ETH": 0.3779159578296879,
    }
    """
    previous_vol_data = DB_CONN.get_event_vols()
    previous_vol_df = pd.DataFrame(
        previous_vol_data,
        columns=[
            "ID",
            "Event Name",
            "Symbol",
            "UTC Time",
            "Vol Before",
            "Vol After",
            "Event Vol",
        ],
    )
    previous_vol_df = previous_vol_df[previous_vol_df["Event Vol"] > 0]
    previous_vol_df["Currency"] = previous_vol_df["Symbol"].apply(lambda x: x.replace("-PERPETUAL", ""))
    previous_vol_df["ID"] = previous_vol_df["Event Name"] + "|" + previous_vol_df["Currency"]
    previous_vol_df = previous_vol_df[["ID", "Event Vol"]]
    est_historical_vol = previous_vol_df.groupby(["ID"]).last()
    logger.info("Save estimated vol to redis")
    RDS.set(name="EstEventVol", value=json.dumps(est_historical_vol.to_dict()["Event Vol"]))


if __name__ == "__main__":
    update_previous_event_vol()
    estimate_event_vol()
