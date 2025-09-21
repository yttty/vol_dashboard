import argparse
import datetime
import multiprocessing
import time

import pandas as pd
import requests
from config import INSTRUMENTS as KLINE_INSTRUMENTS
from db_connector import VolDbConnector
from loguru import logger


def fetch_data_for_window(
    instrument_name: str,
    start_utc: datetime.datetime,
    end_utc: datetime.datetime,
) -> pd.DataFrame | None:
    """Helper function to fetch Deribit data and return a DataFrame."""

    # API return maximal 5001 1m_kline
    url = "https://www.deribit.com/api/v2/public/get_tradingview_chart_data"

    req_end_dt = end_utc
    kline_df_l = []
    while req_end_dt > start_utc:
        req_start_dt = max(req_end_dt - datetime.timedelta(minutes=4320), start_utc)
        req_start_ts_ms = int(req_start_dt.timestamp() * 1000)
        req_end_ts_ms = int(req_end_dt.timestamp() * 1000)
        params = {
            "instrument_name": instrument_name,
            "resolution": "1",
            "start_timestamp": req_start_ts_ms,
            "end_timestamp": req_end_ts_ms,
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if "result" in data and data["result"]["status"] == "ok" and len(data["result"]["ticks"]) > 0:
                df = pd.DataFrame(data["result"])
                df.rename(columns={"ticks": "timestamp"}, inplace=True)
                df = df[["timestamp", "open", "high", "low", "close", "volume"]]
                df["symbol"] = instrument_name
                df["interval"] = "1m"
                df["exchange"] = "DERIBIT"
                df["timestamp"] = df["timestamp"].apply(lambda t: int(t / 1000))
                kline_df_l.append(df)
        except requests.exceptions.RequestException as e:
            logger.error(f"API Error: {e}")
        req_end_dt = req_start_dt
        time.sleep(0.1)

    if kline_df_l:
        return pd.concat(kline_df_l)
    else:
        return None


def fetch_kline(instrument_name: str, start_dt_utc: datetime.datetime, end_dt_utc: datetime.datetime):
    db_conn = VolDbConnector()
    logger.info(f"Fetch {instrument_name} kline from {start_dt_utc} to {end_dt_utc}")
    _dt = datetime.datetime.now()
    kline_df = fetch_data_for_window(instrument_name=instrument_name, start_utc=start_dt_utc, end_utc=end_dt_utc)
    if kline_df is not None:
        logger.info(
            f"Fetched {len(kline_df)} kline of {instrument_name}! Elps time: {(datetime.datetime.now() - _dt)}"
        )
        kline_df = kline_df[["symbol", "interval", "timestamp", "exchange", "open", "high", "low", "close", "volume"]]
        _dt = datetime.datetime.now()
        db_conn.insert_klines(list(kline_df.itertuples(index=False, name=None)))
        logger.info(
            f"Inserted {len(kline_df)} kline of {instrument_name}! Elps time: {(datetime.datetime.now() - _dt)}"
        )
    else:
        logger.error("Fail to fetch kline!")


def check_kline(instrument_name: str, start_dt_utc: datetime.datetime, end_dt_utc: datetime.datetime):
    db_conn = VolDbConnector()
    kline_data = db_conn.get_klines(
        symbol=instrument_name,
        interval="1m",
        from_timestamp=int(start_dt_utc.timestamp()),
        to_timestamp=int(end_dt_utc.timestamp()),
        exchange="DERIBIT",
    )
    existing_kline_ts = set(kline[2] for kline in kline_data)
    desired_kline_ts = set(range(int(start_dt_utc.timestamp()), int(end_dt_utc.timestamp()), 60))
    missing_ts = desired_kline_ts - existing_kline_ts
    if len(missing_ts) == 0:
        logger.info(f"Kline of {instrument_name} from {start_dt_utc} to {end_dt_utc} is complete.")
    else:
        logger.warning(f"{len(missing_ts)} missing kline of {instrument_name} from {start_dt_utc} to {end_dt_utc}!")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_dt", type=str, help="20250918")
    parser.add_argument("--end_dt", type=str, help="20250918")
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.start_dt:
        start_dt = datetime.datetime.strptime(args.start_dt, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
    else:
        start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1440)
    start_dt = start_dt.replace(second=0, microsecond=0)

    if args.end_dt:
        end_dt = datetime.datetime.strptime(args.end_dt, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
    else:
        end_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)
    end_dt = end_dt.replace(second=0, microsecond=0)

    assert start_dt < end_dt

    if args.check:
        p_l = [
            multiprocessing.Process(
                target=check_kline,
                name=f"fetch {instrument_name} kline",
                kwargs={
                    "instrument_name": instrument_name,
                    "start_dt_utc": start_dt,
                    "end_dt_utc": end_dt,
                },
            )
            for instrument_name in KLINE_INSTRUMENTS
        ]
        for p in p_l:
            p.start()
        for p in p_l:
            p.join()
    else:
        p_l = [
            multiprocessing.Process(
                target=fetch_kline,
                name=f"fetch {instrument_name} kline",
                kwargs={
                    "instrument_name": instrument_name,
                    "start_dt_utc": start_dt,
                    "end_dt_utc": end_dt,
                },
            )
            for instrument_name in KLINE_INSTRUMENTS
        ]
        for p in p_l:
            p.start()
        for p in p_l:
            p.join()
