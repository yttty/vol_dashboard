import argparse
import datetime
import multiprocessing
import time

import numpy as np
from loguru import logger

from vol_dashboard.config import INSTRUMENTS
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.utils.vol_utils import calculate_realized_volatility


def update_daily_rv(instrument_name: str, start_date: datetime.date, end_date: datetime.date):
    db_conn = VolDbConnector()

    _date = start_date
    while _date < end_date:
        kline_start_dt = datetime.datetime.combine(
            _date,
            datetime.time(0, 0, 0, tzinfo=datetime.timezone.utc),
        )
        kline_end_dt = datetime.datetime.combine(
            _date + datetime.timedelta(1),
            datetime.time(0, 0, 0, tzinfo=datetime.timezone.utc),
        )
        daily_klines: list[tuple] = db_conn.get_klines(
            symbol=instrument_name,
            interval="1m",
            from_timestamp=int(kline_start_dt.timestamp()),
            to_timestamp=int(kline_end_dt.timestamp()),
            exchange="DERIBIT",
        )
        if len(daily_klines) != 24 * 60:
            logger.warning(f"Not enough kline on {_date.isoformat()}, skip processing RV.")
        else:
            daily_close = np.array(list(map(lambda x: x[7], daily_klines)))
            rv = calculate_realized_volatility(daily_close)
            logger.info(
                f"Inst[{instrument_name}] Date[{_date.isoformat()}] n_klines[{len(daily_klines)}] RV[{rv:.4f}]"
            )
            success = db_conn.insert_daily_rv(
                rv_data=(
                    kline_start_dt,
                    instrument_name,
                    "DERIBIT",
                    rv,
                    datetime.datetime.now(tz=datetime.timezone.utc),
                )
            )
            if not success:
                logger.error("DB update failed!")
        _date += datetime.timedelta(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true", help="Run as a background service")
    parser.add_argument("-s", "--start_dt", type=str, help="20250918")
    parser.add_argument("-e", "--end_dt", type=str, help="20250918")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.daemon:
        logger.info("Run as a background service")
        while True:
            now_dt = datetime.datetime.now(tz=datetime.timezone.utc)
            if now_dt.hour == 0 and now_dt.minute == 10:
                end_dt = datetime.datetime.now(datetime.timezone.utc).date()
                start_dt = end_dt - datetime.timedelta(days=1)
                p_l = [
                    multiprocessing.Process(
                        target=update_daily_rv,
                        name=f"update {instrument_name} rv",
                        kwargs={
                            "instrument_name": instrument_name,
                            "start_date": start_dt,
                            "end_date": end_dt,
                        },
                    )
                    for instrument_name in INSTRUMENTS
                ]
                for p in p_l:
                    p.start()
                for p in p_l:
                    p.join()

            try:
                time.sleep(60)
            except KeyboardInterrupt:
                logger.info("Stop update RV daemon")
                exit()
    else:
        assert args.start_dt and args.end_dt, "Must specify start_dt and end_dt if not running as a service"
        start_dt = datetime.datetime.strptime(args.start_dt, "%Y%m%d").date()
        end_dt = datetime.datetime.strptime(args.end_dt, "%Y%m%d").date()
        assert start_dt < end_dt

        p_l = [
            multiprocessing.Process(
                target=update_daily_rv,
                name=f"update {instrument_name} rv",
                kwargs={
                    "instrument_name": instrument_name,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            for instrument_name in INSTRUMENTS
        ]
        for p in p_l:
            p.start()
        for p in p_l:
            p.join()
