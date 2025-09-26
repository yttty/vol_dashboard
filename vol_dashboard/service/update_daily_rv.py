import argparse
import datetime
import multiprocessing
import time
from operator import itemgetter
from typing import Tuple

import numpy as np
import pandas as pd
from datetimerange import DateTimeRange
from loguru import logger

from vol_dashboard.config import EMA_RV_DAYS, INSTRUMENTS, MINUTES_AFTER_RELEASE, YEARLY_TRADING_DAYS
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.utils.event_utils import get_previous_events
from vol_dashboard.utils.vol_utils import calculate_realized_volatility


def update_ema_rv(instrument_name: str, start_date: datetime.date, end_date: datetime.date):
    db_conn = VolDbConnector()

    _ema_rv_record_date = start_date
    while _ema_rv_record_date < end_date:
        # this daily rv record date should be included, so shift by 1min
        rv_record_end_dt = datetime.datetime.combine(
            _ema_rv_record_date,
            datetime.time(8, 0, 0, tzinfo=datetime.timezone.utc),
        ) + datetime.timedelta(minutes=1)
        # this daily rv record should not be included, so shift by 1min
        rv_record_start_dt = datetime.datetime.combine(
            _ema_rv_record_date - datetime.timedelta(EMA_RV_DAYS),
            datetime.time(8, 0, 0, tzinfo=datetime.timezone.utc),
        ) + datetime.timedelta(minutes=1)
        daily_rv_records: list[tuple] = db_conn.get_daily_rv(
            instrument_name,
            exchange="DERIBIT",
            from_date=rv_record_start_dt,
            to_date=rv_record_end_dt,
        )
        if len(daily_rv_records) != EMA_RV_DAYS:
            logger.warning(
                f"Not enough daily rv record before {_ema_rv_record_date.isoformat()}, skip processing EMA RV."
            )
            _ema_rv_record_date += datetime.timedelta(days=1)
            continue

        # TODO

        _ema_rv_record_date += datetime.timedelta(days=1)


def update_daily_rv(instrument_name: str, start_date: datetime.date, end_date: datetime.date, fill_ema: bool = False):
    db_conn = VolDbConnector()
    previous_events = get_previous_events()
    excluded_time: dict[str, DateTimeRange] = {}
    for event_name, date_str, time_et_str, utc_dt in previous_events:
        event_id = f"{event_name}/{date_str}/{time_et_str}"
        # left: should be excluded, right: should be included
        excluded_time[event_id] = DateTimeRange(
            utc_dt,
            utc_dt + datetime.timedelta(minutes=MINUTES_AFTER_RELEASE),
        )

    _date = start_date
    while _date < end_date:
        kline_start_dt = datetime.datetime.combine(
            _date,
            datetime.time(8, 0, 0, tzinfo=datetime.timezone.utc),
        )
        kline_end_dt = datetime.datetime.combine(
            _date + datetime.timedelta(1),
            datetime.time(8, 0, 0, tzinfo=datetime.timezone.utc),
        )

        # check the kline availability
        daily_klines: list[tuple] = db_conn.get_klines(
            symbol=instrument_name,
            interval="1m",
            from_timestamp=int(kline_start_dt.timestamp()),
            to_timestamp=int(kline_end_dt.timestamp()),
            exchange="DERIBIT",
        )
        if len(daily_klines) != 24 * 60:
            logger.warning(f"Not enough kline on {_date.isoformat()}, skip processing RV.")
            _date += datetime.timedelta(1)
            continue

        # process raw_rv
        daily_close = np.array(list(map(lambda x: x[7], daily_klines)))
        raw_rv = calculate_realized_volatility(daily_close)
        logger.info(f"Inst[{instrument_name}] Date[{_date.isoformat()}] RawRV[{raw_rv:.4f}]")

        # process event removed rv, assume only one event in one day
        dtr_orig = DateTimeRange(kline_start_dt, kline_end_dt)
        event_s = ""
        for event_id, exclude_range in excluded_time.items():
            range_intersection = dtr_orig.intersection(exclude_range)
            if range_intersection.start_datetime and range_intersection.get_timedelta_second() > 0:
                # event in the range
                dtr_remain = dtr_orig.subtract(exclude_range)
                event_s = event_id
            else:
                # event not in the range
                continue
        if not event_s:
            # no event
            er_rv = raw_rv
            daily_total_n = len(daily_klines)
            logger.info(
                f"Inst[{instrument_name}] Date[{_date.isoformat()}] ER_RV[{er_rv:.4f}] N[{daily_total_n}] EventRemoved[-]"
            )
        else:
            daily_total_rv = 0
            daily_total_n = 0
            for _dtr in dtr_remain:
                dtr_klines: list[tuple] = db_conn.get_klines(
                    symbol=instrument_name,
                    interval="1m",
                    from_timestamp=int(_dtr.start_datetime.timestamp()),
                    to_timestamp=int(_dtr.end_datetime.timestamp()),
                    exchange="DERIBIT",
                )
                dtr_close = np.array(list(map(lambda x: x[7], dtr_klines)))
                if len(dtr_close) < 2:
                    continue
                realized_variance = np.sum(np.diff(np.log(dtr_close)) ** 2)
                daily_total_rv += realized_variance
                daily_total_n += len(dtr_close)

            annualized_volatility = np.sqrt(daily_total_rv) * np.sqrt(YEARLY_TRADING_DAYS * 24 * 60 / daily_total_n)
            er_rv = float(annualized_volatility)
            logger.info(
                f"Inst[{instrument_name}] Date[{_date.isoformat()}] ER_RV[{er_rv:.4f}] N[{daily_total_n}] EventRemoved[{event_s}]"
            )

        insert_rv_success = db_conn.insert_daily_rv(
            rv_data=(
                kline_end_dt,
                instrument_name,
                "DERIBIT",
                raw_rv,
                er_rv,
                daily_total_n,
                event_s,
                datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )
        if not insert_rv_success:
            logger.error("Update daily rv failed!")
        _date += datetime.timedelta(1)

    if fill_ema:
        update_ema_rv(instrument_name, start_date, end_date)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true", help="Run as a background service")
    parser.add_argument("-s", "--start_dt", type=str, help="20250918")
    parser.add_argument("-e", "--end_dt", type=str, help="20250918")
    parser.add_argument("--ema", action="store_true", help="Also fill the ema rv")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.daemon:
        logger.info("Run as a background service")
        while True:
            now_dt = datetime.datetime.now(tz=datetime.timezone.utc)
            if now_dt.hour == 8 and now_dt.minute == 10:
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
                            "fill_ema": args.ema,
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
                    "fill_ema": args.ema,
                },
            )
            for instrument_name in INSTRUMENTS
        ]
        for p in p_l:
            p.start()
        for p in p_l:
            p.join()
