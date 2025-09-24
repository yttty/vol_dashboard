import json

import pandas as pd
import redis

from vol_dashboard.config import CURRENCY_LIST, EVENT_LIST
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.connector.redis_connector import get_redis_instance


class FwdVolEstimator:
    def __init__(self):
        self.db_conn = VolDbConnector()
        self.rds: redis.Redis = get_redis_instance()

    def prepare_historical_vol_data(self) -> pd.DataFrame:
        previous_vol_data = self.db_conn.get_event_vols()
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
        # previous_vol_df["Symbol"] = previous_vol_df["Symbol"].apply(lambda x: x.replace("-PERPETUAL", ""))
        # previous_vol_df["Time"] = previous_vol_df["Time"].apply(lambda x: x.isoformat())
        previous_vol_df["Vol Before"] = previous_vol_df["Vol Before"].apply(lambda x: f"{x:.4f}")
        previous_vol_df["Vol After"] = previous_vol_df["Vol After"].apply(lambda x: f"{x:.4f}")
        previous_vol_df["Event Vol"] = previous_vol_df["Event Vol"].apply(lambda x: f"{x:.4f}")
        previous_vol_df.drop(columns=["ID"], inplace=True)
        return previous_vol_df

    def get_upcoming_event_data_from_redis(self, currency: str):
        data = self.rds.get(f"FwdVol:{currency}")
        if data:
            return json.loads(data)
        return {}

    def prepare_fwd_vol_data(self, vol_col_name: str) -> pd.DataFrame:
        all_expirations_l = self.rds.get(name=f"Expirations")
        all_expirations = json.loads(all_expirations_l)
        columns = ["Currency"] + all_expirations
        fwd_vol_rows_l = []
        for currency in CURRENCY_LIST:
            row = {}
            row["Currency"] = currency
            upcoming_event_vol = self.get_upcoming_event_data_from_redis(currency)
            for upcoming_event_record in upcoming_event_vol:
                row[upcoming_event_record["Col_ID"]] = upcoming_event_record[vol_col_name]
            fwd_vol_rows_l.append(row)
        fwd_vol_df = pd.DataFrame(fwd_vol_rows_l, columns=columns)
        for expiration in all_expirations:
            fwd_vol_df[expiration] = fwd_vol_df[expiration].apply(lambda x: f"{x:.4f}")
        return fwd_vol_df
