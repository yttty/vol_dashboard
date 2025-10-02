import pandas as pd

from vol_dashboard.connector.db_connector import VolDbConnector


class HistoricalDataLoader:
    def __init__(self):
        self.db_conn = VolDbConnector()

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
