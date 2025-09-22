import json

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import redis
from config import INSTRUMENTS
from dash import Input, Output, dash_table, dcc, html
from dash.dependencies import Input, Output
from db_connector import VolDbConnector
from redis_connector import get_redis_instance

CURRENCY_LIST = [instrument_name.replace("-PERPETUAL", "") for instrument_name in INSTRUMENTS]
EVENT_LIST = ["FOMC", "NFP", "CPI", "PPI"]

DB_CONN = VolDbConnector()
RDS: redis.Redis = get_redis_instance()


class DataLoader:
    def prepare_historical_vol_data(self) -> pd.DataFrame:
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
        # previous_vol_df["Symbol"] = previous_vol_df["Symbol"].apply(lambda x: x.replace("-PERPETUAL", ""))
        # previous_vol_df["Time"] = previous_vol_df["Time"].apply(lambda x: x.isoformat())
        previous_vol_df["Vol Before"] = previous_vol_df["Vol Before"].apply(lambda x: f"{x:.4f}")
        previous_vol_df["Vol After"] = previous_vol_df["Vol After"].apply(lambda x: f"{x:.4f}")
        previous_vol_df["Event Vol"] = previous_vol_df["Event Vol"].apply(lambda x: f"{x:.4f}")
        previous_vol_df.drop(columns=["ID"], inplace=True)
        return previous_vol_df

    def get_upcoming_event_data_from_redis(self, currency: str):
        data = RDS.get(f"FwdVol:{currency}")
        if data:
            return json.loads(data)
        return {}

    def prepare_fwd_vol_data(self, remove_event: bool = False) -> pd.DataFrame:
        all_expirations_l = RDS.get(name=f"Expirations")
        all_expirations = json.loads(all_expirations_l)
        columns = ["Currency"] + all_expirations
        fwd_vol_rows_l = []
        for currency in CURRENCY_LIST:
            row = {}
            row["Currency"] = currency
            upcoming_event_vol = self.get_upcoming_event_data_from_redis(currency)
            for upcoming_event_record in upcoming_event_vol:
                if not remove_event:
                    row[upcoming_event_record["Col_ID"]] = upcoming_event_record["Fwd_Vol"]
                else:
                    row[upcoming_event_record["Col_ID"]] = upcoming_event_record["Event_Removed_Vol"]
            fwd_vol_rows_l.append(row)
        fwd_vol_df = pd.DataFrame(fwd_vol_rows_l, columns=columns)
        for expiration in all_expirations:
            fwd_vol_df[expiration] = fwd_vol_df[expiration].apply(lambda x: f"{x:.4f}")
        return fwd_vol_df


data_loader = DataLoader()
previous_vol_df = data_loader.prepare_historical_vol_data()
fwd_vol_df = data_loader.prepare_fwd_vol_data(remove_event=False)
event_rm_fwd_vol_df = data_loader.prepare_fwd_vol_data(remove_event=True)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = html.Div(
    [
        html.H1("Vol Dashboard", style={"textAlign": "center"}),
        html.H2("Forward Volatility", style={"textAlign": "center"}),
        html.Div(
            [
                dash_table.DataTable(
                    id="fwd-vol-table",
                    columns=[{"name": i, "id": i} for i in fwd_vol_df.columns],  # 定义表格列
                    data=fwd_vol_df.to_dict("records"),  # 初始数据
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "left", "padding": "5px"},
                    style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"},
                )
            ],
            style={"width": "80%", "margin": "20px auto"},
        ),
        html.H2("Event Removed Forward Volatility", style={"textAlign": "center"}),
        html.Div(
            [
                dash_table.DataTable(
                    id="fwd-vol-table",
                    columns=[{"name": i, "id": i} for i in event_rm_fwd_vol_df.columns],  # 定义表格列
                    data=event_rm_fwd_vol_df.to_dict("records"),  # 初始数据
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "left", "padding": "5px"},
                    style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"},
                )
            ],
            style={"width": "80%", "margin": "20px auto"},
        ),
        html.H2("Historical Event Volatility", style={"textAlign": "center"}),
        # 下拉框组件, 选择currency
        html.Div(
            [
                html.Label("Select currency:"),
                dcc.Dropdown(
                    id="currency-dropdown",
                    options=[{"label": currency, "value": currency} for currency in CURRENCY_LIST],  # 下拉选项
                    value=CURRENCY_LIST,
                    multi=True,
                    placeholder="Select currency...",
                ),
            ],
            style={"width": "50%", "margin": "20px auto"},
        ),
        # 下拉框组件, 选择event
        html.Div(
            [
                html.Label("Select event:"),
                dcc.Dropdown(
                    id="event-dropdown",
                    options=[{"label": event, "value": event} for event in EVENT_LIST],  # 下拉选项
                    value=EVENT_LIST,
                    multi=True,
                    placeholder="Select event...",
                ),
            ],
            style={"width": "50%", "margin": "20px auto"},
        ),
        # 图表组件
        # html.Div([dcc.Graph(id="gdp-line-chart")], style={"width": "80%", "margin": "20px auto"}),
        # 表格组件
        html.Div(
            [
                dash_table.DataTable(
                    id="hist-vol-table",
                    columns=[{"name": i, "id": i} for i in previous_vol_df.columns],  # 定义表格列
                    data=previous_vol_df.to_dict("records"),  # 初始数据
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "left", "padding": "5px"},
                    style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"},
                )
            ],
            style={"width": "80%", "margin": "20px auto"},
        ),
    ]
)


# --- 4. 定义回调函数 ---
@app.callback(
    Output("hist-vol-table", "data"),  # 输出到表格数据
    Input("currency-dropdown", "value"),
    Input("event-dropdown", "value"),
)
def update_hist_vol_currency(selected_currencies, selected_events):
    previous_vol_df = data_loader.prepare_historical_vol_data()
    if selected_currencies and len(selected_currencies) > 0:
        currency_cond = previous_vol_df["Symbol"].isin([f"{c}-PERPETUAL" for c in selected_currencies])
    else:
        currency_cond = previous_vol_df["Symbol"].isin([f"{c}-PERPETUAL" for c in CURRENCY_LIST])
    if selected_events and len(selected_events) > 0:
        event_cond = previous_vol_df["Event Name"].isin(selected_events)
    else:
        event_cond = previous_vol_df["Event Name"].isin(EVENT_LIST)
    filtered_df = previous_vol_df[currency_cond & event_cond]

    return filtered_df.to_dict("records")


if __name__ == "__main__":
    print("Dash app running on http://127.0.0.1:9090")
    app.run(debug=True)
