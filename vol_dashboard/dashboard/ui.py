import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import Input, Output, dash_table, dcc, html
from dash.dependencies import Input, Output

from vol_dashboard.config import CURRENCY_LIST, EVENT_LIST
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.connector.redis_connector import get_redis_instance
from vol_dashboard.dashboard.fwd_estimator import FwdVolEstimator
from vol_dashboard.dashboard.historical_loader import HistoricalDataLoader

estimator = FwdVolEstimator()
data_loader = HistoricalDataLoader()
historical_vol_df = data_loader.prepare_historical_vol_data()
atm_iv_df, fwd_vol_df = estimator.prepare_vol_data()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = html.Div(
    [
        html.H1("Vol Dashboard", style={"textAlign": "center"}),
        html.H2("Fwd Vol", style={"textAlign": "center"}),
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
        html.H2("ATM IV", style={"textAlign": "center"}),
        html.Div(
            [
                dash_table.DataTable(
                    id="atm-iv-table",
                    columns=[{"name": i, "id": i} for i in atm_iv_df.columns],  # 定义表格列
                    data=atm_iv_df.to_dict("records"),  # 初始数据
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
                    columns=[{"name": i, "id": i} for i in historical_vol_df.columns],  # 定义表格列
                    data=historical_vol_df.to_dict("records"),  # 初始数据
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
def update_hist_vol_table(selected_currencies, selected_events):
    previous_vol_df = estimator.prepare_historical_vol_data()
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
    app.run(host="0.0.0.0", port=8082, debug=True)
