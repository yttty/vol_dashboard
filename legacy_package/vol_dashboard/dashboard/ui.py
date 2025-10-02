import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import Input, Output, dash_table, dcc, html
from dash.dependencies import Input, Output, State

from vol_dashboard.config import CURRENCY_LIST, EVENT_LIST
from vol_dashboard.dashboard.fwd_estimator import FwdVolEstimator
from vol_dashboard.dashboard.historical_loader import HistoricalDataLoader

estimator = FwdVolEstimator()
data_loader = HistoricalDataLoader()


def gen_est_vol_divs() -> list[html.Base]:
    est_event_vol_df = pd.DataFrame(
        [
            {"Event": k.split("|")[0], "Currency": k.split("|")[1], "Event Vol": f"{v:.4f}"}
            for k, v in estimator.est_event_vol.items()
        ]
    )

    est_vol_divs = []
    est_vol_divs.append(html.H4("Update Vol Estimations", style={"textAlign": "center"}))
    choices = []
    for event in EVENT_LIST:
        for currency in CURRENCY_LIST:
            choices.append(f"{event} ({currency})")

    est_vol_divs.extend(
        [
            html.Div(
                [
                    html.Label("Select target:"),
                    dcc.Dropdown(
                        id="event-vol-dropdown",
                        options=[{"label": choice, "value": choice} for choice in choices],  # 下拉选项
                        value=choices,
                        multi=False,
                        placeholder="Select currency...",
                    ),
                    html.Label("Input new estimation:"),
                    dcc.Input(
                        id=f"input-est-vol",
                        type="text",
                        placeholder="",
                        style={"width": "180px", "margin": "10px auto"},
                    ),
                    html.Button(
                        "Submit",
                        id="submit-est-vol-button",
                        n_clicks=0,
                        style={"width": "100px", "margin": "5px auto"},
                    ),
                ],
                style={"width": "180px", "margin": "10px auto", "textAlign": "center"},
            ),
        ]
    )
    est_vol_divs.extend(
        [
            html.H3("Estimations of Event Vol", style={"textAlign": "center"}),
            html.Div(
                [
                    dash_table.DataTable(
                        id="est-event-vol-table",
                        columns=[{"name": i, "id": i} for i in est_event_vol_df.columns],  # 定义表格列
                        data=est_event_vol_df.to_dict("records"),  # 初始数据
                        style_table={"overflowX": "auto"},
                        style_cell={"textAlign": "left", "padding": "2px"},
                        style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"},
                    )
                ],
                style={"width": "25%", "margin": "20px auto"},
            ),
        ]
    )
    return est_vol_divs


def gen_upcoming_vol_divs() -> list[html.Base]:
    atm_iv_df, fwd_vol_df = estimator.prepare_vol_data()
    return [
        html.H3("Fwd Implied Vol", style={"textAlign": "center"}),
        html.Div(
            [
                html.Button(
                    "Re-estimate vol",
                    id="estimate-vol-button",
                    n_clicks=0,
                    style={"width": "150px", "margin": "5px auto"},
                ),
            ],
            style={"width": "180px", "margin": "10px auto", "textAlign": "center"},
        ),
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
        html.H3("ATM Implied Vol", style={"textAlign": "center"}),
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
    ]


def gen_historical_event_vol_divs() -> list[html.Base]:
    historical_vol_df = data_loader.prepare_historical_vol_data()
    return [
        html.H2("Historical Event Vol", style={"textAlign": "center"}),
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


heading_divs = [html.H1("Vol Dashboard", style={"textAlign": "center"})]
est_vol_divs = gen_est_vol_divs()
# upcoming_vol_divs = []
upcoming_vol_divs = gen_upcoming_vol_divs()
historical_event_vol_divs = gen_historical_event_vol_divs()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = html.Div(heading_divs + est_vol_divs + upcoming_vol_divs + historical_event_vol_divs)


# --- 4. 定义回调函数 ---
@app.callback(
    Output("hist-vol-table", "data"),  # 输出到表格数据
    Input("currency-dropdown", "value"),
    Input("event-dropdown", "value"),
)
def update_hist_vol_table(selected_currencies, selected_events):
    historical_vol_df = data_loader.prepare_historical_vol_data()
    if selected_currencies and len(selected_currencies) > 0:
        currency_cond = historical_vol_df["Symbol"].isin([f"{c}-PERPETUAL" for c in selected_currencies])
    else:
        currency_cond = historical_vol_df["Symbol"].isin([f"{c}-PERPETUAL" for c in CURRENCY_LIST])
    if selected_events and len(selected_events) > 0:
        event_cond = historical_vol_df["Event Name"].isin(selected_events)
    else:
        event_cond = historical_vol_df["Event Name"].isin(EVENT_LIST)
    filtered_df = historical_vol_df[currency_cond & event_cond]

    return filtered_df.to_dict("records")


@app.callback(
    Output("est-event-vol-table", "data"),  # 输出到表格数据
    Input("submit-est-vol-button", "n_clicks"),
    State("event-vol-dropdown", "value"),
    State("input-est-vol", "value"),
    prevent_initial_call=True,
)
def update_est_vol_table(n_clicks, selected_event_ccy_pair, inputed_est):
    if selected_event_ccy_pair is not None and inputed_est is not None:
        key = selected_event_ccy_pair.replace(" (", "|").replace(")", "")
        estimator.est_event_vol[key] = float(inputed_est)
    est_event_vol_df = pd.DataFrame(
        [
            {"Event": k.split("|")[0], "Currency": k.split("|")[1], "Event Vol": f"{v:.4f}"}
            for k, v in estimator.est_event_vol.items()
        ]
    )
    return est_event_vol_df.to_dict("records")


@app.callback(
    Output("fwd-vol-table", "data"),
    Output("atm-iv-table", "data"),
    Input("estimate-vol-button", "n_clicks"),
    prevent_initial_call=True,
)
def reestimate_vol(n_clicks):
    atm_iv_df, fwd_vol_df = estimator.prepare_vol_data()
    return fwd_vol_df.to_dict("records"), atm_iv_df.to_dict("records")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8082, debug=True)
