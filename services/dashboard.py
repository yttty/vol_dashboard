import json

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import redis
from config import INSTRUMENTS
from dash import dash_table, dcc, html
from dash.dependencies import Input, Output
from db_connector import VolDbConnector
from redis_connector import get_redis_instance

r = get_redis_instance()
db_conn = VolDbConnector()


def get_upcoming_event_data_from_redis(currency: str):
    data = r.get(f"FwdVol:{currency}")
    if data:
        return json.loads(data)
    return {}


upcoming_vol_data = get_upcoming_event_data_from_redis("BTC")
previous_vol_data = db_conn.get_event_vols()


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
for vol in upcoming_vol_data:
    if len(vol["Events_Included"]) == 0:
        vol["Events_Included_str"] = "None"
    else:
        vol["Events_Included_str"] = ",".join([event["event_id"] for event in vol["Events_Included"]])

previous_vol_df = pd.DataFrame(
    previous_vol_data,
    columns=[
        "ID",
        "Event Name",
        "Symbol",
        "Time",
        "Vol Before",
        "Vol After",
        "Event Vol",
    ],
)
previous_vol_df["Time"] = previous_vol_df["Time"].apply(lambda x: x.isoformat())
previous_vol_df["Vol Before"] = previous_vol_df["Vol Before"].apply(lambda x: f"{x:.4f}")
previous_vol_df["Vol After"] = previous_vol_df["Vol After"].apply(lambda x: f"{x:.4f}")
previous_vol_df["Event Vol"] = previous_vol_df["Event Vol"].apply(lambda x: f"{x:.4f}")
# df = pd.DataFrame(vol_data) if vol_data else pd.DataFrame()


if upcoming_vol_data:
    dbc_cards = [
        dbc.Card(
            [
                dbc.CardHeader(
                    html.H5(
                        "{} to {}".format(
                            vol["Prev_Option"],
                            vol["Next_Option"],
                        )
                    )
                ),
                dbc.CardBody(
                    [
                        html.P(
                            "Fwd Vol: {:.4f}".format(vol["Fwd_Vol"]),
                            className="mb-1",
                        ),
                    ]
                    + [
                        html.P("Events:"),
                        html.Ul(
                            [html.Li("{}".format(event["event_id"])) for event in vol["Events_Included"]],
                            className="mb-1",
                        ),
                    ]
                    if len(vol["Events_Included"]) > 0
                    else [
                        html.P(
                            "Fwd Vol: {:.4f}".format(vol["Fwd_Vol"]),
                            className="mb-1",
                        ),
                        html.P("No Events."),
                    ]
                ),
            ],
            className="shadow-sm",
        )
        for vol in upcoming_vol_data
    ]
else:
    dbc_cards = [
        dbc.Card(
            [
                dbc.CardHeader(html.H5("Error Msg")),
                dbc.CardBody([html.P("No data", className="text-muted")]),
            ],
            className="shadow-sm",
        )
    ]

app.layout = dbc.Container(
    [
        html.H2("📊 Upcoming Vol Data", className="my-4 text-center"),
        # 方式1：卡片式展示
        dbc.Row(
            [
                dbc.Col(
                    dbc_cards,
                    width=6,
                )
            ],
            justify="center",
            className="mb-5",
        ),
        html.Hr(),
        # 方式2：表格展示
        html.H2("📊 Historical Event Vol", className="my-4 text-center"),
        dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in previous_vol_df.columns] if not previous_vol_df.empty else [],
            data=previous_vol_df.to_dict("records"),
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "8px", "fontFamily": "Arial"},
            style_header={"backgroundColor": "#f8f9fa", "fontWeight": "bold"},
            style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#f2f2f2"}],
        ),
        # 自动刷新按钮（可选）
        html.Div(
            [
                dbc.Button("🔄 Refresh", id="refresh-btn", color="primary", className="mt-4"),
                html.Div(id="output-div", className="mt-2"),
            ],
            className="text-center",
        ),
    ],
    fluid=True,
    className="py-4",
)


@app.callback(
    Output("output-div", "children"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_data(n_clicks):
    return f"Data refreshed! (No. clicks: {n_clicks})"


if __name__ == "__main__":
    print("Dash app running on http://127.0.0.1:8050")
    app.run(debug=True)
