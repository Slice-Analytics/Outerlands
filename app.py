# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, DiskcacheManager, CeleryManager, html, dcc, dash_table, Input, Output
from dash.dash_table.Format import Format, Group, Scheme
from datetime import timezone, datetime
from supabase import create_client
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import requests
import os

from protocols import fetchProtocolData
from wallettracker import fetchWalletTrackerData


# from dotenv import load_dotenv
# load_dotenv()  # take environment variables from .env.


if 'REDIS_URL' in os.environ:
    # Use Redis & Celery if REDIS_URL set as an env variable
    from celery import Celery
    celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
    background_callback_manager = CeleryManager(celery_app)
else:
    # Diskcache for non-production apps when developing locally
    import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)


# Style Variables
primary_color = 'rgb(247,247,247)'
font_color = 'rgb(9,75,215)'
sm_height = '30vh'
sm_border_radius = '1vh'
sm_background = 'rgb(224,224,255)'
sm_style = {
    'background': sm_background,
    'height': sm_height,
    'border-radius': sm_border_radius,
    'padding': '0',
    'color': font_color
}
H1_style = {'color': font_color}
table_style = {'background': sm_background}
btn_style = {'background': font_color, 'color': 'white', 'border': font_color,}


app = Dash(__name__, external_stylesheets=[dbc.themes.GRID, dbc.themes.LUX])

server = app.server


def fetchSupabaseLastUpdate():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY_SECRET")
    supabase = create_client(url, key)
    data, count = supabase.table('lastupdated').select("*").execute()
    return float(data[1][0]['last_updated'])


def updateLUsupabase():
    lut = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY_SECRET")
    supabase = create_client(url, key)

   # Removes all table data
    supabase.table('lastupdated').delete().neq('id', -1).execute()

    # Inserts Data
    supabase.table('lastupdated').insert({'id': 0, 'last_updated': lut}).execute()

    return None


def fetchSupabaseProtocolData():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY_SECRET")
    supabase = create_client(url, key)
    data, count = supabase.table('Protocol_Data').select("*").execute()
    pd_sb = pd.DataFrame(data[1])
    return pd_sb


def fetchSupabaseWTData():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY_SECRET")
    supabase = create_client(url, key)
    data, count = supabase.table('WT_Data').select("*").execute()
    wt_sb = pd.DataFrame(data[1])
    return wt_sb


def checkForUpdate():
    print('Checking required update...')

    lut = fetchSupabaseLastUpdate()
    current_unix_time = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).timestamp()
    if current_unix_time > lut+86400:
        print('Updating Wallet Tracker')
        fetchWalletTrackerData()
        print('Updating Protocols')
        fetchProtocolData()
        print('Updating lastupdate tracker')
        updateLUsupabase()
        return True
    else:
        print('No update required')
        return None


@app.callback(
    Output(component_id='last_updated', component_property='children'),
    Output(component_id='protocol-table', component_property='data'),
    Output(component_id='wt-table', component_property='data'),
    Input('interval-component-long', 'n_intervals'),
    Input('last_updated', component_property='children'),
    Input(component_id='protocol-table', component_property='data'),
    Input(component_id='wt-table', component_property='data'),
    background=True,
    manager=background_callback_manager,
    running=[
        (Output("btn-download-PD", "disabled"), True, False),
        (Output("btn-download-WTD", "disabled"), True, False),
    ],
)
def updateLongPull(n, last_updated, protocol_data, wt_data):
    status = checkForUpdate()
    last_updated = fetchSupabaseLastUpdate()
    last_updated = datetime.utcfromtimestamp(float(last_updated)).strftime('%Y-%m-%d')
    last_updated = f"Last Updated: {last_updated} UTC"
    if status:
        print(f'Status: {status}')
        protocol_data = fetchSupabaseProtocolData()
        protocol_data[numeric_pd] = protocol_data[numeric_pd].apply(pd.to_numeric)
        protocol_data = protocol_data.to_dict('records')
        wt_data = fetchSupabaseWTData()
        wt_data[numeric_wt] = wt_data[numeric_wt].apply(pd.to_numeric)
        wt_data = wt_data.to_dict('records')
        print('Update Process Complete')
        return last_updated, protocol_data, wt_data
    else:
        print('Update Process Complete')
        return last_updated, protocol_data, wt_data


# Download Callback for Protocol Data
@app.callback(
    Output("download-pd", "data"),
    Input("btn-download-PD", "n_clicks"),
    prevent_initial_call=True,
)
def func(n_clicks):
    cur_date =  str(datetime.now(timezone.utc).date()).replace("-","")
    return dcc.send_data_frame(protocol_data.to_csv, f"Protocol_Data_{cur_date}.csv", index=False)

# Download Callback for Wallet Tracking Data
@app.callback(
    Output("download-wt", "data"),
    Input("btn-download-WTD", "n_clicks"),
    prevent_initial_call=True,
)
def func(n_clicks):
    cur_date =  str(datetime.now(timezone.utc).date()).replace("-","")
    return dcc.send_data_frame(wt_data.to_csv, f"WalletTracker_{cur_date}.csv")



money = dash_table.FormatTemplate.money(0)
price = dash_table.FormatTemplate.money(2)
percentage = dash_table.FormatTemplate.percentage(2)


cols_list = [
    'category', 'name', 'tvl', 'mcap',
    'TVL (7dma)', 'TVL (1mma)', 'Volume (7dma)', 'Volume (1mma)', 'Holders (7dd)', 'Holders (1md)', 'DAU (7dma)', 'DAU (1mma)',
    # 'TX_7DMA', 'TX_30DMA', 'AVG_RETURNING_USERS_7D', 'AVG_RETURNING_USERS_30D', 'AVG_NEW_USERS_7D', 'AVG_NEW_USERS_30D',
    'Status',
]
protocol_data = fetchSupabaseProtocolData()
numeric_pd = ['tvl', 'mcap', 'TVL (7dma)', 'TVL (1mma)', 'Volume (7dma)', 'Volume (1mma)', 'Holders (7dd)', 'Holders (1md)', 'DAU (7dma)', 'DAU (1mma)'] 
protocol_data[numeric_pd] = protocol_data[numeric_pd].apply(pd.to_numeric)
columns1 = [
    dict(id='category', name='category'),
    dict(id='name', name='name'),
    dict(id='tvl', name='tvl', type='numeric', format=money),
    dict(id='mcap', name='mcap', type='numeric', format=money),
    dict(id='TVL (7dma)', name='TVL (7dma)', type='numeric', format=percentage),
    dict(id='TVL (1mma)', name='TVL (1mma)', type='numeric', format=percentage),
    dict(id='Volume (7dma)', name='Volume (7dma)', type='numeric', format=percentage),
    dict(id='Volume (1mma)', name='Volume (1mma)', type='numeric', format=percentage),
    dict(id='Holders (7dd)', name='Holders (7dd)', type='numeric', format=percentage),
    dict(id='Holders (1md)', name='Holders (1md)', type='numeric', format=percentage),
    dict(id='DAU (7dma)', name='DAU (7dma)', type='numeric', format=percentage),
    dict(id='DAU (1mma)', name='DAU (1mma)', type='numeric', format=percentage),
    dict(id='Status', name='Status'),
]
protocol_data = protocol_data[cols_list]
protocol_table = dash_table.DataTable(
    id='protocol-table',
    columns=columns1,
    data=protocol_data.to_dict('records'),
    cell_selectable=False,
    sort_action='native',
    virtualization=True,
    fixed_rows={'headers': True},
    style_table={
        'height': '90vh',
        'width': '85vw',
    },
    style_header={
        'backgroundColor': font_color,
        'color': 'rgb(255,255,255)',
        'fontWeight': 'bold',
        'whiteSpace': 'normal',
        'width': 'auto',
        'minWidth': '5vw',
    },
    style_data={
        'backgroundColor': sm_background,
        'color': 'rgb(0,0,0)',
        'whiteSpace': 'normal',
        'height': 'auto',
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': '10vw',
        'minWidth': '5vw',
    },
)


wt_data = fetchSupabaseWTData()
numeric_wt = ['Total Value (USD)', 'Token Price (USD)', 'Token Liquidity', 'Token Qty']
wt_data[numeric_wt] = wt_data[numeric_wt].apply(pd.to_numeric)
columns1 = [
    dict(id='Date', name='Date', type='datetime'),
    dict(id='Symbol', name='Symbol'),
    dict(id='Type', name='Type'),
    dict(id='Total Value (USD)', name='Total Value (USD)', type='numeric', format=money),
    dict(id='Token Price (USD)', name='Token Price (USD)', type='numeric', format=price),
    dict(id='From Entity', name='From Entity'),
    dict(id='To Entity', name='To Entity'),
    dict(id='Token Liquidity', name='Token Liquidity', type='numeric', format=money),
    dict(id='Token Qty', name='Token Qty', type='numeric', format=Format(
        group=Group.yes,
        precision=2,
        scheme=Scheme.fixed,
    )),
    dict(id='From(Address)', name='From(Address)'),
    dict(id='To(Address)', name='To(Address)'),
    dict(id='Token Contract Address', name='Token Contract Address'),
    dict(id='Txn Hash', name='Txn Hash'),
    dict(id='From Context', name='From Context'),
    dict(id='To Context', name='To Context'),
]
wt_table = dash_table.DataTable(
    id='wt-table',
    columns=columns1,
    data=wt_data.to_dict('records'),
    cell_selectable=False,
    sort_action='native',
    virtualization=True,
    fixed_rows={'headers': True},
    style_table={
        'height': '90vh',
        'width': '85vw',
    },
    style_header={
        'backgroundColor': font_color,
        'color': 'rgb(255,255,255)',
        'fontWeight': 'bold',
        'whiteSpace': 'normal',
        'height': 'auto',
        'width': 'auto',
        'minWidth': '5vw',
    },
    style_data={
        'backgroundColor': sm_background,
        'color': 'rgb(0,0,0)',
        'whiteSpace': 'normal',
        'height': 'auto',
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': '10vw',
        'minWidth': '5vw',
    },
)


section1_notes = [
    "Notes:",
    html.Br(),
    "- Search Parameters: mcap > $200M and TVL > $50M",
    html.Br(),
    "- MCAP refers to a protocol's token. If $0, then the protocol has no token or no token data.",
    html.Br(),
    "- DAU: Daily Active User",
    html.Br(),
    "- 7dma: 7-Day Moving Average",
    html.Br(),
    "- 1mma: 1-Month Moving Average",
    html.Br(),
    "- 7dd: Total % change over the last 7 days",
    html.Br(),
    "- 1md: Total % change over the last month ",
]
section2_notes = [
    "Notes:",
    html.Br(),
    "- To & From Entities are based on the Variant provided list",
    html.Br(),
    "- Token Liquidity is calculated by summing the top 20 liquidity pools",
    # html.Br(),
    # "***Add notes for this section***",
    # html.Br(),
    # "TODO: List: Hyperlinks",
]


app.layout = html.Div(
    style={'background': primary_color, 'color': font_color, 'padding': '3.5vh'},
    children=[
        dbc.Row(
            [
                dbc.Col([html.H1("Variant", style=H1_style)]),
                dbc.Col([html.Div(id='last_updated', children='Last Updated: ', style=H1_style)]),
            ],
            justify="around",
        ),
        html.Br(),
        dbc.Container(
            [
                dbc.Row(html.H2("Protocol Overview", style=H1_style)),
                dbc.Row(protocol_table),
                dbc.Row(
                    [
                        html.Button("Download Protocol Data", id="btn-download-PD", style=btn_style),
                        dcc.Download(id="download-pd"),
                    ],
                    justify='end'
                ),
                html.Div(html.P(section1_notes, style={'padding': '2vh'})),
                html.Br(),
                dbc.Row(html.H2("Wallet Tracker", style=H1_style)),
                dbc.Row(wt_table),
                html.Div([
                    html.Button("Download Wallet Tracker Data", id="btn-download-WTD", style=btn_style),
                    dcc.Download(id="download-wt")
                ]),
                html.Div(html.P(section2_notes, style={'padding': '2vh'})),
                html.A(
                    "Powered by Slice Analytics",
                    href="https://www.sliceanalytics.xyz/",
                    style={'color': 'rgb(0,0,0)', 'padding': '4vh'},
                ),
            ]
        ),
        html.Div(children=[
            # All elements from the top of the page
            dcc.Interval(
                id='interval-component-long',
                interval=18000*1000,  # in milliseconds
                n_intervals=0,
            ),
        ])
    ]
)


if __name__ == '__main__':
    app.run_server(debug=False)
