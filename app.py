# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import requests
from datetime import timezone
import datetime


# Style Variables
primary_color = 'rgb(247,247,247)'
secondary_color = ''
tertiary_color = ''
font_color = 'rgb(9,75,215)'

sm_height = '30vh'
sm_border_radius = '1vh'
sm_background = 'rgb(224,224,255)'
sm_style = {'background': sm_background, 'height': sm_height,
            'border-radius': sm_border_radius, 'padding': '0', 'color': font_color}
H1_style = {'color': font_color}
table_style = {'background': sm_background}


last_update = datetime.datetime.now(timezone.utc)
print(last_update.replace(tzinfo=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()) # Returns a copy)
last_update = last_update.replace(tzinfo=timezone.utc)
last_update = last_update.timestamp()
print(last_update)
print(datetime.datetime.now(timezone.utc).replace(tzinfo=timezone.utc).timestamp())

app = Dash(__name__, external_stylesheets=[dbc.themes.GRID, dbc.themes.LUX])


def checkForUpdate():
    with open('last_update.txt', 'r') as file:
        lut = float(file.read())
    current_unix_time = datetime.datetime.now(timezone.utc).replace(tzinfo=timezone.utc).timestamp()
    if current_unix_time > lut+86400:
        print('Update Required')
        #TODO: Trigger Update routine.
    else:
        print('Not time to update')
        pass


def fetchEthPrice():
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd'
    r = requests.get(url)
    # print(r.status_code)
    # print(r.text)
    # print(r.json()['data']['tokenDayDatas'][0]['priceUSD'])
    return r.json()["ethereum"]["usd"]


def generateSingleMetricDelta(title_text="Example", value=400, reference=320):
    return go.Figure(go.Indicator(
        mode="number+delta",
        title={"text": f"{title_text}<br><span style='font-size:0.2em'>",
               'font_color': font_color},
        value=float(value),
        number={'prefix': "$", 'valueformat': '.2f', 'font_color': font_color},
        delta={'position': "top", 'reference': reference, 'valueformat': '.2f'},
        domain={'x': [0, 1], 'y': [0, 1]})).update_layout(paper_bgcolor='rgba(0,0,0,0)')  # "lightgray"


def generateSingleMetric(title_text="Example", value=400):
    return go.Figure(go.Indicator(
        mode="number+delta",
        title={"text": f"{title_text}<br><span style='font-size:0.2em'>",
               'font_color': font_color},
        value=float(value),
        number={'prefix': "$", 'valueformat': '.2f', 'font_color': font_color},
        # delta={'position': "top", 'reference': reference, 'valueformat': '.2f'},
        domain={'x': [0, 1], 'y': [0, 1]})).update_layout(paper_bgcolor='rgba(0,0,0,0)')  # "lightgray"


def generateTable():
    print('finish generate table')


@app.callback(
    Output(component_id='prev_price', component_property='children'),
    Output(component_id='new-price', component_property='children'),
    Input('interval-component', 'n_intervals'),
    Input(component_id='new-price', component_property='children'),
)
def updateAll(n, np):
    checkForUpdate()
    print(np)
    if np == 'New Price:':
        eth_old = float(str(np).replace('New Price:', '0'))
    else:
        eth_old = float(np.replace('New Price: ', ''))

    eth_new = fetchEthPrice()

    metric_list = ['L:S', 'Vault Used (%)', 'Vault Used ($)', 'Vault Available (%)', 'Vault Available ($)', '$ WL Ratio 1D', '$ WL Ratio 7D', '$ WL Ratio 30D', '$ WL Ratio All Time', '# WL Ratio 1D', '# WL Ratio 7D', '# WL Ratio 30D', '# WL Ratio All Time', '% Long 1D', '% Long 7D', '% Long 30D', '% Long All Time', '% Short 1D', '% Short 7D', '% Short 30D', '% Short All Time', 'Long WL % 1D', 'Long WL % 7D', 'Long WL % 30D', 'Long WL % All Time', 'Short WL % 1D', 'Short WL % 7D', 'Short WL % 30D', 'Short WL % All Time', '% traders > $500', '% traders > 5 times']

    # figs_deltas = [generateSingleMetricDelta(
        # item, eth_new, eth_old) for item in metric_list]

    # figs = [generateSingleMetric(item, eth_new) for item in metric_list]

    return f'Prev. Price: {eth_old}', f'New Price: {eth_new}'



df = pd.read_csv(
    'https://raw.githubusercontent.com/plotly/datasets/master/solar.csv')
table = dash_table.DataTable(df.to_dict('records'), [
                             {"name": i, "id": i} for i in df.columns], cell_selectable=False)


active_trades_data = [['Aave', 'Compound', 'Unicrypt', 'protocol1', 'protocol2'], [180, 73, 135, 469, 10], ['06/16/2023 05:33:49', '06/16/2023 05:34:49',
                                                                                                         '06/16/2023 05:31:49', '06/16/2023 05:35:49', '06/16/2023 05:37:49'], ['1m', '5m', '1m', '2m', '15m'], ['Long', 'Short', 'Short', 'Long', 'Long']]
protocol_data = pd.read_csv('Protocols_20230508.csv')
# print(protocol_data.columns.tolist())

money = dash_table.FormatTemplate.money(0)
percentage = dash_table.FormatTemplate.percentage(2)

columns = [
    dict(id='category', name='category'),
    dict(id='name', name='name'),
    dict(id='tvl', name='tvl', type='numeric', format=money),
    dict(id='mcap', name='mcap', type='numeric', format=money),
    dict(id='TVL (7dma)', name='TVL (7dma)', type='numeric', format=percentage),
    dict(id='TVL (1mma)', name='TVL (1mma)', type='numeric', format=percentage),
    dict(id='Volume (7dma)', name='Volume (7dma)', type='numeric', format=percentage),
    dict(id='Volume (1mma)', name='Volume (1mma)', type='numeric', format=percentage),
    dict(id='Holder Counts (7day)', name='Holder Counts (7day)', type='numeric', format=percentage),
    dict(id='Holder Counts (1mma)', name='Holder Counts (1mma)', type='numeric', format=percentage),
]

protocol_data = protocol_data[['category', 'name', 'tvl', 'mcap', 'TVL (7dma)', 'TVL (1mma)', 'Volume (7dma)', 'Volume (1mma)', 'Holder Counts (7day)', 'Holder Counts (1mma)']]

protocol_table = dash_table.DataTable(
    columns=columns,
    data=protocol_data.to_dict('records'),
    cell_selectable=False,
    sort_action='native',
    fixed_rows={'headers': True},
    style_table={
        'height': '90vh',
    },
    style_header={
        'backgroundColor': font_color,
        'color': 'rgb(255,255,255)',
        'fontWeight': 'bold',
        'whiteSpace': 'normal',
    },
    style_data={
        'backgroundColor': sm_background,
        'color': 'rgb(0,0,0)',
        'whiteSpace': 'normal',
        'height': 'auto',
    },
)


# Breakdown of vault in use: % and $ value of vault allocated for these trades, as well as % and $ amount of vault available
metric_list = ['L:S', 'Vault Used (%)', 'Vault Used ($)', 'Vault Available (%)', 'Vault Available ($)', '$ WL Ratio 1D', '$ WL Ratio 7D', '$ WL Ratio 30D', '$ WL Ratio All Time', '# WL Ratio 1D', '# WL Ratio 7D', '# WL Ratio 30D', '# WL Ratio All Time', '% Long 1D', '% Long 7D',
               '% Long 30D', '% Long All Time', '% Short 1D', '% Short 7D', '% Short 30D', '% Short All Time', 'Long WL % 1D', 'Long WL % 7D', 'Long WL % 30D', 'Long WL % All Time', 'Short WL % 1D', 'Short WL % 7D', 'Short WL % 30D', 'Short WL % All Time', '% traders > $500', '% traders > 5 times']
single_metric_list = [generateSingleMetricDelta(item) for item in metric_list]
single_metric = generateSingleMetricDelta("Long to Short Ratio")


section1_notes = [
    "Notes:",
    html.Br(),
    "- MCAP refers to a protocol's token. If $0, then the protocol has no token or no token data.",
    html.Br(),
    "- 7DMA: 7-Day Moving Average",
    html.Br(),
    "- 1MMA: 1-Month Moving Average",
]
section2_notes = [
    "Notes:",
    html.Br(),
    "***Add notes for this section***",
]


app.layout = html.Div(
    style={'border-radius': 10, 'background': primary_color, 'color': font_color},
    children=[
        html.H1("Variant", style=H1_style),
        dbc.Container(
            [
                dbc.Row(html.H1("Protocol Overview", style=H1_style)),
                dbc.Row(protocol_table),
                html.Div(html.P(section1_notes)),
                html.Br(),
                dbc.Row(html.H1("Wallet Tracker", style=H1_style)),
                dbc.Row(protocol_table),
                html.Div(html.P(section2_notes)),
                html.Br(),
            ]
        ),
        html.Div(children=[
            # All elements from the top of the page
            dcc.Interval(
                            id='interval-component',
                            interval=15*1000,  # in milliseconds
                            n_intervals=0,
                        ),
            html.H6("ETH Price", style=H1_style),
            html.Div(id='prev_price', children='Prev. Price:', style=H1_style),
            html.Br(),
            html.Div(id='new-price', children='New Price:', style=H1_style),
        ])])


if __name__ == '__main__':
    app.run_server(debug=True)
