# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

# from dash import Dash, DiskcacheManager, CeleryManager, html, dcc, dash_table, Input, Output
# from dash.dash_table.Format import Format, Group, Scheme


from datetime import timezone, datetime
from supabase import create_client
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import requests
import os

from datetime import date
from tqdm import tqdm
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import Query
import pandas as pd
import math
import os
from time import perf_counter, sleep
# import numpy as np
# from defillama import getCoinPrices
# from geckoterminal import getTopPoolsByToken
from supabase import create_client

import pandas as pd
import snowflake.connector
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from web3 import Web3
from time import perf_counter, sleep
from datetime import datetime, timezone, date
from supabase import create_client
import os

import pandas as pd
import snowflake.connector
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from web3 import Web3
from time import perf_counter, sleep
from datetime import datetime, timezone, date
from supabase import create_client
import os




# from dotenv import load_dotenv
# load_dotenv()  # take environment variables from .env.


# if 'REDIS_URL' in os.environ:
#     # Use Redis & Celery if REDIS_URL set as an env variable
#     from celery import Celery
#     celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
#     background_callback_manager = CeleryManager(celery_app)
# else:
#     # Diskcache for non-production apps when developing locally
#     import diskcache
#     cache = diskcache.Cache("./cache")
#     background_callback_manager = DiskcacheManager(cache)





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




def updateWTsupabase(dfwt = pd.DataFrame()):
    dfwt = dfwt.reset_index().fillna('')
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY_SECRET")
    supabase = create_client(url, key)
    # Removes all table data
    supabase.table('WT_Data').delete().neq('index', -1).execute()
    # Inserts Data
    supabase.table('WT_Data').insert(dfwt.to_dict(orient='records')).execute()
    return None


def fetchWalletTrackerData():
    start = perf_counter()

    wallet_dict = {
        '0xdbf5e9c5206d0db70a90108bf936da60221dc080': ['Wintermute', 'Wallet that was outlined in our Canto trade'],
        '0xA1614eC01d13E04522ED0b085C7a178ED9E99bc9': ['Wintermute', 'USDC transfer wallet'],
        '0x4f3a120E72C76c22ae802D129F599BFDbc31cb81': ['Wintermute', 'Multisig'],
        '0xfE484F5B965C7f34248FB0a8cA2f1360E1d9bD2A': ['Wintermute', 'None'],
        '0x00000000ae347930bd1e7b0f35588b92280f9e75': ['Wintermute', 'Market maker'],
        '0x000018bbb8df8de9e3eaf772db1c4eec228ef06c': ['Wintermute', 'fund'],
        '0xf584f8728b874a6a5c7a8d4d387c9aae9172d621': ['Jump', 'None'],
        '0x9507c04b10486547584c37bcbd931b2a4fee9a41': ['Jump', 'Swap Contract created by 0x69e906c33722f61b854929cd2c6f3427dea9f090'],
        '0xF05e2A70346560d3228c7002194Bb7C5dC8Fe100': ['Jump', 'Binance deposit'],
        '0xd628f7c481c7dd87f674870bec5d7a311fb1d9a2': ['Genesis', 'None'],
        '0x66b870ddf78c975af5cd8edc6de25eca81791de1': ['Oapital?', 'None'],
        # VC
        '0x641ce4240508eae5dcaeffe991f80941d683ad64': ['Dragonfly', 'VC'],
        '0x002A5dc50bbB8d5808e418Aeeb9F060a2Ca17346': ['Dragonfly', 'VC'],
        '0xa44ed7d06cbee6f7d166a7298ec61724c08163f5': ['Dragonfly', 'VC'],
        '0xad0eeed8e2bc9531804f4ad3a9818b5c1a7b5a98': ['Framework Ventures', 'VC'],
        # Notable individuals:
        '0x9c5083dd4838E120Dbeac44C052179692Aa5dAC5': ['Tetranode', 'Notable Individual'],
        '0xca86d57519dbfe34a25eef0923b259ab07986b71': ['Messi', 'Notable Individual'],
        '0x10e7d26a02bd124250ea00c41dcd16fc791ccd78': ['Messi', 'Notable Individual'],
    }

    API_KEY = os.getenv('DUNE_API_KEY_UC')

    # Dune Analytics Query Section
    query = Query(
        name="G_WT_results",
        query_id=2758194,
        params=[
            QueryParameter.number_type(name="days_ago", value=1),
        ],
    )
    print("Results available at", query.url())
    dune = DuneClient(API_KEY)
    response = dune.refresh(query)
    data = pd.DataFrame(response.result.rows)

    # Price/Symbol/Decimals Data Section
    addresses_df = data['contract_address'].tolist()
    indices = [i for i, x in enumerate(addresses_df) if x == "0x"]
    # Replaces ETH contract "Ox" for WETH contract for API intergration purposes
    addresses = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower() if address == "0x" else address for address in addresses_df]

    u_addresses = list(set(addresses))
    unique_addresses = len(u_addresses)
    if unique_addresses > 50:
        print(f'getCoinPrices Batches Required: {math.ceil(unique_addresses/50)}')
        total_count = 0
        counter = 1
        price = {}
        while total_count < unique_addresses:
            bg = (counter-1)*50
            ed = counter*50
            price_results = getCoinPrices(u_addresses[bg:ed])
            price.update(price_results.get('coins', {}))
            counter += 1
            total_count += 50
    else:
        price_results = getCoinPrices(u_addresses)
        price = price_results.get('coins', 'N/A')


    price_list, symbol_list, decimal_list = [],[],[]
    for i, address in enumerate(addresses):
        input = f'ethereum:{address}'
        price_list.append(price.get(input, {}).get('price'))
        if i in indices:
            symbol_list.append('ETH')
        else:
            symbol_list.append(price.get(input, {}).get('symbol', 'N/A'))
        decimal_list.append(price.get(input, {}).get('decimals'))

    data['Token Price (USD)'] = price_list
    data['Symbol'] = symbol_list
    data['decimal'] = decimal_list


    # Liquidity Data Section
    liquidity_dict = {}
    for address in tqdm(u_addresses):
        rl_st = perf_counter()
        liquidity_results = getTopPoolsByToken(address)
        liquidity = 0
        if liquidity_results.get('data'):
            for lp in liquidity_results['data']:
                if lp.get('attributes', {}).get('reserve_in_usd', 0):
                    liquidity += float(lp.get('attributes', {}).get('reserve_in_usd', 0))
                else:
                    pass
        liquidity_dict[address] = liquidity
        rl_dur = perf_counter()-rl_st
        if rl_dur < 2.5:
            sleep(2.5-rl_dur)

    liquidity_list = [liquidity_dict.get(address, 0) for address in addresses]

    data['Token Liquidity'] = liquidity_list


    # Add Labels Section ->
    wallet_lower = {k.lower(): v for k, v in wallet_dict.items()}
    to_address = data['to'].tolist()    
    from_address = data['from'].tolist()    

    #zip the two lists together into a list of tuples
    address_list = list(zip(to_address, from_address))
    from_labels_list, from_context_list = [], []
    to_labels_list, to_context_list = [], []
    buy_sell = []
    for i, j in address_list:
        if i in list(wallet_lower.keys()) and j in list(wallet_lower.keys()):
            buy_sell.append('Tracked Wallet Buy & Sell')
        elif i in list(wallet_lower.keys()):
            buy_sell.append('Buy')
        elif j in list(wallet_lower.keys()):
            buy_sell.append('Sell')
        else:
            buy_sell.append('None')

        if wallet_lower.get(j):
            from_labels_list.append(wallet_lower.get(j)[0])
        else:
            from_labels_list.append('')
        if wallet_lower.get(j):
            from_context_list.append(wallet_lower.get(j)[1]) 
        else:
            from_context_list.append('')

        if wallet_lower.get(i):
            to_labels_list.append(wallet_lower.get(i)[0])
        else:
            to_labels_list.append('')
        if wallet_lower.get(i):
            to_context_list.append(wallet_lower.get(i)[1])
        else:
            to_context_list.append('')

    data['Type'] = buy_sell
    data['From Entity'] = from_labels_list
    data['From Context'] = from_context_list
    data['To Entity'] = to_labels_list
    data['To Context'] = to_context_list     


    # Calc token value and amount section
    data = data.assign(token_amount=data['value'].astype(float) / (10 ** data['decimal']))
    data = data.assign(token_value=data['token_amount'] * data['Token Price (USD)'])
    data = data.rename(columns={"token_amount": "Token Qty", "token_value": "Total Value (USD)", "evt_tx_hash": "Txn Hash", "evt_block_time": "Date", "from": "From(Address)", "to": "To(Address)", "contract_address": "Token Contract Address"})
    data = data[['Date', 'Symbol', 'Type', 'Total Value (USD)', 'Token Price (USD)', 'From Entity',	'To Entity',
                'Token Liquidity', 'Token Qty', 'From(Address)', 'To(Address)', 'Token Contract Address', 'Txn Hash', 'From Context', 'To Context', 'value', 'decimal']]


    data = data.drop(columns=['value', 'decimal'])

    # data.to_parquet('WT_Data.gzip', index=False)
    
    # Posts data to supabase WT_Data table
    updateWTsupabase(data)

    end = perf_counter()
    print(f"Run Time: {end-start} seconds | {(end-start)/60} minutes")


import pandas as pd
import snowflake.connector
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from web3 import Web3
from time import perf_counter, sleep
from datetime import datetime, timezone, date
from supabase import create_client
import os


def updatePDsupabase(df = pd.DataFrame()):
    df['DAU (7dma)'] = df['DAU (7dma)'].astype(float)
    df['DAU (1mma)'] = df['DAU (1mma)'].astype(float)
    df['TX_7DMA'] = df['TX_7DMA'].astype(float)
    df['TX_30DMA'] = df['TX_30DMA'].astype(float)
    df['AVG_RETURNING_USERS_7D'] = df['AVG_RETURNING_USERS_7D'].astype(float)
    df['AVG_RETURNING_USERS_30D'] = df['AVG_RETURNING_USERS_30D'].astype(float)
    df['AVG_NEW_USERS_7D'] = df['AVG_NEW_USERS_7D'].astype(float)
    df['AVG_NEW_USERS_30D'] = df['AVG_NEW_USERS_30D'].astype(float)
    df = df.fillna('')

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY_SECRET")
    supabase = create_client(url, key)

    # Removes all table data
    supabase.table('Protocol_Data').delete().neq('id', -1).execute()

    # Inserts Data
    supabase.table('Protocol_Data').insert(df.to_dict(orient='records')).execute()

    return None


def getProtocols():
    resp = requests.get('https://api.llama.fi/protocols')
    return resp.json()


def getProtocolTVLHistorical(protocol_slug):
    url = f'https://api.llama.fi/protocol/{protocol_slug}'
    resp = requests.get(url)
    if resp.status_code == 200:
        resp = resp.json()
        temp_dates, temp_tvls = [], []
        for dict in resp['tvl']:
            temp_dates.append(dict['date'])
            temp_tvls.append(dict['totalLiquidityUSD'])
        return temp_dates, temp_tvls
    else:
        print(f'Attempting Retry: {protocol_slug}')
        resp = requests.get(url)
        if resp.status_code == 200:
            resp = resp.json()
            temp_dates, temp_tvls = [], []
            for dict in resp['tvl']:
                temp_dates.append(dict['date'])
                temp_tvls.append(dict['totalLiquidityUSD'])
            return temp_dates, temp_tvls
        else:
            print(f'Retry Failed: {protocol_slug}')
   

def processTVLData(tdates, ttvls):
    tdf = pd.DataFrame(data={'timestamp': tdates, 'tvls': ttvls})
    tdf['dates'] = tdf['timestamp'].values.astype(dtype='datetime64[s]')
    tdf['dates'] = pd.to_datetime(tdf['dates']).dt.date
    tdf['7dma'] = tdf['tvls'].rolling(window=7, min_periods=1).mean()
    tdf['7dma_%'] = tdf['7dma'].pct_change(periods=7).fillna(0)
    tdf['1mma'] = tdf['tvls'].rolling(window=30, min_periods=1).mean()
    tdf['1mma_%'] = tdf['1mma'].pct_change(periods=30).fillna(0)

    sevenday = tdf['7dma_%'].values.tolist()
    thirtyday = tdf['1mma_%'].values.tolist()
    return sevenday[-1], thirtyday[-1]


def getVolumeData():
    # Fetches DEX Volume Data
    url = 'https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume'
    resp = requests.get(url).json()
    df_vol = pd.json_normalize(resp['protocols'])
    # df_vol = df_vol.rename(columns={'defillamaId': 'id', 'change_7d': 'Volume_7d', 'change_1m': 'Volume_1m'})
    df_vol = df_vol[['defillamaId', 'change_7dover7d', 'change_30dover30d']]
    # df_vol = df_vol[['id', 'Volume_7d', 'Volume_1m']]

    # Fetches Options Volume Data
    url = 'https://api.llama.fi/overview/options?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyPremiumVolume'
    resp = requests.get(url).json()
    df_vol2 = pd.json_normalize(resp['protocols'])
    # df_vol2 = df_vol2.rename(columns={'defillamaId': 'id', 'change_7d': 'Volume_7d_%', 'change_1m': 'Volume_1m_%'})
    df_vol2 = df_vol2[['defillamaId', 'change_7dover7d', 'change_30dover30d']]

    # Combines DEX & Option Volume Data to single Dataframe
    df_vol = pd.concat([df_vol, df_vol2]).reset_index(drop=True)
    df_vol = df_vol.rename(columns={'defillamaId': 'id', 'change_7dover7d': 'Volume (7dma)', 'change_30dover30d': 'Volume (1mma)'})
    df_vol['id'].astype(int)
    df_vol['Volume (7dma)'] = df_vol['Volume (7dma)'].div(100)
    df_vol['Volume (1mma)'] = df_vol['Volume (1mma)'].div(100)
    return df_vol


# limit to 100k credits (2 credits per token holders request)
def getTokenHolderCount(tokenAddress, block=0):
    chainName = 'eth-mainnet'
    if block == 0:
        url = f"https://api.covalenthq.com/v1/{chainName}/tokens/{tokenAddress}/token_holders_v2/"
    else:
        url = f"https://api.covalenthq.com/v1/{chainName}/tokens/{tokenAddress}/token_holders_v2/?block-height={block}"
    
    headers = {
        "accept": "application/json",
    }
    coval_apikey = os.getenv('COVALENT_API_KEY')
    basic = HTTPBasicAuth(coval_apikey, '')
    response = requests.get(url, headers=headers, auth=basic)
    sleep(1)
    if response.status_code == 200:
        resp = response.json()
        holder_count = resp.get('data', {}).get('pagination', {}).get('total_count', "NA")
        # if check handlea tokens wihtout 7 or 30 days of data
        if holder_count == None:
            return 0
        else:
            return holder_count
    else:
        print(f'Response Status Code: {response.status_code}')
        covalent_status_dict = {
        200: 'OK, Everything worked as expected.',
        400: 'Bad Request: The request could not be accepted, usually due to a missing required parameter.',
        401: 'Unauthorized:	No valid API key was provided.',
        404: 'Not Found: The request path does not exist.',
        429: 'Too Many Requests: You are being rate-limited. Please see the rate limiting section for more information.',
        500: "Server Errors: Something went wrong on Covalent's servers.",
        502: "Server Errors: Something went wrong on Covalent's servers.",
        503: "Server Errors: Something went wrong on Covalent's servers.",
        }
        print(f'Response Status Code = {response.status_code}: {covalent_status_dict[response.status_code]}')
        # # TODO: Replace quit() with a proper error handling if possible
        # quit()


def getBlockByTimestamp(timestamp):
    # takes timestamp and returns block height
    m_apikey = os.getenv('MORALIS_API_KEY')
    headers = {"X-API-Key": m_apikey, "accept": "application/json",}
    url = f'https://deep-index.moralis.io/api/v2/dateToBlock?chain=eth&date={timestamp}'
    resp = requests.get(url, headers=headers)
    results = resp.json()
    return results['block']


def getTokenHolderCountMetrics(tokens):
    print('Fetching Token Holder Count Meitrcs')
    # generating daily, -7 day, -30 day timestamps
    date_example = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    ts_day = datetime.timestamp(date_example)
    ts = [ts_day, ts_day-(60*60*24*7), ts_day-(60*60*24*30)]
    # inputs timestamp and returns closest block number for the given timestamp
    blks = [getBlockByTimestamp(t) for t in ts]

    # generates lookup dictionary for tokens
    thcs_7day_dict, thcs_30day_dict = {}, {}
    for token in tqdm(tokens):
        start = perf_counter()
        temp_thc = [getTokenHolderCount(token, blk) for blk in blks]
        if temp_thc[1] == 0:
            day7 = (temp_thc[0]-1)*100
        else:
            day7 = (temp_thc[0]-temp_thc[1])/temp_thc[1]*100
        thcs_7day_dict[token] = day7
        if temp_thc[2] == 0:
            day30 = (temp_thc[0]-1)*100
        else:
            day30 = (temp_thc[0]-temp_thc[2])/temp_thc[2]*100
        thcs_30day_dict[token] = day30
        end = perf_counter()
        # rarte limiter for API call of 4 requests/sec
        if (end-start) < 1:
            sleep(1-(end-start))
    
    return thcs_7day_dict, thcs_30day_dict


def fetchSnowFlakeData():
    print('Fetching SnowFlake Data...')
    #create connection
    conn = snowflake.connector.connect(
        user=os.getenv('SN_USER'),
        password=os.getenv('SN_PASSWORD'),
        account=os.getenv('SN_ACCOUNT'),
        database="ARTEMIS_ANALYTICS",
        schema="PROD"
    )
    try:
        curs = conn.cursor()
        #execute SQL statement
        with open('DAU.sql', 'r') as f:
            sql = f.read()
        curs.execute(sql)
        df = curs.fetch_pandas_all()
    finally:
        # Closing the connection
        conn.close()
        df_key = pd.read_csv('llama_art_map.csv')
        df_key = df_key.drop(columns=['category'])
        # ['name', 'NAMESPACE', 'FRIENDLY_NAME', 'AVG_DAU_7D', 'AVG_DAU_7D_PREV', 'DAU_7DMA_PER', 'AVG_DAU_30D', 'AVG_DAU_30D_PREV', 'DAU_30DMA_PER', 'AVG_TX_7D', 'AVG_TX_7D_PREV', 'TX_7DMA_PER', 'AVG_TX_30D', 'AVG_TX_30D_PREV', 'TX_30DMA_PER', 'AVG_RETURNING_USERS_7D', 'AVG_RETURNING_USERS_30D', 'AVG_NEW_USERS_7D', 'AVG_NEW_USERS_30D']
        results = df_key.merge(df, how='left', on='NAMESPACE').drop(columns=['NAMESPACE', 'AVG_DAU_7D', 'AVG_DAU_7D_PREV','AVG_DAU_30D', 'AVG_DAU_30D_PREV', 'AVG_TX_7D', 'AVG_TX_7D_PREV', 'AVG_TX_30D', 'AVG_TX_30D_PREV'])
        results['FRIENDLY_NAME'] = results['FRIENDLY_NAME'].fillna('No Data')
        results.loc[results['FRIENDLY_NAME'] != 'No Data', 'FRIENDLY_NAME'] = '1'
        results = results.rename(columns={'FRIENDLY_NAME': 'Status'})
        return results


def fetchProtocolData(): 
    start_time = perf_counter()
    # Fetches all protocols on Defi Llama
    protocols = getProtocols()
    # Initializes Dataframe
    df = pd.json_normalize(protocols)
    # Filters Protocols based on minimum tvl requirement
    min_mcap = 200_000_000
    min_tvl = 50_000_000
    print(f'Min TVL: {min_tvl} | Min mcap: {min_mcap}')
    df = df[(df['tvl'] >= min_tvl) | (df['mcap'] >= min_mcap)]
    print(f'Protocols Tracked: {len(df)}')
    # Reduces Dataframe to only required Columns
    df = df[['id', 'category', 'name', 'address', 'symbol', 'tvl', 'mcap', 'slug']]
    df['id'].astype(int)
    # Custom removals
    # Removes 'Chains'
    df = df[df['category'] != 'Chain']
    # Removes Avax & Imx
    df = df[~df['id'].isin([3139, 3140])]

    # Creates List of Protocal names
    slugs = df['slug'].values.tolist()

    sevendayma, thirtydayma = [], []
    # Remove index for full run
    print('Fetching Historical TVL Data')
    failed_slugs = []
    for slug in tqdm(slugs):
        # gets historical TVL data for given Protocol
        dates, tvls = getProtocolTVLHistorical(slug)
        
        if len(dates) > 0 and len(tvls) > 0:
            # Calculates 7 day & 1 month moving average
            tempa, tempb = processTVLData(dates, tvls)
            sevendayma.append(tempa)
            thirtydayma.append(tempb)
        else:
            print('\nFailed to fetch Historical TVL Data:', slug)
            failed_slugs.append(slug)
            sevendayma.append(None)
            thirtydayma.append(None)
        sleep(0.5)

    # Adds TVL moving averages to Dataframe
    df['TVL (7dma)'] = sevendayma
    df['TVL (1mma)'] = thirtydayma
    # df['TVL (7dma)'] = df['TVL (7dma)'].div(100)
    # df['TVL (1mma)'] = df['TVL (1mma)'].div(100)

    # Adds Volume metrics (Does NOT get all Historical Volume Data)
    vol = getVolumeData()
    vol['id'].astype(int)

    # Joins Volume data onto main Dataframe
    df = df.merge(vol, how='left', on='id')
    print(f'Protocols Tracked Post Vol Merge: {len(df)}')


    #Fetchs Token Holder Count metrics
    hdc = df['address'].values.tolist()
    eth_tokens = [address for address in hdc if Web3.is_address(address)]
    # chain_stats = ["ETH" if Web3.is_address(address) else "TBD" for address in hdc]
    thcs_7day_dict, thcs_30day_dict = getTokenHolderCountMetrics(eth_tokens)

    df['Holders (7dd)'] = [thcs_7day_dict.get(token, None) for token in hdc]
    df['Holders (1md)'] = [thcs_30day_dict.get(token, None) for token in hdc]
    df['Holders (7dd)'] = df['Holders (7dd)'].div(100)
    df['Holders (1md)'] = df['Holders (1md)'].div(100)


    # Users of Protocol
    user_metrics = fetchSnowFlakeData()
    user_metrics = user_metrics.rename(columns={'DAU_7DMA': 'DAU (7dma)', 'DAU_30DMA': 'DAU (1mma)',})
    df = df.merge(user_metrics, how='left', on='name')
    df = df[~df['id'].isin([1531, 3139, 3140, '1531','3139', '3140'])]
    # ['name', 'FRIENDLY_NAME', 'DAU_7DMA_', 'DAU_30DMA', 'TX_7DMA', 'TX_30DMA', 'AVG_RETURNING_USERS_7D', 'AVG_RETURNING_USERS_30D', 'AVG_NEW_USERS_7D', 'AVG_NEW_USERS_30D']
    print(df.loc[~df['Status'].isin(['No Data', '1']), 'Status'].values.tolist())
    df.loc[~df['Status'].isin(['No Data', '1']), 'Status'] = 'Requires Update'
    # df.to_parquet('Protocols_Data.gzip', index=False)

    # Posts data to supabase WT_Data table
    updatePDsupabase(df)

    print(f"Runtime: {(perf_counter()-start_time)/60} mintues")


def checkStatusCode(status_code):
    if status_code == 200:
        pass
    elif status_code == 502:
        print('Internal error')


def getCoinPrices(address_list, chain="ethereum", searchWidth=0):
    print(f'addresslist length: {len(address_list)}')
    url = "https://coins.llama.fi/prices/current/"
    # EX: ethereum:0xcdf7028ceab81fa0c6971208e83fa7872994bee5,ethereum:0x31c8eacbffdd875c74b94b077895bd78cf1e64a3
    address_text =  f'{chain}:' + f',{chain}:'.join(address_list)
    url = f'{url}{address_text}'
    if searchWidth > 0:
        url = f'{url}?searchWidth={searchWidth}h'
    # print(url)
    response = requests.get(url)
    print(f'getCoinPrices Resp{response.status_code}')
    if response.status_code == 200:
        return response.json()
    else:
        checkStatusCode(response.status_code)



def checkStatusCode(status_code):
    if status_code == 200:
        pass
    elif status_code == 404:
        print('Internal error')


def getChains():
    url = 'https://api.geckoterminal.com/api/v2/networks?page=1'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print('Error')


# /networks/{chain}/tokens/{token_address}/pools - Returns the Top 20 Pools for a Token
def getTopPoolsByToken(token_address, chain="eth"):
    url = f"https://api.geckoterminal.com/api/v2/networks/{chain}/tokens/{token_address}/pools"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        # print('Token for specified address not found')
        # print(f'  {token_address}')
        return {}
    














