import pandas as pd
import snowflake.connector
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from web3 import Web3
from time import perf_counter, sleep
from datetime import datetime, timezone, date
import os


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
    coval_apikey = 'cqt_rQVhYWhkHKYBPwPGYqQPKtDKbMCm'
    # coval_apikey = os.getenv('covalent_api_key')
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
        # TODO: Replace quit() with a proper error handling if possible
        quit()


def getBlockByTimestamp(timestamp):
    # takes timestamp and returns block height
    m_apikey = "I42NRodUvq7iUeKVvs86RZZ7sFVYXvY9K1ZKrvzin4dJZK2aJC9GXYictplGAIpr"
    # m_apikey = os.getenv('moralis_api_key')
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
        user="ALLENSLICEANALYTICS",
        password="Sl!ceJAT2022",
        account="msb68270.us-east-1",
        # user=os.getenv('sn_user'),
        # password=os.getenv('sn_password'),
        # account=os.getenv('sn_account'),
        database="ARTEMIS_ANALYTICS",
        schema="PROD"
    )
    try:
        curs = conn.cursor()
        #execute SQL statement
        with open('DAU.sql', 'r') as f:
            sql = f.read()
        curs.execute(sql)
        # curs.execute_async(sql)
        df = curs.fetch_pandas_all()
        # print(len(df))
    finally:
        # Closing the connection
        conn.close()
        df_key = pd.read_csv('llama_art_map.csv')
        df_key = df_key.drop(columns=['category'])
        # ['name', 'NAMESPACE', 'FRIENDLY_NAME', 'AVG_DAU_7D', 'AVG_DAU_7D_PREV', 'DAU_7DMA_PER', 'AVG_DAU_30D', 'AVG_DAU_30D_PREV', 'DAU_30DMA_PER', 'AVG_TX_7D', 'AVG_TX_7D_PREV', 'TX_7DMA_PER', 'AVG_TX_30D', 'AVG_TX_30D_PREV', 'TX_30DMA_PER', 'AVG_RETURNING_USERS_7D', 'AVG_RETURNING_USERS_30D', 'AVG_NEW_USERS_7D', 'AVG_NEW_USERS_30D']
        results = df_key.merge(df, how='left', on='NAMESPACE').drop(columns=['NAMESPACE', 'AVG_DAU_7D', 'AVG_DAU_7D_PREV','AVG_DAU_30D', 'AVG_DAU_30D_PREV', 'AVG_TX_7D', 'AVG_TX_7D_PREV', 'AVG_TX_30D', 'AVG_TX_30D_PREV'])
        results['FRIENDLY_NAME'] = results['FRIENDLY_NAME'].fillna('No Data')
        results.loc[results['FRIENDLY_NAME'] != 'No Data', 'FRIENDLY_NAME'] = 1
        results = results.rename(columns={'FRIENDLY_NAME': 'Status'})
        # print(results)
        return results


# if __name__ == '__main__':
def fetchProtocolData(): 
    start_time = perf_counter()
    # Fetches all protocols on Defi Llama
    protocols = getProtocols()
    # Initializes Dataframe
    df = pd.json_normalize(protocols)
    # Filters Protocols based on minimum tvl requirement
    min_mcap = 100_000_000  
    min_tvl = 10_000_000  
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

    df['Holders (7d delta)'] = [thcs_7day_dict.get(token, None) for token in hdc]
    df['Holders (1m delta)'] = [thcs_30day_dict.get(token, None) for token in hdc]
    df['Holders (7d delta)'] = df['Holders (7d delta)'].div(100)
    df['Holders (1m delta)'] = df['Holders (1m delta)'].div(100)


    # Users of Protocol
    user_metrics = fetchSnowFlakeData()
    df = df.merge(user_metrics, how='left', on='name')
    df = df[~df['id'].isin([3139, 3140])]
    # ['name', 'FRIENDLY_NAME', 'DAU_7DMA_PER', 'DAU_30DMA_PER', 'TX_7DMA_PER', 'TX_30DMA_PER', 'AVG_RETURNING_USERS_7D', 'AVG_RETURNING_USERS_30D', 'AVG_NEW_USERS_7D', 'AVG_NEW_USERS_30D']
    print(df.loc[~df['Status'].isin(['No Data', '1', 1]), 'Status'].values.tolist())
    df.loc[~df['Status'].isin(['No Data', '1', 1]), 'Status'] = 'Requires Update'

    # # Preparation for .csv save
    # date_ts = date.today()
    # date_ts = str(date_ts).replace("-","")
    # file_name = f"Protocols_{date_ts}.csv"

    df.to_csv('Protocols_Data.csv', index=False)
    print(f"Runtime: {(perf_counter()-start_time)/60} mintues")

