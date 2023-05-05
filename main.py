import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from tqdm import tqdm
from time import perf_counter, time


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
    tdf['7dma_%'] = tdf['7dma'].pct_change(periods=1).fillna(0)
    tdf['1mma'] = tdf['tvls'].rolling(window=30, min_periods=1).mean()
    tdf['1mma_%'] = tdf['1mma'].pct_change(periods=1).fillna(0)
    sevenday = tdf['7dma_%'].values.tolist()
    thirtyday = tdf['1mma_%'].values.tolist()
    return sevenday[-1], thirtyday[-1]


def getVolumeData():
    # Fetches DEX Volume Data
    url = 'https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume'
    resp = requests.get(url).json()
    df_vol = pd.json_normalize(resp['protocols'])
    # df_vol = df_vol.rename(columns={'defillamaId': 'id', 'change_7d': 'Volume_7d', 'change_1m': 'Volume_1m'})
    df_vol = df_vol[['defillamaId', 'change_7d', 'change_1m']]
    # df_vol = df_vol[['id', 'Volume_7d', 'Volume_1m']]

    # Fetches Options Volume Data
    url = 'https://api.llama.fi/overview/options?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyPremiumVolume'
    resp = requests.get(url).json()
    df_vol2 = pd.json_normalize(resp['protocols'])
    # df_vol2 = df_vol2.rename(columns={'defillamaId': 'id', 'change_7d': 'Volume_7d_%', 'change_1m': 'Volume_1m_%'})
    df_vol2 = df_vol2[['defillamaId', 'change_7d', 'change_1m']]

    # Combines DEX & Option Volume Data to single Dataframe
    df_vol = pd.concat([df_vol, df_vol2]).reset_index(drop=True)
    df_vol = df_vol.rename(columns={'defillamaId': 'id', 'change_7d': 'Volume_7d_%', 'change_1m': 'Volume_1m_%'})
    df_vol['id'].astype(int)
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

    basic = HTTPBasicAuth('cqt_rQVhYWhkHKYBPwPGYqQPKtDKbMCm', '')
    response = requests.get(url, headers=headers, auth=basic)
    resp = response.json()
    holder_count = resp.get('data', {}).get('pagination', {}).get('total_count', "NA")

    print(response.json())
    return holder_count


def getBlockByTimestamp(timestamp):
    m_apikey = "I42NRodUvq7iUeKVvs86RZZ7sFVYXvY9K1ZKrvzin4dJZK2aJC9GXYictplGAIpr"
    headers = {"X-API-Key": m_apikey, "accept": "application/json",}
    url = f'https://deep-index.moralis.io/api/v2/dateToBlock?chain=eth&date={timestamp}'
    resp = requests.get(url, headers=headers)
    results = resp.json()
    return results





if __name__ == '__main__':
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

    # Creates List of Protocal names
    slugs = df['slug'].values.tolist()

    sevendayma, thirtydayma = [], []
    # Remove index for full run
    print('Fetching Historical TVL Data')
    for slug in tqdm(slugs):
        # gets historical TVL data for given Protocol
        dates, tvls = getProtocolTVLHistorical(slug)
        
        if len(dates) > 0 and len(tvls) > 0:
            # Calculates 7 day & 1 month moving average
            tempa, tempb = processTVLData(dates, tvls)
            sevendayma.append(tempa)
            thirtydayma.append(tempb)
        else:
            print('\nelsed')
            sevendayma.append("NA")
            thirtydayma.append("NA")

    # Adds moving averages to Dataframe
    df['7dma_%'] = sevendayma
    df['1mma_%'] = thirtydayma

    # Adds Volume metrics (Does NOT get all Historical Volume Data)
    vol = getVolumeData()
    vol['id'].astype(int)

    # Joins Volume data onto main Dataframe
    df = df.merge(vol, how='left', on='id')
    # print(df['change_7d'].count())


    # TODO: Add Token Holder Count metric
    ts = 1683261036
    blk = 14190032
    addy = '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0'
    # timestamps
    now = int(time())
    sevenday = now - 60*60*24*7
    print(sevenday)
    thirtyday = now - 60*60*24*30
    print(thirtyday)


    # TODO: Add Users metric
    # Users of Protocol Token
    # 


    df.to_csv('variant_db.csv', index=False)

    print(f"Runtime: {(perf_counter()-start_time)/60} mintues")

