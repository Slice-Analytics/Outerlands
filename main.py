import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from web3 import Web3
from time import perf_counter, time, sleep
from datetime import datetime, timezone


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
    
    return holder_count


def getBlockByTimestamp(timestamp):
    # takes timestamp and returns block height
    m_apikey = "I42NRodUvq7iUeKVvs86RZZ7sFVYXvY9K1ZKrvzin4dJZK2aJC9GXYictplGAIpr"
    headers = {"X-API-Key": m_apikey, "accept": "application/json",}
    url = f'https://deep-index.moralis.io/api/v2/dateToBlock?chain=eth&date={timestamp}'
    resp = requests.get(url, headers=headers)
    results = resp.json()
    return results['block']


def getTokenHolderCountMetrics(tokens):
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
        day7 = (temp_thc[0]-temp_thc[1])/temp_thc[1]*100
        thcs_7day_dict[token] = day7
        day30 = (temp_thc[0]-temp_thc[2])/temp_thc[2]*100
        thcs_30day_dict[token] = day30
        end = perf_counter()
        # rarte limiter for API call of 4 requests/sec
        if (end-start) < 1:
            sleep(1-(end-start))

    thcs_7day = [thcs_7day_dict.get(token, 'NA') for token in tokens]
    thcs_30day = [thcs_30day_dict.get(token, 'NA') for token in tokens]
    
    return thcs_7day, thcs_30day




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
        sleep(0.5)

    # Adds moving averages to Dataframe
    df['7dma_%'] = sevendayma
    df['1mma_%'] = thirtydayma

    # Adds Volume metrics (Does NOT get all Historical Volume Data)
    vol = getVolumeData()
    vol['id'].astype(int)

    # Joins Volume data onto main Dataframe
    df = df.merge(vol, how='left', on='id')
    # print(df['change_7d'].count())


    #Fetchs Token Holder Count metrics
    hdc = df['address'].values.tolist()
    eth_tokens = [address for address in hdc if Web3.is_address(address)]
    chain_stats = ["ETH" if Web3.is_address(address) else "TBD" for address in hdc]
    thcs_7day_dict, thcs_30day_dict = getTokenHolderCountMetrics(eth_tokens)

    df['HolderCounts_7day%'] = [thcs_7day_dict.get(token, 'NA') for token in hdc]
    df['HolderCounts_30day%'] = [thcs_30day_dict.get(token, 'NA') for token in hdc]


    # TODO: Add Users metric
    # Users of Protocol Token
    # TBD -> Dependant on Atremis or dappradar data
    # long term would be great to setup our own db to manage


    df.to_csv('variant_db2.csv', index=False)

    print(f"Runtime: {(perf_counter()-start_time)/60} mintues")

