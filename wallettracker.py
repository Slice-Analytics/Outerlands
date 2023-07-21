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
from defillama import getCoinPrices
from geckoterminal import getTopPoolsByToken
from supabase import create_client



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
                liquidity += float(lp.get('attributes', {}).get('reserve_in_usd', 0))
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

