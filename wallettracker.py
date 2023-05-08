


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


#import dotenv
import os
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import Query
import pandas as pd
import numpy as np
from defillama import getCoinPrices
from geckoterminal import getTopPoolsByToken 


API_KEY = "4j6BpVadKDiYMQBy53l8e7KEl43FTpLF"

# Dune Analytics Query Section -> 
query = Query(
    name="API TEST",
    query_id=2448228,
    params=[
        # QueryParameter.text_type(name="TextField", value="Word"),
        QueryParameter.number_type(name="days_ago", value=1),
        # QueryParameter.date_type(name="DateField", value="2022-05-04 00:00:00"),
        # QueryParameter.enum_type(name="EnumField", value="Option 1"),
    ],
)
print("Results available at", query.url())


dune = DuneClient(API_KEY)
response = dune.refresh(query)
data = pd.DataFrame(response.result.rows)
data.to_csv("data.csv", index=False)
#data = pd.read_csv("data.csv")



# Price/Symbol/Decimals Data Section -> 

addresses = data['contract_address'].tolist()
price_results = getCoinPrices(addresses)
price = price_results.get('coins', 'N/A')
price_list,symbol_list,decimal_list = [],[],[]

for i in addresses:
    input = 'ethereum:' + i 
    price_list.append(price.get(input, {}).get('price', 'N/A'))
    symbol_list.append(price.get(input, {}).get('symbol', 'N/A'))
    decimal_list.append(price.get(input, {}).get('decimals', 'N/A'))

data['price'] = price_list
data['symbol'] = symbol_list
data['decimal'] = decimal_list

data.to_csv("data2.csv", index=False)    


# Liquidity Data Section -> 
data = pd.read_csv("data2.csv")
addresses = data['contract_address'].tolist()
liquidity_list = []
for i in addresses:
    liquidity_results = getTopPoolsByToken(i)
    liquidity = 0

    for lp in liquidity_results['data']:
        liquidity += float(lp.get('attributes', {}).get('reserve_in_usd', 0))

    liquidity_list.append(liquidity)

print(liquidity_list)
data['liquidity'] = liquidity_list
data.to_csv("data3.csv", index=False)



# Add Labels Section ->

data = pd.read_csv("data3.csv")

# convert keys to lowercase to match dataframe
#wallet_lower = {k.lower(): v for k, v in wallet_dict.items()}
wallet_lower ={}
for k,v in wallet_dict.items(): 
    wallet_lower[k.lower()] = v

to_address = data['to'].tolist()    
from_address = data['from'].tolist()    
#zip the two lists together into a list of tuples
address_list = list(zip(to_address, from_address))

from_labels_list = []
from_context_list = []
to_labels_list = []
to_context_list = []
buy_sell = []
for i,j in address_list:
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


data['Transaction Type'] = buy_sell
data['From Label'] = from_labels_list
data['From Context'] = from_context_list
data['To Label'] = to_labels_list
data['To Context'] = to_context_list
print(data) 
data.to_csv("data4.csv", index=False)        

# Calc token value and amount section ->


data = pd.read_csv("data4.csv")


#Learn more about datatypes
data = data.assign(token_amount=data['value'].astype(float) / (10 ** data['decimal']))
data = data.assign(token_value=data['token_amount'] * data['price'])
print(data)
data.to_csv("Final.csv", index=False)

# TODO: Add wallet labels & context - TIM use .get function to pull data from dictionary (within dictionary are lists)
# TODO: Calculate True Token Amount - TIM (mulitply several rows and add to end of column)
# TODO: Calculate Token Value in USD - TIM (mulitply several rows and add to end of column)
# TODO: Add available liquditiy for token (geckoterminal.py, getTopPoolsByToken, no API key needed. #response.get(data,{})[0])
# TODO: Add simple action label (sell or buy) (For loop use logic to determine what is buy and what is sell)
# TODO: Gather any additional Contract information
# TODO: Save as .CSV
# TODO: single .CSV database

# Daily updates -> Dictates info limit 2.5M -> 