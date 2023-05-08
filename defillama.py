import requests

# Functions for Defillama API, https://defillama.com/docs/api


def checkStatusCode(status_code):
    if status_code == 200:
        pass
    elif status_code == 502:
        print('Internal error')


# /prices/current/{coins}
def getCoinPrices(address_list, chain="ethereum", searchWidth=0):
    url = "https://coins.llama.fi/prices/current/"
    # EX: ethereum:0xcdf7028ceab81fa0c6971208e83fa7872994bee5,ethereum:0x31c8eacbffdd875c74b94b077895bd78cf1e64a3
    address_text =  f'{chain}:' + f',{chain}:'.join(address_list)
    url = f'{url}{address_text}'
    if searchWidth > 0:
        url = f'{url}?searchWidth={searchWidth}h'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        checkStatusCode(response.status_code)
    
    # Schema Example
    # {
    #   "coins": {
    #       "ethereum:0xdF574c24545E5FfEcb9a659c229253D4111d87e1": {
    #           "decimals": 8,
    #           "price": 0.022053735051098835,
    #           "symbol": "cDAI",
    #           "timestamp": 0.99
    #       }
    #   }
    # }
    

adde = ['0xcdf7028ceab81fa0c6971208e83fa7872994bee5', '0x31c8eacbffdd875c74b94b077895bd78cf1e64a3']
print(getCoinPrices(adde))