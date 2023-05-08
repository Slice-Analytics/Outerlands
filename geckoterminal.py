import requests

# Functions for GeckoTerminal API, https://api.geckoterminal.com/docs/index.html

# Our free API is limited to 30 calls/minute


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
        print('Token for specified address not found')
    


# {
#   "data": [
#     {
#       "id": "string",
#       "type": "string",
#       "attributes": {
#         "name": "string",
#         "address": "string",
#         "token_price_usd": "string",
#         "base_token_price_usd": "string",
#         "quote_token_price_usd": "string",
#         "base_token_price_native_currency": "string",
#         "quote_token_price_native_currency": "string",
#         "pool_created_at": "string",
#         "reserve_in_usd": "string"
#       },
#       "relationships": {
#         "data": {
#           "id": "string",
#           "type": "string"
#         }
#       }
#     }
#   ]
# }
    

