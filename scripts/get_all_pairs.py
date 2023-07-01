import requests
import pandas as pd


# Methods to get all traded pairs from different exchanges
def get_kucoin_pairs():
    url = "https://api.kucoin.com/api/v2/symbols"
    response = requests.get(url)
    data = response.json()
    pairs = [item['baseCurrency'] + '-' + item['quoteCurrency'] for item in data['data']]
    return pairs


def get_kucoin_futures_pairs():
    url = "https://api-futures.kucoin.com/api/v1/contracts/active"
    response = requests.get(url)
    data = response.json()
    pairs = [item['baseCurrency'] + '-' + item['quoteCurrency'] for item in data['data']]
    return pairs


def get_gateio_pairs():
    url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    response = requests.get(url)
    data = response.json()
    pairs = [item['base'] + '-' + item['quote'] for item in data]
    return pairs


def get_gateio_futures_pairs():
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    response = requests.get(url)
    data = response.json()
    pairs = [item['name'].replace("_", "-") for item in data]
    return pairs


def get_binance_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    pairs = [item['baseAsset'] + '-' + item['quoteAsset'] for item in data['symbols'] if item['status'] == 'TRADING']
    return pairs


def get_binance_futures_pairs():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    pairs = [item['baseAsset'] + '-' + item['quoteAsset'] for item in data['symbols'] if item['status'] == 'TRADING']
    return pairs


# Get trading pairs
kucoin = get_kucoin_pairs()
kucoin_futures = get_kucoin_futures_pairs()
gateio = get_gateio_pairs()
gateio_futures = get_gateio_futures_pairs()
binance = get_binance_pairs()
binance_futures = get_binance_futures_pairs()

# Define here the exchanges
exchange_1 = kucoin
exchange_2 = binance_futures

# Find common pairs
common_pairs = list(set(exchange_1) & set(exchange_2))

# Save to Excel
df = pd.DataFrame(common_pairs, columns=['Pairs'])
print(common_pairs)
print(len(common_pairs))
df.to_csv('common_pairs.csv', index=False)
