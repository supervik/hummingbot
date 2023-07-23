import requests
import pandas as pd


# Methods to get all traded pairs from different exchanges
def get_kucoin_pairs():
    """
    Returns list of all pairs traded on Kucoin with "-" as a divider between base and quote asset
        ["ETH-USDT", "BTC-USDT"]
    """
    url = "https://api.kucoin.com/api/v2/symbols"
    data = requests.get(url).json()
    pairs = [item['baseCurrency'] + '-' + item['quoteCurrency'] for item in data['data']]
    return pairs


def get_kucoin_volume():
    """
    Returns dictionary of trading-pair volumes in quote asset:
        {"ETH-USDT": 1600000, "ADA-USDT": 980000, "ETH-BTC": 30.4}
    """
    url = "https://api.kucoin.com/api/v1/market/allTickers"
    data = requests.get(url).json()
    volumes = {}
    for row in data["data"]["ticker"]:
        volumes[row["symbol"]] = float(row["volValue"])
    return volumes


def get_gate_io_pairs():
    """
    Returns list of all pairs traded on Gate.io with "-" as a divider between base and quote asset
        ["ETH-USDT", "BTC-USDT"]
    """
    url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    data = requests.get(url).json()
    pairs = [item['base'] + '-' + item['quote'] for item in data]
    return pairs


def get_gate_io_volume():
    """
    Returns dictionary of trading-pair volumes in quote asset:
        {"ETH-USDT": 1600000, "ADA-USDT": 980000, "ETH-BTC": 30.4}
    """
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    data = requests.get(url=url).json()
    volumes = {}
    for row in data:
        if all([row["highest_bid"], row["lowest_ask"], row["base_volume"], row["quote_volume"]]):
            symbol = row["currency_pair"].replace("_", "-")
            volumes[symbol] = float(row["quote_volume"])
    return volumes


def get_binance_pairs():
    """
    Returns list of all pairs traded on Binance with "-" as a divider between base and quote asset
        ["ETH-USDT", "BTC-USDT"]
    """
    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = requests.get(url).json()
    pairs = [item['baseAsset'] + '-' + item['quoteAsset'] for item in data['symbols'] if item['status'] == 'TRADING']
    return pairs


def get_binance_volume():
    """
    Returns dictionary of trading-pair volumes in quote asset:
        {"ETH-USDT": 1600000, "ADA-USDT": 980000, "ETH-BTC": 30.4}
    """
    url = "https://api.binance.com/api/v3/ticker/24hr"
    data = requests.get(url).json()
    volumes = {}
    translation = get_symbols_translation_binance()
    for row in data:
        if row["symbol"] in translation:
            pair = translation[row["symbol"]]
            volumes[pair] = float(row["quoteVolume"])
        else:
            print(f"Symbol {row['symbol']} is not found in the binance translation dict")
    return volumes


def get_symbols_translation_binance():
    """
    Returns the dictionary for translating pairs in Binance format to hummingbot format
    Structure:
        {"BTCUSDT": "BTC-USDT", "ETHBTC": "ETH-BTC"}
    """
    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = requests.get(url).json()
    symbols_translate = {row["symbol"]: f"{row['baseAsset']}-{row['quoteAsset']}" for row in data["symbols"]}

    return symbols_translate


def divide_pairs_by_quote_asset(pairs):
    """
    Returns dictionary of assets that are filtered by quote asset:
        {"BTC": ["ETH", "ADA", "XMR"],
        "USDT": ["ETH", "BTC", "ADA"]}
    """
    quotes = {}
    for pair in pairs:
        base, quote = pair.split("-")
        if quote in quotes:
            quotes[quote].append(base)
        else:
            quotes[quote] = [base]
    return quotes


# Config parameters. Quote assets and volume threshold denominated in quote
quote_1 = "USDT"
quote_2 = "ETH"
quote_1_vol_thrsh = 10000
quote_2_vol_thrsh = 5

# get all pairs
all_pairs_volume = get_binance_volume()
all_pairs = [pair for pair in all_pairs_volume]
#
# print(f"len1 = {len(all_pairs)}")
# print(f"{[pair for pair in all_pairs_volume]}")
pairs_by_quote = divide_pairs_by_quote_asset(all_pairs)

for quote_asset, base_assets in pairs_by_quote.items():
    print(quote_asset, len(base_assets), base_assets)

filtered_quote_1 = [base for base in pairs_by_quote[quote_1] if float(all_pairs_volume[f"{base}-{quote_1}"]) >= quote_1_vol_thrsh]
filtered_quote_2 = [base for base in pairs_by_quote[quote_2] if float(all_pairs_volume[f"{base}-{quote_2}"]) >= quote_2_vol_thrsh]

triangle_bases = set(filtered_quote_1) & set(filtered_quote_2)
triangle_bases_list = sorted(list(triangle_bases))

maker_pairs = [f"{base}-{quote_1}" for base in triangle_bases_list]
taker_pairs = [f"{base}-{quote_2}" for base in triangle_bases_list]

# maker_pairs_with_volume_thrsh = [pair for pair in maker_pairs if float(all_pairs_volume[pair]) >= quote_1_vol_thrsh]
# taker_pairs_with_volume_thrsh = [pair for pair in maker_pairs if float(all_pairs_volume[pair]) >= quote_1_vol_thrsh]
print(len(filtered_quote_1), filtered_quote_1)
print(len(filtered_quote_2), filtered_quote_2)

print(maker_pairs)
print(taker_pairs)
print(len(triangle_bases_list))
# print(all_pairs_volume)


# Save to Excel
# df = pd.DataFrame(all_pairs, columns=['Pairs'])
# df.to_csv('common_pairs.csv', index=False)
