import ccxt
import datetime
import sys
import pandas as pd
import os


# --------------------
# Configuration
# --------------------
CONFIG = {
    "exchange_name": "binance",
    "days": 5,
    "limit_pairs": False,
    "limit_pairs_threshold": 20,
    "get_upper_tail": True,
    "get_lower_tail": True,
    'get_pairs_from_list': False,
    'pairs_list': ['XMR-ETH', 'XMR-BTC']
}

# --------------------
# Utility Functions
# --------------------


def get_trading_pairs(exchange):
    """Fetch all trading pairs for the given exchange."""
    markets = exchange.load_markets()
    return list(markets.keys())


def compute_max_tail_percentage(df, get_upper_tail=True, get_lower_tail=True):
    """Compute the maximum tail percentage based on given parameters."""
    if get_upper_tail and not get_lower_tail:
        return df['Upper_Tail_Percentage']
    elif get_lower_tail and not get_upper_tail:
        return df['Lower_Tail_Percentage']
    elif get_upper_tail and get_lower_tail:
        return df[['Upper_Tail_Percentage', 'Lower_Tail_Percentage']].max(axis=1)
    else:
        return [None] * len(df)


# --------------------
# Main Functionalities
# --------------------

def fetch_ohlc(exchange, symbol, timeframe='1m', duration_days=3, limit=1000):
    """Fetch OHLC data for a specific symbol and timeframe."""
    # Calculate total needed candles
    minutes_in_days = 60 * 24
    total_candles = duration_days * minutes_in_days
    all_candles = []
    end_time = datetime.datetime.utcnow()
    since = exchange.parse8601((end_time - datetime.timedelta(days=duration_days)).isoformat())

    while len(all_candles) < total_candles:
        ohlc = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
        if not ohlc:
            break
        all_candles.extend(ohlc)
        since = ohlc[-1][0] + 1  # Start from the next millisecond after the last fetched candle

    return all_candles


def calculate_tails(df, get_upper_tail=True, get_lower_tail=True):
    """Calculate tails and count the number of rows that exceed each threshold."""
    df['Upper_Tail'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['Lower_Tail'] = df[['Open', 'Close']].min(axis=1) - df['Low']
    df['Upper_Tail_Percentage'] = (df['Upper_Tail'] / df[['Open', 'Close']].max(axis=1)) * 100
    df['Lower_Tail_Percentage'] = (df['Lower_Tail'] / df[['Open', 'Close']].min(axis=1)) * 100
    df['Max_Tail_Percentage'] = compute_max_tail_percentage(df, get_upper_tail, get_lower_tail)
    thresholds = [0.5, 1, 1.5, 2, 2.5, 3]
    return {str(threshold): (df['Max_Tail_Percentage'] > threshold).sum() for threshold in thresholds}


def get_volume_in_usd(exchange, pair):
    """Fetch the 24-hour volume for a trading pair denominated in USDT."""
    try:
        ticker_data = exchange.fetch_ticker(pair)
        volume = ticker_data.get('baseVolume', 0)  # Getting volume in base currency
        volume_quote = ticker_data.get('quoteVolume', 0)  # Getting volume in quote currency
        base_currency, quote_currency = pair.split('/')

        if "USD" in quote_currency:
            return volume_quote
        elif "USD" in base_currency:
            return volume
        else:
            # Try to fetch the equivalent USDT pair for the base currency to convert the volume
            base_usd_pair = f"{base_currency}/USDT"
            if base_usd_pair in exchange.symbols:
                usd_ticker_data = exchange.fetch_ticker(base_usd_pair)
                usd_price = usd_ticker_data.get('last', 1)  # If not found, assume price of 1 to avoid division by zero
                return volume * usd_price
            else:
                # If base/USDT pair doesn't exist, try the quote/USDT pair
                quote_usd_pair = f"{quote_currency}/USDT"
                if quote_usd_pair in exchange.symbols:
                    usd_ticker_data = exchange.fetch_ticker(quote_usd_pair)
                    usd_price = usd_ticker_data.get('last', 1)
                    return volume / usd_price  # Use division as we are converting base volume using quote price
                else:
                    return None
    except Exception as e:
        print(f"Error fetching volume for {pair}: {e}")
        return None


def main(exchange_name, days, limit_pairs, limit_pairs_threshold, get_upper_tail, get_lower_tail, get_pairs_from_list, pairs_list):
    exchange = getattr(ccxt, exchange_name)()
    print(f"Start getting trading pairs for {exchange_name}")
    pairs = pairs_list if get_pairs_from_list else get_trading_pairs(exchange)
    pairs = [pair for pair in pairs if ':' not in pair]
    print(f"{len(pairs)} pairs found")
    total_pairs = limit_pairs_threshold if limit_pairs else len(pairs)
    results = []

    for idx, pair in enumerate(pairs, 1):
        try:
            ohlc_data = fetch_ohlc(exchange, pair, duration_days=days)
            df = pd.DataFrame(ohlc_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
            df.drop('Timestamp', axis=1, inplace=True)
            tails_result = calculate_tails(df, get_upper_tail, get_lower_tail)
            volume_usd = get_volume_in_usd(exchange, pair)
            tails_result['trading_pair'] = pair
            tails_result['volume_usd'] = volume_usd
            results.append(tails_result)
        except Exception as e:
            print(f"\nError fetching data for {pair}: {e}")

        # Progress display
        sys.stdout.write("\rFetching data: {:.2f}% completed.".format((idx / total_pairs) * 100))
        sys.stdout.flush()

        if limit_pairs and idx == limit_pairs_threshold:
            break

    results_df = pd.DataFrame(results).reindex(columns=['trading_pair', 'volume_usd', '0.5', '1', '1.5', '2', '2.5', '3'])
    results_df = results_df.sort_values(by='volume_usd', ascending=False)  # Sorting the DataFrame by volume_usd

    print("\nData fetching completed.")
    print("\n", results_df)

    # Saving results to Excel
    directory = "crypto_data"
    if not os.path.exists(directory):
        os.makedirs(directory)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    results_df.to_excel(f'{directory}/tails_{exchange_name}_{timestamp}.xlsx', index=False)


if __name__ == "__main__":
    main(**CONFIG)
