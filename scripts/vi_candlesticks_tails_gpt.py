import ccxt
import datetime
import sys
import pandas as pd
import os


def get_trading_pairs(exchange):
    markets = exchange.load_markets()
    trading_pairs = list(markets.keys())
    return trading_pairs


def fetch_ohlc(exchange, symbol, timeframe='1m', duration_days=1, limit=1000):
    """
    Fetch OHLC data for a specific symbol and timeframe.
    """
    # Calculate total needed candles
    minutes_in_days = 60 * 24
    total_candles = duration_days * minutes_in_days

    # Initialize empty list to store results
    all_candles = []

    # Calculate since (time in milliseconds)
    end_time = datetime.datetime.utcnow()
    since = exchange.parse8601((end_time - datetime.timedelta(days=duration_days)).isoformat())

    while len(all_candles) < total_candles:
        try:
            ohlc = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            if len(ohlc) == 0:
                break
            all_candles.extend(ohlc)
            since = ohlc[-1][0] + 1  # Start from the next millisecond after the last fetched candle
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            break

    return all_candles


def calculate_tails(df, get_upper_tail=True, get_lower_tail=True):
    # Calculate upper and lower tails for each candlestick
    df['Upper_Tail'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['Lower_Tail'] = df[['Open', 'Close']].min(axis=1) - df['Low']

    # Convert tails to percentage values
    df['Upper_Tail_Percentage'] = (df['Upper_Tail'] / df[['Open', 'Close']].max(axis=1)) * 100
    df['Lower_Tail_Percentage'] = (df['Lower_Tail'] / df[['Open', 'Close']].min(axis=1)) * 100

    # Compute the Max_Tail_Percentage based on given parameters
    df['Max_Tail_Percentage'] = compute_max_tail_percentage(df, get_upper_tail, get_lower_tail)

    # Count the number of rows that exceed each threshold
    thresholds = [0.5, 1, 1.5, 2, 2.5, 3]
    counts_exceeding_thresholds = {str(threshold): (df['Max_Tail_Percentage'] > threshold).sum() for threshold in thresholds}

    return counts_exceeding_thresholds


def compute_max_tail_percentage(df, get_upper_tail=True, get_lower_tail=True):
    if get_upper_tail and not get_lower_tail:
        return df['Upper_Tail_Percentage']
    elif get_lower_tail and not get_upper_tail:
        return df['Lower_Tail_Percentage']
    elif get_upper_tail and get_lower_tail:
        return df[['Upper_Tail_Percentage', 'Lower_Tail_Percentage']].max(axis=1)
    else:
        return [None] * len(df)


def main():
    exchange_name = "binance"
    days = 1
    limit_pairs = True
    pairs_threshold = 10
    exchange = getattr(ccxt, exchange_name)()

    print(f"Start getting trading pairs for {exchange_name}")
    pairs = get_trading_pairs(exchange)
    total_pairs = len(pairs)
    print(f"{total_pairs} pairs found")

    all_data = {}
    results = []

    for idx, pair in enumerate(pairs, 1):
        try:
            ohlc_data = fetch_ohlc(exchange, pair, duration_days=days)
            all_data[pair] = ohlc_data

            # Progress display
            progress = (idx / pairs_threshold) * 100 if limit_pairs else (idx / total_pairs) * 100
            sys.stdout.write("\rFetching data: {:.2f}% completed.".format(progress))
            sys.stdout.flush()
        except Exception as e:
            print(f"\nError fetching data for {pair}: {e}")
            continue

        df = pd.DataFrame(ohlc_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
        df.drop('Timestamp', axis=1, inplace=True)

        tails_result = calculate_tails(df)
        tails_result['trading_pair'] = pair
        results.append(tails_result)

        if limit_pairs and idx == pairs_threshold:
            break

    results_df = pd.DataFrame(results)
    results_df = results_df[['trading_pair', '0.5', '1', '1.5', '2', '2.5', '3']]
    print("\n", results_df)

    print("\nData fetching completed.")

    directory = "crypto_data"
    if not os.path.exists(directory):
        os.makedirs(directory)
    results_df.to_excel(f'{directory}/tails_{exchange_name}_.xlsx', index=False)


if __name__ == "__main__":
    main()
