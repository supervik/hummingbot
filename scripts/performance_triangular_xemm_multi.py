import pandas as pd

# Load the CSV file into a pandas DataFrame
directory = 'trades'
filename = 'trades_triangular_xemm_multi_test_ETH_USDT_spread_diffe.csv'
# df = pd.read_csv('trades_triangular_xemm_mul.csv')
df = pd.read_csv(f'{directory}/{filename}')

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# Sort dataframe by timestamp
df = df.sort_values('timestamp')

# Calculate time difference between current row and previous row in seconds
df['time_diff'] = df['timestamp'].diff().dt.total_seconds()

# Create a new column 'trading_round' which increments when time_diff > 5
df['trading_round'] = (df['time_diff'] > 5).cumsum()

# Remove temporary 'time_diff' column
df.drop('time_diff', axis=1, inplace=True)

# Group by 'trading_round' and count the unique 'symbol' for each group
unique_symbols_per_round = df.groupby('trading_round')['symbol'].nunique()

# Identify the trading rounds which have exactly 3 unique symbols
valid_rounds = unique_symbols_per_round[unique_symbols_per_round == 3].index

# Filter the dataframe to keep only valid trading rounds
df_clean = df[df['trading_round'].isin(valid_rounds)]

# Group by 'trading_round' and 'symbol', and calculate the average 'price'
df_grouped = df_clean.groupby(['trading_round', 'symbol']).agg(
    timestamp=('timestamp', 'first'),
    base_asset=('base_asset', 'first'),
    quote_asset=('quote_asset', 'first'),
    trade_type=('trade_type', 'first'),
    average_price=('price', 'mean')).reset_index()

# Sort the DataFrame by 'timestamp'
df_sorted = df_grouped.sort_values('timestamp')

# Add new column 'same_base_asset' which is 1 if the base_asset is the same as the previous row, 0 otherwise
df_sorted['same_base_asset'] = (df_sorted['base_asset'] == df_sorted['base_asset'].shift()).astype(int)


def calculate_performance(group):
    # Sort the group by timestamp to make sure the trades are in order
    group = group.sort_values('timestamp')

    # The first trade in the round is the initial asset BUY price
    buy_price = group.iloc[0]['average_price']

    # Calculate the SELL price based on the second and third trades
    if group.iloc[1]['quote_asset'] == group.iloc[2]['quote_asset']:
        if group.iloc[1]['same_base_asset'] == group.iloc[2]['same_base_asset']:
            sell_price = group.iloc[2]['average_price'] / group.iloc[1]['average_price']
        else:
            sell_price = group.iloc[1]['average_price'] / group.iloc[2]['average_price']
    else:
        sell_price = group.iloc[2]['average_price'] * group.iloc[1]['average_price']

    # Calculate performance
    if group.iloc[0]['trade_type'] == "BUY":
        performance = 100 * (sell_price / buy_price - 1)
    else:
        performance = 100 * (buy_price / sell_price - 1)

    return pd.Series({
        'timestamp': group['timestamp'].min(),
        # 'triangle_symbols': group['symbol'].tolist(),
        'triangle_symbols': group.iloc[0]['symbol'],
        'performance': performance
    })


df_performance = df_sorted.groupby('trading_round').apply(calculate_performance).reset_index()

# Subtract fee from performance
df_performance['performance'] = df_performance['performance'] - 0.24

# Save the DataFrame to an Excel file
df_performance.to_excel(f'{directory}/performance_{filename}.xlsx', index=False)

# Print dataframe
print(df_performance)
