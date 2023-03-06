import logging
import os

import pandas as pd
import time
import requests

from hummingbot import data_path
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import Decimal, ScriptStrategyBase


class CandleSticksTails(ScriptStrategyBase):
    """
    This script has 2 functions
    - Fetch all trading pairs from the exchange with the help of hb all_trading_pairs method and saves them to csv file
    - Get 1000 5 min candles for each trading pair and saves maximum up and down tails number into a csv file
    """
    # Config params
    connector_name: str = "kucoin"
    fake_pair = "SOL-USDT"
    tail_pct_threshold = [0.5, 1, 1.5, 2, 2.5, 3]

    all_pairs = ""
    status = "FETCH_TAILS"
    # status = "FETCH_ALL_PAIRS"
    is_fetch_all_pairs_called = False

    markets = {connector_name: {fake_pair}}

    counter = 0
    long_tails = pd.DataFrame([])
    limit = 0
    # if pairs_threshold = 0 all available pairs will be calculated
    pairs_threshold = 0

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    async def call_fetch_pairs(self):
        """
        Async method to get all trading pairs
        """
        self.all_pairs = await self.connector.all_trading_pairs()

    def get_ohlc(self, connector_name, trading_pair):
        """
        Helper method to choose which method to use
        """
        if connector_name == "binance":
            return self.get_ohlc_binance(trading_pair)
        if connector_name == "kucoin":
            return self.get_ohlc_kucoin(trading_pair)

    def get_ohlc_binance(self, trading_pair):
        """
        Fetches binance candle stick data and returns a list of dictionaries OHLC
        Structure:
        [{"open": 1.2, "high": 2.3, "low": 1, "close": 2.1}]
        """
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": trading_pair.replace("-", ""),
                  "interval": "5m", "limit": 1000}
        records = requests.get(url=url, params=params).json()
        return [{"open": Decimal(str(record[1])),
                 "high": Decimal(str(record[2])),
                 "low": Decimal(str(record[3])),
                 "close": Decimal(str(record[4]))} for record in records]

    def get_ohlc_kucoin(self, trading_pair):
        """
        Fetches kucoin candle stick data and returns a list of dictionaries OHLC
        Structure:
        [{"open": 1.2, "high": 2.3, "low": 1, "close": 2.1}]
        Checks for api rate limits and trys again in case of error
        """
        url = "https://api.kucoin.com/api/v1/market/candles"
        end_time = int(self.current_timestamp) - 10
        start_time = int(end_time - 1000 * 60 * 5)
        params = {"symbol": trading_pair,
                  "type": "5min",
                  "startAt": start_time,
                  "endAt": end_time}
        records = requests.get(url=url, params=params).json()
        if records['code'] == "200000":
            return [{"open": Decimal(str(record[1])),
                     "high": Decimal(str(record[3])),
                     "low": Decimal(str(record[4])),
                     "close": Decimal(str(record[2]))} for record in records['data']]
        elif records['code'] == "429000":
            self.log_with_clock(logging.INFO, f"Too many requests for {trading_pair}. Waiting...")
            time.sleep(2)
            return self.get_ohlc_kucoin(trading_pair)
        else:
            self.log_with_clock(logging.INFO, f"trading_pair = {trading_pair}, records = {records}")
            return False

    def on_tick(self):
        if self.status == "NOT_ACTIVE":
            return
        if self.status == "FETCH_ALL_PAIRS":
            self.fetch_all_trading_pairs()
            return
        if self.status == "FETCH_TAILS":
            self.all_pairs = pd.read_csv(os.path.join(data_path(), f"all_pairs_{self.connector_name}.csv"),
                                         names=["pair"]).pair.tolist()
            self.limit = self.pairs_threshold if self.pairs_threshold else len(self.all_pairs)
            self.status = "FETCH_TAILS_STARTED"

        if self.counter > self.limit - 1:
            self.finish_and_save_result()
            return

        self.process_pair(self.all_pairs[self.counter])

    def fetch_all_trading_pairs(self):
        """
        Get all trading pairs from the exchange and saves them to a csv file
        - Checks if the request to fetch trading pairs was sent
        - Waits while we got all trading pairs and saves them to a file
        - Set status to not active
        """
        if not self.is_fetch_all_pairs_called:
            safe_ensure_future(self.call_fetch_pairs())
            self.is_fetch_all_pairs_called = True
        self.log_with_clock(logging.INFO, f"all_pairs {self.all_pairs}")
        if self.all_pairs:
            csv_filename = f"all_pairs_{self.connector_name}.csv"
            csv_path = os.path.join(data_path(), csv_filename)
            csv_df = pd.DataFrame(self.all_pairs)
            csv_df.to_csv(csv_path, index=False)
            self.all_pairs = False
            self.log_with_clock(logging.INFO, f"Fetching all pairs finished")
            self.status = "NOT_ACTIVE"

    def process_pair(self, pair):
        """
        Fetches OHLC for a pair, calculates the number of tails that exceeds the threshold and saves to self.long_tails
        """
        self.log_with_clock(logging.INFO, f"Processing {self.counter + 1} / {self.limit} pair: {pair}")
        candles_df = self.get_ohlc(self.connector_name, pair)
        if not candles_df:
            self.counter += 1
            return
        candles_df = pd.DataFrame(candles_df)
        candles_df_up = candles_df[candles_df.close >= candles_df.open]
        candles_df_down = candles_df[candles_df.close < candles_df.open]
        candles_df_up = candles_df_up.assign(up_tail=100 * (candles_df_up['high'] - candles_df_up['close']) / candles_df_up['close'])
        candles_df_up = candles_df_up.assign(down_tail=100 * (candles_df_up['open'] - candles_df_up['low']) / candles_df_up['open'])
        candles_df_down = candles_df_down.assign(up_tail=100 * (candles_df_down['high'] - candles_df_down['open']) / candles_df_down['open'])
        candles_df_down = candles_df_down.assign(down_tail=100 * (candles_df_down['close'] - candles_df_down['low']) / candles_df_down['close'])

        candles_df = [candles_df_up, candles_df_down]
        candles_df = pd.concat(candles_df)
        candles_df = candles_df[['up_tail', 'down_tail']]
        df_max_tails = candles_df.max(axis=1)

        data_list = [pair]

        for threshold in self.tail_pct_threshold:
            df_max_tails = df_max_tails[df_max_tails >= threshold]
            data_list.append(df_max_tails.count())

        data_df = pd.DataFrame([data_list])
        self.long_tails = pd.concat([self.long_tails, data_df])
        self.counter += 1

    def finish_and_save_result(self):
        """
        Saves the tails dataframe to a csv file and sets the status to NOT_ACTIVE
        """
        path = os.path.join(data_path(), f"long_tails_{self.connector_name}.csv")
        self.long_tails.to_csv(path, index=False)
        self.log_with_clock(logging.INFO, f"Long tail calculation finished")
        self.log_with_clock(logging.INFO, f"long_tails df: {self.long_tails}")
        self.status = "NOT_ACTIVE"


