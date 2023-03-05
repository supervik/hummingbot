import logging
import os

import pandas as pd
import time
import pandas_ta as ta
import requests
from hummingbot.connector.connector_base import ConnectorBase, Dict

from hummingbot import data_path
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class CandleSticksTails(ScriptStrategyBase):
    # Config params
    connector_name: str = "binance"
    fake_pair = "SOL-USDT"
    tail_pct_threshold = 1

    fetch_all_trading_pairs = False
    is_fetch_all_pairs_called = False

    all_pairs = ""
    status = "ACTIVE"

    markets = {connector_name: {fake_pair}}

    counter = 0
    long_tails = {}
    # if pairs_threshold = 0 all available pairs will be calculated
    pairs_threshold = 0

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    async def call_fetch_pairs(self):
        self.all_pairs = await self.connector.all_trading_pairs()

    def get_ohlc(self, connector_name, trading_pair):
        if connector_name == "binance":
            return self.get_ohlc_binance(trading_pair)
        if connector_name == "kucoin":
            return self.get_ohlc_kucoin(trading_pair)

    def get_ohlc_binance(self, trading_pair):
        """
        Fetches binance candle stick data and returns a dict OHLC
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
        Fetches binance candle stick data and returns a dict OHLC
        """
        url = "https://api.kucoin.com/api/v1/market/candles"
        end_time = int(self.current_timestamp) - 10
        start_time = int(end_time - 1000 * 60 * 5)
        # self.log_with_clock(logging.INFO, f"start_time = {start_time}, end_time = {end_time}")
        params = {"symbol": trading_pair,
                  "type": "5min",
                  "startAt": start_time,
                  "endAt": end_time}
        records = requests.get(url=url, params=params).json()
        if records['code'] == "200000":
            # self.log_with_clock(logging.INFO, f"trading_pair = {trading_pair}, records = {records}")
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

        if self.fetch_all_trading_pairs:
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
            return

        filename = f"all_pairs_{self.connector_name}.csv"
        path = os.path.join(data_path(), filename)
        df = pd.read_csv(path, names=["pair"])
        all_trading_pairs = df.pair.tolist()

        limit = self.pairs_threshold if self.pairs_threshold else len(all_trading_pairs)

        if self.counter > limit - 1:
            self.log_with_clock(logging.INFO, f"Long tail calculation finished")
            long_tails_all_sorted = dict(sorted(self.long_tails.items(), key=lambda item: item[1]))

            filename = f"long_tails_{self.connector_name}.csv"
            path = os.path.join(data_path(), filename)
            df = pd.DataFrame(long_tails_all_sorted.items(), columns=['Pair', 'Tails_number'])
            df.to_csv(path, index=False)
            self.log_with_clock(logging.INFO, f"long_tails_all_sorted {long_tails_all_sorted}")
            self.status = "NOT_ACTIVE"
            return

        pair = all_trading_pairs[self.counter]
        self.log_with_clock(logging.INFO, f"Processing {self.counter + 1} / {limit} pair: {pair}")
        candles_df = self.get_ohlc(self.connector_name, pair)
        if not candles_df:
            self.counter += 1
            return
        candles_df = pd.DataFrame(candles_df)
        # self.log_with_clock(logging.INFO, f"candles_df = {candles_df}")
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
        df_max_tails = df_max_tails[df_max_tails >= self.tail_pct_threshold]
        long_tails = df_max_tails.count()

        self.long_tails[pair] = long_tails
        self.counter += 1


