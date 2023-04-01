import logging
import os

import pandas as pd
import time
import requests

from hummingbot import data_path
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import Decimal, ScriptStrategyBase
from hummingbot.client.hummingbot_application import HummingbotApplication


class CandleSticksTails(ScriptStrategyBase):
    """
    This script has 2 functions
    - Fetch all trading pairs from the exchange with the help of hb all_trading_pairs method and saves them to csv file
    - Get 1000 5 min candles for each trading pair and saves maximum up and down tails number into a csv file
    """
    # Config params
    connector_name: str = "bittrex"
    fake_pair = "ETH-USDT"
    tail_pct_threshold = [0.5, 1, 1.5, 2, 2.5, 3]

    all_pairs = ""
    status = "FETCH_TAILS"
    # status = "FETCH_ALL_PAIRS"
    get_down_tails = True
    get_up_tails = False

    is_fetch_all_pairs_called = False

    markets = {connector_name: {fake_pair}}

    counter = 0
    trading_pair = ""
    long_tails = pd.DataFrame([])
    limit = 0

    # if pairs_threshold = 0 all available pairs will be calculated
    pairs_threshold = 0
    pairs_data = {}

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

    def get_ohlc(self):
        """
        Helper method to choose which method to use
        """
        if self.connector_name == "binance":
            return self.get_ohlc_binance()
        if self.connector_name == "kucoin":
            return self.get_ohlc_kucoin()
        if self.connector_name == "gate_io":
            return self.get_ohlc_gate_io()
        if self.connector_name == "kraken":
            return self.get_ohlc_kraken()
        if self.connector_name == "bittrex":
            return self.get_ohlc_bittrex()

    def get_pairs_data(self):
        """
        Helper method to choose which method to use
        """
        if self.connector_name == "binance":
            return self.get_pairs_data_binance()
        if self.connector_name == "kucoin":
            return self.get_pairs_data_kucoin()
        if self.connector_name == "gate_io":
            return self.get_pairs_data_gate_io()
        if self.connector_name == "kraken":
            return self.get_pairs_data_kraken()
        if self.connector_name == "bittrex":
            return self.get_pairs_data_bittrex()

    def get_pairs_data_kucoin(self):
        """
        Creates pairs_data dictionary in the following structure:
        'NEO-BTC': {'bid': '0.0005159', 
                    'ask': '0.0005169', 
                    'vol_base': '2809.305794', 
                    'vol_quote': '1.5022394780543', 
                    'volume_usd': '32453.6623934468'}
        """
        url = "https://api.kucoin.com/api/v1/market/allTickers"
        records = requests.get(url=url).json()
        if records["code"] != "200000":
            return None
        self.pairs_data = {row["symbol"]: {"bid": Decimal(row["buy"]),
                                           "ask": Decimal(row["sell"]),
                                           "vol_base": Decimal(row["vol"]),
                                           "vol_quote": Decimal(row["volValue"])}
                           for row in records["data"]["ticker"]}

    def get_pairs_data_binance(self):
        """
        Creates pairs_data dictionary in the following structure:
        'NEO-BTC': {'bid': '0.0005159',
                    'ask': '0.0005169',
                    'vol_base': '2809.305794',
                    'vol_quote': '1.5022394780543'}
        """
        translation = self.get_symbols_translation_binance()
        url = "https://api.binance.com/api/v3/ticker/24hr"
        records = requests.get(url=url).json()
        for row in records:
            if row["symbol"] in translation:
                pair = translation[row["symbol"]]
                self.pairs_data[pair] = {"bid": Decimal(row["bidPrice"]),
                                         "ask": Decimal(row["askPrice"]),
                                         "vol_base": Decimal(row["volume"]),
                                         "vol_quote": Decimal(row["quoteVolume"])}
            else:
                self.log_with_clock(logging.INFO,
                                    f"Symbol {row['symbol']} is not found in the binance translation dict")

    def get_symbols_translation_binance(self):
        """
        Returns the dictionary for translating pairs in Binnace format to hummingbot format
        Structure:
            {"BTCUSDT": "BTC-USDT", "ETHBTC": "ETH-BTC"}
        """
        url = "https://api.binance.com/api/v3/exchangeInfo"
        records = requests.get(url=url).json()
        symbols_translate = {row["symbol"]: f"{row['baseAsset']}-{row['quoteAsset']}" for row in records["symbols"]}

        return symbols_translate

    def get_pairs_data_gate_io(self):
        """
        Creates pairs_data dictionary in the following structure:
        'NEO-BTC': {'bid': '0.0005159',
                    'ask': '0.0005169',
                    'vol_base': '2809.305794',
                    'vol_quote': '1.5022394780543',
                    'volume_usd': '32453.6623934468'}
        """
        url = "https://api.gateio.ws/api/v4/spot/tickers"
        records = requests.get(url=url).json()
        for row in records:
            if all([row["highest_bid"], row["lowest_ask"], row["base_volume"], row["quote_volume"]]):
                pair = row["currency_pair"].replace("_", "-")
                self.pairs_data[pair] = {"bid": Decimal(row["highest_bid"]),
                                         "ask": Decimal(row["lowest_ask"]),
                                         "vol_base": Decimal(row["base_volume"]),
                                         "vol_quote": Decimal(row["quote_volume"])}

    def get_pairs_data_kraken(self):
        """
        Creates pairs_data dictionary in the following structure:
        'NEO-BTC': {'bid': '0.0005159',
                    'ask': '0.0005169',
                    'vol_base': '2809.305794',
                    'vol_quote': '1.5022394780543',
                    'volume_usd': '32453.6623934468'}
        """
        url = "https://api.kraken.com/0/public/Ticker"
        records = requests.get(url=url).json()
        if records["error"]:
            return None

        translation = self.get_symbols_translation_kraken()
        for symbol, row in records["result"].items():
            if symbol in translation:
                pair = translation[symbol]
                self.pairs_data[pair] = {"bid": Decimal(row["b"][0]),
                                         "ask": Decimal(row["a"][0]),
                                         "vol_base": Decimal(row["v"][1]),
                                         "vol_quote": Decimal(row["v"][1]) * Decimal(row["c"][0])}
            else:
                self.log_with_clock(logging.INFO, f"Symbol {symbol} is not found in the kraken translation dict")

    def get_symbols_translation_kraken(self):
        """
        Returns the dictionary for translating pairs in Kraken format to hummingbot format
        Structure:
            {"BTCUSDT": "BTC-USDT", "ETHBTC": "ETH-BTC"}
        """
        url = "https://api.kraken.com/0/public/AssetPairs"
        records = requests.get(url=url).json()
        symbols_translate = {symbol: f"{row['base']}-{row['quote']}" for symbol, row in records["result"].items()}
        self.log_with_clock(logging.INFO, f"symbols_translate = {symbols_translate}")
        return symbols_translate

    def get_pairs_data_bittrex(self):
        url_ticker = "https://api.bittrex.com/v3/markets/tickers"
        records_ticker = requests.get(url=url_ticker).json()
        url_summary = "https://api.bittrex.com/v3/markets/summaries"
        records_volumes = requests.get(url=url_summary).json()

        data_bid_asks = {row["symbol"]: {"bid": Decimal(row["bidRate"]),
                                         "ask": Decimal(row["askRate"])}
                         for row in records_ticker}

        data_volume = {row["symbol"]: {"vol_base": Decimal(row["volume"]),
                                       "vol_quote": Decimal(row["quoteVolume"])}
                       for row in records_volumes}
        for symbol in data_bid_asks:
            if symbol in data_volume:
                self.pairs_data[symbol] = {**data_bid_asks[symbol], **data_volume[symbol]}

    def get_ohlc_binance(self):
        """
        Fetches binance candle stick data and returns a list of dictionaries OHLC
        Structure:
        [{"open": 1.2, "high": 2.3, "low": 1, "close": 2.1}]
        """
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": self.trading_pair.replace("-", ""),
                  "interval": "5m", "limit": 1000}
        records = requests.get(url=url, params=params).json()
        return [{"open": Decimal(str(record[1])),
                 "high": Decimal(str(record[2])),
                 "low": Decimal(str(record[3])),
                 "close": Decimal(str(record[4]))} for record in records]

    def get_ohlc_kucoin(self):
        """
        Fetches kucoin candle stick data and returns a list of dictionaries OHLC
        Structure:
        [{"open": 1.2, "high": 2.3, "low": 1, "close": 2.1}]
        Checks for api rate limits and trys again in case of error
        """
        url = "https://api.kucoin.com/api/v1/market/candles"
        end_time = int(self.current_timestamp) - 10
        start_time = int(end_time - 1000 * 60 * 5)
        params = {"symbol": self.trading_pair,
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
            self.log_with_clock(logging.INFO, f"Too many requests for {self.trading_pair}. Waiting...")
            time.sleep(2)
            return self.get_ohlc_kucoin(self.trading_pair)
        else:
            self.log_with_clock(logging.INFO, f"trading_pair = {self.trading_pair}, records = {records}")
            return False

    def get_ohlc_gate_io(self):
        """
        Fetches gate_io candle stick data and returns a list of dictionaries OHLC
        Structure:
        [{"open": 1.2, "high": 2.3, "low": 1, "close": 2.1}]
        """
        url = "https://api.gateio.ws/api/v4/spot/candlesticks"
        params = {"currency_pair": self.trading_pair.replace("-", "_"),
                  "interval": "5m", "limit": 1000}
        records = requests.get(url=url, params=params).json()
        return [{"open": Decimal(str(record[5])),
                 "high": Decimal(str(record[3])),
                 "low": Decimal(str(record[4])),
                 "close": Decimal(str(record[2]))} for record in records]

    def get_ohlc_kraken(self):
        """
        Fetches kraken candle stick data and returns a list of dictionaries OHLC
        Structure:
        [{"open": 1.2, "high": 2.3, "low": 1, "close": 2.1}]
        """
        url = "https://api.kraken.com/0/public/OHLC"
        params = {"pair": self.trading_pair.replace("-", ""),
                  "interval": "5"}
        records = requests.get(url=url, params=params).json()
        data = []

        for row in records["result"].values():
            if isinstance(row, int):
                continue
            for record in row:
                try:
                    data.append({"open": Decimal(str(record[1])),
                                 "high": Decimal(str(record[2])),
                                 "low": Decimal(str(record[3])),
                                 "close": Decimal(str(record[4]))})
                except Exception as e:
                    self.log_with_clock(logging.INFO, f"Record {record} can't be written")
                    continue

        return data

    def get_ohlc_bittrex(self):
        url_symbol = self.trading_pair
        interval = "MINUTE_1"
        url = f"https://api.bittrex.com/v3/markets/{url_symbol}/candles/{interval}/recent"
        records = requests.get(url=url).json()
        return [{"open": Decimal(str(record["open"])),
                 "high": Decimal(str(record["high"])),
                 "low": Decimal(str(record["low"])),
                 "close": Decimal(str(record["close"]))} for record in records]

    def get_volume_usd(self):
        """
        adds volume_usd to the pairs_data dictionary
        'volume_usd': '32453.6623934468'
        """
        for pair, data in self.pairs_data.items():
            base, quote = split_hb_trading_pair(pair)
            if "USD" in quote:
                volume_usd = data["vol_quote"]
            elif "USD" in base:
                volume_usd = data["vol_base"]
            else:
                conversion_pair = f"{base}-USDT"
                conversion_pair_quote = f"{quote}-USDT"
                if conversion_pair in self.pairs_data:
                    conversion_price = self.pairs_data[conversion_pair]["bid"]
                    volume_usd = data["vol_base"] * conversion_price
                elif conversion_pair_quote in self.pairs_data:
                    conversion_price = self.pairs_data[conversion_pair_quote]["bid"]
                    volume_usd = data["vol_quote"] * conversion_price
                else:
                    self.log_with_clock(logging.INFO, f"Can't find price for calculating volume for {pair}")
                    continue
            self.pairs_data[pair]["volume_usd"] = round(volume_usd)

    def on_tick(self):
        if self.status == "NOT_ACTIVE":
            return
        if self.status == "FETCH_ALL_PAIRS":
            self.fetch_all_trading_pairs()
            return
        if self.status == "FETCH_TAILS":
            self.log_with_clock(logging.INFO, f"Starting fetching ticker data")
            self.get_pairs_data()
            self.get_volume_usd()
            # self.log_with_clock(logging.INFO, f"self.pairs_data = {self.pairs_data}")
            # HummingbotApplication.main_application().stop()
            self.all_pairs = pd.read_csv(os.path.join(data_path(), f"all_pairs_{self.connector_name}.csv"),
                                         names=["pair"]).pair.tolist()
            self.limit = self.pairs_threshold if self.pairs_threshold else len(self.all_pairs)
            self.status = "FETCH_TAILS_STARTED"

        if self.counter > self.limit - 1:
            self.finish_and_save_result()
        else:
            self.trading_pair = self.all_pairs[self.counter]
            self.process_pair()
            self.counter += 1

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
            self.log_with_clock(logging.INFO, f"The number of all pairs is {len(self.all_pairs)}")
            csv_filename = f"all_pairs_{self.connector_name}.csv"
            csv_path = os.path.join(data_path(), csv_filename)
            csv_df = pd.DataFrame(self.all_pairs)
            csv_df.to_csv(csv_path, index=False, header=False)
            self.all_pairs = False
            self.log_with_clock(logging.INFO, f"Fetching all pairs finished")
            self.status = "NOT_ACTIVE"
            HummingbotApplication.main_application().stop()

    def process_pair(self):
        """
        Fetches OHLC for a pair, calculates the number of tails that exceeds the threshold and saves to self.long_tails
        """
        self.log_with_clock(logging.INFO, f"Processing {self.counter + 1} / {self.limit} pair: {self.trading_pair}")
        candles_df = self.get_ohlc()
        # self.log_with_clock(logging.INFO, f"candles_df = {candles_df}")
        self.log_with_clock(logging.INFO, f"candles_df size = {len(candles_df)}")
        # HummingbotApplication.main_application().stop()
        if not candles_df:
            return
        candles_df = pd.DataFrame(candles_df)
        candles_df_up = candles_df[candles_df.close >= candles_df.open]
        candles_df_down = candles_df[candles_df.close < candles_df.open]
        if self.get_up_tails:
            candles_df_up = candles_df_up.assign(
                up_tail=100 * (candles_df_up['high'] - candles_df_up['close']) / candles_df_up['close'])
            candles_df_down = candles_df_down.assign(
                up_tail=100 * (candles_df_down['high'] - candles_df_down['open']) / candles_df_down['open'])
        if self.get_down_tails:
            candles_df_up = candles_df_up.assign(
                down_tail=100 * (candles_df_up['open'] - candles_df_up['low']) / candles_df_up['open'])
            candles_df_down = candles_df_down.assign(
                down_tail=100 * (candles_df_down['close'] - candles_df_down['low']) / candles_df_down['close'])

        candles_df = pd.concat([candles_df_up, candles_df_down])
        if self.get_down_tails and self.get_up_tails:
            candles_df = candles_df[['up_tail', 'down_tail']]
            df_max_tails = candles_df.max(axis=1)
        elif self.get_down_tails:
            df_max_tails = candles_df['down_tail']
        elif self.get_up_tails:
            df_max_tails = candles_df['up_tail']

        total_volume_usd = self.pairs_data[self.trading_pair][
            "volume_usd"] if self.trading_pair in self.pairs_data else Decimal("0")
        data_list = [self.trading_pair, total_volume_usd]

        for threshold in self.tail_pct_threshold:
            df_max_tails = df_max_tails[df_max_tails >= threshold]
            data_list.append(df_max_tails.count())

        data_df = pd.DataFrame([data_list], columns=["pair", "volume_usd", "0_5", "1", "1_5", "2", "2_5", "3"])
        self.long_tails = pd.concat([self.long_tails, data_df])

    def finish_and_save_result(self):
        """
        Saves the tails dataframe to a csv file and sets the status to NOT_ACTIVE
        """
        path = os.path.join(data_path(), f"long_tails_{self.connector_name}.csv")
        self.log_with_clock(logging.INFO, f"path: {path}")
        self.long_tails.to_csv(path, index=False)
        self.log_with_clock(logging.INFO, f"Long tail calculation finished")
        self.log_with_clock(logging.INFO, f"long_tails df: {self.long_tails}")
        self.status = "NOT_ACTIVE"
        HummingbotApplication.main_application().stop()
