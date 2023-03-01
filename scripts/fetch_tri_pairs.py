import logging
import os

import pandas as pd
import requests

from hummingbot import data_path
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class FetchTriPairs(ScriptStrategyBase):
    # Config params
    connector_name: str = "kucoin"
    trading_pair = "XMR-ETH"
    quote_assets_reverse = False
    url = "https://api.kucoin.com/api/v1/market/allTickers"

    taker_pair_2: str = "ETH-USDT"
    volume_threshold_usd = 20000

    follow_markets = [{"maker": "XMR-ETH", "taker": "XMR-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "SDAO-ETH", "taker": "SDAO-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "XRP-ETH", "taker": "XRP-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "TRX-ETH", "taker": "TRX-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "FTM-ETH", "taker": "FTM-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "ALGO-ETH", "taker": "ALGO-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "KCS-ETH", "taker": "KCS-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "AGIX-ETH", "taker": "AGIX-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "OCEAN-ETH", "taker": "OCEAN-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "ALICE-ETH", "taker": "ALICE-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0}
                      ]
    # follow_markets = [{"maker": "XMR-ETH", "taker": "XMR-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "AGIX-ETH", "taker": "AGIX-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0}
    #                   ]
    min_profitability = 1
    check_delay = 60

    status = "NOT_INIT"
    triangles = {}
    pairs_data = {}

    markets = {connector_name: {trading_pair}}


    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    def get_api_data(self):
        """
        Fetches data and returns a list daily close
        This is the API response data structure:
        {
            "time":1602832092060,
            "ticker":[
                {
                    "symbol": "BTC-USDT",   // symbol
                    "symbolName":"BTC-USDT", // Name of trading pairs, it would change after renaming
                    "buy": "11328.9",   // bestAsk
                    "sell": "11329",    // bestBid
                    "changeRate": "-0.0055",    // 24h change rate
                    "changePrice": "-63.6", // 24h change price
                    "high": "11610",    // 24h highest price
                    "low": "11200", // 24h lowest price
                    "vol": "2282.70993217", // 24h volume，the aggregated trading volume in BTC
                    "volValue": "25984946.157790431",   // 24h total, the trading volume in quote currency of last 24 hours
                    "last": "11328.9",  // last price
                    "averagePrice": "11360.66065903",   // 24h average transaction price yesterday
                    "takerFeeRate": "0.001",    // Basic Taker Fee
                    "makerFeeRate": "0.001",    // Basic Maker Fee
                    "takerCoefficient": "1",    // Taker Fee Coefficient
                    "makerCoefficient": "1" // Maker Fee Coefficient
                }
            ]
        }
        returns ticker list
        """
        records = requests.get(url=self.url).json()
        if records["code"] != "200000":
            return None
        return records["data"]["ticker"]

    def get_pairs_data(self):
        """"
        returns dictionary in the structure:
        {
            "XMR-USDT": {"bid": 161.3, "ask": 165.4, "last": 163.8},
            "AGIX-USDT": {"bid": 0.391, "ask": 0.423, "last": 0.401}
        }
        """
        records = self.get_api_data()
        if not records:
            return False
        # try:
        for record in records:
            pair = record["symbol"]
            if record["buy"] and record["sell"] and record["last"]:
                if pair not in self.pairs_data:
                    self.pairs_data[pair] = {}

                self.pairs_data[pair]["bid"] = Decimal(str(record["buy"]))
                self.pairs_data[pair]["ask"] = Decimal(str(record["sell"]))
                self.pairs_data[pair]["last"] = Decimal(str(record["last"]))
                # if self.status == "NOT_INIT" and record["vol"] and record["volValue"]:
                #     self.pairs_data[pair]["vol_base"] = Decimal(str(record["vol"]))
                #     self.pairs_data[pair]["vol_quote"] = Decimal(str(record["volValue"]))
        return True

    def create_quoted_pairs(self):
        """
        creates dictionary or quoted pairs with the structure:
        {
            "ETH": ["ALGO", "ADA", "XMR"],
            "BTC": ["ADA", "TRX", "ETH"],
        }
        """
        records = self.get_api_data()
        quoted_pairs = {}
        total_volume = Decimal("0")
        if records:
            for row in records:
                base, quote = split_hb_trading_pair(row["symbol"])
                vol_quote = Decimal(row["volValue"])
                vol_base = Decimal(row["vol"])
                if "USD" in quote:
                    total_volume = vol_quote
                elif "USD" in base:
                    total_volume = vol_base
                else:
                    conversion_pair = f"{base}-USDT"
                    conversion_pair_quote = f"{quote}-USDT"
                    if conversion_pair in self.pairs_data:
                        conversion_price = self.pairs_data[conversion_pair]["bid"]
                        total_volume = vol_base * conversion_price
                    elif conversion_pair_quote in self.pairs_data:
                        conversion_price = self.pairs_data[conversion_pair_quote]["bid"]
                        total_volume = vol_quote * conversion_price
                    else:
                        self.log_with_clock(logging.INFO, f"Can't find price for calculating volume for {row['symbol']}")
                if Decimal(total_volume) > self.volume_threshold_usd:
                    if quote not in quoted_pairs:
                        quoted_pairs[quote] = [base]
                    else:
                        quoted_pairs[quote].append(base)
        self.log_with_clock(logging.INFO, f"Quoted pairs: {quoted_pairs}")
        return quoted_pairs

    def create_triangles(self):
        """
        Creates all avaialble triangles on the market in the structure:
        {
            "ETH-USDT": ["ADA", "ALGO", "TRX"],
            "BTC-USDT": ["XEM", "XRP"]
        }
        """
        quoted_pairs = self.create_quoted_pairs()
        quoted_pairs_copy = dict(quoted_pairs)
        for quote_asset, base_assets in quoted_pairs.items():
            # self.log_with_clock(logging.INFO, f"Iterating. New row: {quote_asset}: {base_assets}")
            quoted_pairs_copy.pop(quote_asset, None)
            if quoted_pairs_copy:
                for base_asset in base_assets:
                    for quote_asset_next, base_assets_next in quoted_pairs_copy.items():
                        if base_asset in base_assets_next:
                            if quote_asset in base_assets_next:
                                main_pair = f"{quote_asset}-{quote_asset_next}"
                            elif quote_asset_next in base_assets:
                                main_pair = f"{quote_asset_next}-{quote_asset}"
                            else:
                                self.log_with_clock(logging.INFO, f"No pair exist for {quote_asset} and {quote_asset_next}")
                                continue
                            if main_pair in self.triangles:
                                self.triangles[main_pair].append(base_asset)
                            else:
                                self.triangles[main_pair] = [base_asset]
        self.log_with_clock(logging.INFO, f"self.triangles {self.triangles}")
        pairs_number = sum(len(pairs) for pairs in self.triangles)
        self.log_with_clock(logging.INFO, f"Triangles number {pairs_number}")
        return None

    def create_pairs_data(self):
        self.get_pairs_data()
        for pair in self.pairs_data:
            self.pairs_data[pair]["last_prev"] = self.pairs_data[pair]["last"]
            self.pairs_data[pair]["bid_timestamp"] = 0
            self.pairs_data[pair]["ask_timestamp"] = 0
        # self.log_with_clock(logging.INFO, f"pairs_data {self.pairs_data}")
        self.log_with_clock(logging.INFO, f"pairs_data length {len(self.pairs_data)}")

    def strategy_init(self):
        self.create_pairs_data()
        self.create_triangles()

    def on_tick(self):
        if self.status == "NOT_INIT":
            self.strategy_init()
            self.status = "READY"
        # self.log_with_clock(logging.INFO, "New tick")

        if not self.get_pairs_data():
            return

        for cross_pair, base_assets in self.triangles.items():
            quote_1, quote_2 = split_hb_trading_pair(cross_pair)
            for base_asset in base_assets:
                pair_1 = f"{base_asset}-{quote_1}"
                pair_2 = f"{base_asset}-{quote_2}"

                # direct triangle pair_1, pair_2, cross_pair
                if self.pairs_data[pair_1]['last'] != self.pairs_data[pair_1]['last_prev']:
                    self.pairs_data[pair_1]['last_prev'] = self.pairs_data[pair_1]['last']
                    self.get_profitability(True, True, pair_1, pair_2, cross_pair)
                    self.get_profitability(False, True, pair_1, pair_2, cross_pair)
                # reverse triangle pair_2, pair_1, cross_pair
                if self.pairs_data[pair_2]['last'] != self.pairs_data[pair_2]['last_prev']:
                    self.pairs_data[pair_2]['last_prev'] = self.pairs_data[pair_2]['last']
                    self.get_profitability(True, False, pair_2, pair_1, cross_pair)
                    self.get_profitability(False, False, pair_2, pair_1, cross_pair)

    def get_profitability(self, is_bid, is_taker_quotes_equal, maker_pair, taker_pair_1, taker_pair_2):
        last_price = self.pairs_data[maker_pair]['last']
        taker_1_bid = self.pairs_data[taker_pair_1]['bid']
        taker_1_ask = self.pairs_data[taker_pair_1]['ask']
        taker_2_bid = self.pairs_data[taker_pair_2]['bid']
        taker_2_ask = self.pairs_data[taker_pair_2]['ask']

        timestamp = "bid_timestamp" if is_bid else "ask_timestamp"
        if self.current_timestamp > self.pairs_data[maker_pair][timestamp]:
            maker_price = last_price
            if is_taker_quotes_equal:
                taker_price = taker_1_bid / taker_2_ask if is_bid else taker_1_ask / taker_2_bid
            else:
                taker_price = taker_1_bid * taker_2_bid if is_bid else taker_1_ask * taker_2_ask
            result = taker_price / maker_price if is_bid else maker_price / taker_price
            profitability = round(100 * (result - 1), 2)

            if profitability > Decimal(self.min_profitability):
                side = "BUY" if is_bid else "SELL"
                msg = f"{side} Profitability for {maker_pair}, {taker_pair_1} is {profitability}%"
                data_to_save = [self.current_timestamp, f"{maker_pair} {taker_pair_1}", side, profitability,
                                last_price, taker_1_bid, taker_1_ask, taker_2_bid, taker_2_ask]
                self.create_and_save_to_file_dataframe(data_to_save)
                self.log_with_clock(logging.INFO, msg)
                self.notify_hb_app_with_timestamp(msg)
                self.pairs_data[maker_pair][timestamp] = self.current_timestamp + self.check_delay
        #     maker_buy_price = last_price
        #     taker_sell_price = taker_1_bid / taker_2_ask if taker_1_quote == taker_2_quote else taker_1_bid * taker_2_bid
        #     buy_profitability = round(100 * (taker_sell_price / maker_buy_price - 1), 2)
        #     # self.log_with_clock(logging.INFO, f"BUY profitability for {maker_pair}, {taker_pair_1} = {buy_profitability}%")
        #
        #     if buy_profitability > Decimal(self.min_profitability):
        #         msg = f"Buy profitability for {maker_pair}, {taker_pair_1} is {buy_profitability}%"
        #         data_to_save = [self.current_timestamp, f"{maker_pair} {taker_pair_1}", "BUY", buy_profitability,
        #                         last_price, taker_1_bid, taker_1_ask, taker_2_bid, taker_2_ask]
        #         self.create_and_save_to_file_dataframe(data_to_save)
        #         self.log_with_clock(logging.INFO, msg)
        #         self.notify_hb_app_with_timestamp(msg)
        #         self.pairs_data[maker_pair]['bid_timestamp'] = self.current_timestamp + self.check_delay
        #
        # if self.current_timestamp > self.pairs_data[maker_pair]['ask_timestamp']:
        #     maker_sell_price = last_price
        #     taker_buy_price = taker_1_ask / taker_2_bid if taker_1_quote == taker_2_quote else taker_1_ask * taker_2_ask
        #     sell_profitability = round(100 * (maker_sell_price / taker_buy_price - 1), 2)
        #     # self.log_with_clock(logging.INFO, f"SELL profitability for {maker_pair}, {taker_pair_1} = {sell_profitability}%")
        #
        #     if sell_profitability > Decimal(self.min_profitability):
        #         msg = f"Sell profitability for {maker_pair}, {taker_pair_1} is {sell_profitability}%"
        #         data_to_save = [self.current_timestamp, f"{maker_pair} {taker_pair_1}", "SELL", sell_profitability,
        #                         last_price, taker_1_bid, taker_1_ask, taker_2_bid, taker_2_ask]
        #         self.create_and_save_to_file_dataframe(data_to_save)
        #         self.log_with_clock(logging.INFO, msg)
        #         self.notify_hb_app_with_timestamp(msg)
        #         self.pairs_data[maker_pair]['ask_timestamp'] = self.current_timestamp + self.check_delay

    def create_and_save_to_file_dataframe(self, data_list):
        columns_headers = ["Time", "Pairs", "Side", "Profitability", "maker_last_price",
                           "taker_1_bid", "taker_1_ask", "taker_2_bid", "taker_2_ask"]
        data_df = pd.DataFrame([data_list], columns=columns_headers)

        # Save to file separately for each trading pair
        self.export_to_csv(data_df, columns_headers)

    def export_to_csv(self, data_df, columns_headers):
        """
        Appends new data to a csv file, separate for each trading pair.
        """
        csv_filename = f"data_fetch_tri_pairs_{self.connector_name}.csv"
        csv_path = os.path.join(data_path(), csv_filename)
        csv_df = data_df

        add_header = False if os.path.exists(csv_path) else True
        csv_df.to_csv(csv_path, mode='a', header=add_header, index=False, columns=columns_headers)

