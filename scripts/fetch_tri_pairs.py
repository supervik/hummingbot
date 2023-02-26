import logging

import pandas as pd
import requests

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
    min_profitability = 2
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
        return True


        # except Exception as e:
        #     self.log_with_clock(logging.INFO, f"Error in getting data. record= {record}, Exception: {e}")
        #
        #     return False
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
        if records:
            for row in records:
                base, quote = split_hb_trading_pair(row["symbol"])
                if quote not in quoted_pairs:
                    quoted_pairs[quote] = [base]
                else:
                    quoted_pairs[quote].append(base)
        # self.log_with_clock(logging.INFO, f"Quoted pairs: {quoted_pairs}")
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
        self.create_triangles()

        self.create_pairs_data()

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

                # direct
                self.get_profitability(pair_1, pair_2, cross_pair)
                # reverse
                self.get_profitability(pair_2, pair_1, cross_pair)

    def get_profitability(self, maker_pair, taker_pair_1, taker_pair_2):
        last_price = self.pairs_data[maker_pair]['last']
        taker_1_bid = self.pairs_data[taker_pair_1]['bid']
        taker_1_ask = self.pairs_data[taker_pair_1]['ask']
        taker_2_bid = self.pairs_data[taker_pair_2]['bid']
        taker_2_ask = self.pairs_data[taker_pair_2]['ask']
        taker_1_base, taker_1_quote = split_hb_trading_pair(taker_pair_1)
        taker_2_base, taker_2_quote = split_hb_trading_pair(taker_pair_2)

        if last_price != self.pairs_data[maker_pair]['last_prev']:
            self.pairs_data[maker_pair]['last_prev'] = last_price

            if self.current_timestamp > self.pairs_data[maker_pair]['bid_timestamp']:
                maker_buy_price = last_price
                taker_sell_price = taker_1_bid / taker_2_ask if taker_1_quote == taker_2_quote else taker_1_bid * taker_2_bid
                buy_profitability = 100 * (taker_sell_price / maker_buy_price - 1)
                if maker_pair == "XMR-ETH" and taker_pair_1 == "XMR-USDT":
                    self.log_with_clock(logging.INFO, f"BUY profitability for {maker_pair}, {taker_pair_1} = {buy_profitability}%")

                if buy_profitability > Decimal(self.min_profitability):
                    msg = f"Buy profitability for {maker_pair}, {taker_pair_1} is {buy_profitability}%"
                    self.log_with_clock(logging.INFO, msg)
                    self.notify_hb_app_with_timestamp(msg)
                    self.pairs_data[maker_pair]['bid_timestamp'] = self.current_timestamp + self.check_delay

            if self.current_timestamp > self.pairs_data[maker_pair]['ask_timestamp']:
                maker_sell_price = last_price
                taker_buy_price = taker_1_ask / taker_2_bid if taker_1_quote == taker_2_quote else taker_1_ask * taker_2_ask
                sell_profitability = 100 * (maker_sell_price / taker_buy_price - 1)
                if maker_pair == "XMR-ETH" and taker_pair_1 == "XMR-USDT":
                    self.log_with_clock(logging.INFO, f"SELL profitability for {maker_pair}, {taker_pair_1} = {sell_profitability}%")

                if sell_profitability > Decimal(self.min_profitability):
                    msg = f"Sell profitability for {maker_pair}, {taker_pair_1} is {sell_profitability}%"
                    self.log_with_clock(logging.INFO, msg)
                    self.notify_hb_app_with_timestamp(msg)
                    self.pairs_data[maker_pair]['ask_timestamp'] = self.current_timestamp + self.check_delay

    #
    #
    #
    # taker_2 = self.taker_pair_2
    #     taker_2_bid = prices_updated[taker_2]['bid']
    #     taker_2_ask = prices_updated[taker_2]['ask']
    #
    #     for num, market in enumerate(self.follow_markets):
    #         maker_pair = market['maker']
    #         taker_pair = market['taker']
    #
    #         last_price = prices_updated[maker_pair]['last']
    #         taker_1_bid = prices_updated[taker_pair]['bid']
    #         taker_1_ask = prices_updated[taker_pair]['ask']
    #
    #         # self.log_with_clock(logging.INFO, f"market = {market}, last_price = {last_price}, "
    #         #                                   f"taker_1_bid = {taker_1_bid}, taker_1_ask = {taker_1_ask}, "
    #         #                                   f"taker_2_bid = {taker_2_bid}, taker_2_ask = {taker_2_ask}")
    #         if last_price != market['last']:
    #             self.follow_markets[num]['last'] = last_price
    #
    #             if self.current_timestamp > market['bid_timestamp']:
    #                 maker_buy_price = last_price
    #                 taker_sell_price = taker_1_bid / taker_2_ask
    #                 buy_profitability = 100 * (taker_sell_price / maker_buy_price - 1)
    #                 self.log_with_clock(logging.INFO, f"BUY profitability for {maker_pair} = {buy_profitability}%")
    #
    #                 if buy_profitability > Decimal(self.min_profitability):
    #                     msg = f"Buy profitability for {market['maker']} is {buy_profitability}%"
    #                     self.log_with_clock(logging.INFO, msg)
    #                     self.notify_hb_app_with_timestamp(msg)
    #                     self.follow_markets[num]['bid_timestamp'] = self.current_timestamp + self.check_delay
    #
    #             if self.current_timestamp > market['ask_timestamp']:
    #                 maker_sell_price = last_price
    #                 taker_buy_price = taker_1_ask / taker_2_bid
    #                 sell_profitability = 100 * (maker_sell_price / taker_buy_price - 1)
    #                 self.log_with_clock(logging.INFO, f"SELL profitability for {maker_pair} = {sell_profitability}%")
    #
    #                 if sell_profitability > Decimal(self.min_profitability):
    #                     msg = f"Sell profitability for {market['maker']} is {sell_profitability}%"
    #                     self.log_with_clock(logging.INFO, msg)
    #                     self.notify_hb_app_with_timestamp(msg)
    #                     self.follow_markets[num]['ask_timestamp'] = self.current_timestamp + self.check_delay


            # self.log_with_clock(logging.INFO, f"market = {markt}, bid = {maker_bid}, ask = {maker_ask}")

    #     for trading_pair in self.pairs_data:
    #         if self.current_timestamp > trading_pair['bid_timestamp']:
    #             bid_profitability = self.calculate_profitability(pair=trading_pair, is_bid=True)
    #             if bid_profitability > Decimal(self.min_profitability):
    #                 msg = f"Bid profitability for {trading_pair['maker']} is {bid_profitability}%"
    #                 self.log_with_clock(logging.INFO, msg)
    #                 self.notify_hb_app_with_timestamp(msg)
    #                 trading_pair['bid_timestamp'] = self.current_timestamp + self.check_delay
    #
    #         if self.current_timestamp > trading_pair['ask_timestamp']:
    #             ask_profitability = self.calculate_profitability(pair=trading_pair, is_bid=False)
    #             if ask_profitability > Decimal(self.min_profitability):
    #                 msg = f"Ask profitability for {trading_pair['maker']} is {ask_profitability}%"
    #                 self.log_with_clock(logging.INFO, msg)
    #                 self.notify_hb_app_with_timestamp(msg)
    #                 trading_pair['ask_timestamp'] = self.current_timestamp + self.check_delay
    #
    # def calculate_profitability(self, pair, is_bid):
    #     order_amount = self.order_amount_quote / self.connector.get_mid_price(pair['maker'])
    #     best_maker_price = self.connector.get_price(pair['maker'], not is_bid)
    #     taker_price = self.calculate_taker_price(is_bid, order_amount, pair['taker'], self.taker_pair_2)
    #     result = taker_price / best_maker_price if is_bid else best_maker_price / taker_price
    #     profitability = 100 * (result - 1)
    #
    #     return profitability
    #
    # def calculate_taker_price(self, is_bid, order_amount, taker_pair_1, taker_pair_2):
    #     side_taker_1 = not is_bid
    #     exchanged_amount_1 = self.connector.get_quote_volume_for_base_amount(taker_pair_1, side_taker_1,
    #                                                                          order_amount).result_volume
    #     if not self.quote_assets_reverse:
    #         side_taker_2 = not side_taker_1
    #         exchanged_amount_2 = self.get_base_amount_for_quote_volume(taker_pair_2, side_taker_2,
    #                                                                    exchanged_amount_1)
    #     else:
    #         side_taker_2 = side_taker_1
    #         exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(taker_pair_2, side_taker_2,
    #                                                                              exchanged_amount_1).result_volume
    #     final_price = exchanged_amount_2 / order_amount
    #     return final_price
    #
    # def get_base_amount_for_quote_volume(self, pair, side, quote_volume) -> Decimal:
    #     """
    #     Calculates base amount that you get for the quote volume using the orderbook entries
    #     """
    #     orderbook = self.connector.get_order_book(pair)
    #     orderbook_entries = orderbook.ask_entries() if side else orderbook.bid_entries()
    #
    #     cumulative_volume = 0.
    #     cumulative_base_amount = 0.
    #     quote_volume = float(quote_volume)
    #
    #     for order_book_row in orderbook_entries:
    #         row_amount = order_book_row.amount
    #         row_price = order_book_row.price
    #         row_volume = row_amount * row_price
    #         if row_volume + cumulative_volume >= quote_volume:
    #             row_volume = quote_volume - cumulative_volume
    #             row_amount = row_volume / row_price
    #         cumulative_volume += row_volume
    #         cumulative_base_amount += row_amount
    #         if cumulative_volume >= quote_volume:
    #             break
    #
    #     return Decimal(cumulative_base_amount)

