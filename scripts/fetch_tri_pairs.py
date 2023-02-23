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

    # follow_markets = [{"maker": "XMR-ETH", "taker": "XMR-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "SDAO-ETH", "taker": "SDAO-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "XRP-ETH", "taker": "XRP-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "TRX-ETH", "taker": "TRX-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "FTM-ETH", "taker": "FTM-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "ALGO-ETH", "taker": "ALGO-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "KCS-ETH", "taker": "KCS-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "AGIX-ETH", "taker": "AGIX-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "OCEAN-ETH", "taker": "OCEAN-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
    #                   {"maker": "ALICE-ETH", "taker": "ALICE-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0}
    #                   ]
    follow_markets = [{"maker": "XMR-ETH", "taker": "XMR-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0},
                      {"maker": "AGIX-ETH", "taker": "AGIX-USDT", "last": 0, "bid_timestamp": 0, "ask_timestamp": 0}
                      ]
    min_profitability = 1
    check_delay = 60

    markets = {connector_name: {trading_pair}}


    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    def get_pairs_data(self):
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
        returns dictionary in the structure:
        {
            "XMR-USDT": {"bid": 161.3, "ask": 165.4, "last": 163.8},
            "AGIX-USDT": {"bid": 0.391, "ask": 0.423, "last": 0.401}
        }
        """
        records = requests.get(url=self.url).json()
        records = records["data"]["ticker"]
        pairs_data = {}
        for record in records:
            pairs_data[record["symbol"]] = {"bid": Decimal(str(record["buy"])),
                                            "ask": Decimal(str(record["sell"])),
                                            "last": Decimal(str(record["last"]))}
        return pairs_data

    def find_cross_market(self, maker_quote, taker_quote, vol_maker_thrsh, vol_taker_thrsh):
        maker_market = {}
        taker_market = {}
        for d in data:
            symbol = d['symbol']
            base, quote = symbol.split('-')
            volume_in_quote = float(d['volValue'])
            if quote == maker_quote and volume_in_quote > vol_maker_thrsh:
                maker_market[base] = volume_in_quote
            elif quote == taker_quote and volume_in_quote > vol_taker_thrsh:
                taker_market[base] = volume_in_quote
        maker_market = {k: maker_market[k] for k in sorted(maker_market, key=maker_market.get, reverse=True)}
        return [market for market in maker_market if market in taker_market]

    def on_tick(self):
        # self.log_with_clock(logging.INFO, "New tick")
        prices_updated = self.get_pairs_data()

        taker_2 = self.taker_pair_2
        taker_2_bid = prices_updated[taker_2]['bid']
        taker_2_ask = prices_updated[taker_2]['ask']

        for num, market in enumerate(self.follow_markets):
            maker_pair = market['maker']
            taker_pair = market['taker']

            last_price = prices_updated[maker_pair]['last']
            taker_1_bid = prices_updated[taker_pair]['bid']
            taker_1_ask = prices_updated[taker_pair]['ask']

            # self.log_with_clock(logging.INFO, f"market = {market}, last_price = {last_price}, "
            #                                   f"taker_1_bid = {taker_1_bid}, taker_1_ask = {taker_1_ask}, "
            #                                   f"taker_2_bid = {taker_2_bid}, taker_2_ask = {taker_2_ask}")
            if last_price != market['last']:
                self.follow_markets[num]['last'] = last_price

                if self.current_timestamp > market['bid_timestamp']:
                    maker_buy_price = last_price
                    taker_sell_price = taker_1_bid / taker_2_ask
                    buy_profitability = 100 * (taker_sell_price / maker_buy_price - 1)
                    self.log_with_clock(logging.INFO, f"BUY profitability for {maker_pair} = {buy_profitability}%")

                    if buy_profitability > Decimal(self.min_profitability):
                        msg = f"Buy profitability for {market['maker']} is {buy_profitability}%"
                        self.log_with_clock(logging.INFO, msg)
                        self.notify_hb_app_with_timestamp(msg)
                        self.follow_markets[num]['bid_timestamp'] = self.current_timestamp + self.check_delay

                if self.current_timestamp > market['ask_timestamp']:
                    maker_sell_price = last_price
                    taker_buy_price = taker_1_ask / taker_2_bid
                    sell_profitability = 100 * (maker_sell_price / taker_buy_price - 1)
                    self.log_with_clock(logging.INFO, f"SELL profitability for {maker_pair} = {sell_profitability}%")

                    if sell_profitability > Decimal(self.min_profitability):
                        msg = f"Sell profitability for {market['maker']} is {sell_profitability}%"
                        self.log_with_clock(logging.INFO, msg)
                        self.notify_hb_app_with_timestamp(msg)
                        self.follow_markets[num]['ask_timestamp'] = self.current_timestamp + self.check_delay


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

