import logging

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class FetchTriPairs(ScriptStrategyBase):
    # Config params
    connector_name: str = "kucoin"
    # maker_pairs = ["XMR-ETH", "AGIX-ETH", "LTC-ETH"]
    # taker_pairs = ["XMR-USDT", "AGIX-USDT", "LTC-USDT"]
    # taker_pair_2 = "ETH-USDT"
    # base_assets = ['XMR', 'LTC', 'UBX', 'VET', 'TRAC', 'FTM', 'TRX', 'XRP', 'TEL', 'BAX', 'HIGH', 'AGIX']
    base_assets = ['AGIX', 'TRX', 'SDAO']
    quote_asset_1 = "ETH"
    quote_asset_2 = "USDT"
    quote_assets_reverse = False

    pairs_data = []
    pairs_set = set()
    for asset in base_assets:
        maker_pair = f"{asset}-{quote_asset_1}"
        taker_pair = f"{asset}-{quote_asset_2}"
        pairs_set.add(maker_pair)
        pairs_set.add(taker_pair)
        pairs_data.append({"maker": maker_pair,
                           "taker": taker_pair,
                           "ask_timestamp": 0,
                           "bid_timestamp": 0})

    taker_pair_2: str = f"{quote_asset_1}-{quote_asset_2}" if not quote_assets_reverse else f"{quote_asset_2}-{quote_asset_1}"
    pairs_set.add(taker_pair_2)

    order_amount_quote: Decimal = Decimal("0.1")
    min_profitability = 0.5

    markets = {connector_name: pairs_set}

    check_delay = 60
    order_amount = 0

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    def on_tick(self):
        self.log_with_clock(logging.INFO, "New tick")
        for trading_pair in self.pairs_data:
            if self.current_timestamp > trading_pair['bid_timestamp']:
                bid_profitability = self.calculate_profitability(pair=trading_pair, is_bid=True)
                if bid_profitability > Decimal(self.min_profitability):
                    msg = f"Bid profitability for {trading_pair['maker']} is {bid_profitability}%"
                    self.log_with_clock(logging.INFO, msg)
                    self.notify_hb_app_with_timestamp(msg)
                    trading_pair['bid_timestamp'] = self.current_timestamp + self.check_delay

            if self.current_timestamp > trading_pair['ask_timestamp']:
                ask_profitability = self.calculate_profitability(pair=trading_pair, is_bid=False)
                if ask_profitability > Decimal(self.min_profitability):
                    msg = f"Ask profitability for {trading_pair['maker']} is {ask_profitability}%"
                    self.log_with_clock(logging.INFO, msg)
                    self.notify_hb_app_with_timestamp(msg)
                    trading_pair['ask_timestamp'] = self.current_timestamp + self.check_delay

    def calculate_profitability(self, pair, is_bid):
        order_amount = self.order_amount_quote / self.connector.get_mid_price(pair['maker'])
        best_maker_price = self.connector.get_price(pair['maker'], not is_bid)
        taker_price = self.calculate_taker_price(is_bid, order_amount, pair['taker'], self.taker_pair_2)
        result = taker_price / best_maker_price if is_bid else best_maker_price / taker_price
        profitability = 100 * (result - 1)

        return profitability

    def calculate_taker_price(self, is_bid, order_amount, taker_pair_1, taker_pair_2):
        side_taker_1 = not is_bid
        exchanged_amount_1 = self.connector.get_quote_volume_for_base_amount(taker_pair_1, side_taker_1,
                                                                             order_amount).result_volume
        if not self.quote_assets_reverse:
            side_taker_2 = not side_taker_1
            exchanged_amount_2 = self.get_base_amount_for_quote_volume(taker_pair_2, side_taker_2,
                                                                       exchanged_amount_1)
        else:
            side_taker_2 = side_taker_1
            exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(taker_pair_2, side_taker_2,
                                                                                 exchanged_amount_1).result_volume
        final_price = exchanged_amount_2 / order_amount
        return final_price

    def get_base_amount_for_quote_volume(self, pair, side, quote_volume) -> Decimal:
        """
        Calculates base amount that you get for the quote volume using the orderbook entries
        """
        orderbook = self.connector.get_order_book(pair)
        orderbook_entries = orderbook.ask_entries() if side else orderbook.bid_entries()

        cumulative_volume = 0.
        cumulative_base_amount = 0.
        quote_volume = float(quote_volume)

        for order_book_row in orderbook_entries:
            row_amount = order_book_row.amount
            row_price = order_book_row.price
            row_volume = row_amount * row_price
            if row_volume + cumulative_volume >= quote_volume:
                row_volume = quote_volume - cumulative_volume
                row_amount = row_volume / row_price
            cumulative_volume += row_volume
            cumulative_base_amount += row_amount
            if cumulative_volume >= quote_volume:
                break

        return Decimal(cumulative_base_amount)