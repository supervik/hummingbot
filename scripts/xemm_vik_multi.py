import logging
import math
from decimal import Decimal

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent, BuyOrderCreatedEvent, SellOrderCreatedEvent, \
    BuyOrderCompletedEvent, SellOrderCompletedEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class XEMMVik(ScriptStrategyBase):
    """
    Multi-pair strategy. It only opens maker orders but considers the taker prices as well.
    The objective is to identify pairs that are suitable for the XEMM strategy on two exchanges.
    The pair is considered more suitable for XEMM if it has more orders opened on both sides (first buy and then sell)
    """
    maker_exchange = "kucoin"
    taker_exchange = "gate_io"
    trading_pairs = {"KDA-BTC", "OMG-BTC", "ASTR-BTC", "ETC-BTC", "IOTA-BTC", "LTC-BTC", "ADA-BTC", "WAVES-BTC",
                     "BNB-BTC", "IOST-BTC", "XRP-BTC", "CKB-BTC", "DASH-BTC", "STORJ-BTC", "BCH-BTC", "XMR-BTC",
                     "XLM-BTC", "NEO-BTC", "ZEC-BTC", "DOGE-BTC", "XEM-BTC", "LRC-BTC", "EOS-BTC", "DOT-BTC",
                     "XTZ-BTC", "QTUM-BTC", "ATOM-BTC", "VRA-BTC", "ZRX-BTC"}

    order_amount_in_quote = Decimal("1.5")
    min_quote_balance = Decimal("400")  # bot places only sell orders if the total balance is less than defined here
    spread_bps = 100  # bot places maker orders at this spread to taker price
    min_spread_bps = 60  # bot refreshes order if spread is lower than min-spread
    max_spread_bps = 120  # bot refreshes order if spread is higher than max-spread
    max_order_age = 900  # bot refreshes orders after this age
    mid_price_min_position = 30  # bot don't open buy orders if the mid-price on maker and taker are too different

    markets = {maker_exchange: trading_pairs, taker_exchange: trading_pairs}

    pair = ""
    status = "ACTIVE"
    open_buy = True
    open_sell = True
    buy_order_placed = False
    sell_order_placed = False
    taker_buy_hedging_price = 0
    taker_sell_hedging_price = 0
    buy_order_amount = 0
    sell_order_amount = 0
    order_delay = 15
    next_maker_order_timestamp = 0

    @property
    def maker_connector(self):
        return self.connectors[self.maker_exchange]

    @property
    def taker_connector(self):
        return self.connectors[self.taker_exchange]

    def on_tick(self):
        """
        Every tick bot loops through all pairs, cancels orders, calculate params and places new maker orders
        """
        for self.pair in self.trading_pairs:
            self.calculate_order_amount()

            self.calculate_hedging_price()

            self.check_existing_orders_for_cancellation()

            self.check_open_maker_orders_conditions()

            # self.check_mid_price_position()

            if self.current_timestamp < self.next_maker_order_timestamp:
                return

            self.place_maker_orders()

    def calculate_order_amount(self):
        """
        sell order - the order amount is the maximum amount that we have in base asset
        buy order - the order amount defined in the config nominated into the base asset
        """
        base_asset, quote_asset = self.pair.split("-")
        base_amount = self.maker_connector.get_balance(base_asset)
        base_amount_quantized = self.maker_connector.quantize_order_amount(self.pair, base_amount)

        mid_price = self.maker_connector.get_mid_price(self.pair)
        self.sell_order_amount = base_amount_quantized
        self.buy_order_amount = self.order_amount_in_quote / mid_price

    def calculate_hedging_price(self):
        """
        Calculate the price on the taker exchange for a market order with the order amount specified in the config
        Calculate the mid_price difference between taker and maker prices and add it to the taker_sell_hedging_price
        This adjustment accounts for situations where the prices on the maker and taker diverge,
        making buying easy but selling challenging
        """
        self.taker_buy_hedging_price = self.taker_connector.get_price_for_volume(self.pair, True,
                                                                                 self.sell_order_amount).result_price
        self.taker_sell_hedging_price = self.taker_connector.get_price_for_volume(self.pair, False,
                                                                                  self.buy_order_amount).result_price
        mid_maker_price = self.maker_connector.get_mid_price(self.maker_pair)
        mid_taker_price = (self.taker_sell_hedging_price + self.taker_buy_hedging_price) / 2
        mid_dif = mid_taker_price - mid_maker_price
        if mid_dif > 0:
            self.taker_sell_hedging_price -= mid_dif

    def check_open_maker_orders_conditions(self):
        """
        Check if we have enough base asset to sell. If yes, open sell order, if no open buy order
        Also check if the quote balance is less than the minimum (too many assets bought)
        Open only sell orders in this case
        """
        if self.sell_order_amount == Decimal("0"):
            self.open_sell = False
            self.open_buy = True
        else:
            self.open_sell = True
            self.open_buy = False

        base_asset, quote_asset = self.pair.split("-")
        quote_balance = self.maker_connector.get_balance(quote_asset)

        if quote_balance < self.min_quote_balance:
            self.open_buy = False

    # def check_mid_price_position(self):
    #     """
    #     Check if the mid price on maker and taker are too different
    #     --- The old method. Remove it probably
    #     """
    #     mid_price = self.maker_connector.get_mid_price(self.pair)
    #
    #     mid_price_position = Decimal("100") * (mid_price - self.taker_sell_hedging_price) / (self.taker_buy_hedging_price - self.taker_sell_hedging_price)
    #     if mid_price_position < Decimal(self.mid_price_min_position):
    #         self.open_buy = False

    def check_existing_orders_for_cancellation(self):
        """"
        Check if current open maker order need to be cancelled.
        Conditions: if order age is large enough or order price is below the min_spread or above the max_spread
        """
        self.buy_order_placed = False
        self.sell_order_placed = False
        for order in self.get_active_orders(connector_name=self.maker_exchange):
            if order.trading_pair == self.pair:
                cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
                if order.is_buy:
                    self.buy_order_placed = True
                    buy_cancel_threshold_up = self.taker_sell_hedging_price * Decimal(1 - self.min_spread_bps / 10000)
                    buy_cancel_threshold_down = self.taker_sell_hedging_price * Decimal(1 - self.max_spread_bps / 10000)
                    if order.price > buy_cancel_threshold_up or order.price < buy_cancel_threshold_down \
                            or cancel_timestamp < self.current_timestamp:
                        self.logger().info(f"Cancelling buy order {order.trading_pair}: {order.client_order_id}")
                        self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)
                else:
                    self.sell_order_placed = True
                    sell_cancel_threshold_up = self.taker_buy_hedging_price * Decimal(1 + self.max_spread_bps / 10000)
                    sell_cancel_threshold_down = self.taker_buy_hedging_price * Decimal(1 + self.min_spread_bps / 10000)
                    if order.price < sell_cancel_threshold_down or order.price > sell_cancel_threshold_up \
                            or cancel_timestamp < self.current_timestamp:
                        self.logger().info(f"Cancelling sell order {order.trading_pair}: {order.client_order_id}")
                        self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)

    def place_maker_orders(self):
        """
        Calculates current order price considering the profitability (spread) and places the order if it is not placed
        """
        if not self.buy_order_placed and self.open_buy:
            maker_buy_price = self.taker_sell_hedging_price * Decimal(1 - self.spread_bps / 10000)
            buy_order = OrderCandidate(trading_pair=self.pair,
                                       is_maker=True,
                                       order_type=OrderType.LIMIT,
                                       order_side=TradeType.BUY,
                                       amount=Decimal(self.buy_order_amount),
                                       price=maker_buy_price)
            buy_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(buy_order, all_or_none=False)
            if buy_order_adjusted.amount != Decimal("0") and buy_order_adjusted.price:
                self.send_order_to_exchange(exchange=self.maker_exchange, candidate=buy_order_adjusted)

        if not self.sell_order_placed and self.open_sell:
            maker_sell_price = self.taker_buy_hedging_price * Decimal(1 + self.spread_bps / 10000)
            sell_order = OrderCandidate(trading_pair=self.pair,
                                        is_maker=True,
                                        order_type=OrderType.LIMIT,
                                        order_side=TradeType.SELL,
                                        amount=self.sell_order_amount,
                                        price=maker_sell_price)
            sell_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(sell_order, all_or_none=False)
            if sell_order_adjusted.amount != Decimal("0") and sell_order_adjusted.price:
                self.send_order_to_exchange(exchange=self.maker_exchange, candidate=sell_order_adjusted)

    def did_fill_order(self, event: OrderFilledEvent):
        """
        Logs the filled order
        Ads the delay for the next order timestamp
        """
        self.next_maker_order_timestamp = self.current_timestamp + self.order_delay
        msg = f"*** {event.trade_type.name} {round(event.amount, 6)} {event.trading_pair} at {round(event.price, 6)}"
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def send_order_to_exchange(self, exchange, candidate):
        """
        Basic method to send order to exchange maker or taker
        """
        if candidate.order_side == TradeType.SELL:
            self.sell(exchange, candidate.trading_pair, candidate.amount, candidate.order_type, candidate.price)
        else:
            self.buy(exchange, candidate.trading_pair, candidate.amount, candidate.order_type, candidate.price)

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Spread", "Age"]
        data = []
        for order in self.get_active_orders(connector_name=self.maker_exchange):
            mid_price = self.maker_connector.get_mid_price(order.trading_pair)
            if order.is_buy:
                spread_mid = (mid_price - order.price) / mid_price * 100
            else:
                spread_mid = (order.price - mid_price) / mid_price * 100

            age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
            data.append([
                self.maker_exchange,
                order.trading_pair,
                "buy" if order.is_buy else "sell",
                float(order.price),
                float(order.quantity),
                float(round(spread_mid, 2)),
                age_txt
            ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Pair"], inplace=True)
        return df

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)
