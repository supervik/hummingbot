import logging
from decimal import Decimal
from typing import List

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class PMMWithVolume(ScriptStrategyBase):
    bid_spread = 2
    ask_spread = 1
    bid_volume_threshold = 50
    ask_volume_threshold = 10
    max_order_age = 30
    order_amount = 20
    order_amount_mult = 0.99
    create_timestamp = 0
    trading_pair = "GBYTE-BTC"
    exchange = "bittrex"

    # Here you can use for example the LastTrade price to use in your strategy
    price_source = PriceType.MidPrice
    buy_price = 0
    sell_price = 0

    has_open_bid = False
    has_open_ask = False
    bid_delay_started = False
    ask_delay_started = False
    orders_delay = 1
    bid_delay_timestamp = 0
    ask_delay_timestamp = 0
    markets = {exchange: {trading_pair}}

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.exchange]

    def on_tick(self):
        self.calculate_maker_price()

        if self.check_and_cancel_maker_orders():
            return

        if not self.has_open_bid:
            if not self.bid_delay_started:
                self.bid_delay_started = True
                self.bid_delay_timestamp = self.current_timestamp
                return

            if self.current_timestamp > self.bid_delay_timestamp + self.orders_delay:
                self.place_order(True)
                self.bid_delay_started = False

        if not self.has_open_ask:
            if not self.ask_delay_started:
                self.ask_delay_started = True
                self.ask_delay_timestamp = self.current_timestamp
                return

            if self.current_timestamp > self.ask_delay_timestamp + self.orders_delay:
                self.place_order(False)
                self.ask_delay_started = False

    def calculate_maker_price(self):
        ref_price = self.connector.get_price_by_type(self.trading_pair, self.price_source)
        buy_price = ref_price * Decimal(1 - self.bid_spread / 100)
        sell_price = ref_price * Decimal(1 + self.ask_spread / 100)
        buy_price_threshold = self.connector.get_price_for_volume(self.trading_pair, False, self.bid_volume_threshold).result_price
        sell_price_threshold = self.connector.get_price_for_volume(self.trading_pair, True, self.ask_volume_threshold).result_price
        ref_buy_price = min(buy_price, buy_price_threshold)
        ref_sell_price = max(sell_price, sell_price_threshold)
        self.buy_price = self.get_better_price(False, ref_buy_price)
        self.sell_price = self.get_better_price(True, ref_sell_price)
        # self.log_with_clock(logging.INFO, f"ref_price = {ref_price}, "
        #                                   f"buy_price = {buy_price}, sell_price = {sell_price}, "
        #                                   f"buy_price_threshold = {buy_price_threshold}, "
        #                                   f"sell_price_threshold = {sell_price_threshold}, "
        #                                   f"self.buy_price = {self.buy_price}, self.sell_price  = {self.sell_price}")

    def check_and_cancel_maker_orders(self):
        self.has_open_bid = False
        self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.exchange):
            order_age = self.current_timestamp - order.creation_timestamp / 1000000
            if order_age > self.max_order_age:
                self.log_with_clock(logging.INFO, f"Order {order.client_order_id} age = {order_age} "
                                                  f"is higher than maximum. Cancelling order.")
                self.cancel(self.exchange, order.trading_pair, order.client_order_id)
                return True
            # if order.is_buy:
            #     self.has_open_bid = True
            #     if order.price > self.buy_price:
            #         self.log_with_clock(logging.INFO, f"BUY order price {order.price} is higher than "
            #                                           f"{self.buy_price}. Cancelling order.")
            #         self.cancel(self.exchange, order.trading_pair, order.client_order_id)
            #         return True
            # else:
            #     self.has_open_ask = True
            #     if order.price < self.sell_price:
            #         self.log_with_clock(logging.INFO, f"SELL order price {order.price} is lower than "
            #                                           f"{self.sell_price}. Cancelling order.")
            #         self.cancel(self.exchange, order.trading_pair, order.client_order_id)
            #         return True
        return False

    def place_order(self, is_buy):
        order_side = TradeType.BUY if is_buy else TradeType.SELL
        order_price = self.buy_price if is_buy else self.sell_price
        candidate = OrderCandidate(trading_pair=self.trading_pair,
                                   is_maker=True,
                                   order_type=OrderType.LIMIT,
                                   order_side=order_side,
                                   amount=Decimal(self.order_amount),
                                   price=order_price)
        candidate_adjusted = self.connector.budget_checker.adjust_candidate(candidate, all_or_none=False)
        # self.log_with_clock(logging.INFO, f"candidate_adjusted = {candidate_adjusted}")
        if candidate_adjusted.amount > Decimal("0"):
            if candidate_adjusted.amount < Decimal(self.order_amount):
                candidate_adjusted.amount *= Decimal(self.order_amount_mult)

            if is_buy:
                self.buy(self.exchange, self.trading_pair, candidate_adjusted.amount,
                         candidate_adjusted.order_type, candidate_adjusted.price)
            else:
                self.sell(self.exchange, self.trading_pair, candidate_adjusted.amount,
                          candidate_adjusted.order_type, candidate_adjusted.price)

    def cancel_all_orders(self):
        for order in self.get_active_orders(connector_name=self.exchange):
            self.cancel(self.exchange, order.trading_pair, order.client_order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        msg = (f"{event.trade_type.name} {round(event.amount, 5)} {event.trading_pair} {self.exchange} "
               f"at {round(event.price, 5)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def get_better_price(self, side, price) -> Decimal:
        """
        Calculates
        """
        orderbook = self.connector.get_order_book(self.trading_pair)
        increment = float(self.connector.get_order_price_quantum(self.trading_pair, Decimal("1")))

        if side:
            for order_book_row in orderbook.ask_entries():
                row_price = order_book_row.price
                if order_book_row.price > price:
                    break
            price_incremented = row_price - increment
        else:
            for order_book_row in orderbook.bid_entries():
                row_price = order_book_row.price
                if order_book_row.price < price:
                    break
            price_incremented = row_price + increment

        return Decimal(price_incremented)
