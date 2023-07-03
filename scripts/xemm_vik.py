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
    """
    maker_exchange = "kucoin"
    maker_pair = "IGU-USDT"
    taker_exchange = "gate_io"
    taker_pair = "IGU-USDT"

    order_amount = 50
    min_order_amount = 25
    spread_bps = 120  # bot places maker orders at this spread to taker price
    min_spread_bps = 80  # bot refreshes order if spread is lower than min-spread
    slippage_buffer_spread_bps = 150  # buffer applied to limit taker hedging trades on taker exchange
    order_delay = 30
    max_order_age = 180  # bot refreshes orders after this age
    dry_run = False

    markets = {maker_exchange: {maker_pair}, taker_exchange: {taker_pair}}

    status = "ACTIVE"
    buy_order_placed = False
    sell_order_placed = False
    open_sell = False
    open_buy = False
    taker_buy_hedging_price = 0
    taker_sell_hedging_price = 0
    buy_order_amount = 0
    sell_order_amount = 0
    filled_event_buffer = []
    taker_candidate_buffer = []
    place_order_trials_delay = 5
    place_order_trials_limit = 10
    next_maker_order_timestamp = 0
    maker_order_ids = {}
    maker_order_ids_clean_interval = 30 * 60
    maker_order_ids_clean_timestamp = 0

    @property
    def maker_connector(self):
        return self.connectors[self.maker_exchange]

    @property
    def taker_connector(self):
        return self.connectors[self.taker_exchange]

    def on_tick(self):

        if self.status == "NOT_ACTIVE":
            return

        if self.status == "HEDGE":
            self.not_filled_taker_orders()
            return

        if self.current_timestamp > self.maker_order_ids_clean_timestamp:
            self.clean_maker_order_ids()

        self.calculate_order_amount()

        self.calculate_hedging_price()

        self.check_existing_orders_for_cancellation()

        if self.current_timestamp < self.next_maker_order_timestamp:
            return

        self.place_maker_orders()

    def not_filled_taker_orders(self):
        if not self.taker_candidate_buffer:
            return False
        else:
            for i, candidate in enumerate(self.taker_candidate_buffer):
                if self.current_timestamp <= candidate["sent_timestamp"] + self.place_order_trials_delay:
                    delay = candidate['sent_timestamp'] + self.place_order_trials_delay - self.current_timestamp
                    self.log_with_clock(logging.INFO, f"Too early to place an order. Try again. {delay} sec left.")
                    return True

                if candidate["trials"] > self.place_order_trials_limit:
                    msg = f"Error placing {candidate['order_candidate'].trading_pair} " \
                          f"{candidate['order_candidate'].order_side} order. Stop trading"
                    self.notify_hb_app_with_timestamp(msg)
                    self.log_with_clock(logging.WARNING, msg)
                    self.status = "NOT_ACTIVE"
                    return True

                self.log_with_clock(logging.INFO, f"Failed to place {candidate['order_candidate'].trading_pair}"
                                                  f" {candidate['order_candidate'].order_side} order. Trying again. "
                                                  f"Trial number {candidate['trials']}")
                self.taker_candidate_buffer[i]["sent_timestamp"] = self.current_timestamp
                self.taker_candidate_buffer[i]["trials"] += 1
                order_id = self.send_order_to_exchange(exchange=self.taker_exchange, candidate=candidate['order_candidate'])
                self.logger().info(f"New trial to send_order_to_exchange order_id = {order_id}")
                if order_id:
                    self.logger().info(f"Clear taker_candidate_buffer: {self.taker_candidate_buffer[i]}")
                    self.taker_candidate_buffer.pop(i)

            return True

    def calculate_order_amount(self):
        base_asset, quote_asset = split_hb_trading_pair(self.maker_pair)
        base_taker_balance = self.taker_connector.get_available_balance(base_asset)
        quote_taker_balance = self.taker_connector.get_available_balance(quote_asset)
        base_maker_balance = self.maker_connector.get_available_balance(base_asset)
        base_maker_balance_quantized = self.maker_connector.quantize_order_amount(self.maker_pair, base_maker_balance)
        if base_maker_balance_quantized > self.min_order_amount:
            self.open_sell = True
            self.open_buy = False
        else:
            self.open_sell = True
            self.open_buy = True

        taker_buy_price = self.taker_connector.get_price_for_volume(self.taker_pair, True, self.order_amount).result_price
        taker_buy_price_with_slippage = taker_buy_price * Decimal(1 + self.slippage_buffer_spread_bps / 10000)
        self.buy_order_amount = min(base_taker_balance, self.order_amount)
        self.sell_order_amount = min(quote_taker_balance / taker_buy_price_with_slippage, self.order_amount)

    def calculate_hedging_price(self):
        self.taker_buy_hedging_price = self.taker_connector.get_price_for_volume(self.taker_pair, True,
                                                                                 self.sell_order_amount).result_price
        self.taker_sell_hedging_price = self.taker_connector.get_price_for_volume(self.taker_pair, False,
                                                                                  self.buy_order_amount).result_price

    def check_existing_orders_for_cancellation(self):
        self.buy_order_placed = False
        self.sell_order_placed = False
        for order in self.get_active_orders(connector_name=self.maker_exchange):
            cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
            if order.is_buy:
                self.buy_order_placed = True
                buy_cancel_threshold = self.taker_sell_hedging_price * Decimal(1 - self.min_spread_bps / 10000)
                if order.price > buy_cancel_threshold or cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling buy order: {order.client_order_id}")
                    self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)
            else:
                self.sell_order_placed = True
                sell_cancel_threshold = self.taker_buy_hedging_price * Decimal(1 + self.min_spread_bps / 10000)
                if order.price < sell_cancel_threshold or cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling sell order: {order.client_order_id}")
                    self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)

    def place_maker_orders(self):
        if not self.buy_order_placed and self.open_buy:
            maker_buy_price = self.taker_sell_hedging_price * Decimal(1 - self.spread_bps / 10000)
            buy_order = OrderCandidate(trading_pair=self.maker_pair,
                                       is_maker=True,
                                       order_type=OrderType.LIMIT,
                                       order_side=TradeType.BUY,
                                       amount=Decimal(self.buy_order_amount),
                                       price=maker_buy_price)
            buy_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(buy_order, all_or_none=False)
            amount_less = buy_order_adjusted.amount * Decimal("0.999")
            buy_order_adjusted.amount = self.maker_connector.quantize_order_amount(self.maker_pair, amount_less)
            if buy_order_adjusted.amount != Decimal("0") and buy_order_adjusted.amount > self.min_order_amount:
                order_id = self.send_order_to_exchange(exchange=self.maker_exchange, candidate=buy_order_adjusted)
                if order_id:
                    self.maker_order_ids[order_id] = self.current_timestamp

        if not self.sell_order_placed and self.open_sell:
            maker_sell_price = self.taker_buy_hedging_price * Decimal(1 + self.spread_bps / 10000)
            amount = Decimal("0.995") * self.sell_order_amount
            sell_order = OrderCandidate(trading_pair=self.maker_pair,
                                        is_maker=True,
                                        order_type=OrderType.LIMIT,
                                        order_side=TradeType.SELL,
                                        amount=amount,
                                        price=maker_sell_price)
            sell_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(sell_order, all_or_none=False)
            amount_less = sell_order_adjusted.amount * Decimal("0.999")
            sell_order_adjusted.amount = self.maker_connector.quantize_order_amount(self.maker_pair, amount_less)
            if sell_order_adjusted.amount != Decimal("0") and sell_order_adjusted.amount > self.min_order_amount:
                order_id = self.send_order_to_exchange(exchange=self.maker_exchange, candidate=sell_order_adjusted)
                if order_id:
                    self.maker_order_ids[order_id] = self.current_timestamp

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.maker_exchange):
            self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        if self.is_active_maker_order(event):
            self.filled_event_buffer.append(event)
            self.logger().info(f"New filled event was added to filled_event_buffer = {self.filled_event_buffer}")
            self.next_maker_order_timestamp = self.current_timestamp + self.order_delay
            self.place_taker_order()
            msg = (f"--> {event.trade_type.name} at price {round(event.price, 6)} maker order filled "
                   f"with amount {round(event.amount, 6)} on {event.trading_pair}")
            self.logger().info(msg)
            self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        if not self.is_active_maker_order(event):
            self.taker_completed_order_process(event, "BUY")

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        if not self.is_active_maker_order(event):
            self.taker_completed_order_process(event, "SELL")

    def taker_completed_order_process(self, event, side):
        complete_price = event.quote_asset_amount / event.base_asset_amount
        msg = f"<<< {side} at price {complete_price} taker order completed with amount {event.base_asset_amount}"
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)
        self.status = "ACTIVE"

    def is_active_maker_order(self, event: OrderFilledEvent):
        """
        Helper function that checks if order is an active order on the maker exchange
        """
        return True if event.order_id in self.maker_order_ids else False

    def clean_maker_order_ids(self):
        """
        Cleans up old maker order IDs to avoid using expired ones
        """
        updated_maker_order_ids = {}
        for order_id, timestamp in self.maker_order_ids.items():
            if timestamp > self.current_timestamp - self.maker_order_ids_clean_interval:
                updated_maker_order_ids[order_id] = timestamp

        self.maker_order_ids = updated_maker_order_ids
        self.maker_order_ids_clean_timestamp = self.current_timestamp + self.maker_order_ids_clean_interval

    def place_taker_order(self):
        amount_total = 0
        for event in self.filled_event_buffer:
            if event.trade_type == TradeType.BUY:
                amount_total += event.amount
            else:
                amount_total -= event.amount
        taker_amount = Decimal(abs(amount_total))
        self.logger().info(f"taker_amount = {taker_amount}")
        if amount_total > 0:
            taker_side = TradeType.SELL
            taker_price = self.taker_connector.get_price_for_volume(self.taker_pair, False, taker_amount).result_price
            taker_price_with_slippage = taker_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending sell on taker with price {taker_price}")
        else:
            taker_side = TradeType.BUY
            taker_price = self.taker_connector.get_price_for_volume(self.taker_pair, True, taker_amount).result_price
            taker_price_with_slippage = taker_price * Decimal(1 + self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending buy on taker with price {taker_price}")

        taker_candidate = OrderCandidate(trading_pair=self.taker_pair, is_maker=False, order_type=OrderType.LIMIT,
                                         order_side=taker_side, amount=taker_amount, price=taker_price_with_slippage)
        taker_candidate_adjusted = self.taker_connector.budget_checker.adjust_candidate(taker_candidate,
                                                                                        all_or_none=True)

        if taker_candidate_adjusted.amount != Decimal("0"):
            self.status = "HEDGE"
            self.logger().info(f"Delete all events from filled_event_buffer")
            self.filled_event_buffer = []
            self.taker_candidate_buffer.append({"order_candidate": taker_candidate_adjusted,
                                                "sent_timestamp": self.current_timestamp, "trials": 0})
            self.logger().info(
                f"Add new taker_candidate_buffer self.taker_candidate_buffer={self.taker_candidate_buffer}")
            order_id = self.send_order_to_exchange(exchange=self.taker_exchange, candidate=taker_candidate_adjusted)
            self.logger().info(f"Taker send_order_to_exchange order_id = {order_id}")
            if order_id:
                self.logger().info(f"Clear taker_candidate_buffer: {self.taker_candidate_buffer[-1]}")
                self.taker_candidate_buffer.pop(-1)
            else:
                self.logger().info(f"Order_id is not defined. Try to send order to exchange again")
        else:
            msg = f"Can't create taker order with {taker_candidate.amount} amount. Check minimum amount requirement or balance"
            self.logger().info(msg)
            self.notify_hb_app_with_timestamp(msg)

    def send_order_to_exchange(self, exchange, candidate):
        if self.dry_run:
            return

        if candidate.order_side == TradeType.SELL:
            order_id = self.sell(exchange, candidate.trading_pair, candidate.amount, candidate.order_type, candidate.price)
        else:
            order_id = self.buy(exchange, candidate.trading_pair, candidate.amount, candidate.order_type, candidate.price)
        return order_id

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Spread", "Age"]
        data = []
        mid_price = self.maker_connector.get_mid_price(self.maker_pair)
        for order in self.get_active_orders(connector_name=self.maker_exchange):
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
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df

    def get_markets_df(self):
        best_bid_maker = self.maker_connector.get_price(self.maker_pair, False)
        best_ask_maker = self.maker_connector.get_price(self.maker_pair, True)
        best_bid_taker = self.taker_connector.get_price(self.maker_pair, False)
        best_ask_taker = self.taker_connector.get_price(self.maker_pair, True)
        mid_price_maker = (best_bid_maker + best_ask_maker) / 2
        mid_price_taker = (best_bid_taker + best_ask_taker) / 2
        columns = ["Market", "Pair", "Best_Bid", "Best_Ask", "Mid_Price"]
        data = [[self.maker_exchange, self.maker_pair, best_bid_maker, best_ask_maker, mid_price_maker],
                [self.taker_exchange, self.taker_pair, best_bid_taker, best_ask_taker, mid_price_taker]]
        df = pd.DataFrame(data=data, columns=columns)
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

        markets_df = self.get_markets_df()
        lines.extend(["", "  Markets:"] +
                     ["    " + line for line in str(markets_df).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)
