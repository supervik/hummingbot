import logging

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType, PositionAction, PositionSide
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class SpotPerpetualXEMM(ScriptStrategyBase):
    """

    """
    # Config params
    maker_connector_name: str = "kucoin"
    taker_connector_name: str = "gate_io_perpetual"
    maker_pair: str = "IGU-USDT"
    taker_pair: str = "IGU-USDT"
    # is_maker_spot = True

    order_amount_in_quote = Decimal("2")
    spread_bps = 60
    min_spread_bps = 30
    max_order_age = 120

    slippage_buffer_spread_bps = 200
    leverage = Decimal("20")

    # class parameters
    status = "NOT_INIT"
    maker_order_ids = {}
    maker_order_ids_clean_interval = 30 * 60
    maker_order_ids_clean_timestamp = 0
    order_amount_in_base = 0
    sell_order_amount = 0
    buy_order_amount = 0
    taker_sell_hedging_price = 0
    taker_buy_hedging_price = 0
    maker_side = "BUY"
    buy_order_placed = False
    sell_order_placed = False
    filled_event_buffer = []
    maker_base_asset, maker_quote_asset = "", ""
    taker_base_asset, taker_quote_asset = "", ""
    order_delay = 20
    next_maker_order_timestamp = 0

    markets = {maker_connector_name: {maker_pair},
               taker_connector_name: {taker_pair}}

    @property
    def maker_connector(self):
        """
        The maker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.maker_connector_name]

    @property
    def taker_connector(self):
        """
        The taker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.taker_connector_name]

    def on_tick(self):
        """
        """
        if self.status == "NOT_INIT":
            self.init_strategy()

        if self.status == "NOT_ACTIVE":
            return

        if self.current_timestamp > self.maker_order_ids_clean_timestamp:
            self.clean_maker_order_ids()

        self.calculate_maker_order_amount()

        self.calculate_taker_hedging_price()

        self.check_existing_orders_for_cancellation()

        if self.current_timestamp < self.next_maker_order_timestamp:
            return

        self.get_maker_order_side()

        self.place_maker_orders()

    def init_strategy(self):
        self.notify_hb_app_with_timestamp("Strategy started")
        self.set_base_quote_assets()
        self.status = "ACTIVE"

    def set_base_quote_assets(self):
        """
        """
        self.maker_base_asset, self.maker_quote_asset = split_hb_trading_pair(self.maker_pair)
        self.taker_base_asset, self.taker_quote_asset = split_hb_trading_pair(self.taker_pair)

    def clean_maker_order_ids(self):
        updated_maker_order_ids = {}
        for order_id, timestamp in self.maker_order_ids.items():
            if timestamp > self.current_timestamp - self.maker_order_ids_clean_interval:
                updated_maker_order_ids[order_id] = timestamp

        self.maker_order_ids = updated_maker_order_ids
        self.maker_order_ids_clean_timestamp = self.current_timestamp + self.maker_order_ids_clean_interval

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.maker_connector_name):
            self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)

    def calculate_maker_order_amount(self):
        # get sell amount
        base_amount = self.maker_connector.get_balance(self.maker_base_asset)
        self.sell_order_amount = self.quantize_amount_on_maker_and_taker(base_amount)

        # get buy amount
        mid_price = self.maker_connector.get_mid_price(self.maker_pair)
        self.order_amount_in_base = self.order_amount_in_quote / mid_price
        self.buy_order_amount = self.quantize_amount_on_maker_and_taker(self.order_amount_in_base)

    def quantize_amount_on_maker_and_taker(self, amount):
        amount_quantized_on_maker = self.maker_connector.quantize_order_amount(self.maker_pair, amount)
        amount_quantized_on_taker = self.taker_connector.quantize_order_amount(self.taker_pair, amount)
        return min(amount_quantized_on_maker, amount_quantized_on_taker)

    def calculate_taker_hedging_price(self):
        self.taker_sell_hedging_price = self.taker_connector.get_price_for_volume(self.taker_pair, False,
                                                                                  self.order_amount_in_base).result_price
        self.taker_buy_hedging_price = self.taker_connector.get_price_for_volume(self.taker_pair, True,
                                                                                 self.order_amount_in_base).result_price
        mid_taker_price = (self.taker_sell_hedging_price + self.taker_buy_hedging_price) / 2
        mid_maker_price = self.maker_connector.get_mid_price(self.maker_pair)

        mid_dif = mid_taker_price - mid_maker_price
        # self.logger().info(
        #     f"mid_maker_price = {mid_maker_price} mid_taker_price = {mid_taker_price}, mid_dif = {mid_dif}, "
        #     f"self.taker_sell_hedging_price = {self.taker_sell_hedging_price}")
        if mid_dif > 0:
            self.taker_sell_hedging_price -= mid_dif

    def check_existing_orders_for_cancellation(self):
        self.buy_order_placed = False
        self.sell_order_placed = False
        for order in self.get_active_orders(connector_name=self.maker_connector_name):
            cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
            if order.is_buy:
                self.buy_order_placed = True
                buy_cancel_threshold = self.taker_sell_hedging_price * Decimal(1 - self.min_spread_bps / 10000)
                if order.price > buy_cancel_threshold or cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling buy order: {order.client_order_id}")
                    self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)
            else:
                self.sell_order_placed = True
                sell_cancel_threshold = self.taker_buy_hedging_price * Decimal(1 + self.min_spread_bps / 10000)
                if order.price < sell_cancel_threshold or cancel_timestamp < self.current_timestamp:
                    self.logger().info(f"Cancelling sell order: {order.client_order_id}")
                    self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)

    def get_maker_order_side(self):
        self.maker_side = TradeType.BUY if self.sell_order_amount == Decimal("0") else TradeType.SELL

    def place_maker_orders(self):
        if self.buy_order_placed or self.sell_order_placed:
            return

        if self.maker_side == TradeType.BUY:
            maker_price = self.taker_sell_hedging_price * Decimal(1 - self.spread_bps / 10000)
            maker_order_amount = self.buy_order_amount
        else:
            maker_price = self.taker_buy_hedging_price * Decimal(1 + self.spread_bps / 10000)
            maker_order_amount = self.sell_order_amount

        maker_order = OrderCandidate(trading_pair=self.maker_pair, is_maker=True, order_type=OrderType.LIMIT,
                                     order_side=self.maker_side, amount=maker_order_amount, price=maker_price)

        maker_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(maker_order, all_or_none=False)
        if maker_order_adjusted.amount != Decimal("0"):
            order_id = self.send_order_to_exchange(candidate=maker_order_adjusted, connector_name=self.maker_connector_name)
            if order_id:
                self.maker_order_ids[order_id] = self.current_timestamp

    def send_order_to_exchange(self, candidate, connector_name):
        if candidate.order_side == TradeType.SELL:
            order_id = self.sell(connector_name, candidate.trading_pair, candidate.amount, candidate.order_type,
                      candidate.price, PositionAction.OPEN)
        else:
            order_id = self.buy(connector_name, candidate.trading_pair, candidate.amount, candidate.order_type,
                     candidate.price, PositionAction.OPEN)

        return order_id

    # def test_open_perpetual_order(self):
    #     amount = Decimal("20")
    #     taker_price = self.taker_connector.get_price_for_volume(self.taker_pair, True, amount).result_price
    #     taker_price_with_slippage = taker_price * Decimal(1 + self.slippage_buffer_spread_bps / 10000)
    #     self.logger().info(f"taker_price = {taker_price}, taker_price_with_slippage = {taker_price_with_slippage}")
    #     taker_candidate = PerpetualOrderCandidate(trading_pair=self.taker_pair, is_maker=False,
    #                                               order_type=OrderType.LIMIT,
    #                                               order_side=TradeType.BUY, amount=amount,
    #                                               price=taker_price_with_slippage, leverage=self.leverage)
    #
    #     taker_candidate_adjusted = self.taker_connector.budget_checker.adjust_candidate(taker_candidate,
    #                                                                                     all_or_none=True)
    #     if taker_candidate_adjusted.amount != Decimal("0"):
    #         self.send_order_to_exchange(candidate=taker_candidate_adjusted, connector_name=self.taker_connector_name)
    #         self.logger().info(f"send_order_to_exchange. Candidate adjusted {taker_candidate_adjusted}")
    #
    # def get_open_positions(self):
    #     open_positions = self.taker_connector.account_positions
    #     self.logger().info(f"open_positions = {open_positions}")
    #     for key, position in open_positions.items():
    #         self.logger().info(f"position = {position}")
    #         position_amount = position.amount
    #         self.logger().info(f"Current short_position_amount = {position_amount}")

    def is_active_maker_order(self, event: OrderFilledEvent):
        """
        Helper function that checks if order is an active order on the maker exchange
        """
        if event.order_id in self.maker_order_ids:
            return True
        else:
            return False

    def did_fill_order(self, event: OrderFilledEvent):
        exchange = "Taker"
        if self.is_active_maker_order(event):
            self.next_maker_order_timestamp = self.current_timestamp + self.order_delay
            exchange = "-- Maker"
            self.filled_event_buffer.append(event)
            self.logger().info(f"New filled event was added to filled_event_buffer = {self.filled_event_buffer}")
            self.place_taker_orders()
        msg = (f"{exchange} {event.trade_type.name} {round(event.amount, 8)} {event.trading_pair} "
               f"at {round(event.price, 8)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def place_taker_orders(self):
        amount_total = Decimal("0")
        for event in self.filled_event_buffer:
            amount_total += event.amount
            event_side = event.trade_type

        self.logger().info(f"amount_total = {amount_total}")
        quantized_amount_total = self.taker_connector.quantize_order_amount(self.taker_pair, amount_total)
        if quantized_amount_total == Decimal("0"):
            msg = f"Not enough amount filled to open a hedge order. Current total amount = {amount_total}"
            self.logger().info(msg)
            self.notify_hb_app_with_timestamp(msg)
            return

        self.cancel_all_orders()

        if event_side == TradeType.BUY:
            taker_side = TradeType.SELL
            taker_amount = quantized_amount_total
            taker_price = self.taker_connector.get_price_for_volume(self.maker_pair, False, taker_amount).result_price
            taker_price_with_slippage = taker_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending sell on taker with price {taker_price}. "
                               f"Price with slippage {taker_price_with_slippage}")

        else:
            # get amount of short positions to close them
            open_positions = self.taker_connector.account_positions
            short_position_amount = Decimal("0")
            self.logger().info(f"open_positions = {open_positions}")
            for key, position in open_positions.items():
                if position.position_side == PositionSide.SHORT:
                    short_position_amount = abs(position.amount)
                    self.logger().info(f"Current short_position_amount = {short_position_amount}")

            taker_side = TradeType.BUY
            taker_amount = min(short_position_amount, amount_total)
            taker_price = self.taker_connector.get_price_for_volume(self.taker_pair, True, taker_amount).result_price
            taker_price_with_slippage = taker_price * Decimal(1 + self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending buy on taker with price {taker_price}."
                               f" Price with slippage {taker_price_with_slippage}")

        taker_candidate = PerpetualOrderCandidate(trading_pair=self.taker_pair, is_maker=False,
                                                  order_type=OrderType.LIMIT,
                                                  order_side=taker_side, amount=taker_amount,
                                                  price=taker_price_with_slippage, leverage=self.leverage)

        taker_candidate_adjusted = self.taker_connector.budget_checker.adjust_candidate(taker_candidate,
                                                                                        all_or_none=True)
        if taker_candidate_adjusted.amount != Decimal("0"):
            self.logger().info(f"Delete all events from filled_event_buffer")
            self.filled_event_buffer = []
            self.send_order_to_exchange(candidate=taker_candidate_adjusted, connector_name=self.taker_connector_name)
            self.logger().info(f"send_order_to_exchange. Candidate adjusted {taker_candidate_adjusted}")

        else:
            msg = f"Can't create taker order with {taker_candidate.amount} amount. " \
                  f"Check minimum amount requirement or balance"
            self.logger().info(msg)
            self.notify_hb_app_with_timestamp(msg)
            self.status = "NOT_ACTIVE"

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])
        # lines.extend(["", "  order_ids:"] + ["    " + str(self.maker_order_ids)])

        lines.extend(["", "  Maker side:"] + ["    " + str(self.maker_side)])
        lines.extend(["", "  taker_sell_hedging_price:"] + ["    " + str(self.taker_sell_hedging_price)])
        lines.extend(["", "  taker_sell_hedging_price:"] + ["    " + str(self.taker_buy_hedging_price)])
        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Spread", "Age"]
        data = []
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                mid_price = connector.get_mid_price(order.trading_pair)
                if order.is_buy:
                    spread_mid = (mid_price - order.price) / mid_price * 100
                else:
                    spread_mid = (order.price - mid_price) / mid_price * 100

                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    connector_name,
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
