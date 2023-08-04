import logging

import pandas as pd

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.core.data_type.common import TradeType, PositionAction
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class HedgePerpetual(ScriptStrategyBase):
    """
    This script buys and sells the same amount on spot and perpetual for hedging purposes.
    For example you need an asset to trade on spot market but you don't want to hold it.
    The script will buy this asset and open short position on futures
    To buy the script places limit orders in the orderbook and waits for it to be filled.
    The spread is defined in spread_bps. The min_spread at which the order is canceled is min_spread_bps
    The script will do multiple iterations with max_order_amount until buys total_amount reached

    The script also works in the opposite direction to close the position and sell the asset on spot market
    """
    # Config params
    maker_connector_name: str = "gate_io"
    taker_connector_name: str = "gate_io_perpetual"
    maker_pair: str = "IGU-USDT"
    taker_pair: str = "IGU-USDT"
    is_maker_spot = True
    open_hedge = False

    max_order_amount = Decimal("100")
    total_amount = Decimal("100")
    kill_switch_balance = Decimal("100")
    processed_amount = Decimal("0")
    processed_amount_quote = Decimal("0")

    min_spread_bps = 100
    spread_bps = 200
    max_order_age = 120

    slippage_buffer_spread_bps = 100
    leverage = Decimal("20")

    # class parameters
    status = "NOT_INIT"
    maker_order_amount = 0
    taker_sell_hedging_price = 0
    taker_buy_hedging_price = 0
    buy_order_placed = False
    sell_order_placed = False
    buy_on_maker = False
    filled_event_buffer = []

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
        if self.status == "NOT_INIT":
            self.init_strategy()
        if self.status == "NOT_ACTIVE":
            return

        if self.check_kill_switch_balance():
            return
        
        self.calculate_maker_order_amount()

        self.calculate_taker_hedging_price()

        self.check_existing_orders_for_cancellation()

        self.place_maker_orders()

    def init_strategy(self):
        self.buy_on_maker = False if self.is_maker_spot ^ self.open_hedge else True
        self.status = "ACTIVE"

    def check_kill_switch_balance(self):
        if self.buy_on_maker:
            base_asset, quote_asset = self.maker_pair.split("-")
            base_amount = self.maker_connector.get_balance(base_asset)
            if base_amount >= self.kill_switch_balance:
                self.logger().info(f"Kill switch balance reached. Stop trading")
                self.cancel_all_orders()
                self.status = "NOT_ACTIVE"
                return True
        return False

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.maker_connector_name):
            self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)

    def calculate_maker_order_amount(self):
        amount_left = self.total_amount - self.processed_amount
        self.maker_order_amount = min(amount_left, self.max_order_amount)
        quantized_amount = self.maker_connector.quantize_order_amount(self.maker_pair, self.maker_order_amount)
        if quantized_amount == Decimal("0"):
            final_price = round(self.processed_amount_quote / self.processed_amount, 8) if self.processed_amount else -1
            self.logger().info(f"Left amount {self.maker_order_amount} is less than minimum threshold. Stop the bot")
            self.logger().info(f"Total amount processed = {self.processed_amount}.  Amount_quote = {self.processed_amount_quote} "
                               f"Final price = {final_price}")
            self.status = "NOT_ACTIVE"

    def calculate_taker_hedging_price(self):
        self.taker_sell_hedging_price = self.taker_connector.get_price_for_volume(self.taker_pair, False,
                                                                                  self.maker_order_amount).result_price
        self.taker_buy_hedging_price = self.taker_connector.get_price_for_volume(self.taker_pair, True,
                                                                                 self.maker_order_amount).result_price
        mid_maker_price = self.maker_connector.get_mid_price(self.maker_pair)
        mid_taker_price = (self.taker_sell_hedging_price + self.taker_buy_hedging_price) / 2
        mid_dif = mid_taker_price - mid_maker_price
        # self.logger().info(
        #     f"mid_maker_price = {mid_maker_price} mid_taker_price = {mid_taker_price}, mid_dif = {mid_dif}, "
        #     f"self.taker_sell_hedging_price = {self.taker_sell_hedging_price}")
        if mid_dif > 0 and self.open_hedge and self.buy_on_maker:
            self.taker_sell_hedging_price -= mid_dif
            # self.logger().info(f"Shifted self.taker_sell_hedging_price = {self.taker_sell_hedging_price}")
        if mid_dif < 0 and self.open_hedge and not self.buy_on_maker:
            self.taker_buy_hedging_price += abs(mid_dif)

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

    def place_maker_orders(self):
        if self.buy_order_placed or self.sell_order_placed:
            return

        if self.buy_on_maker:
            if not self.buy_order_placed:
                maker_price = self.taker_sell_hedging_price * Decimal(1 - self.spread_bps / 10000)
                maker_side = TradeType.BUY
        else:
            if not self.sell_order_placed:
                maker_price = self.taker_buy_hedging_price * Decimal(1 + self.spread_bps / 10000)
                maker_side = TradeType.SELL

        if self.is_maker_spot:
            maker_order = OrderCandidate(trading_pair=self.maker_pair, is_maker=True, order_type=OrderType.LIMIT,
                                         order_side=maker_side, amount=self.maker_order_amount, price=maker_price)
        else:
            maker_order = PerpetualOrderCandidate(trading_pair=self.maker_pair, is_maker=True,
                                                  order_type=OrderType.LIMIT, order_side=maker_side,
                                                  amount=self.maker_order_amount, price=maker_price,
                                                  leverage=self.leverage)

        maker_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(maker_order, all_or_none=False)
        if maker_order_adjusted.amount != Decimal("0"):
            self.send_order_to_exchange(candidate=maker_order_adjusted, connector_name=self.maker_connector_name)

    def send_order_to_exchange(self, candidate, connector_name):
        if candidate.order_side == TradeType.SELL:
            self.sell(connector_name, candidate.trading_pair, candidate.amount, candidate.order_type,
                      candidate.price, PositionAction.OPEN)
        else:
            self.buy(connector_name, candidate.trading_pair, candidate.amount, candidate.order_type,
                     candidate.price, PositionAction.OPEN)

    def is_active_maker_order(self, event: OrderFilledEvent):
        """
        Helper function that checks if order is an active order on the maker exchange
        """
        # for order in self.get_active_orders(connector_name=self.maker_connector_name):
        #     if order.client_order_id == event.order_id:
        #         return True
        if (event.trade_type == TradeType.BUY and self.buy_on_maker) or \
                (event.trade_type == TradeType.SELL and not self.buy_on_maker):
            return True
        return False

    def did_fill_order(self, event: OrderFilledEvent):
        exchange = "Taker"
        if self.is_active_maker_order(event):
            exchange = "-- Maker"
            self.processed_amount += event.amount
            self.processed_amount_quote += event.amount * event.price
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
    
        self.logger().info(f"amount_total = {amount_total}")
        quantized_amount_total = self.taker_connector.quantize_order_amount(self.taker_pair, amount_total)
        if quantized_amount_total == Decimal("0"):
            self.logger().info(f"Not enough amount filled to open a hedge order")
            return

        if self.buy_on_maker:
            taker_side = TradeType.SELL
            taker_price = self.taker_connector.get_price_for_volume(self.maker_pair, False, quantized_amount_total).result_price
            taker_price_with_slippage = taker_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending sell on taker with price {taker_price}")
        else:
            taker_side = TradeType.BUY
            taker_price = self.taker_connector.get_price_for_volume(self.maker_pair, True, quantized_amount_total).result_price
            taker_price_with_slippage = taker_price * Decimal(1 + self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending buy on taker with price {taker_price}")

        if self.is_maker_spot:
            taker_candidate = PerpetualOrderCandidate(trading_pair=self.taker_pair, is_maker=False,
                                                      order_type=OrderType.LIMIT,
                                                      order_side=taker_side, amount=amount_total,
                                                      price=taker_price_with_slippage, leverage=self.leverage)

        else:
            taker_candidate = OrderCandidate(trading_pair=self.taker_pair, is_maker=False, order_type=OrderType.LIMIT,
                                             order_side=taker_side, amount=amount_total,
                                             price=taker_price_with_slippage)

        taker_candidate_adjusted = self.taker_connector.budget_checker.adjust_candidate(taker_candidate,
                                                                                        all_or_none=True)
        if taker_candidate_adjusted.amount != Decimal("0"):
            self.logger().info(f"Delete all events from filled_event_buffer")
            self.filled_event_buffer = []
            self.send_order_to_exchange(candidate=taker_candidate_adjusted, connector_name=self.taker_connector_name)
            self.logger().info(f"send_order_to_exchange. Candidate adjusted {taker_candidate_adjusted}")

        else:
            msg = f"Can't create taker order with {taker_candidate.amount} amount. Check minimum amount requirement or balance"
            self.logger().info(msg)
            self.notify_hb_app_with_timestamp(msg)
            self.status = "NOT_ACTIVE"

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Spread", "Age"]
        data = []
        mid_price = self.maker_connector.get_mid_price(self.maker_pair)
        # if self.is_maker_spot:
        for order in self.get_active_orders(connector_name=self.maker_connector_name):
            spread_mid = abs(mid_price - order.price) / mid_price * 100
            age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
            data.append([
                self.maker_connector_name,
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
