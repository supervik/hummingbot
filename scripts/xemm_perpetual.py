import logging

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType, PositionAction
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class XEMMPerpetual(ScriptStrategyBase):
    # Config params
    maker_exchange: str = "gate_io_perpetual"
    taker_exchange: str = "binance_perpetual"
    maker_pair: str = "NEAR-USDT"
    taker_pair: str = "NEAR-USDT"

    spread: Decimal = Decimal("0.8")
    min_spread: Decimal = Decimal("0.5")
    max_spread: Decimal = Decimal("1")

    order_amount: Decimal = Decimal("10")
    slippage_buffer = Decimal("1")
    max_order_age = 120
    leverage = Decimal("1")
    #
    # fee_asset = "KCS"
    # fee_asset_target_amount = 1
    # fee_pair = "KCS-USDT"
    # fee_asset_check_interval = 300

    # kill_switch_enabled: bool = True
    # kill_switch_rate = Decimal("-2")

    # Class params
    status: str = "NOT_INIT"

    markets = {maker_exchange: {maker_pair}, taker_exchange: {taker_pair}}

    buy_order_placed = False
    sell_order_placed = True
    taker_buy_price = 0
    taker_sell_price = 0

    @property
    def connector_maker(self):
        """
        The maker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.maker_exchange]

    @property
    def connector_taker(self):
        """
        The taker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.taker_exchange]

    def on_tick(self):
        # self.taker_buy_price = self.connector_taker.get_price(self.taker_pair, True)
        self.taker_buy_price = self.connector_taker.get_price_for_volume(self.taker_pair, True,
                                                                         self.order_amount).result_price
        self.taker_sell_price = self.connector_taker.get_price_for_volume(self.taker_pair, False,
                                                                          self.order_amount).result_price

        if not self.buy_order_placed:
            maker_buy_price = self.taker_sell_price * Decimal(1 - self.spread / 100)
            buy_order = PerpetualOrderCandidate(trading_pair=self.maker_pair,
                                                is_maker=True,
                                                order_type=OrderType.LIMIT,
                                                order_side=TradeType.BUY,
                                                amount=Decimal(self.order_amount),
                                                price=maker_buy_price,
                                                leverage=self.leverage)
            buy_order_adjusted = self.connector_taker.budget_checker.adjust_candidate(buy_order, all_or_none=True)
            if buy_order_adjusted.amount > Decimal("0"):
                self.buy_order_placed = True
                self.buy(self.maker_exchange, self.maker_pair, buy_order_adjusted.amount, buy_order_adjusted.order_type,
                         buy_order_adjusted.price, PositionAction.OPEN)


        if not self.sell_order_placed:
            maker_sell_price = self.taker_buy_price * Decimal(1 + self.spread / 100)
            sell_order = PerpetualOrderCandidate(trading_pair=self.maker_pair,
                                                 is_maker=True,
                                                 order_type=OrderType.LIMIT,
                                                 order_side=TradeType.SELL,
                                                 amount=Decimal(self.order_amount),
                                                 price=maker_sell_price,
                                                 leverage=self.leverage)
            sell_order_adjusted = self.connector_taker.budget_checker.adjust_candidate(sell_order, all_or_none=True)
            if sell_order_adjusted.amount > Decimal("0"):
                self.sell_order_placed = True
                self.sell(self.maker_exchange, self.maker_pair, sell_order_adjusted.amount,
                          sell_order_adjusted.order_type,
                          sell_order_adjusted.price, PositionAction.OPEN)


        active_orders = self.get_active_orders(connector_name=self.maker_exchange)

        # limit_orders = self.connector_maker.limit_orders
        # self.logger().info(f"active_orders: {active_orders}")
        # self.logger().info(f"limit_orders: {limit_orders}")
        for order in self.connector_maker.limit_orders:
            # cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
            if order.is_buy:
                buy_cancel_threshold_upper = self.taker_sell_price * Decimal(1 - self.min_spread / 100)
                buy_cancel_threshold_lower = self.taker_sell_price * Decimal(1 - self.max_spread / 100)
                if order.price > buy_cancel_threshold_upper or order.price < buy_cancel_threshold_lower:
                    self.logger().info(
                        f"Cancelling buy order: {order.client_order_id} out of range ({buy_cancel_threshold_lower} - {buy_cancel_threshold_upper})")
                    self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)
                    self.buy_order_placed = False
            else:
                sell_cancel_threshold_lower = self.taker_buy_price * Decimal(1 + self.min_spread / 100)
                sell_cancel_threshold_upper = self.taker_buy_price * Decimal(1 + self.max_spread / 100)

                if order.price > sell_cancel_threshold_upper or order.price < sell_cancel_threshold_lower:
                    self.logger().info(f"Cancelling sell order: {order.client_order_id}")
                    self.cancel(self.maker_exchange, order.trading_pair, order.client_order_id)
                    self.sell_order_placed = False
        return

    # def is_active_maker_order(self, event: OrderFilledEvent):
    #     """
    #     Helper function that checks if order is an active order on the maker exchange
    #     """
    #     for order in self.get_active_orders(connector_name=self.maker_exchange):
    #         if order.client_order_id == event.order_id:
    #             return True
    #     return False

    def did_fill_order(self, event: OrderFilledEvent):
        mid_price = self.connector_maker.get_mid_price(self.maker_pair)
        if event.trade_type == TradeType.BUY:
            self.taker_sell_price = self.connector_taker.get_price_for_volume(self.taker_pair, False,
                                                                              self.order_amount).result_price
            sell_price_with_slippage = self.taker_sell_price * Decimal(1 - self.slippage_buffer / 100)
            self.logger().info(f"Filled maker buy order with price: {event.price}")
            self.logger().info(f"Sending taker sell order at price: {self.taker_sell_price}")
            sell_order = PerpetualOrderCandidate(trading_pair=self.taker_pair,
                                                 is_maker=False,
                                                 order_type=OrderType.LIMIT,
                                                 order_side=TradeType.SELL,
                                                 amount=Decimal(event.amount),
                                                 price=sell_price_with_slippage,
                                                 leverage=self.leverage)
            sell_order_adjusted = self.connector_taker.budget_checker.adjust_candidate(sell_order, all_or_none=False)
            if sell_order_adjusted.amount > Decimal("0"):
                self.sell(self.taker_exchange, self.taker_pair, sell_order_adjusted.amount,
                          sell_order_adjusted.order_type,
                          sell_order_adjusted.price, PositionAction.OPEN)
        else:
            if event.trade_type == TradeType.SELL:
                self.taker_buy_price = self.connector_taker.get_price_for_volume(self.taker_pair, True,
                                                                                 self.order_amount).result_price
                buy_price_with_slippage = self.taker_buy_price * Decimal(1 + self.slippage_buffer / 100)
                self.logger().info(f"Filled maker sell order at price: {event.price}")
                self.logger().info(f"Sending taker buy order: {self.taker_buy_price}")
                buy_order = PerpetualOrderCandidate(trading_pair=self.taker_pair,
                                                    is_maker=False,
                                                    order_type=OrderType.LIMIT,
                                                    order_side=TradeType.BUY,
                                                    amount=Decimal(event.amount),
                                                    price=buy_price_with_slippage,
                                                    leverage=self.leverage)
                buy_order_adjusted = self.connector_taker.budget_checker.adjust_candidate(buy_order, all_or_none=False)
                self.buy(self.taker_exchange, self.taker_pair, buy_order_adjusted.amount, buy_order_adjusted.order_type,
                         buy_order_adjusted.price, buy_order_adjusted.price, PositionAction.OPEN)

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Min price", "Max price", "Spread", "Age"]
        data = []
        mid_price = self.connector_maker.get_mid_price(self.maker_pair)
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                if order.is_buy:
                    upper_price = self.taker_sell_price * Decimal(1 - self.min_spread / 100)
                    lower_price = self.taker_sell_price * Decimal(1 - self.max_spread / 100)
                    spread_mid = (mid_price - order.price) / mid_price * 100
                else:
                    upper_price = self.taker_buy_price * Decimal(1 + self.max_spread / 100)
                    lower_price = self.taker_buy_price * Decimal(1 + self.min_spread / 100)
                    spread_mid = (order.price - mid_price) / mid_price * 100

                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    connector_name,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    float(lower_price),
                    float(upper_price),
                    float(round(spread_mid, 2)),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        # lines.extend(["", "  Strategy status:"] + ["    " + self.status])

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)
