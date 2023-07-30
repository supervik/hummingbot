from decimal import Decimal
from enum import Enum

import pandas as pd

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionSide, PriceType, PositionMode, TradeType, PositionAction
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, SellOrderCreatedEvent, OrderFilledEvent
from hummingbot.smart_components.position_executor.data_types import PositionConfig, PositionExecutorStatus, \
    TrackedOrder
from hummingbot.smart_components.position_executor.position_executor import PositionExecutor
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class BotStatus(Enum):
    NOT_INIT = 1
    NOT_ACTIVE = 2
    ACTIVE_PRICE_LEVELS = 3
    POSITION_OPEN = 4
    TIMEOUT = 5


class BostonBot(ScriptStrategyBase):
    """

    """
    # config parameters
    exchange: str = "binance_perpetual"
    trading_pair: str = "ETH-USDT"

    size_long_usdt = Decimal("15")
    size_short_usdt = Decimal("12")
    trailing_long_percentage = Decimal("0.01")
    trailing_short_percentage = Decimal("0.2")
    check_trailing_sec = 30

    stop_loss_long_percentage = Decimal("0.1")
    stop_loss_short_percentage = Decimal("0.1")
    leverage = 10
    # use_time_frame = True
    check_stop_loss_sec = 20
    timeout_sec = 60

    # class parameters
    status = BotStatus.NOT_INIT
    trailing_price_long = Decimal("0")
    trailing_price_short = Decimal("0")
    trailing_price_update_timestamp = 0
    stop_loss_price_update_timestamp = 0
    next_cycle_timestamp = 0
    rounding_digits = 2
    previous_price = Decimal("0")
    check_num = 0
    filled_order_log_printed = False
    account_positions = None
    filled_position_side = None
    dry_run = False
    long_level_order_id = None
    short_level_order_id = None
    stop_loss_order_id = None
    stop_loss_order_price = Decimal("0")
    open_order: TrackedOrder = TrackedOrder()
    close_order: TrackedOrder = TrackedOrder()
    server_time = 0

    markets = {exchange: {trading_pair}}

    @property
    def connector(self):
        """
        The maker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.exchange]

    def on_tick(self):
        if self.status == BotStatus.NOT_ACTIVE:
            return

        self.account_positions = self.connector.account_positions

        if self.status == BotStatus.NOT_INIT:
            self.init_strategy()
            return

        self.server_time = self.current_timestamp
        # time_current = self.current_timestamp
        # new_time = self.connector._time_synchronizer
        # self.notify_app_and_log(f"time() = {new_time.time()}, offset = {new_time.time_offset_ms}, self.current_timestamp = {self.current_timestamp}")
        # return

        if self.status == BotStatus.TIMEOUT:
            if self.current_timestamp < self.next_cycle_timestamp:
                return
            if self.is_open_positions_and_orders():
                self.status = BotStatus.NOT_ACTIVE
                return
            self.status = BotStatus.ACTIVE_PRICE_LEVELS
            self.update_params_for_new_cycle()

        # set stop orders with price levels above and below the last price
        if self.status == BotStatus.ACTIVE_PRICE_LEVELS:
            if self.current_timestamp > self.trailing_price_update_timestamp:
                self.trailing_price_update_timestamp = self.current_timestamp + self.check_trailing_sec
                self.cancel_all_orders()
                self.update_price_levels()

        # manage open positions
        if self.status == BotStatus.POSITION_OPEN:
            if not self.filled_order_log_printed:
                self.output_filled_order_info()
                self.filled_order_log_printed = True
                return
            if not len(self.account_positions):
                self.logger().info("No open positions found")
                self.cancel_all_orders()
            else:
                if self.current_timestamp > self.stop_loss_price_update_timestamp:
                    self.update_stop_loss_orders()
                    self.check_num += 1

    def init_strategy(self):
        if len(self.account_positions):
            self.notify_app_and_log(f"There are open positions. Bot status is NOT_ACTIVE")
            self.status = BotStatus.NOT_ACTIVE
            return

        self.set_leverage()
        # self.set_position_mode()
        base, quote = split_hb_trading_pair(self.trading_pair)
        balance = round(self.connector.get_balance(quote), self.rounding_digits)
        self.notify_app_and_log(f"Start of Boston bot | Available balance: {balance} {quote}")
        self.status = BotStatus.ACTIVE_PRICE_LEVELS
        return

    def set_leverage(self):
        self.connector.set_leverage(trading_pair=self.trading_pair, leverage=self.leverage)

    # def set_position_mode(self):
    #     self.logger().info(f"{self.connector.position_mode}")
    #     if self.connector.position_mode != PositionMode.ONEWAY:
    #         self.logger().info(f"{self.connector.position_mode} This strategy supports only Oneway position mode. Attempting to switch ...")
    #     self.connector.set_position_mode(PositionMode.ONEWAY)

    def update_price_levels(self):
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        amount_long = self.connector.quantize_order_amount(self.trading_pair, self.size_long_usdt / last_price)
        amount_short = self.connector.quantize_order_amount(self.trading_pair, self.size_short_usdt / last_price)

        self.trailing_price_long = last_price * (1 + self.trailing_long_percentage / Decimal("100"))
        self.trailing_price_short = last_price * (1 - self.trailing_short_percentage / Decimal("100"))

        candidate_long = PerpetualOrderCandidate(trading_pair=self.trading_pair, is_maker=True,
                                                 order_type=OrderType.STOP_MARKET,
                                                 order_side=TradeType.BUY, amount=amount_long,
                                                 price=self.trailing_price_long, leverage=Decimal(self.leverage))
        candidate_short = PerpetualOrderCandidate(trading_pair=self.trading_pair, is_maker=True,
                                                  order_type=OrderType.STOP_MARKET,
                                                  order_side=TradeType.SELL, amount=amount_short,
                                                  price=self.trailing_price_short, leverage=Decimal(self.leverage))
        candidate_long_adjusted = self.connector.budget_checker.adjust_candidate(candidate_long, all_or_none=True)
        candidate_short_adjusted = self.connector.budget_checker.adjust_candidate(candidate_short, all_or_none=True)

        self.notify_app_and_log(f"{self.trading_pair} {round(last_price, self.rounding_digits)} | "
                                f"Check trailing {self.check_trailing_sec} sec")
        self.long_level_order_id = self.send_order_to_exchange(candidate_long_adjusted, PositionAction.OPEN)
        self.short_level_order_id = self.send_order_to_exchange(candidate_short_adjusted, PositionAction.OPEN)

    def update_stop_loss_orders(self):
        self.cancel_all_orders()
        self.stop_loss_price_update_timestamp = self.current_timestamp + self.check_stop_loss_sec
        current_price = round(self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade), self.rounding_digits)
        self.logger().info(f"Check prices. Previous price = {self.previous_price}. Current price = {current_price}")

        if self.filled_position_side == TradeType.BUY:
            if self.previous_price <= current_price or self.check_num == 0:
                self.logger().info("Update stop loss price for long position")
                self.stop_loss_order_price = current_price * (1 - self.stop_loss_long_percentage / Decimal("100"))
                self.logger().info(f"sl_price_updated = {self.stop_loss_order_price}")

                candidate_short = PerpetualOrderCandidate(trading_pair=self.trading_pair,
                                                          is_maker=True,
                                                          order_type=OrderType.STOP_MARKET,
                                                          order_side=TradeType.SELL,
                                                          amount=Decimal("0"),
                                                          price=self.stop_loss_order_price,
                                                          leverage=Decimal(self.leverage))
                self.stop_loss_order_id = self.send_order_to_exchange(candidate_short, PositionAction.CLOSE)

                if self.check_num == 0:
                    self.notify_app_and_log(f"Long +{self.trailing_long_percentage}% | "
                                            f"SL update: {self.stop_loss_long_percentage}%: "
                                            f"{round(self.stop_loss_order_price, self.rounding_digits)} | "
                                            f"Check in {self.check_stop_loss_sec} sec")
                else:
                    self.notify_app_and_log(f"Check no.{self.check_num} Long +{self.trailing_long_percentage}% | "
                                            f"Previous price: {round(self.previous_price, self.rounding_digits)} <= "
                                            f"Current price: {round(current_price, self.rounding_digits)} | "
                                            f"SL: {self.stop_loss_long_percentage}%: "
                                            f"{round(self.stop_loss_order_price, self.rounding_digits)} | "
                                            f"Check in {self.check_stop_loss_sec} sec")
                    self.previous_price = current_price
            else:
                self.logger().info("Price changed. Close long position")
                candidate_short = PerpetualOrderCandidate(trading_pair=self.trading_pair,
                                                          is_maker=False,
                                                          order_type=OrderType.MARKET,
                                                          order_side=TradeType.SELL,
                                                          amount=self.open_order.executed_amount_base,
                                                          price=Decimal("NaN"),
                                                          leverage=Decimal(self.leverage))
                self.stop_loss_order_id = self.send_order_to_exchange(candidate_short, PositionAction.CLOSE)
                self.notify_app_and_log(f"Check no.{self.check_num} Long +{self.trailing_long_percentage}% | "
                                        f"Previous price: {round(self.previous_price, self.rounding_digits)} > "
                                        f"Current price: {round(current_price, self.rounding_digits)} | "
                                        f"Close with Market price")

        else:
            if self.previous_price >= current_price or self.check_num == 0:
                self.logger().info("Update stop loss price for short position")
                self.stop_loss_order_price = current_price * (1 + self.stop_loss_short_percentage / Decimal("100"))
                self.logger().info(f"sl_price_updated = {self.stop_loss_order_price}")

                candidate_long = PerpetualOrderCandidate(trading_pair=self.trading_pair, is_maker=True,
                                                         order_type=OrderType.STOP_MARKET,
                                                         order_side=TradeType.BUY, amount=Decimal("0"),
                                                         price=self.stop_loss_order_price,
                                                         leverage=Decimal(self.leverage))
                self.stop_loss_order_id = self.send_order_to_exchange(candidate_long, PositionAction.CLOSE)

                if self.check_num == 0:
                    self.notify_app_and_log(f"Short -{self.trailing_short_percentage}% | "
                                            f"SL update: {self.stop_loss_short_percentage}%: "
                                            f"{round(self.stop_loss_order_price, self.rounding_digits)} | "
                                            f"Check in {self.check_stop_loss_sec} sec")
                else:
                    self.notify_app_and_log(f"Check no.{self.check_num} Short -{self.trailing_short_percentage}% | "
                                            f"Previous price: {round(self.previous_price, self.rounding_digits)} >= "
                                            f"Current price: {round(current_price, self.rounding_digits)} | "
                                            f"SL: {self.stop_loss_short_percentage}%: "
                                            f"{round(self.stop_loss_order_price, self.rounding_digits)} | "
                                            f"Check in {self.check_stop_loss_sec} sec")
                    self.previous_price = current_price
            else:
                self.logger().info("Price changed. Close short position")
                candidate_long = PerpetualOrderCandidate(trading_pair=self.trading_pair,
                                                         is_maker=False,
                                                         order_type=OrderType.MARKET,
                                                         order_side=TradeType.BUY,
                                                         amount=self.open_order.executed_amount_base,
                                                         price=Decimal("NaN"),
                                                         leverage=Decimal(self.leverage))
                self.stop_loss_order_id = self.send_order_to_exchange(candidate_long, PositionAction.CLOSE)
                self.notify_app_and_log(f"Check no.{self.check_num} Short -{self.trailing_short_percentage}% | "
                                        f"Previous price: {round(self.previous_price, self.rounding_digits)} < "
                                        f"Current price: {round(current_price, self.rounding_digits)} | "
                                        f"Close with Market price")

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """
        Logs any info about created orders
        """
        if self.status == BotStatus.ACTIVE_PRICE_LEVELS:
            amount_usd = round(event.amount * event.price, 2)
            margin_usd = round(amount_usd / self.leverage, 2)
            self.notify_app_and_log(f"Long +{self.trailing_long_percentage}% | "
                                    f"Price: {event.price} | Size: {event.amount} (${amount_usd}) | "
                                    f"margin ${margin_usd} | SL: {self.stop_loss_long_percentage}%")

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """
        Logs any info about created orders
        """
        if self.status == BotStatus.ACTIVE_PRICE_LEVELS:
            amount_usd = round(event.amount * event.price, 2)
            margin_usd = round(amount_usd / self.leverage, 2)
            self.notify_app_and_log(f"Short -{self.trailing_short_percentage}% | "
                                    f"Price: {event.price} | Size: {event.amount} (${amount_usd}) | "
                                    f"margin ${margin_usd} | SL: {self.stop_loss_short_percentage}%")

    def did_fill_order(self, event: OrderFilledEvent):
        if event.order_id in (self.long_level_order_id, self.short_level_order_id):
            self.status = BotStatus.POSITION_OPEN
            self.cancel_all_orders()
            self.filled_position_side = TradeType.BUY if event.order_id == self.long_level_order_id else TradeType.SELL
            self.open_order = self.connector._order_tracker.fetch_order(event.order_id)
        elif event.order_id == self.stop_loss_order_id:
            self.logger().info(f"Stop loss filled or close with market order filled")
            self.next_cycle_timestamp = self.current_timestamp + self.timeout_sec
            self.status = BotStatus.TIMEOUT
            self.close_order = self.connector._order_tracker.fetch_order(event.order_id)
            self.finalize_position()
        else:
            self.logger().info(f"Unknown order is filled. Event data = {event}")

    def update_params_for_new_cycle(self):
        self.check_num = 0
        self.filled_order_log_printed = False
        self.trailing_price_update_timestamp = 0
        self.stop_loss_price_update_timestamp = 0

    def is_open_positions_and_orders(self):
        if len(self.account_positions):
            self.logger().info(f"There are open positions. Stop trading.")
            return True
        if len(self.get_active_orders(self.exchange)):
            self.logger().info(f"There are open orders. Stop trading.")
            return True
        return False

    def output_filled_order_info(self):
        if len(self.account_positions) > 0:
            self.logger().info(f"account positions = {self.account_positions}")
            self.previous_price = self.open_order.average_executed_price
            if self.filled_position_side == TradeType.BUY:
                self.notify_app_and_log(f"Long +{self.trailing_long_percentage}% | Filled 100% | "
                                        f"Entry price: {self.open_order.average_executed_price} | "
                                        f"Check in {self.check_stop_loss_sec} sec")
            else:
                self.notify_app_and_log(f"Short -{self.trailing_short_percentage}% | Filled 100% | "
                                        f"Entry price: {self.open_order.average_executed_price} | "
                                        f"Check in {self.check_stop_loss_sec} sec")
        else:
            self.logger().info(f"No open positions found. Trying again.")

    def finalize_position(self):
        base, quote = split_hb_trading_pair(self.trading_pair)
        balance = round(self.connector.get_balance(quote), self.rounding_digits)
        side = "Long" if self.filled_position_side == TradeType.BUY else "Short"
        entry_price = self.open_order.average_executed_price
        close_price = self.close_order.average_executed_price
        executed_amount_quote = self.open_order.executed_amount_base * entry_price
        pnl = (close_price - entry_price) / entry_price if self.filled_position_side == TradeType.BUY \
            else (entry_price - close_price) / entry_price
        pnl_quote = pnl * executed_amount_quote
        # fees = self.open_order.cum_fees + self.close_order.cum_fees

        self.notify_app_and_log(
            f"{side} | Closed | Entry price: {entry_price} | "
            f"Close price: {close_price} | Size,$: {executed_amount_quote} | "
            f"pnl,%: {round(pnl, 4)} | pnl,$: {round(pnl_quote, 4)}")
        self.notify_app_and_log(f"Available balance: {balance} {quote}")
        self.notify_app_and_log(f"Meow | Pause {self.timeout_sec} sec")

    def send_order_to_exchange(self, candidate, position_action):
        if self.dry_run:
            return 0

        if candidate.order_side == TradeType.BUY:
            order_id = self.buy(self.exchange, candidate.trading_pair, candidate.amount,
                                candidate.order_type, candidate.price, position_action)
        else:
            order_id = self.sell(self.exchange, candidate.trading_pair, candidate.amount,
                                 candidate.order_type, candidate.price, position_action)

        return order_id

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.exchange):
            self.cancel(self.exchange, order.trading_pair, order.client_order_id)

    def notify_app_and_log(self, msg):
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["", "  Bot status", f"    {self.status.name}"])
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        if self.status == BotStatus.ACTIVE_PRICE_LEVELS:
            update_sec = self.trailing_price_update_timestamp - self.current_timestamp
            lines.extend(["", f"  Trailing price long:   {round(self.trailing_price_long, self.rounding_digits)}"])
            lines.extend([f"  Trailing price short:  {round(self.trailing_price_short, self.rounding_digits)}"])
            lines.extend([f"  Last price:  {round(last_price, self.rounding_digits)}"])
            lines.extend([f"  Next price check in:  {update_sec} sec"])

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
        columns = ["Market", "Pair", "Side", "Price", "Size", "Age"]
        data = []
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    connector_name,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        return df
