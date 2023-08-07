import math
from datetime import datetime
from decimal import Decimal
from enum import Enum

import pandas as pd

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionSide, PriceType, PositionMode, TradeType, PositionAction
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, SellOrderCreatedEvent, OrderFilledEvent, \
    OrderCancelledEvent
from hummingbot.smart_components.position_executor.data_types import TrackedOrder
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class BotStatus(Enum):
    NOT_INIT = 1
    NOT_ACTIVE = 2
    ACTIVE_PRICE_LEVELS = 3
    POSITION_OPEN = 4
    TIMEOUT = 5


def next_timeframe(current_timestamp, interval) -> int:
    """
    The helper function that calculates the next timeframe timestamp
    """
    # Calculate the number of intervals that have passed since Unix Epoch
    intervals_passed = math.ceil(current_timestamp / interval)
    # Calculate the start of the next interval
    next_timestamp = intervals_passed * interval

    return next_timestamp


class BostonBot(ScriptStrategyBase):
    """
    - The bot places long and short orders below or under the last market price
      - If reverse_mode set to False bot places long +% and Short -% of the current price
      - If reverse_mode set to True bot places long -% and Short +% of the current price
    - The bot updates the order prices every check_trailing_sec seconds timeframe
    - If an order is filled into the position, the opposite order is closed and the filled order stop loss is set
    - Every check_stop_loss_sec the bot compares the current and the previous price and close the position if it goes
      into unprofitable direction
    - If position remains open every check_stop_loss_sec seconds stop loss order is updated
    """
    # config parameters
    exchange: str = "binance_perpetual"
    trading_pair: str = "ETH-USDT"
    reverse_mode = True

    size_long_usdt = Decimal("15")
    size_short_usdt = Decimal("12")
    trailing_long_percentage = Decimal("0.001")
    trailing_short_percentage = Decimal("0.01")
    check_trailing_sec = 60

    stop_loss_long_percentage = Decimal("0.5")
    stop_loss_short_percentage = Decimal("0.5")
    check_stop_loss_sec = 120
    leverage = 10
    timeout_sec = 60
    rounding_digits = 2

    # class parameters
    status = BotStatus.NOT_INIT
    trailing_price_long = Decimal("0")
    trailing_price_short = Decimal("0")
    trailing_price_update_timestamp = 0
    stop_loss_update_timestamp = 0
    next_cycle_timestamp = 0
    previous_price = Decimal("0")
    current_price = Decimal("0")
    check_num = 0
    log_filled_order_printed = False
    log_trailing_price_printed = False
    log_stop_loss_printed = False
    log_finalize_position_printed = False
    account_positions = None
    filled_position_side = None
    long_placed = False
    short_placed = False
    sl_placed = False
    long_level_order_id = None
    short_level_order_id = None
    stop_loss_order_id = None
    close_order_id = None
    stop_loss_order_price = Decimal("0")
    open_order: TrackedOrder = TrackedOrder()
    close_order: TrackedOrder = TrackedOrder()
    server_time = 0
    long_sign = ""
    short_sign = ""
    dry_run = False

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
        self.server_time = self.connector._time_synchronizer.time()

        if self.status == BotStatus.NOT_INIT:
            self.init_strategy()
            return

        if self.status == BotStatus.TIMEOUT:
            if not self.log_finalize_position_printed:
                self.finalize_position()
            if self.current_timestamp < self.next_cycle_timestamp:
                return
            if self.is_open_positions_and_orders():
                self.status = BotStatus.NOT_ACTIVE
                return
            self.status = BotStatus.ACTIVE_PRICE_LEVELS
            self.update_params_for_new_cycle()

        # set stop orders with price levels above and below the last price
        if self.status == BotStatus.ACTIVE_PRICE_LEVELS:
            if self.server_time > self.trailing_price_update_timestamp - 1 and not self.log_trailing_price_printed:
                self.log_trailing_price()
                self.cancel_all_orders()
                self.log_trailing_price_printed = True
            if self.server_time > self.trailing_price_update_timestamp:
                if self.long_placed or self.short_placed:
                    self.logger().info("OrderCancelledEvent for long or short order hasn't arrived yet")
                    self.cancel_all_orders()
                    return
                self.trailing_price_update_timestamp = next_timeframe(self.server_time + 1, self.check_trailing_sec)
                self.update_price_levels()
                self.log_trailing_price_printed = False

        # manage open positions
        if self.status == BotStatus.POSITION_OPEN:
            if not self.log_filled_order_printed:
                self.log_filled_order()
                self.log_filled_order_printed = True
                return
            open_positions = [position for position in self.account_positions.values() if position.trading_pair == self.trading_pair]
            if not len(open_positions):
                self.logger().info("No open positions found")
                self.cancel_all_orders()
            else:
                if self.server_time > self.stop_loss_update_timestamp - 1 and not self.log_stop_loss_printed:
                    self.cancel_all_orders()
                    self.log_stop_loss_printed = True
                if self.server_time > self.stop_loss_update_timestamp:
                    if self.sl_placed:
                        self.logger().info("OrderCancelledEvent for SL hasn't arrived yet")
                        self.cancel_all_orders()
                        return
                    self.stop_loss_update_timestamp = next_timeframe(self.server_time + 1, self.check_stop_loss_sec)
                    self.update_stop_loss_orders()
                    if self.check_num != 0:
                        self.check_num += 1
                    self.log_stop_loss_printed = False

    def init_strategy(self):
        for position in self.account_positions.values():
            if position.trading_pair == self.trading_pair:
                self.notify_app_and_log(f"There are open positions on {self.trading_pair}. Bot status is NOT_ACTIVE")
                self.status = BotStatus.NOT_ACTIVE
                return

        self.set_leverage()
        self.check_and_set_position_mode_one_way()
        self.trailing_price_update_timestamp = next_timeframe(self.server_time, self.check_trailing_sec)
        trailing_price_update_str = datetime.fromtimestamp(self.trailing_price_update_timestamp).strftime("%H:%M:%S")

        base, quote = split_hb_trading_pair(self.trading_pair)
        balance = round(self.connector.get_balance(quote), self.rounding_digits)
        mode = "REVERSE MODE |" if self.reverse_mode else ""
        self.long_sign = "-" if self.reverse_mode else "+"
        self.short_sign = "+" if self.reverse_mode else "-"
        self.notify_app_and_log(f"Start of Boston bot | {mode} Available balance: {balance} {quote} | Check trailing {trailing_price_update_str} ")
        self.status = BotStatus.ACTIVE_PRICE_LEVELS
        return

    def set_leverage(self):
        self.connector.set_leverage(trading_pair=self.trading_pair, leverage=self.leverage)

    def check_and_set_position_mode_one_way(self):
        self.logger().info(f"{self.connector.position_mode}")
        if self.connector.position_mode != PositionMode.ONEWAY:
            self.logger().info(f"{self.connector.position_mode} This strategy supports "
                               f"only Oneway position mode. Attempting to switch ...")
        self.connector.set_position_mode(PositionMode.ONEWAY)

    def log_trailing_price(self):
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        trailing_price_update_str = datetime.fromtimestamp(self.trailing_price_update_timestamp).strftime("%H:%M:%S")
        self.notify_app_and_log(f"{self.trading_pair} {round(last_price, self.rounding_digits)} | "
                                f"Check trailing {self.check_trailing_sec} sec: {trailing_price_update_str}")

    def update_price_levels(self):
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        amount_long = self.connector.quantize_order_amount(self.trading_pair, self.size_long_usdt / last_price)
        amount_short = self.connector.quantize_order_amount(self.trading_pair, self.size_short_usdt / last_price)

        if self.reverse_mode:
            self.trailing_price_long = last_price * (1 - self.trailing_long_percentage / Decimal("100"))
            self.trailing_price_short = last_price * (1 + self.trailing_short_percentage / Decimal("100"))
        else:
            self.trailing_price_long = last_price * (1 + self.trailing_long_percentage / Decimal("100"))
            self.trailing_price_short = last_price * (1 - self.trailing_short_percentage / Decimal("100"))

        order_type = OrderType.LIMIT if self.reverse_mode else OrderType.STOP_MARKET

        candidate_long = PerpetualOrderCandidate(trading_pair=self.trading_pair, is_maker=True,
                                                 order_type=order_type,
                                                 order_side=TradeType.BUY, amount=amount_long,
                                                 price=self.trailing_price_long, leverage=Decimal(self.leverage))
        candidate_short = PerpetualOrderCandidate(trading_pair=self.trading_pair, is_maker=True,
                                                  order_type=order_type,
                                                  order_side=TradeType.SELL, amount=amount_short,
                                                  price=self.trailing_price_short, leverage=Decimal(self.leverage))
        candidate_long_adjusted = self.connector.budget_checker.adjust_candidate(candidate_long, all_or_none=True)
        candidate_short_adjusted = self.connector.budget_checker.adjust_candidate(candidate_short, all_or_none=True)

        self.long_level_order_id = self.send_order_to_exchange(candidate_long_adjusted, PositionAction.OPEN)
        self.short_level_order_id = self.send_order_to_exchange(candidate_short_adjusted, PositionAction.OPEN)

    def log_stop_loss(self):
        current_price = round(self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade),
                              self.rounding_digits)
        stop_loss_update_str = datetime.fromtimestamp(self.stop_loss_update_timestamp).strftime("%H:%M:%S")
        self.notify_app_and_log(f"{self.trading_pair} {round(current_price, self.rounding_digits)} | "
                                f"Check no {self.check_num}: {stop_loss_update_str}")

    def update_stop_loss_orders(self):
        self.current_price = round(self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade), self.rounding_digits)
        self.logger().info(f"Check prices. Previous price = {self.previous_price}. Current price = {self.current_price}")

        if self.filled_position_side == TradeType.BUY:
            if self.previous_price <= self.current_price or self.check_num == 0:
                self.logger().info("Update stop loss price for long position")
                self.stop_loss_order_price = self.current_price * (1 - self.stop_loss_long_percentage / Decimal("100"))
                self.logger().info(f"sl_price_updated = {self.stop_loss_order_price}")

                candidate_short = PerpetualOrderCandidate(trading_pair=self.trading_pair,
                                                          is_maker=True,
                                                          order_type=OrderType.STOP_MARKET,
                                                          order_side=TradeType.SELL,
                                                          amount=Decimal("0"),
                                                          price=self.stop_loss_order_price,
                                                          leverage=Decimal(self.leverage))
                self.stop_loss_order_id = self.send_order_to_exchange(candidate_short, PositionAction.CLOSE)
            else:
                self.logger().info("Price changed. Close long position")
                candidate_short = PerpetualOrderCandidate(trading_pair=self.trading_pair,
                                                          is_maker=False,
                                                          order_type=OrderType.MARKET,
                                                          order_side=TradeType.SELL,
                                                          amount=self.open_order.executed_amount_base,
                                                          price=Decimal("NaN"),
                                                          leverage=Decimal(self.leverage))
                self.close_order_id = self.send_order_to_exchange(candidate_short, PositionAction.CLOSE)
        else:
            if self.previous_price >= self.current_price or self.check_num == 0:
                self.logger().info("Update stop loss price for short position")
                self.stop_loss_order_price = self.current_price * (1 + self.stop_loss_short_percentage / Decimal("100"))
                self.logger().info(f"sl_price_updated = {self.stop_loss_order_price}")

                candidate_long = PerpetualOrderCandidate(trading_pair=self.trading_pair, is_maker=True,
                                                         order_type=OrderType.STOP_MARKET,
                                                         order_side=TradeType.BUY, amount=Decimal("0"),
                                                         price=self.stop_loss_order_price,
                                                         leverage=Decimal(self.leverage))
                self.stop_loss_order_id = self.send_order_to_exchange(candidate_long, PositionAction.CLOSE)
            else:
                self.logger().info("Price changed. Close short position")
                candidate_long = PerpetualOrderCandidate(trading_pair=self.trading_pair,
                                                         is_maker=False,
                                                         order_type=OrderType.MARKET,
                                                         order_side=TradeType.BUY,
                                                         amount=self.open_order.executed_amount_base,
                                                         price=Decimal("NaN"),
                                                         leverage=Decimal(self.leverage))
                self.close_order_id = self.send_order_to_exchange(candidate_long, PositionAction.CLOSE)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """
        Logs any info about created orders
        """
        if event.order_id == self.short_level_order_id:
            self.short_placed = True
            amount_usd = round(event.amount * event.price, 2)
            margin_usd = round(amount_usd / self.leverage, 2)
            self.notify_app_and_log(f"Short {self.short_sign}{self.trailing_short_percentage}% | "
                                    f"Price: {event.price} | Size: {event.amount} (${amount_usd}) | "
                                    f"margin ${margin_usd} | SL: {self.stop_loss_short_percentage}%")
        elif event.order_id == self.stop_loss_order_id:
            self.sl_placed = True
            stop_loss_update_str = datetime.fromtimestamp(self.stop_loss_update_timestamp).strftime("%H:%M:%S")
            if self.check_num == 0:
                self.check_num = 1
                self.notify_app_and_log(f"Long {self.long_sign}{self.trailing_long_percentage}% | "
                                        f"SL update: {self.stop_loss_long_percentage}%: {event.price} | "
                                        f"Next check No.1 {stop_loss_update_str}")
            else:
                self.notify_app_and_log(f"Check no.{self.check_num-1} Long {self.long_sign}{self.trailing_long_percentage}% | "
                                        f"Previous price: {round(self.previous_price, self.rounding_digits)} <= "
                                        f"Current price: {round(self.current_price, self.rounding_digits)} | "
                                        f"SL update: {self.stop_loss_long_percentage}%: {event.price} | "
                                        f"Next check No.{self.check_num} {stop_loss_update_str}")
            self.previous_price = self.current_price
        elif event.order_id == self.close_order_id:
            self.notify_app_and_log(f"Check no.{self.check_num-1} Long {self.long_sign}{self.trailing_long_percentage}% | "
                                    f"Previous price: {round(self.previous_price, self.rounding_digits)} > "
                                    f"Current price: {round(self.current_price, self.rounding_digits)} | "
                                    f"Close with Market price")

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """
        Logs any info about created orders
        """
        if event.order_id == self.long_level_order_id:
            self.long_placed = True
            amount_usd = round(event.amount * event.price, 2)
            margin_usd = round(amount_usd / self.leverage, 2)
            self.notify_app_and_log(f"Long {self.long_sign}{self.trailing_long_percentage}% | "
                                    f"Price: {event.price} | Size: {event.amount} (${amount_usd}) | "
                                    f"margin ${margin_usd} | SL: {self.stop_loss_long_percentage}%")
        elif event.order_id == self.stop_loss_order_id:
            self.sl_placed = True
            stop_loss_update_str = datetime.fromtimestamp(self.stop_loss_update_timestamp).strftime("%H:%M:%S")
            if self.check_num == 0:
                self.check_num = 1
                self.notify_app_and_log(f"Short {self.short_sign}{self.trailing_short_percentage}% | "
                                        f"SL update: {self.stop_loss_short_percentage}%: {event.price} | "
                                        f"Next check No.1 {stop_loss_update_str}")
            else:
                self.notify_app_and_log(f"Check no.{self.check_num-1} Short {self.short_sign}"
                                        f"{self.trailing_short_percentage}% | "
                                        f"Previous price: {round(self.previous_price, self.rounding_digits)} >= "
                                        f"Current price: {round(self.current_price, self.rounding_digits)} | "
                                        f"SL update: {self.stop_loss_short_percentage}%: {event.price} | "
                                        f"Next check No.{self.check_num} {stop_loss_update_str}")
            self.previous_price = self.current_price
        elif event.order_id == self.close_order_id:
            self.notify_app_and_log(f"Check no.{self.check_num-1} Short {self.short_sign}{self.trailing_short_percentage}% | "
                                    f"Previous price: {round(self.previous_price, self.rounding_digits)} < "
                                    f"Current price: {round(self.current_price, self.rounding_digits)} | "
                                    f"Close with Market price")

    def did_cancel_order(self, event: OrderCancelledEvent):
        if event.order_id == self.long_level_order_id:
            self.long_placed = False
            self.notify_app_and_log(f"Long {self.long_sign}{self.trailing_long_percentage}% cancelled ")
        elif event.order_id == self.short_level_order_id:
            self.short_placed = False
            self.notify_app_and_log(f"Short {self.short_sign}{self.trailing_short_percentage}% cancelled ")
        elif event.order_id == self.stop_loss_order_id:
            self.sl_placed = False
            self.notify_app_and_log(f"Stop Loss order cancelled")
        elif event.order_id == self.close_order_id:
            self.notify_app_and_log(f"Close order cancelled")

    def did_fill_order(self, event: OrderFilledEvent):
        if event.order_id in (self.long_level_order_id, self.short_level_order_id):
            self.status = BotStatus.POSITION_OPEN
            self.cancel_all_orders()
            self.filled_position_side = TradeType.BUY if event.order_id == self.long_level_order_id else TradeType.SELL
            self.open_order = self.connector._order_tracker.fetch_order(event.order_id)
        elif event.order_id in (self.stop_loss_order_id, self.close_order_id):
            self.logger().info(f"Stop loss filled or close with market order filled")
            self.next_cycle_timestamp = self.current_timestamp + self.timeout_sec
            self.status = BotStatus.TIMEOUT
            self.close_order = self.connector._order_tracker.fetch_order(event.order_id)
        else:
            self.logger().info(f"Unknown order is filled. Event data = {event}")
            self.cancel_all_orders()
            self.status = BotStatus.NOT_ACTIVE

    def log_filled_order(self):
        for position in self.account_positions.values():
            if position.trading_pair == self.trading_pair:
                self.logger().info(f"account position = {position}")
                self.previous_price = self.open_order.average_executed_price
                if self.filled_position_side == TradeType.BUY:
                    self.notify_app_and_log(f"Long {self.long_sign}{self.trailing_long_percentage}% | Filled 100% | "
                                            f"Entry price: {self.open_order.average_executed_price} ")
                else:
                    self.notify_app_and_log(f"Short {self.short_sign}{self.trailing_short_percentage}% | Filled 100% | "
                                            f"Entry price: {self.open_order.average_executed_price} ")
            else:
                self.logger().info(f"No open positions found. Trying again.")

    def finalize_position(self):
        self.log_finalize_position_printed = True
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

    def update_params_for_new_cycle(self):
        self.check_num = 0
        self.log_filled_order_printed = False
        self.log_finalize_position_printed = False
        self.trailing_price_update_timestamp = self.server_time
        self.stop_loss_update_timestamp = 0
        self.long_placed = False
        self.short_placed = False
        self.sl_placed = False

    def is_open_positions_and_orders(self):
        for position in self.account_positions.values():
            if position.trading_pair == self.trading_pair:
                self.logger().info(f"There are open positions. Stop trading.")
                return True
        if len(self.get_active_orders(self.exchange)):
            self.logger().info(f"There are open orders. Stop trading.")
            return True
        return False

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
            update_sec = int(self.trailing_price_update_timestamp - self.server_time)
            lines.extend(["", f"  Trailing price long:   {round(self.trailing_price_long, self.rounding_digits)}"])
            lines.extend([f"  Trailing price short:  {round(self.trailing_price_short, self.rounding_digits)}"])
            lines.extend([f"  Last price:  {round(last_price, self.rounding_digits)}"])
            lines.extend([f"  Next price check:  {datetime.fromtimestamp(self.trailing_price_update_timestamp).strftime('%H:%M:%S')}"])
            lines.extend([f"  Next price check in:  {update_sec} sec"])
        if self.status == BotStatus.POSITION_OPEN:
            update_sec = int(self.stop_loss_update_timestamp - self.server_time)
            lines.extend([f"  Last price:  {round(last_price, self.rounding_digits)}"])
            lines.extend([f"  Next stop loss check:  {datetime.fromtimestamp(self.stop_loss_update_timestamp).strftime('%H:%M:%S')}"])
            lines.extend([f"  Next stop loss check in:  {update_sec} sec"])

        # balance_df = self.get_balance_df()
        # lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        for position in self.account_positions.values():
            if position.trading_pair == self.trading_pair:
                positions_df = self.active_positions_df()
                lines.extend(
                    ["", "  Active Positions:"] + ["    " + line for line in positions_df.to_string(index=False).split("\n")])

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
        columns = ["Exchange", "Pair", "Side", "Price", "Size"]
        data = []
        for order in self.get_active_orders(self.exchange):
            data.append([
                self.exchange,
                order.trading_pair,
                "buy" if order.is_buy else "sell",
                float(order.price),
                float(order.quantity),
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def active_positions_df(self) -> pd.DataFrame:
        columns = ["Exchange", "Pair", "Side", "Entry", "Amount", "Unrealized PNL", "Leverage"]
        data = []
        for position in self.account_positions.values():
            data.append(
                [
                    self.exchange,
                    position.trading_pair,
                    position.position_side.name,
                    position.entry_price,
                    position.amount,
                    position.unrealized_pnl,
                    position.leverage,
                ]
            )
        return pd.DataFrame(data=data, columns=columns)

