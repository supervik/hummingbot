from decimal import Decimal

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionSide, PriceType, PositionMode, TradeType
from hummingbot.smart_components.position_executor.data_types import PositionConfig, PositionExecutorStatus
from hummingbot.smart_components.position_executor.position_executor import PositionExecutor
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class BostonBot(ScriptStrategyBase):
    """

    """
    # config parameters
    exchange: str = "binance_perpetual"
    trading_pair: str = "ETH-USDT"

    size_long_usdt = Decimal("15")
    size_short_usdt = Decimal("11")
    trailing_long_percentage = Decimal("0.01")
    trailing_short_percentage = Decimal("0.1")
    check_trailing_sec = 300

    stop_loss_long_percentage = Decimal("1.5")
    stop_loss_short_percentage = Decimal("1.5")
    leverage = 10
    use_time_frame = True
    check_stop_loss_sec = 20

    markets = {exchange: {trading_pair}}

    # class parameters
    trailing_price_long = Decimal("0")
    trailing_price_short = Decimal("0")
    trailing_price_update_timestamp = 0
    stop_loss_price_update_timestamp = 0
    active_executor = None
    rounding_digits = 5
    status = "NOT_INIT"
    previous_price = Decimal("0")
    check_num = 0

    @property
    def connector(self):
        """
        The maker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.exchange]

    def on_tick(self):
        if self.status == "NOT_INIT":
            self.init_strategy()

        # manage active positions
        if self.active_executor:
            if self.active_executor.executor_status != PositionExecutorStatus.NOT_STARTED:
                # self.logger().info(f"account_positions = {self.connector.account_positions}")
                if self.active_executor.is_closed:
                    self.finalize_position()
                    return
                if self.current_timestamp > self.stop_loss_price_update_timestamp:
                    self.update_stop_loss()
            else:
                self.logger().info(f"Position not started")
            return

        # update trailing prices
        if self.current_timestamp > self.trailing_price_update_timestamp:
            self.update_trailing_price()
            return

        # check signal to open a position
        signal_value = self.get_signal()
        if signal_value != 0:
            self.create_position(signal_value)

    def init_strategy(self):
        self.check_and_set_leverage()
        base, quote = split_hb_trading_pair(self.trading_pair)
        balance = round(self.connector.get_balance(quote), self.rounding_digits)
        self.notify_app_and_log(f"Start of Boston bot | Available balance: {balance} {quote}")
        self.status = "ACTIVE"

    def check_and_set_leverage(self):
        for connector in self.connectors.values():
            for trading_pair in connector.trading_pairs:
                connector.set_position_mode(PositionMode.HEDGE)
                connector.set_leverage(trading_pair=trading_pair, leverage=self.leverage)

    def finalize_position(self):
        base, quote = split_hb_trading_pair(self.trading_pair)
        balance = round(self.connector.get_balance(quote), self.rounding_digits)
        side = "Long" if self.active_executor.side == TradeType.BUY else "Short"
        size = self.active_executor.amount
        entry_price = self.active_executor.entry_price
        close_price = self.active_executor.close_price
        pnl = self.active_executor.trade_pnl
        pnl_quote = self.active_executor.trade_pnl_quote
        fees = self.active_executor.cum_fee_quote
        self.notify_app_and_log(
            f"{side} | Closed | Entry price: {entry_price} | Size: {round(size, self.rounding_digits)} |"
            f" Realized PNL: ${round(pnl_quote, 5)} |"
            f" ROE: %{round(pnl, 3)} | Close price: {close_price} | fees: {fees}")
        self.notify_app_and_log(f"Available balance: {balance} {quote}")
        self.notify_app_and_log(f"Meow | Pause 60 sec")
        self.trailing_price_update_timestamp = self.current_timestamp + 60
        self.active_executor = None

    def update_stop_loss(self):
        self.logger().info("Update stop loss price")
        self.check_num += 1
        self.stop_loss_price_update_timestamp = self.current_timestamp + self.check_stop_loss_sec
        current_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        previous_price = self.previous_price
        if self.active_executor.side == TradeType.BUY:
            if self.previous_price < current_price:
                stop_loss_price_updated = current_price * (1 - self.stop_loss_long_percentage / Decimal("100"))
                self.logger().info(f"stop_loss_price_updated = {stop_loss_price_updated}")
                stop_loss_percentage_updated = self.active_executor.entry_price / stop_loss_price_updated - Decimal("1")
                self.active_executor.position_config.stop_loss = stop_loss_percentage_updated
                self.previous_price = current_price
            self.notify_app_and_log(f"Check no.{self.check_num} Long +{self.trailing_long_percentage}% | "
                                    f"Prev price: {round(previous_price, self.rounding_digits)} | "
                                    f"Current price: {round(current_price, self.rounding_digits)} | "
                                    f"SL: {self.stop_loss_long_percentage}%: "
                                    f"{round(self.active_executor.stop_loss_price, self.rounding_digits)} | "
                                    f"Check in {self.check_stop_loss_sec} sec")

        else:
            if self.previous_price > current_price:
                stop_loss_price_updated = current_price * (1 + self.stop_loss_short_percentage / Decimal("100"))
                self.logger().info(f"stop_loss_price_updated = {stop_loss_price_updated}")
                stop_loss_percentage_updated = stop_loss_price_updated / self.active_executor.entry_price - Decimal("1")
                self.active_executor.position_config.stop_loss = stop_loss_percentage_updated
                self.previous_price = current_price
            self.notify_app_and_log(f"Check no.{self.check_num} Short -{self.trailing_short_percentage}% | "
                                    f"Prev price: {round(previous_price, self.rounding_digits)} | "
                                    f"Current price: {round(current_price, self.rounding_digits)} | "
                                    f"SL: {self.stop_loss_short_percentage}%: "
                                    f"{round(self.active_executor.stop_loss_price, self.rounding_digits)} | "
                                    f"Check in {self.check_stop_loss_sec} sec")

    def update_trailing_price(self):
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        amount_long = self.connector.quantize_order_amount(self.trading_pair, self.size_long_usdt / last_price)
        amount_short = self.connector.quantize_order_amount(self.trading_pair, self.size_short_usdt / last_price)
        leverage_long = amount_long * last_price / self.leverage
        leverage_short = amount_short * last_price / self.leverage
        self.trailing_price_long = last_price * (1 + self.trailing_long_percentage / Decimal("100"))
        self.trailing_price_short = last_price * (1 - self.trailing_short_percentage / Decimal("100"))
        self.trailing_price_update_timestamp = self.current_timestamp + self.check_trailing_sec
        self.notify_app_and_log(f"{self.trading_pair} {round(last_price, self.rounding_digits)} | "
                                f"Check trailing {self.check_trailing_sec} sec")
        self.notify_app_and_log(f"Long +{self.trailing_long_percentage}% | "
                                f"Price: {round(self.trailing_price_long, self.rounding_digits)} | "
                                f"Size: ${self.size_long_usdt}, Margin: ${round(leverage_long, self.rounding_digits)} | "
                                f"SL: {self.stop_loss_long_percentage}%")
        self.notify_app_and_log(f"Short -{self.trailing_short_percentage}% | "
                                f"Price: {round(self.trailing_price_short, self.rounding_digits)} | "
                                f"Size: ${self.size_short_usdt}, Margin: ${round(leverage_short, self.rounding_digits)} | "
                                f"SL: {self.stop_loss_short_percentage}%")

    def get_signal(self):
        if not self.trailing_price_long or not self.trailing_price_short:
            return 0
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        if last_price >= self.trailing_price_long:
            signal_value = 1
        elif last_price <= self.trailing_price_short:
            signal_value = -1
        else:
            signal_value = 0
        return signal_value

    def create_position(self, signal_value):
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        if signal_value > 0:
            position_side = TradeType.BUY
            position_amount = self.size_long_usdt / last_price
            position_stop_loss = self.stop_loss_long_percentage / Decimal("100")
        else:
            position_side = TradeType.SELL
            position_amount = self.size_short_usdt / last_price
            position_stop_loss = self.stop_loss_short_percentage / Decimal("100")

        self.active_executor = PositionExecutor(
            position_config=PositionConfig(
                timestamp=self.current_timestamp,
                trading_pair=self.trading_pair,
                exchange=self.exchange,
                order_type=OrderType.MARKET,
                side=position_side,
                entry_price=last_price,
                amount=position_amount,
                stop_loss=position_stop_loss),
            strategy=self,
        )
        if position_side == TradeType.BUY:
            self.notify_app_and_log(f"Long +{self.trailing_long_percentage}% | Filled 100% | "
                                    f"Entry price: {round(self.active_executor.entry_price, self.rounding_digits)} | "
                                    f"SL: {round(self.active_executor.stop_loss_price, self.rounding_digits)} | "
                                    f"Check in {self.check_stop_loss_sec} sec")
        else:
            self.notify_app_and_log(f"Short -{self.trailing_short_percentage}% | Filled 100% | "
                                    f"Entry price: {round(self.active_executor.entry_price, self.rounding_digits)} | "
                                    f"SL: {round(self.active_executor.stop_loss_price, self.rounding_digits)} | "
                                    f"Check in {self.check_stop_loss_sec} sec")
        self.trailing_price_update_timestamp = self.current_timestamp
        self.stop_loss_price_update_timestamp = self.current_timestamp + self.check_stop_loss_sec
        self.trailing_price_long = Decimal("0")
        self.trailing_price_short = Decimal("0")
        self.previous_price = self.active_executor.entry_price
        self.check_num = 0

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
        last_price = self.connector.get_price_by_type(self.trading_pair, PriceType.LastTrade)
        update_sec = self.trailing_price_update_timestamp - self.current_timestamp
        lines.extend(["", "  Trailing price long:", f"    {round(self.trailing_price_long, self.rounding_digits)}"])
        lines.extend(["", "  Trailing price short:", f"    {round(self.trailing_price_short, self.rounding_digits)}"])
        lines.extend(["", "  Last price:", f"    {round(last_price, self.rounding_digits)}"])
        lines.extend(["", "  Next price check in:", f"    {update_sec} sec"])

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
