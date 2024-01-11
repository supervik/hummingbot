import time
from decimal import Decimal

import pandas_ta as ta  # noqa: F401

from hummingbot.smart_components.executors.position_executor.position_executor import PositionExecutor
from hummingbot.smart_components.strategy_frameworks.data_types import OrderLevel
from hummingbot.smart_components.strategy_frameworks.directional_trading import DirectionalTradingControllerConfigBase, \
    DirectionalTradingControllerBase


class PriceMoveConfig(DirectionalTradingControllerConfigBase):
    strategy_name: str = "price_move"
    body_size_pct_threshold: float = 0.03


class PriceMoveController(DirectionalTradingControllerBase):
    """
    Directional Market Making Strategy making use of NATR indicator to make spreads dynamic and shift the mid price.
    """

    def __init__(self, config: PriceMoveConfig):
        super().__init__(config)
        self.config = config

    def refresh_order_condition(self, executor: PositionExecutor, order_level: OrderLevel) -> bool:
        """
        Checks if the order needs to be refreshed.
        You can reimplement this method to add more conditions.
        """
        return False

    def early_stop_condition(self, executor: PositionExecutor, order_level: OrderLevel) -> bool:
        """
        If an executor has an active position, should we close it based on a condition.
        """
        return False

    def cooldown_condition(self, executor: PositionExecutor, order_level: OrderLevel) -> bool:
        """
        After finishing an order, the executor will be in cooldown for a certain amount of time.
        This prevents the executor from creating a new order immediately after finishing one and execute a lot
        of orders in a short period of time from the same side.
        """
        if executor.close_timestamp and executor.close_timestamp + order_level.cooldown_time > time.time():
            return True
        return False

    def get_processed_data(self):
        """
        Gets the price and spread multiplier from the last candlestick.
        """
        candles_df = self.candles[0].candles_df
        candles_df["candles_body"] = 100 * (candles_df["close"] - candles_df["open"]) / candles_df["open"]
        candles_body_pct = candles_df["candles_body"]

        long_condition = (candles_body_pct > self.config.body_size_pct_threshold)
        short_condition = (candles_body_pct < -self.config.body_size_pct_threshold)

        candles_df["signal"] = 0
        candles_df.loc[long_condition, "signal"] = 1
        candles_df.loc[short_condition, "signal"] = -1

        return candles_df

    def extra_columns_to_show(self):
        return ["candles_body"]



