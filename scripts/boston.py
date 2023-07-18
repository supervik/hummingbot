import datetime
import os
import time
from decimal import Decimal
from typing import Dict, List

import pandas as pd

from hummingbot import data_path
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, PriceType, TradeType
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.smart_components.position_executor.data_types import (
    CloseType,
    PositionConfig,
    PositionExecutorStatus,
    TrailingStop,
)
from hummingbot.smart_components.position_executor.position_executor import PositionExecutor
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class Boston(ScriptStrategyBase):
    market_making_strategy_name = "boston"
    trading_pair = "ETH-USDT"
    exchange = "binance_perpetual"
    stop_loss: float = 0.03
    take_profit: float = 0.015

    markets = {exchange: {trading_pair}}
    status ="ACTIVE"

    def on_tick(self):
        if self.status == "NOT_ACITVE":
            return

        position_config = PositionConfig(
            timestamp=self.current_timestamp,
            trading_pair=self.trading_pair,
            exchange=self.exchange,
            side=TradeType.BUY,
            amount=Decimal("0.006"),
            take_profit=self.take_profit,
            stop_loss=self.stop_loss,
            entry_price=Decimal("1950"),
            open_order_type=OrderType.LIMIT
        )

        executor = PositionExecutor(
            strategy=self,
            position_config=position_config,
        )
        self.status = "NOT_ACITVE"