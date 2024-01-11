from decimal import Decimal
from typing import Dict

from hummingbot.connector.connector_base import ConnectorBase, TradeType
from hummingbot.core.data_type.common import OrderType
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.smart_components.controllers.price_move import PriceMoveController, PriceMoveConfig
from hummingbot.smart_components.strategy_frameworks.data_types import (
    ExecutorHandlerStatus,
    OrderLevel,
    TripleBarrierConf,
)
from hummingbot.smart_components.strategy_frameworks.directional_trading.directional_trading_executor_handler import (
    DirectionalTradingExecutorHandler,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class V2Playground(ScriptStrategyBase):
    trading_pair = "BTC-USDT"
    leverage = 1

    triple_barrier_conf = TripleBarrierConf(
        stop_loss=Decimal("0.005"), take_profit=Decimal("0.005"),
        time_limit=60 * 60 * 6,
        open_order_type=OrderType.MARKET,
        take_profit_order_type=OrderType.LIMIT
    )

    order_levels = [
        OrderLevel(level=0, side=TradeType.BUY, order_amount_usd=Decimal("15"),
                   spread_factor=Decimal(0.5), order_refresh_time=60 * 5,
                   cooldown_time=60 * 60 * 4, triple_barrier_conf=triple_barrier_conf),
        OrderLevel(level=0, side=TradeType.SELL, order_amount_usd=Decimal("15"),
                   spread_factor=Decimal(0.5), order_refresh_time=60 * 5,
                   cooldown_time=60 * 60 * 4, triple_barrier_conf=triple_barrier_conf)
    ]

    controllers = {}
    markets = {}
    executor_handlers = {}

    config = PriceMoveConfig(
        exchange="binance_perpetual",
        trading_pair=trading_pair,
        order_levels=order_levels,
        candles_config=[
            CandlesConfig(connector="binance_perpetual", trading_pair=trading_pair, interval="1m", max_records=100),
        ],
        leverage=leverage,
        body_size_pct_threshold=0.5,
    )
    price_move_controller = PriceMoveController(config=config)
    markets = price_move_controller.update_strategy_markets_dict(markets)

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.price_move_executor = DirectionalTradingExecutorHandler(strategy=self, controller=self.price_move_controller)

    def on_stop(self):
        self.price_move_executor.stop()

    def on_tick(self):
        """
        This shows you how you can start meta controllers. You can run more than one at the same time and based on the
        market conditions, you can orchestrate from this script when to stop or start them.
        """
        if self.price_move_executor.status == ExecutorHandlerStatus.NOT_STARTED:
            self.price_move_executor.start()

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["Price move", self.price_move_executor.to_format_status()])

        return "\n".join(lines)
