from decimal import Decimal

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class SpotPerpExecutorConfig(ExecutorConfigBase):
    type = "spot_perp_executor"
    maker_exchange: str
    taker_exchange: str
    trading_pair: str
    order_amount: Decimal
    min_order_amount: Decimal
    target_opening_profitability: Decimal
    target_closing_profitability: Decimal
    profitability_range: Decimal
