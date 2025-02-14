from decimal import Decimal

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class XEMMExplorerExecutorConfig(ExecutorConfigBase):
    type = "xemm_explorer_executor"
    maker_exchange: str
    taker_exchange: str
    trading_pair: str
    maker_side: TradeType
    order_amount: Decimal
    paper_trade_amount: Decimal
    paper_trade_amount_in_quote: Decimal
    target_profitability: Decimal
    min_profitability: Decimal
    max_profitability: Decimal
    fee_maker_pct: Decimal
    fee_taker_pct: Decimal
    liquidate_base_assets: bool
