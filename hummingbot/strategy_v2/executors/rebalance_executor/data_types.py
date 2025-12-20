from decimal import Decimal
from typing import Dict, Literal

from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase



class RebalanceExecutorConfig(ExecutorConfigBase):
    type: Literal["rebalance_executor"] = "rebalance_executor"
    connector_name: str
    balances: Dict[str, Decimal]
    rebalance_asset: str
    min_usdt: Decimal
    trigger_on_event: bool
