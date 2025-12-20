from decimal import Decimal
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class TakerOrderInfo(BaseModel):
    order_id: Optional[str] = None
    trading_pair: str
    side: TradeType
    amount: Decimal
    completed: Optional[Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]] = None
    filled_events: List[OrderFilledEvent] = []
    trials: int = 0
    sent_timestamp: Optional[float] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def is_complete(self) -> bool:
        """Check if the taker order is completed."""
        return self.completed is not None


class HedgingState(BaseModel):
    maker_fill: OrderFilledEvent
    taker_1: TakerOrderInfo
    taker_2: TakerOrderInfo
    created_timestamp: float
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def is_complete(self) -> bool:
        """Check if both taker orders are completed."""
        return self.taker_1.is_complete() and self.taker_2.is_complete()

    def is_failed(self, max_retries: int) -> bool:
        """Check if any taker order exceeded max retries."""
        return (self.taker_1.trials > max_retries) or (self.taker_2.trials > max_retries)


class TriangularExecutorConfig(ExecutorConfigBase):
    type: Literal["triangular_executor"] = "triangular_executor"
    connector_name: str
    maker_pair: str
    taker_1_pair: str
    taker_2_pair: str
    base_amount: Decimal
    quote_amount: Decimal
    min_profit: Decimal
    max_profit: Decimal
    fee_maker: Decimal
    fee_taker: Decimal
    min_usdt: Decimal
    max_taker_retries: int = 10
    taker_retry_delay: float = 10.0
    completion_wait_time: float = 5.0
