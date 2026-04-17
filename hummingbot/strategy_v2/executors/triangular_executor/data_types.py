import time
from collections import deque
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


class CancellationState:
    """
    Tracks per-side maker order cancellation with exponential backoff.
    All mutation goes through start() / reset() / increment_retry().
    """

    def __init__(self):
        self.in_progress: bool = False
        self.timestamp: Optional[float] = None
        self.retries: int = 0

    def start(self) -> None:
        self.in_progress = True
        self.timestamp = time.time()

    def reset(self) -> None:
        self.in_progress = False
        self.timestamp = None
        self.retries = 0

    def elapsed(self) -> Optional[float]:
        return time.time() - self.timestamp if self.timestamp is not None else None

    def backoff_delay(self, base: float, cap: float) -> float:
        return min(base * (2 ** self.retries), cap)

    def should_retry(self, base: float, cap: float) -> bool:
        elapsed = self.elapsed()
        return elapsed is not None and elapsed >= self.backoff_delay(base, cap)

    def increment_retry(self) -> None:
        """Reset the in-progress flag and bump the retry counter for the next attempt."""
        self.in_progress = False
        self.retries += 1


class TakerPairDepthTracker:
    """
    Tracks order book depth and best prices for a taker pair.
    Used to calculate target prices for triangular arbitrage.
    """
    
    def __init__(self):
        self.ob_depth_pct_buy = deque(maxlen=600)
        self.ob_depth_pct_sell = deque(maxlen=600)
        self.max_ob_depth_buy = Decimal("0")
        self.max_ob_depth_sell = Decimal("0")
        self.best_bid_price = Decimal("0")
        self.best_ask_price = Decimal("0")
    
    def is_ready(self, min_samples: int = 10) -> bool:
        """Check if both sides have enough samples"""
        return (len(self.ob_depth_pct_buy) >= min_samples and 
                len(self.ob_depth_pct_sell) >= min_samples)
    
    def is_buy_side_ready(self, min_samples: int = 10) -> bool:
        """Check if buy side has enough samples"""
        return len(self.ob_depth_pct_buy) >= min_samples
    
    def is_sell_side_ready(self, min_samples: int = 10) -> bool:
        """Check if sell side has enough samples"""
        return len(self.ob_depth_pct_sell) >= min_samples
    
    def update_depth_buy(self, execution_price: Decimal, best_price: Decimal):
        """Update buy side depth"""
        if best_price == Decimal("0"):
            return
        depth = abs(execution_price - best_price) / best_price
        # Quantize depth (including 0, which is valid - means no slippage)
        depth = depth.quantize(Decimal("0.00000001"))
        self.ob_depth_pct_buy.append(depth)
        self.max_ob_depth_buy = max(self.ob_depth_pct_buy) if self.ob_depth_pct_buy else Decimal("0")
        
        # Temporary fix to avoid max calculation
        # Uncomment this to use the 0 depth (best price) and comment previous self.max_ob_depth_buy calculation
        # self.max_ob_depth_buy = Decimal("0")
    
    def update_depth_sell(self, execution_price: Decimal, best_price: Decimal):
        """Update sell side depth"""
        if best_price == Decimal("0"):
            return
        depth = abs(execution_price - best_price) / best_price
        # Quantize depth (including 0, which is valid - means no slippage)
        depth = depth.quantize(Decimal("0.00000001"))
        self.ob_depth_pct_sell.append(depth)
        self.max_ob_depth_sell = max(self.ob_depth_pct_sell) if self.ob_depth_pct_sell else Decimal("0")
        
        # Temporary fix to avoid max calculation
        # Uncomment this to use the 0 depth (best price) and comment previous self.max_ob_depth_sell calculation
        # self.max_ob_depth_sell = Decimal("0")
    
    def update_best_prices(self, best_bid: Decimal, best_ask: Decimal):
        """Update both best prices from a bid/ask event"""
        self.best_bid_price = best_bid
        self.best_ask_price = best_ask
    
    def get_target_price_buy(self) -> Decimal:
        """Calculate target price for buy (taker): best_ask * (1 + max_ob_depth_buy)"""
        if self.best_ask_price == Decimal("0") or not self.ob_depth_pct_buy:
            return Decimal("0")
        return self.best_ask_price * (1 + self.max_ob_depth_buy)
    
    def get_target_price_sell(self) -> Decimal:
        """Calculate target price for sell (taker): best_bid * (1 - max_ob_depth_sell)"""
        if self.best_bid_price == Decimal("0") or not self.ob_depth_pct_sell:
            return Decimal("0")
        return self.best_bid_price * (1 - self.max_ob_depth_sell)


class TakerOrderInfo(BaseModel):
    order_id: Optional[str] = None
    trading_pair: str
    side: TradeType
    amount: Decimal
    fill_ratio_threshold: Decimal = Decimal("1")
    completed: Optional[Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]] = None
    filled_events: List[OrderFilledEvent] = []
    trials: int = 0
    sent_timestamp: Optional[float] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def filled_amount(self) -> Decimal:
        return sum((e.amount for e in self.filled_events), Decimal("0"))

    def is_complete(self) -> bool:
        """Complete if exchange confirmed full fill, or filled ratio >= threshold."""
        if self.completed is not None:
            return True
        if self.filled_events and self.amount > Decimal("0") and self.fill_ratio_threshold < Decimal("1"):
            return self.filled_amount / self.amount >= self.fill_ratio_threshold
        return False


class HedgingState(BaseModel):
    maker_fill: OrderFilledEvent
    taker_1: TakerOrderInfo
    taker_2: TakerOrderInfo
    created_timestamp: float
    # Internal timing fields for hedge latency analysis (per executor cycle)
    maker_fill_server_ts: Optional[float] = None
    last_taker_fill_server_ts: Optional[float] = None
    maker_fill_exchange_ts: Optional[float] = None
    last_taker_fill_exchange_ts: Optional[float] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def is_complete(self) -> bool:
        """Check if both taker orders are completed."""
        return self.taker_1.is_complete() and self.taker_2.is_complete()

    def is_failed(self, max_retries: int) -> bool:
        """Check if any taker order exceeded max retries."""
        return (self.taker_1.trials >= max_retries) or (self.taker_2.trials >= max_retries)


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
    taker_fill_completion_ratio: Decimal = Decimal("1")
    maker_quote_buffer_inverse: Decimal = Decimal("0")
    max_taker_retries: int = 10
    taker_retry_delay: float = 10.0
    completion_wait_time: float = 5.0
    kill_switch_pnl_threshold: Decimal
    # Extra taker depth to account for cheaper levels that will fill before this one.
    # The executor adds these to its own effective amounts when probing order-book depth.
    # Zero by default — single-level behaviour is completely unchanged.
    extra_base_amount: Decimal = Decimal("0")
    extra_quote_amount: Decimal = Decimal("0")
    # Optional label shown in log prefix when multiple levels share the same triangle
    # (e.g. "L1", "L2"). None means no label is shown — keeps logs clean for solo triangles.
    level_label: Optional[str] = None
