import logging
import time
from decimal import Decimal
from typing import Dict

from hummingbot.connector.connector_base import ConnectorBase, Union
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.xemm_explorer_executor.data_types import XEMMExplorerExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class XEMMExplorerExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: XEMMExplorerExecutorConfig, update_interval: float = 1.0,
                 max_retries: int = 10):
        super().__init__(strategy=strategy,
                         connectors=[config.maker_exchange, config.taker_exchange],
                         config=config, update_interval=update_interval)
        
        self.config = config
        self.taker_result_price = Decimal("1")
        self.maker_target_price = Decimal("1")
        self.taker_hedge_price = Decimal("0")
        self.total_fee_pct = self.config.fee_maker_pct + self.config.fee_taker_pct
        self.maker_order = None
        self.pending_hedge_price_update = False
        self.maker_fill_timestamp = None
        self.taker_hedge_timestamp = None
        self.slippage_buffer_pct = Decimal("1")
        self.failed_orders = []
        self._current_retries = 0
        self._max_retries = max_retries

    # 1. Initialization & Startup Methods
    async def on_start(self):
        """
        Initializes the executor. If liquidate_base_assets is configured,
        liquidates assets and stops. Otherwise proceeds with normal startup.
        """
        if self.config.liquidate_base_assets:
            self.liquidate_base_assets()
            self.close_type = CloseType.EARLY_STOP
            self.stop()
        else:
            await super().on_start()
    
    def liquidate_base_assets(self):
        """
        Liquidates remaining base assets on the maker exchange by placing a sell order
        with configured slippage buffer.
        """
        self.logger().info(f"Liquidating remaining base assets of {self.config.trading_pair} on {self.config.maker_exchange}")
        bid_price = self.get_price(self.config.maker_exchange, self.config.trading_pair, price_type=PriceType.BestBid) 
        sell_price = bid_price * (1 - self.slippage_buffer_pct / Decimal("100"))
        self.place_order(
            connector_name=self.config.maker_exchange,
            trading_pair=self.config.trading_pair,
            order_type=OrderType.LIMIT,
            side=self.config.maker_side,
            amount=self.config.order_amount,
            price=sell_price)
    
    def early_stop(self):
        """
        Initiates an early stop of the executor.
        Sets appropriate status flags and triggers shutdown sequence.
        """
        self.logger().info(f"Early stopping executor on {self.config.trading_pair}")
        self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN
        self.stop()

    async def validate_sufficient_balance(self):
        """
        Validates if there is sufficient balance to execute trades.
        Stops the executor if balance is insufficient.
        """
        mid_price = self.get_price(self.config.maker_exchange, self.config.trading_pair, price_type=PriceType.MidPrice)
        maker_order_candidate = OrderCandidate(
            trading_pair=self.config.trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=self.config.maker_side,
            amount=self.config.order_amount,
            price=mid_price,)
        maker_adjusted_candidate = self.adjust_order_candidates(self.config.maker_exchange, [maker_order_candidate])[0]
        
        if maker_adjusted_candidate.amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error(f"Not enough budget to open a position on {self.config.trading_pair}. Shutting down executor.")
            self.stop()

    # 2. Main Control Loop Methods
    async def control_task(self):
        """
        Main control loop that manages the executor's operation.
        Updates prices and controls maker orders when running,
        handles shutdown process when stopping.
        """
        if self.status == RunnableStatus.RUNNING:
            await self.update_prices()
            await self.control_maker_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.get_taker_price()
            self.logger().warning(f"Shutting down executor on {self.config.trading_pair}")
            self.stop()
    
    async def update_prices(self):
        """
        Updates maker target prices based on taker prices and configured fees/profitability.
        """
        self.taker_result_price = await self.get_resulting_price_for_amount(
            connector=self.config.taker_exchange,
            trading_pair=self.config.trading_pair,
            is_buy=not self.config.maker_side,
            order_amount=self.config.paper_trade_amount)
        
        profitability_with_fees = (self.config.target_profitability + self.total_fee_pct) / Decimal("100")
        
        if self.config.maker_side == TradeType.SELL:
            self.maker_target_price = self.taker_result_price * (1 + profitability_with_fees)
        else:
            self.maker_target_price = self.taker_result_price * (1 - profitability_with_fees)

    async def get_resulting_price_for_amount(self, connector: str, trading_pair: str, is_buy: bool,
                                             order_amount: Decimal):
        """Get the resulting price for a given amount"""
        return await self.connectors[connector].get_quote_price(trading_pair, is_buy, order_amount)
    
    async def get_taker_price(self):
        """
        Updates the taker hedge price and timestamp when a hedge is pending.
        Used during shutdown to get final pricing.
        """
        if self.pending_hedge_price_update:
            self.logger().info(f"Getting the price on taker side to hedge on {self.config.trading_pair}")
            self.taker_hedge_price = await self.get_resulting_price_for_amount(
                connector=self.config.taker_exchange,
                trading_pair=self.config.trading_pair,
                is_buy=not self.config.maker_side,
                order_amount=self.config.paper_trade_amount)
            self.taker_hedge_timestamp = time.time()
            self.pending_hedge_price_update = False
    
    # 3. Order Management Methods
    async def control_maker_order(self):
        """
        Main order management logic. Creates new maker orders or
        updates existing ones based on current market conditions.
        """
        if self.maker_order is None:
            await self.create_maker_order()
        else:
            await self.control_update_maker_order()

    async def create_maker_order(self):
        """
        Places a new maker order at the target price.
        """
        order_id = self.place_order(
            connector_name=self.config.maker_exchange,
            trading_pair=self.config.trading_pair,
            order_type=OrderType.LIMIT,
            side=self.config.maker_side,
            amount=self.config.order_amount,
            price=self.maker_target_price)
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(f"Sent maker order id = {order_id} at price {self.maker_target_price} on {self.config.trading_pair}")

    async def control_update_maker_order(self):
        """
        Monitors and updates existing maker orders based on profitability thresholds.
        Cancels orders that fall outside acceptable profitability range.
        """
        trade_profitability = self.get_current_trade_profitability() - self.total_fee_pct
        if trade_profitability < self.config.min_profitability:
            self.logger().info(f"Trade profitability {trade_profitability} on {self.config.trading_pair} is below minimum profitability. Cancelling order.")
            self._strategy.cancel(self.config.maker_exchange, self.config.trading_pair, self.maker_order.order_id)
        if trade_profitability > self.config.max_profitability:
            self.logger().info(f"Trade profitability {trade_profitability} on {self.config.trading_pair} is above target profitability. Cancelling order.")
            self._strategy.cancel(self.config.maker_exchange, self.config.trading_pair, self.maker_order.order_id)

    def get_current_trade_profitability(self):
        """
        Calculates the current profitability of the active trade as a percentage.
        """
        trade_profitability = Decimal("0")
        if self.maker_order and self.maker_order.order and self.maker_order.order.is_open:
            maker_price = self.maker_order.order.price
            if self.config.maker_side == TradeType.BUY:
                trade_profitability = (self.taker_result_price - maker_price) / maker_price
            else:
                trade_profitability = (maker_price - self.taker_result_price) / maker_price
        return trade_profitability * Decimal("100")
    
    # 4. Event Handling Methods
    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        """
        Handles order created events from the exchange.
        Updates the maker order tracking and resets retry counter when maker order is created.
        """
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order created, id = {event.order_id} on {self.config.trading_pair}")
            self._current_retries = 0
            self.maker_order.order = self.get_in_flight_order(self.config.maker_exchange, event.order_id)

    def process_order_canceled_event(self,
                                     event_tag: int,
                                     market: ConnectorBase,
                                     event: OrderCancelledEvent):
        """
        Handles order cancelled events from the exchange.
        Clears the maker order tracking when the maker order is cancelled.
        """
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order canceled, id = {event.order_id} on {self.config.trading_pair}")
            self.maker_order = None

    def process_order_filled_event(self,
                                   event_tag: int,
                                   market: ConnectorBase,
                                   event: OrderFilledEvent):
        """
        Handles order filled events from the exchange.
        When maker order is filled, records the timestamp and initiates shutdown sequence.
        """
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order filled, id = {event.order_id} on {self.config.trading_pair}")
            self.maker_fill_timestamp = time.time()
            self.pending_hedge_price_update = True
            self._status = RunnableStatus.SHUTTING_DOWN
            self.close_type = CloseType.COMPLETED
    
    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        """
        Handles order failure events from the exchange.
        Tracks failed orders, increments retry counter, and evaluates if max retries reached.
        """
        if self.maker_order and self.maker_order.order_id == event.order_id:
            self.failed_orders.append(self.maker_order)
            self.maker_order = None
            self.logger().info(f"Failed to open maker order id = {event.order_id} on {self.config.trading_pair}, retry = {self._current_retries}")
            self.evaluate_max_retries()
            self._current_retries += 1
    
    def evaluate_max_retries(self):
        """
        Evaluates if maximum retry attempts have been reached.
        Stops the executor if max retries exceeded.
        """
        if self._current_retries > self._max_retries:
            self.close_type = CloseType.FAILED
            self.logger().error(f"Reached max retries {self._max_retries} to open maker order on {self.config.trading_pair}. Shutting down executor.")
            self.stop()

    # 5. Metrics and Reporting Methods
    def _is_trade_completed(self) -> bool:
        """
        Helper method to check if a trade has completed successfully with all required conditions met.
        """
        return (self.is_closed and 
                self.close_type == CloseType.COMPLETED and 
                self.taker_hedge_price and 
                self.maker_order)

    @property
    def filled_amount_quote(self) -> Decimal:
        """
        Returns the filled amount in quote currency based on the paper trade amount if trade is completed, otherwise returns 0.
        """
        if self._is_trade_completed():
            return self.config.paper_trade_amount_in_quote
        return Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        """
        Returns the cumulative fees in quote currency based on the paper trade amount if trade is completed, otherwise returns 0.
        """
        if self._is_trade_completed():
            return self.config.paper_trade_amount_in_quote * (self.total_fee_pct / Decimal("100"))
        return Decimal("0")

    def get_net_pnl_quote(self) -> Decimal:
        """
        Returns the net profit/loss in quote currency based on the paper trade amount if trade is completed, otherwise returns 0.
        """
        if self._is_trade_completed():
            return self.config.paper_trade_amount_in_quote * self.get_net_pnl_pct()
        return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        """
        Returns the net profit/loss as a percentage if trade is completed, otherwise returns 0.
        """
        if self._is_trade_completed():
            if self.config.maker_side == TradeType.SELL:
                pnl = (self.maker_order.average_executed_price - self.taker_hedge_price) / self.maker_order.average_executed_price
            else:
                pnl = (self.taker_hedge_price - self.maker_order.average_executed_price) / self.maker_order.average_executed_price
            return pnl - self.total_fee_pct / Decimal("100")
        return Decimal("0")

    def get_custom_info(self) -> Dict:
        """
        Returns a dictionary containing custom information about the trade execution.
        """
        maker_price = Decimal("0")
        pnl_with_fees_pct = Decimal("0")
        taker_price = Decimal("0")
        hedge_delay_seconds = Decimal("0")
        
        if self._is_trade_completed():
            pnl_with_fees_pct = Decimal("100") * self.get_net_pnl_pct()
            maker_price = self.maker_order.average_executed_price
            taker_price = self.taker_hedge_price
            if self.maker_fill_timestamp and self.taker_hedge_timestamp:
                hedge_delay_seconds = Decimal(str(self.taker_hedge_timestamp - self.maker_fill_timestamp))

        return {
            "timestamp": self.close_timestamp,
            "maker_exchange": self.config.maker_exchange,
            "taker_exchange": self.config.taker_exchange,
            "trading_pair": self.config.trading_pair,
            "side": self.config.maker_side.name,
            "maker_price": maker_price,
            "taker_price": taker_price,
            "pnl_with_fees_pct": pnl_with_fees_pct,
            "hedge_delay_seconds": hedge_delay_seconds,
        }