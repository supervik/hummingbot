from enum import Enum
import logging
import time
from decimal import Decimal
from typing import Dict

from hummingbot.connector.connector_base import ConnectorBase, Union
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
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
from hummingbot.strategy_v2.executors.spot_perp_executor.data_types import SpotPerpExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

class OrderStatus(Enum):
    PENDING = 0
    OPEN = 1
    CLOSED = 2


class SpotPerpExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: SpotPerpExecutorConfig, update_interval: float = 0.1,
                 max_retries: int = 10):
        super().__init__(strategy=strategy,
                         connectors=[config.maker_exchange, config.taker_exchange],
                         config=config, update_interval=update_interval)
        
        self.config = config
        self.taker_result_price = Decimal("1")
        self.maker_target_price = Decimal("1")
        self.taker_hedge_price = Decimal("0")
        # self.total_fee_pct = self.config.fee_maker_pct + self.config.fee_taker_pct
        self.maker_order = None
        self.pending_taker_order = False
        self.maker_fill_timestamp = None
        self.taker_hedge_timestamp = None
        self.slippage_buffer_pct = Decimal("1")
        self.failed_orders = []
        self.order_status = OrderStatus.PENDING
        self.base_asset, self.quote_asset = self.config.trading_pair.split("-")
        self.next_order_delay = 60
        self.next_order_timestamp = 0
        self._current_retries = 0
        self._max_retries = max_retries

    # 1. Initialization & Startup Methods
    # async def on_start(self):
    #     """
    #     Initializes the executor. If liquidate_base_assets is configured,
    #     liquidates assets and stops. Otherwise proceeds with normal startup.
    #     """
    #     if self.config.liquidate_base_assets:
    #         self.liquidate_base_assets()
    #         self.close_type = CloseType.EARLY_STOP
    #         self.stop()
    #     else:
    #         await super().on_start()
    
    # def liquidate_base_assets(self):
    #     """
    #     Liquidates remaining base assets on the maker exchange by placing a sell order
    #     with configured slippage buffer.
    #     """
    #     self.logger().info(f"Liquidating remaining base assets of {self.config.trading_pair} on {self.config.maker_exchange}")
    #     bid_price = self.get_price(self.config.maker_exchange, self.config.trading_pair, price_type=PriceType.BestBid) 
    #     sell_price = bid_price * (1 - self.slippage_buffer_pct / Decimal("100"))
    #     self.place_order(
    #         connector_name=self.config.maker_exchange,
    #         trading_pair=self.config.trading_pair,
    #         order_type=OrderType.LIMIT,
    #         side=self.config.maker_side,
    #         amount=self.config.order_amount,
    #         price=sell_price)
    
    def early_stop(self):
        """
        Initiates an early stop of the executor.
        Sets appropriate status flags and triggers shutdown sequence.
        """
        self.logger().info(f"vvv: Early stopping executor on {self.config.trading_pair}")
        self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN
        self.stop()

    async def validate_sufficient_balance(self):
        """
        Validates if there is sufficient balance to execute trades.
        Stops the executor if balance is insufficient.
        """
        pass
        # mid_price = self.get_price(self.config.maker_exchange, self.config.trading_pair, price_type=PriceType.MidPrice)
        # maker_order_candidate = OrderCandidate(
        #     trading_pair=self.config.trading_pair,
        #     is_maker=True,
        #     order_type=OrderType.LIMIT,
        #     order_side=self.config.maker_side,
        #     amount=self.config.order_amount,
        #     price=mid_price,)
        # maker_adjusted_candidate = self.adjust_order_candidates(self.config.maker_exchange, [maker_order_candidate])[0]
        
        # if maker_adjusted_candidate.amount == Decimal("0"):
        #     self.close_type = CloseType.INSUFFICIENT_BALANCE
        #     self.logger().error(f"Not enough budget to open a position on {self.config.trading_pair}. Shutting down executor.")
        #     self.stop()

    # 2. Main Control Loop Methods
    async def control_task(self):
        """
        Main control loop that manages the executor's operation.
        Updates prices and controls maker orders when running,
        handles shutdown process when stopping.
        """
        if self.status == RunnableStatus.RUNNING:
            if self.order_status == OrderStatus.OPEN and self.check_closing_conditions():
                self.order_status = OrderStatus.CLOSED
                return
            is_taker_buy = False if self.order_status == OrderStatus.PENDING else True
            await self._handle_order_cycle(is_taker_buy=is_taker_buy)
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            self.logger().warning(f"vvv: Shutting down executor on {self.config.trading_pair}")
            # self.stop()
    
    async def _handle_order_cycle(self, is_taker_buy: bool):
        """
        Handles a complete order cycle including balance check, price updates, and order management.
        
        Args:
            is_taker_buy: Whether the taker side is buying (True) or selling (False)
        """
        amount = self.config.order_amount
        
        diff = self.get_balances_diff()
        balances_aligned = self.check_balances_diff(diff, is_taker_buy)
        if balances_aligned:
            await self.update_prices(is_taker_buy=is_taker_buy, amount=amount)
            await self.control_maker_order(is_taker_buy=is_taker_buy, amount=amount)
        else:
            self.logger().info(f"vvv: Balances difference = {diff} on {self.config.trading_pair}. Placing taker order.")
            self.cancel_maker_order()
            if is_taker_buy:
                await self.place_taker_order(abs(diff), side=TradeType.BUY, position_action=PositionAction.CLOSE)
                self.order_status = OrderStatus.CLOSED
            else:
                await self.place_taker_order(abs(diff), side=TradeType.SELL, position_action=PositionAction.OPEN)
                self.order_status = OrderStatus.OPEN
            
    def check_balances_diff(self, diff, is_taker_buy: bool):
        """
        Checks the difference between the balances of the spot and perp exchanges.
        """
        threshold = self.config.min_order_amount
        if (is_taker_buy and diff < -threshold) or (not is_taker_buy and diff > threshold):
            return False
        return True
    
    def cancel_maker_order(self):
        """
        Cancels the maker order.
        """
        if self.maker_order:
            self.logger().info(f"vvv: Cancelling maker order id = {self.maker_order.order_id} on {self.config.trading_pair}")
            self._strategy.cancel(self.config.maker_exchange, self.config.trading_pair, self.maker_order.order_id)
        
    def get_balances_diff(self):
        """
        Calculates the difference between the spot balance and the open position amount.
        """
        spot_balance = self.get_balance(self.config.maker_exchange, self.base_asset)
        open_positions = self.connectors[self.config.taker_exchange].account_positions
            
        open_position_amount = sum([position.amount for key, position in open_positions.items() if key == self.config.trading_pair])

        diff = spot_balance + open_position_amount
        return diff
        
    async def place_taker_order(self, amount: Decimal, side: TradeType, position_action: PositionAction):
        """
        Places a taker order on the taker exchange.
        """
        self.logger().info(f"vvv: Placing orders on taker side to hedge on {self.config.trading_pair}")
        order_id = self.place_order(
            connector_name=self.config.taker_exchange,
            trading_pair=self.config.trading_pair,
            order_type=OrderType.MARKET,
            side=side,
            amount=amount,
            position_action=position_action,
            price=Decimal("0"))

    def check_closing_conditions(self):
        return False

    
    async def update_prices(self, is_taker_buy: bool, amount: Decimal):
        """
        Updates maker target prices based on taker prices and configured fees/profitability.
        """
        self.taker_result_price = await self.get_resulting_price_for_amount(
            connector=self.config.taker_exchange,
            trading_pair=self.config.trading_pair,
            is_buy=is_taker_buy,
            order_amount=amount)
        
        # profitability_with_fees = (self.config.target_profitability + self.total_fee_pct) / Decimal("100")
        if is_taker_buy:
            profitability = self.config.target_closing_profitability / Decimal("100")
            self.maker_target_price = self.taker_result_price * (1 + profitability)
        else:
            profitability = self.config.target_opening_profitability / Decimal("100")
            self.maker_target_price = self.taker_result_price * (1 - profitability)

    async def get_resulting_price_for_amount(self, connector: str, trading_pair: str, is_buy: bool,
                                             order_amount: Decimal):
        """Get the resulting price for a given amount"""
        return await self.connectors[connector].get_quote_price(trading_pair, is_buy, order_amount)
    
    # 3. Order Management Methods
    async def control_maker_order(self, is_taker_buy: bool, amount: Decimal):
        """
        Main order management logic. Creates new maker orders or
        updates existing ones based on current market conditions.
        """
        if self.maker_order is None:
            if self._strategy.current_timestamp > self.next_order_timestamp:
                await self.create_maker_order(is_taker_buy, amount)
        else:
            await self.update_maker_order(is_taker_buy, amount)

    async def create_maker_order(self, is_taker_buy: bool, amount: Decimal):
        """
        Places a new maker order at the target price.
        """
        amount = self.quantize_amount_on_maker_and_taker(amount)
        order_id = self.place_order(
            connector_name=self.config.maker_exchange,
            trading_pair=self.config.trading_pair,
            order_type=OrderType.LIMIT,
            side=TradeType.SELL if is_taker_buy else TradeType.BUY,
            amount=amount,
            price=self.maker_target_price)
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(f"vvv: Sent maker order id = {order_id} at price {self.maker_target_price} on {self.config.trading_pair}")

    def quantize_amount_on_maker_and_taker(self, amount):
        """
        Quantizes amounts for both maker and taker to match minimal requirements.
        """
        amount_quantized_on_maker = self.connectors[self.config.maker_exchange].quantize_order_amount(self.config.trading_pair, amount)
        amount_quantized_on_taker = self.connectors[self.config.taker_exchange].quantize_order_amount(self.config.trading_pair, amount)
        self.logger().info(f"vvv: amount_quantized_on_maker = {amount_quantized_on_maker}, amount_quantized_on_taker = {amount_quantized_on_taker}")
        return min(amount_quantized_on_maker, amount_quantized_on_taker)
    
    async def update_maker_order(self, is_taker_buy: bool, amount: Decimal):
        """
        Monitors and updates existing maker orders based on profitability thresholds.
        Cancels orders that fall outside acceptable profitability range.
        """
        # trade_profitability = self.get_current_trade_profitability() - self.total_fee_pct
        trade_profitability = self.get_current_trade_profitability()
        config_profitability = self.config.target_closing_profitability if is_taker_buy else self.config.target_opening_profitability
        min_profitability = config_profitability - self.config.profitability_range
        max_profitability = config_profitability + self.config.profitability_range
        if trade_profitability < min_profitability:
            self.logger().info(f"vvv: Trade profitability {trade_profitability} on {self.config.trading_pair} is below minimum profitability. Cancelling order.")
            self._strategy.cancel(self.config.maker_exchange, self.config.trading_pair, self.maker_order.order_id)
        if trade_profitability > max_profitability:
            self.logger().info(f"vvv: Trade profitability {trade_profitability} on {self.config.trading_pair} is above target profitability. Cancelling order.")
            self._strategy.cancel(self.config.maker_exchange, self.config.trading_pair, self.maker_order.order_id)

    def get_current_trade_profitability(self):
        """
        Calculates the current profitability of the active trade as a percentage.
        """
        trade_profitability = Decimal("0")
        if self.maker_order and self.maker_order.order and self.maker_order.order.is_open:
            maker_price = self.maker_order.order.price
            if self.maker_order.order.trade_type == TradeType.BUY:
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
            self.logger().info(f"vvv: Maker order created, id = {event.order_id} on {self.config.trading_pair}")
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
            self.logger().info(f"vvv: Maker order canceled, id = {event.order_id} on {self.config.trading_pair}")
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
            self.logger().info(f"vvv: Maker order filled, id = {event.order_id} on {self.config.trading_pair}")
            self.maker_fill_timestamp = time.time()
            self.next_order_timestamp = self._strategy.current_timestamp + self.next_order_delay
            # self.pending_taker_order = True
            # self._status = RunnableStatus.SHUTTING_DOWN
            # self.close_type = CloseType.COMPLETED
    
    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        """
        Handles order failure events from the exchange.
        Tracks failed orders, increments retry counter, and evaluates if max retries reached.
        """
        if self.maker_order and self.maker_order.order_id == event.order_id:
            self.failed_orders.append(self.maker_order)
            self.maker_order = None
            self.logger().info(f"vvv: Failed to open maker order id = {event.order_id} on {self.config.trading_pair}, retry = {self._current_retries}")
            self.evaluate_max_retries()
            self._current_retries += 1
    
    def evaluate_max_retries(self):
        """
        Evaluates if maximum retry attempts have been reached.
        Stops the executor if max retries exceeded.
        """
        if self._current_retries > self._max_retries:
            self.close_type = CloseType.FAILED
            self.logger().error(f"vvv: Reached max retries {self._max_retries} to open maker order on {self.config.trading_pair}. Shutting down executor.")
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
        # if self._is_trade_completed():
        #     return self.config.paper_trade_amount_in_quote
        return Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        """
        Returns the cumulative fees in quote currency based on the paper trade amount if trade is completed, otherwise returns 0.
        """
        # if self._is_trade_completed():
        #     return self.config.paper_trade_amount_in_quote * (self.total_fee_pct / Decimal("100"))
        return Decimal("0")

    def get_net_pnl_quote(self) -> Decimal:
        """
        Returns the net profit/loss in quote currency based on the paper trade amount if trade is completed, otherwise returns 0.
        """
        # if self._is_trade_completed():
        #     return self.config.paper_trade_amount_in_quote * self.get_net_pnl_pct()
        return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        """
        Returns the net profit/loss as a percentage if trade is completed, otherwise returns 0.
        """
        # if self._is_trade_completed():
        #     if self.config.maker_side == TradeType.SELL:
        #         pnl = (self.maker_order.average_executed_price - self.taker_hedge_price) / self.maker_order.average_executed_price
        #     else:
        #         pnl = (self.taker_hedge_price - self.maker_order.average_executed_price) / self.maker_order.average_executed_price
        #     return pnl - self.total_fee_pct / Decimal("100")
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
            # if self.maker_fill_timestamp and self.taker_hedge_timestamp:
            #     hedge_delay_seconds = Decimal(str(self.taker_hedge_timestamp - self.maker_fill_timestamp))

        return {
            "timestamp": self.close_timestamp,
            "maker_exchange": self.config.maker_exchange,
            "taker_exchange": self.config.taker_exchange,
            "trading_pair": self.config.trading_pair,
            "maker_price": maker_price,
            "taker_price": taker_price,
            "pnl_with_fees_pct": pnl_with_fees_pct
        }