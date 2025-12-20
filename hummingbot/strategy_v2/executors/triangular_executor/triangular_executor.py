import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.triangular_executor.data_types import (
    HedgingState,
    TakerOrderInfo,
    TriangularExecutorConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.connector.trading_rule import TradingRule


class TriangularExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: TriangularExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)
        self.config: TriangularExecutorConfig = config
        self.taker_result_buy_price = Decimal("0")
        self.taker_result_sell_price = Decimal("0")
        self.total_fee_pct = self.config.fee_maker + 2 * self.config.fee_taker
        self.targer_profit_with_fees = (self.config.min_profit + self.config.max_profit) / 2 + self.total_fee_pct
        self.maker_bid_order = None
        self.maker_ask_order = None
        self.usdt_pair = config.taker_1_pair
        self.hedge_mode = False
        self.active_hedging_states: List[HedgingState] = []
        self.completion_timestamp: Optional[float] = None
        self.place_buy_order = False if self.config.quote_amount == Decimal("0") else True
        self.place_sell_order = False if self.config.base_amount == Decimal("0") else True
        # self.trading_rules_taker_1 = self.get_trading_rules(self.config.connector_name, self.config.taker_1_pair)
        # self.trading_rules_taker_2 = self.get_trading_rules(self.config.connector_name, self.config.taker_2_pair)

    async def control_task(self):
        """
        Control the order execution process based on the execution strategy.
        """
        if self.status == RunnableStatus.RUNNING:
            if self.hedge_mode and self.active_hedging_states:
                await self.process_hedging_states()
            await self.update_taker_prices()
            await self.control_maker_order()
            await self.place_maker_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            self.logger().info(f"TriangularExecutor is shutting down")
            self.stop()

    async def process_hedging_states(self):
        """
        Process active hedging states: check completion, retry failed orders, stop executor when done.
        """
        current_time = time.time()
        incomplete_states = [s for s in self.active_hedging_states if not s.is_complete()]
        failed_states = [s for s in self.active_hedging_states if s.is_failed(self.config.max_taker_retries)]

        # Check for failures - stop executor immediately
        if failed_states:
            self.logger().error(f"TriangularExecutor: {len(failed_states)} hedging state(s) failed after max retries. Stopping executor.")
            self.close_type = CloseType.FAILED
            self._status = RunnableStatus.SHUTTING_DOWN
            return

        # Reset completion timestamp if any incomplete states exist
        if incomplete_states:
            self.completion_timestamp = None

            # Retry pending taker orders
            for state in incomplete_states:
                for taker_name, taker_info in [("taker_1", state.taker_1), ("taker_2", state.taker_2)]:
                    if not taker_info.is_complete():
                        # Retry if: order never placed OR order placed but hasn't completed after delay
                        should_retry = (
                            taker_info.order_id is None or
                            taker_info.sent_timestamp is None or
                            (current_time - taker_info.sent_timestamp >= self.config.taker_retry_delay)
                        )
                        
                        if should_retry and taker_info.trials < self.config.max_taker_retries:
                            self.logger().info(f"Retrying {taker_name} order (trial {taker_info.trials + 1}) for state created at {state.created_timestamp}")
                            self.place_taker_order(taker_info, log_retry=True)
        else:
            # All states complete
            if self.completion_timestamp is None:
                # First time all complete - set timestamp
                self.completion_timestamp = current_time
                self.logger().info(f"All hedging states completed. Waiting {self.config.completion_wait_time}s before stopping executor.")
            elif current_time - self.completion_timestamp >= self.config.completion_wait_time:
                # Wait time elapsed - stop executor
                self.logger().info(f"Completion wait time elapsed. Stopping executor.")
                self.close_type = CloseType.COMPLETED
                self._status = RunnableStatus.SHUTTING_DOWN

    async def update_taker_prices(self):
        """
        Update the prices of the maker and taker orders.
        """
        maker_bid_price = self.get_price(self.config.connector_name, self.config.maker_pair, price_type=PriceType.BestBid) 
        maker_ask_price = self.get_price(self.config.connector_name, self.config.maker_pair, price_type=PriceType.BestAsk)

        # Sell order on maker buy on taker
        if self.place_sell_order:
            sell_amount_base = self.config.base_amount
            sell_amount_quote = sell_amount_base * maker_ask_price

            sell_side_taker_1_price = await self.get_resulting_price_for_amount(self.config.connector_name, self.config.taker_1_pair, True, sell_amount_base)
            sell_side_taker_2_price = await self.get_resulting_price_for_amount(self.config.connector_name, self.config.taker_2_pair, False, sell_amount_quote)
            self.taker_result_buy_price = sell_side_taker_1_price / sell_side_taker_2_price
            self.maker_target_sell_price = self.taker_result_buy_price * (1 + self.targer_profit_with_fees / Decimal("100"))
            # self.logger().info(f"sell side taker_1_price: {sell_side_taker_1_price}")
            # self.logger().info(f"sell side taker_2_price: {sell_side_taker_2_price}")
            # self.logger().info(f"sell side target_profit_with_fees: {self.targer_profit_with_fees}")
            # self.logger().info(f"sell side maker_target_sell_price: {self.maker_target_sell_price}")          

        # Buy order on maker sell on taker
        if self.place_buy_order:
            buy_amount_quote = self.config.quote_amount
            buy_amount_base = buy_amount_quote / maker_bid_price

            buy_side_taker_1_price = await self.get_resulting_price_for_amount(self.config.connector_name, self.config.taker_1_pair, False, buy_amount_base)
            buy_side_taker_2_price = await self.get_resulting_price_for_amount(self.config.connector_name, self.config.taker_2_pair, True, buy_amount_quote)
            self.taker_result_sell_price = buy_side_taker_1_price / buy_side_taker_2_price
            self.maker_target_buy_price = self.taker_result_sell_price * (1 - self.targer_profit_with_fees / Decimal("100"))
            # self.logger().info(f"buy side taker_1_price: {buy_side_taker_1_price}")
            # self.logger().info(f"buy side taker_2_price: {buy_side_taker_2_price}")
            # self.logger().info(f"buy side target_profit_with_fees: {self.targer_profit_with_fees}")
            # self.logger().info(f"buy side maker_target_buy_price: {self.maker_target_buy_price}")

    async def get_resulting_price_for_amount(self, connector: str, trading_pair: str, is_buy: bool,
                                             order_amount: Decimal):
        """Get the resulting price for a given amount"""
        return await self.connectors[connector].get_quote_price(trading_pair, is_buy, order_amount)

    
    async def place_maker_order(self):
        """
        Place the maker order.
        """
        if self.place_buy_order and self.maker_bid_order is None and not self.hedge_mode:
            current_price = self.get_price(self.config.connector_name, self.config.maker_pair, price_type=PriceType.BestAsk)
            amount_to_buy = self.config.quote_amount / current_price
            bid_order_id = await self.send_maker_order_to_exchange(side=TradeType.BUY, amount=amount_to_buy, price=self.maker_target_buy_price)
            self.maker_bid_order = TrackedOrder(order_id=bid_order_id)
        if self.place_sell_order and self.maker_ask_order is None and not self.hedge_mode:
            ask_order_id = await self.send_maker_order_to_exchange(side=TradeType.SELL, amount=self.config.base_amount, price=self.maker_target_sell_price)
            self.maker_ask_order = TrackedOrder(order_id=ask_order_id)

    async def send_maker_order_to_exchange(self, side: TradeType, amount: Decimal, price: Decimal):
        """
        Create the maker bid order.
        """
        order_candidate = OrderCandidate(
            trading_pair=self.config.maker_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=side,
            amount=amount,
            price=price)

        adjusted_candidate = self.connectors[self.config.connector_name].budget_checker.adjust_candidate(order_candidate, all_or_none=False)
        if adjusted_candidate.amount == Decimal("0"):
            self.logger().info(f"Not enough balance to place maker {side.name} order amount {amount} at price {price} on {self.config.maker_pair}")
            return None
        
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.maker_pair,
            order_type=OrderType.LIMIT,
            side=side,
            amount=adjusted_candidate.amount,
            price=price)
        self.logger().info(f"Sent maker {side.name} order amount {amount} at price {price} on {self.config.maker_pair}, id = {order_id} ")
        return order_id

    async def control_maker_order(self):
        """
        Control the maker order.
        """
        if self.maker_bid_order and self.maker_bid_order.order and self.maker_bid_order.order.is_open:
            order_buy_price = self.maker_bid_order.order.price
            potential_sell_price = self.taker_result_sell_price
            self.check_and_cancel_maker_order(self.maker_bid_order, order_buy_price, potential_sell_price, "Bid")

        if self.maker_ask_order and self.maker_ask_order.order and self.maker_ask_order.order.is_open:
            order_sell_price = self.maker_ask_order.order.price
            potential_buy_price = self.taker_result_buy_price
            self.check_and_cancel_maker_order(self.maker_ask_order, potential_buy_price, order_sell_price, "Ask")
    
    def check_and_cancel_maker_order(self, order: TrackedOrder, buy_price: Decimal, sell_price: Decimal , type: str):
        profitability = Decimal("100") * (sell_price - buy_price) / buy_price - self.total_fee_pct
        # self.logger().info(f"{type} order {order.order_id} Trade profitability {profitability} on {self.config.maker_pair}")
        if profitability < self.config.min_profit or profitability > self.config.max_profit:
            self.logger().info(f"{type} order {order.order_id} Trade profitability {profitability} on {self.config.maker_pair} is out of profitability range. Cancelling order.")
            self._strategy.cancel(self.config.connector_name, self.config.maker_pair, order.order_id)

    
    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        """
        Handles order created events from the exchange.
        Updates the maker order tracking and resets retry counter when maker order is created.
        """
        if self.maker_bid_order and event.order_id == self.maker_bid_order.order_id:
            self.logger().info(f"Maker bid order created, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_bid_order.order = self.get_in_flight_order(self.config.connector_name, event.order_id)
        if self.maker_ask_order and event.order_id == self.maker_ask_order.order_id:
            self.logger().info(f"Maker ask order created, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_ask_order.order = self.get_in_flight_order(self.config.connector_name, event.order_id)

    def process_order_canceled_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: OrderCancelledEvent):
        """
        Handles order cancelled events from the exchange.
        Clears the maker order tracking when the maker order is cancelled.
        """
        if self.maker_bid_order and event.order_id == self.maker_bid_order.order_id:
            self.logger().info(f"Maker bid order canceled, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_bid_order = None
        if self.maker_ask_order and event.order_id == self.maker_ask_order.order_id:
            self.logger().info(f"Maker ask order canceled, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_ask_order = None

    
    def process_order_filled_event(self,
                                   event_tag: int,
                                   market: ConnectorBase,
                                   event: OrderFilledEvent):
        """
        Handles order filled events from the exchange.
        When maker order is filled, creates HedgingState and places taker orders.
        When taker orders are filled, tracks them in the corresponding HedgingState.
        """
        # Maker order filled
        if (self.maker_bid_order and event.order_id == self.maker_bid_order.order_id) or \
           (self.maker_ask_order and event.order_id == self.maker_ask_order.order_id):
            self.logger().info(f"Maker order {event.trade_type.name} filled, id = {event.order_id} on {event.trading_pair}")
            if self.is_order_size_less_than_min(event.amount):
                self.logger().info(f"Filled order amount {event.amount} is less than the minimum usdt amount. Continue")
            else:
                if not self.hedge_mode:
                    self.hedge_mode = True
                    self.logger().info(f"---- Hedge mode enabled ----")
                
                # Create HedgingState
                taker_1_side = TradeType.SELL if event.trade_type == TradeType.BUY else TradeType.BUY
                taker_2_side = event.trade_type
                taker_2_amount = event.amount * event.price
                
                hedging_state = HedgingState(
                    maker_fill=event,
                    taker_1=TakerOrderInfo(
                        trading_pair=self.config.taker_1_pair,
                        side=taker_1_side,
                        amount=event.amount,
                    ),
                    taker_2=TakerOrderInfo(
                        trading_pair=self.config.taker_2_pair,
                        side=taker_2_side,
                        amount=taker_2_amount,
                    ),
                    created_timestamp=time.time(),
                )
                
                self.active_hedging_states.append(hedging_state)
                self.logger().info(f"Created new HedgingState for maker order {event.order_id}")
                
                # Place taker orders
                self.place_taker_order(hedging_state.taker_1)
                self.place_taker_order(hedging_state.taker_2)
                self.cancel_maker_orders()
        
        # Taker order filled - find matching HedgingState
        else:
            for state in self.active_hedging_states:
                if event.order_id == state.taker_1.order_id:
                    state.taker_1.filled_events.append(event)
                    self.logger().info(f"Taker 1 order {event.trade_type.name} filled, id = {event.order_id} on {event.trading_pair}, amount = {event.amount}")
                    break
                elif event.order_id == state.taker_2.order_id:
                    state.taker_2.filled_events.append(event)
                    self.logger().info(f"Taker 2 order {event.trade_type.name} filled, id = {event.order_id} on {event.trading_pair}, amount = {event.amount}")
                    break

    def process_order_completed_event(self,
                                      event_tag: int,
                                      market: ConnectorBase,
                                      event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        """
        Handles order completed events from the exchange.
        Updates the corresponding HedgingState when taker orders are completed.
        """
        for state in self.active_hedging_states:
            if event.order_id == state.taker_1.order_id:
                state.taker_1.completed = event
                self.logger().info(f"Taker 1 order completed, id = {event.order_id}, "
                                 f"base_amount = {event.base_asset_amount}, quote_amount = {event.quote_asset_amount}")
                break
            elif event.order_id == state.taker_2.order_id:
                state.taker_2.completed = event
                self.logger().info(f"Taker 2 order completed, id = {event.order_id}, "
                                 f"base_amount = {event.base_asset_amount}, quote_amount = {event.quote_asset_amount}")
                break

    def is_order_size_less_than_min(self, order_amount: Decimal):
        """
        Check if the order size is less than the minimum trading rule.
        """
        conversion_rate = self.get_price(self.config.connector_name, self.usdt_pair, price_type=PriceType.MidPrice)
        self.logger().info(f"conversion_rate: {conversion_rate}")
        self.logger().info(f"order_amount in usdt: {order_amount * conversion_rate}")
        return order_amount * conversion_rate < self.config.min_usdt

    
    def place_taker_order(self, taker_info: TakerOrderInfo, log_retry: bool = False) -> Optional[str]:
        """
        Place a taker order and update tracking.
        
        :param taker_info: The TakerOrderInfo to place order for
        :param log_retry: Whether to log retry messages
        :return: The order_id if successful, None otherwise
        """
        order_id = self.send_taker_order_to_exchange(
            taker_info.trading_pair,
            taker_info.side,
            taker_info.amount
        )
        if order_id:
            taker_info.order_id = order_id
            taker_info.sent_timestamp = time.time()
        taker_info.trials += 1
        
        if log_retry:
            if order_id:
                self.logger().info(f"Retried taker order on {taker_info.trading_pair}, new order_id = {order_id}, trial = {taker_info.trials}")
            else:
                self.logger().warning(f"Failed to retry taker order on {taker_info.trading_pair}, trial = {taker_info.trials}")
        
        return order_id

    def send_taker_order_to_exchange(self, trading_pair: str, side: TradeType, amount: Decimal):
        """
        Create the maker bid order.
        """
        price = self.get_price(self.config.connector_name, trading_pair, price_type=PriceType.MidPrice)
        # order_candidate = OrderCandidate(
        #     trading_pair=trading_pair,
        #     is_maker=False,
        #     order_type=OrderType.MARKET,
        #     order_side=side,
        #     amount=amount,
        #     price=price)

        balance_base = self.get_balance(self.config.connector_name, trading_pair.split("-")[0])
        balance_quote = self.get_balance(self.config.connector_name, trading_pair.split("-")[1])
        self.logger().info(f"Opening taker {side.name} order on {trading_pair}, balance_base: {balance_base}, balance_quote: {balance_quote}")
        # adjusted_candidate = self.connectors[self.config.connector_name].budget_checker.adjust_candidate(order_candidate, all_or_none=False)
        # if adjusted_candidate.amount == Decimal("0"):
        #     self.logger().info(f"Not enough balance to place taker {side.name} order amount {amount} on {trading_pair}")
        #     return None
        
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=trading_pair,
            order_type=OrderType.MARKET,
            side=side,
            amount=amount,
            price=Decimal("0"))
        self.logger().info(f"Sent taker {side.name} order amount {amount} on {trading_pair}, id = {order_id} ")
        
        return order_id

    
    def cancel_maker_orders(self):
        """
        Cancels the maker orders.
        """
        if self.maker_bid_order:
            self.logger().info(f"Cancelling maker bid order id = {self.maker_bid_order.order_id} on {self.config.maker_pair}")
            self._strategy.cancel(self.config.connector_name, self.config.maker_pair, self.maker_bid_order.order_id)
            self.maker_bid_order = None
        if self.maker_ask_order:
            self.logger().info(f"Cancelling maker ask order id = {self.maker_ask_order.order_id} on {self.config.maker_pair}")
            self._strategy.cancel(self.config.connector_name, self.config.maker_pair, self.maker_ask_order.order_id)

    
    async def validate_sufficient_balance(self):
        """
        Validates that the executor has sufficient balance to place orders.
        """
        pass

    def early_stop(self, keep_position: bool = False):
        """
        This method allows strategy to stop the executor early.
        """
        self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    def get_net_pnl_pct(self) -> Decimal:
        """
        Get the net profit and loss percentage by aggregating all fills from completed hedging states.

        :return: The net profit and loss percentage.
        """
        if not self.active_hedging_states:
            return Decimal("0")

        # Aggregate all fills from completed states
        maker_fills = []
        taker1_fills = []
        taker2_fills = []

        for state in self.active_hedging_states:
            if not state.is_complete():
                continue

            maker_fills.append(state.maker_fill)
            taker1_fills.extend(state.taker_1.filled_events)
            taker2_fills.extend(state.taker_2.filled_events)

        if not maker_fills or not taker1_fills or not taker2_fills:
            return Decimal("0")

        def calc_vwap(orders):
            total_amount = Decimal("0")
            total_value = Decimal("0")
            for o in orders:
                amt = o.amount
                value = o.price * amt
                total_amount += amt
                total_value += value
            if total_amount == Decimal("0"):
                return Decimal("0")
            return total_value / total_amount

        maker_vwap = calc_vwap(maker_fills)
        taker1_vwap = calc_vwap(taker1_fills)
        taker2_vwap = calc_vwap(taker2_fills)

        # Identify whether maker was buy or sell by inspecting first maker fill
        maker_side = maker_fills[0].trade_type if maker_fills else None
        if maker_side is None:
            return Decimal("0")

        amount = Decimal("1")
        is_buy = maker_side == TradeType.BUY

        # Trade 1: Maker
        amount = amount / maker_vwap if is_buy else amount * maker_vwap

        # Trade 2: Taker1 (always opposite of maker)
        amount = amount * taker1_vwap if is_buy else amount / taker1_vwap

        # Trade 3: Taker2 (direction depends on whether taker quotes match)
        amount = amount / taker2_vwap if is_buy else amount * taker2_vwap

        return Decimal("100") * (amount - Decimal("1")) - self.total_fee_pct

    def get_net_pnl_quote(self) -> Decimal:
        """
        Get the net profit and loss in quote currency.

        :return: The net profit and loss in quote currency.
        """
        # Sum maker amounts from all completed hedging states
        total_maker_amount = Decimal("0")
        for state in self.active_hedging_states:
            if state.is_complete():
                total_maker_amount += state.maker_fill.amount

        if not total_maker_amount:
            return Decimal("0")

        conversion_rate = self.get_price(self.config.connector_name, self.usdt_pair, price_type=PriceType.MidPrice)
        total_maker_amount_in_usdt = total_maker_amount * conversion_rate
        pnl = self.get_net_pnl_pct() / Decimal("100")

        return total_maker_amount_in_usdt * pnl

    def get_cum_fees_quote(self) -> Decimal:
        """
        Get the cumulative fees in quote currency.

        :return: The cumulative fees in quote currency.
        """
        return Decimal("0")