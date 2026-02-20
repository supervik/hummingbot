import csv
import logging
import os
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderBookBestBidAskEvent,
    OrderBookDataSourceEvent,
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
    TakerPairDepthTracker,
    TriangularExecutorConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

# TODO: Make csv file with cancelled order timestamps (transactTime) from exchange: both for hb and ws
# TODO: Run both file with minimal profitaility to check if the ws is faster than hb
# TODO: Run both files in parralel for some time and compare the results
# TODO: Add websocket to Binance connector and repeat the experiment
# Find who is faster: hb or ws and fix hb if needed

class TriangularExecutor(ExecutorBase):
    _logger = None
    MIN_DEPTH_SAMPLES = 10  # Minimum samples required before using depth for calculations
    #1/4 CSV_SYNC_PATH = "scripts/data/triangular_sync_snapshot.csv"  # Disabled: CSV sync not used currently

    def notify(self, level: str, message: str, to_app: bool = False):
        """
        Unified logger + app notifier with maker-pair prefix and wall-clock timestamp (for app).

        :param level: 'info', 'warning', or 'error'
        :param message: The message to log/notify
        :param to_app: Whether to also send the message to the HB app
        """
        full_message = f"({self.config.maker_pair}) {message}"

        # Log
        if level == "error":
            self.logger().error(full_message)
        elif level == "warning":
            self.logger().warning(full_message)
        else:
            self.logger().info(full_message)

        # Notify app if requested, with real-time timestamp including milliseconds
        if to_app:
            try:
                ts = time.time()
                # Format as ISO-like string with millisecond precision
                ts_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                msg_with_ts = f"({ts_str}) {full_message}"
                # ScriptStrategyBase always has notify_hb_app
                self._strategy.notify_hb_app(msg_with_ts)
            except Exception:
                # Never break executor flow because of notification issues
                pass

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
        self.target_profit_with_fees = (self.config.min_profit + self.config.max_profit) / 2 + self.total_fee_pct
        self.maker_target_buy_price = Decimal("0")
        self.maker_target_sell_price = Decimal("0")
        self.maker_bid_order = None
        self.maker_ask_order = None
        self.maker_bid_cancellation_in_progress = False
        self.maker_ask_cancellation_in_progress = False
        # Track all active maker order IDs to handle race conditions (cancel + fill)
        self.current_maker_order_ids = set()
        self.hedge_mode = False
        self.active_hedging_states: List[HedgingState] = []
        self.completion_timestamp: Optional[float] = None
        self.place_buy_order = False if self.config.quote_amount == Decimal("0") else True
        self.place_sell_order = False if self.config.base_amount == Decimal("0") else True
        # Hedge latency metrics (per-executor, single-cycle)
        self.delay_server: Optional[float] = None
        self.delay_exchange: Optional[float] = None
        
        # Track activity timestamps for stuck executor detection
        self.last_maker_order_timestamp: Optional[float] = None
        self.last_taker_order_timestamp: Optional[float] = None
        self.last_activity_timestamp: float = time.time()
        self.trading_rules_maker = self.get_trading_rules(self.config.connector_name, self.config.maker_pair)
        self.trading_rules_taker_1 = self.get_trading_rules(self.config.connector_name, self.config.taker_1_pair)
        self.trading_rules_taker_2 = self.get_trading_rules(self.config.connector_name, self.config.taker_2_pair)

        # Initialize depth trackers for both taker pairs
        self.taker_1_depth = TakerPairDepthTracker()
        self.taker_2_depth = TakerPairDepthTracker()

        # Detect triangle type once at init (TYPE_A or TYPE_B)
        self.triangle_type = self._detect_triangle_type()

        # Find the pair that converts maker_base → usdt currency
        self.usdt_pair = self._find_base_usd_pair()

        self._best_bidask_forwarder = SourceInfoEventForwarder(self.process_best_bidask_event)

    def _detect_triangle_type(self) -> str:
        """
        Detect triangle type based on where the bridge asset sits in the taker pairs.

        TYPE_A: bridge asset is quote of both taker pairs (taker_1_quote == taker_2_quote)
                e.g. ATOM-BTC ATOM-USDT BTC-USDT  (bridge = USDT)
        TYPE_B: bridge asset is quote of taker_1 and base of taker_2 (taker_1_quote == taker_2_base)
                e.g. ATOM-USDT ATOM-BTC BTC-USDT  (bridge = BTC)
        """
        _, t1_quote = self.config.taker_1_pair.split("-")
        t2_base, t2_quote = self.config.taker_2_pair.split("-")

        if t1_quote == t2_quote:
            self.notify("info", f"Triangle type: TYPE_A (bridge={t1_quote})")
            return "TYPE_A"
        elif t1_quote == t2_base:
            self.notify("info", f"Triangle type: TYPE_B (bridge={t1_quote})")
            return "TYPE_B"
        else:
            self.notify(
                "error",
                f"Unsupported triangle: taker_1={self.config.taker_1_pair}, "
                f"taker_2={self.config.taker_2_pair}. "
                f"taker_1 quote must match either taker_2 quote (TYPE_A) or taker_2 base (TYPE_B). "
                f"Stopping executor.",
                to_app=True,
            )
            self.close_type = CloseType.FAILED
            self._status = RunnableStatus.SHUTTING_DOWN
            return "TYPE_A"  # safe default to avoid further attribute errors before shutdown

    def _find_base_usd_pair(self) -> str:
        """
        Find the pair that converts maker base asset → terminal currency (taker_2_quote).
        The terminal currency is always taker_2_quote — the asset that closes the round trip.

        e.g. ATOM-BTC ATOM-USDT BTC-USDT → terminal=USDT, returns ATOM-USDT (taker_1)
             ATOM-USDT ATOM-BTC BTC-USDT → terminal=USDT, returns ATOM-USDT (maker)
             BTC-EUR BTC-USDT USDT-EUR   → terminal=USDT,  returns BTC-USDT  (taker_1)
        """
        if 'USD' in self.config.taker_1_pair:
            return self.config.taker_1_pair
        elif 'USD' in self.config.maker_pair:
            return self.config.maker_pair
        else:
            self.notify("error", f"Could not find USD pair for conversion, Stopping executor", to_app=True)
            self.close_type = CloseType.FAILED
            self._status = RunnableStatus.SHUTTING_DOWN
        return None

    async def on_start(self):
        """
        Initializes the executor. If liquidate_base_assets is configured,
        liquidates assets and stops. Otherwise proceeds with normal startup.
        """
        self.subscribe_to_events()
        # self.notify("info", f"Maker trading rules: {self.trading_rules_maker}")
        # self.notify("info", f"Taker 1 trading rules: {self.trading_rules_taker_1}")
        # self.notify("info", f"Taker 2 trading rules: {self.trading_rules_taker_2}")
        
        #2/4 CSV sync disabled for now (file creation and order-id logging)
        # csv_path = self.CSV_SYNC_PATH
        # os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        # with open(csv_path, 'w', newline='') as f:
        #     writer = csv.writer(f)
        #     writer.writerow(['timestamp', 'minute', 'target_price', 'order_id'])
        # self.notify("info", f"Initialized CSV sync file: {csv_path}", to_app=True)
        
        await super().on_start()

    def on_stop(self):
        self.unsubscribe_from_events()
        super().on_stop()

    def subscribe_to_events(self):
        self.notify("info", "Subscribing to best bid and ask")
        self.connectors[self.config.connector_name].add_listener(OrderBookDataSourceEvent.BEST_BID_ASK_EVENT, self._best_bidask_forwarder)

    def unsubscribe_from_events(self):
        self.notify("info", "Unsubscribing from best bid and ask")
        self.connectors[self.config.connector_name].remove_listener(OrderBookDataSourceEvent.BEST_BID_ASK_EVENT, self._best_bidask_forwarder)

    async def control_task(self):
        """
        Control the order execution process based on the execution strategy.
        """
        if self.status == RunnableStatus.RUNNING:
            if self.hedge_mode and self.active_hedging_states:
                await self.process_hedging_states()
            await self.calculate_taker_depth()
            await self.update_maker_target_prices()
            await self.place_maker_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            self.notify("info", f"TriangularExecutor is shutting down. Reason: {self.close_type.name}")
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
            self.notify(
                "error",
                f"Triangular executor failed after max taker retries "
                f"({len(failed_states)} hedging state(s) failed). Stopping executor."
            )
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
                            self.notify("info", f"Retrying {taker_name} order (trial {taker_info.trials + 1}) for state created at {state.created_timestamp}")
                            self.place_taker_order(taker_info, log_retry=True)
        else:
            # All states complete
            if self.completion_timestamp is None:
                # First time all complete - compute hedge latency metrics and set timestamp
                try:
                    # Use the latest hedging state by creation time (per executor cycle)
                    latest_state = max(self.active_hedging_states, key=lambda s: s.created_timestamp)

                    # Server-side hedge delay (maker fill to last taker fill, as seen by our process)
                    if latest_state.maker_fill_server_ts is not None and latest_state.last_taker_fill_server_ts is not None:
                        self.delay_server = latest_state.last_taker_fill_server_ts - latest_state.maker_fill_server_ts

                    # Exchange-side hedge delay based on TradeUpdate.fill_timestamp
                    if (latest_state.maker_fill_exchange_ts is not None and
                            latest_state.last_taker_fill_exchange_ts is not None):
                        self.delay_exchange = (
                            latest_state.last_taker_fill_exchange_ts - latest_state.maker_fill_exchange_ts
                        )
                except Exception as e:
                    # Never fail the executor due to metrics calculation
                    self.notify("warning", f"Error computing hedge latency metrics: {e}")

                self.completion_timestamp = current_time
                self.notify("info", f"All hedging states completed. Waiting {self.config.completion_wait_time}s before stopping executor.")
            elif current_time - self.completion_timestamp >= self.config.completion_wait_time:
                # Wait time elapsed - check PnL and stop executor
                pnl_pct = round(self.get_net_pnl_pct(),2)
                pnl_quote = round(self.get_net_pnl_quote(),2)
                self.notify("info", f"Completion wait time elapsed. Stopping executor. PNL: {pnl_pct}%")
                
                # Check if PnL is below kill switch threshold
                if pnl_pct < self.config.kill_switch_pnl_threshold:
                    self.close_type = CloseType.STOP_LOSS
                    self.notify(
                        "warning",
                        f"----- !!!!!! STOP_LOSS triggered. PNL {pnl_pct}% is below threshold "
                        f"{self.config.kill_switch_pnl_threshold}%.",
                        to_app=True,
                    )
                else:
                    self.close_type = CloseType.COMPLETED
                    self.notify("info", f"------ Executor completed. Final PnL: {pnl_pct}% ({pnl_quote})", to_app=True)
                
                self._status = RunnableStatus.SHUTTING_DOWN

    def _calculate_depth_for_side(self, depth_tracker: TakerPairDepthTracker, trading_pair: str,
                                   is_buy: bool, volume: Decimal, use_quote_volume: bool = False):
        """
        Helper method to calculate depth for a specific side.
        
        :param depth_tracker: The depth tracker to update
        :param trading_pair: Trading pair to calculate depth for
        :param is_buy: True for buy side, False for sell side
        :param volume: Volume to calculate depth for
        :param use_quote_volume: If True, use get_price_for_quote_volume, else get_price_for_volume
        """
        try:
            if use_quote_volume:
                result = self.connectors[self.config.connector_name].get_price_for_quote_volume(
                    trading_pair=trading_pair,
                    is_buy=is_buy,
                    volume=volume
                )
            else:
                result = self.connectors[self.config.connector_name].get_price_for_volume(
                    trading_pair=trading_pair,
                    is_buy=is_buy,
                    volume=volume
                )
            
            execution_price = result.result_price if result.result_price else Decimal("0")
            best_price_type = PriceType.BestAsk if is_buy else PriceType.BestBid
            best_price = self.get_price(self.config.connector_name, trading_pair, price_type=best_price_type)
            
            if execution_price > Decimal("0") and best_price > Decimal("0"):
                if is_buy:
                    depth_tracker.update_depth_buy(execution_price, best_price)
                else:
                    depth_tracker.update_depth_sell(execution_price, best_price)
        except Exception as e:
            side_name = "buy" if is_buy else "sell"
            self.notify("warning", f"Error calculating {trading_pair} {side_name} depth: {e}")

    def _get_last_fill_exchange_timestamp(self, order_id: str) -> Optional[float]:
        """
        Helper to fetch the latest exchange-side fill timestamp for a given order id
        using TradeUpdate.fill_timestamp from the in-flight order.
        Metrics-only: failures are swallowed and reported as None.
        """
        try:
            in_flight = self.get_in_flight_order(self.config.connector_name, order_id)
            if in_flight is not None and in_flight.order_fills:
                return max(trade.fill_timestamp for trade in in_flight.order_fills.values())
        except Exception:
            # Metrics-only path; ignore failures
            pass
        return None

    async def calculate_taker_depth(self):
        """
        Calculate order book depth for taker pairs based on order amounts.
        This runs continuously in the control loop to build depth history.

        TYPE_A (e.g. ATOM-BTC ATOM-USDT BTC-USDT, bridge=USDT in quote of both takers):
          Sell on maker → taker_1 BUY, taker_2 SELL
          Buy on maker  → taker_1 SELL, taker_2 BUY

        TYPE_B (e.g. ATOM-USDT ATOM-BTC BTC-USDT, bridge=BTC in quote of taker_1, base of taker_2):
          Sell on maker → taker_1 BUY, taker_2 BUY
          Buy on maker  → taker_1 SELL, taker_2 SELL
        """
        maker_bid_price = self.get_price(self.config.connector_name, self.config.maker_pair, price_type=PriceType.BestBid)
        maker_ask_price = self.get_price(self.config.connector_name, self.config.maker_pair, price_type=PriceType.BestAsk)

        # Sell order on maker, buy on taker_1 (same base, opposite side)
        if self.place_sell_order:
            sell_amount_base = self.config.base_amount
            sell_amount_quote = sell_amount_base * maker_ask_price

            # taker_1: always BUY when selling on maker (opposite side, same base amount)
            self._calculate_depth_for_side(
                self.taker_1_depth, self.config.taker_1_pair, is_buy=True,
                volume=sell_amount_base, use_quote_volume=False
            )

            if self.triangle_type == "TYPE_A":
                # taker_2 SELL: sell_amount_quote is the maker quote = taker_2 base (e.g. BTC)
                self._calculate_depth_for_side(
                    self.taker_2_depth, self.config.taker_2_pair, is_buy=False,
                    volume=sell_amount_quote, use_quote_volume=False
                )
            else:  # TYPE_B
                # taker_2 BUY: maker quote (e.g. USDT) is taker_2 quote → use quote volume directly
                self._calculate_depth_for_side(
                    self.taker_2_depth, self.config.taker_2_pair, is_buy=True,
                    volume=sell_amount_quote, use_quote_volume=True
                )

        # Buy order on maker, sell on taker_1 (same base, opposite side)
        if self.place_buy_order:
            buy_amount_quote = self.config.quote_amount
            buy_amount_base = buy_amount_quote / maker_bid_price if maker_bid_price > Decimal("0") else Decimal("0")

            # taker_1: always SELL when buying on maker (opposite side, same base amount)
            if buy_amount_base > Decimal("0"):
                self._calculate_depth_for_side(
                    self.taker_1_depth, self.config.taker_1_pair, is_buy=False,
                    volume=buy_amount_base, use_quote_volume=False
                )

            if self.triangle_type == "TYPE_A":
                # taker_2 BUY: buy_amount_quote is the maker quote = taker_2 base (e.g. BTC)
                self._calculate_depth_for_side(
                    self.taker_2_depth, self.config.taker_2_pair, is_buy=True,
                    volume=buy_amount_quote, use_quote_volume=False
                )
            else:  # TYPE_B
                # taker_2 SELL: maker quote (e.g. USDT) is taker_2 quote → use quote volume directly
                self._calculate_depth_for_side(
                    self.taker_2_depth, self.config.taker_2_pair, is_buy=False,
                    volume=buy_amount_quote, use_quote_volume=True
                )

    async def update_maker_target_prices(self):
        """
        Update the maker target prices based on current taker result prices.
        Taker result prices are calculated in event handler, this only calculates maker target prices.
        This is called from control loop to update maker order prices when placing new orders.
        """
        # Only calculate maker target prices if we have valid taker result prices (from events)
        # Sell order on maker buy on taker
        if self.place_sell_order and self.taker_result_buy_price > Decimal("0"):
            target_sell_price = self.taker_result_buy_price * (1 + self.target_profit_with_fees / Decimal("100"))
            tick_size = self.trading_rules_maker.min_price_increment
            # quantize the target sell price
            self.maker_target_sell_price = target_sell_price // tick_size * tick_size
            # self.logger().info(f"Maker target sell price: {self.maker_target_sell_price}, price before quantization: {target_sell_price}")
            
        # Buy order on maker sell on taker
        if self.place_buy_order and self.taker_result_sell_price > Decimal("0"):
            target_buy_price = self.taker_result_sell_price * (1 - self.target_profit_with_fees / Decimal("100"))
            tick_size = self.trading_rules_maker.min_price_increment
            # quantize the target buy price
            self.maker_target_buy_price = target_buy_price // tick_size * tick_size
            # self.logger().info(f"Maker target buy price: {self.maker_target_buy_price}, price before quantization: {target_buy_price}")

    #3/4 def write_price_snapshot(self, target_price: Decimal, order_id: str):
    #     """
    #     Write price snapshot to CSV file for slave synchronization.
    #     Appends to file (header should already exist from on_start).
    #     """
    #     csv_path = self.CSV_SYNC_PATH
    #     current_time = time.time()
    #     current_minute = int(current_time) // 60
    #     
    #     with open(csv_path, 'a', newline='') as f:
    #         writer = csv.writer(f)
    #         writer.writerow([current_time, current_minute, str(target_price), order_id])

    async def place_maker_order(self):
        """
        Place the maker order. Only places at sync time (minute boundaries).
        """
        if self.place_buy_order and self.maker_bid_order is None and not self.hedge_mode:
            if self.taker_result_sell_price == Decimal("0"):
                self.notify("info", "Waiting for taker result price (buy side)")
                return
            current_price = self.get_price(self.config.connector_name, self.config.maker_pair, price_type=PriceType.BestAsk)
            amount_to_buy = self.config.quote_amount / current_price
            bid_order_id = await self.send_maker_order_to_exchange(side=TradeType.BUY, amount=amount_to_buy, price=self.maker_target_buy_price)
            if bid_order_id:
                self.maker_bid_order = TrackedOrder(order_id=bid_order_id)
                self.current_maker_order_ids.add(bid_order_id)
                self.last_maker_order_timestamp = time.time()
                self.last_activity_timestamp = time.time()
                #4/4 Write price snapshot to CSV for slave synchronization
                # self.write_price_snapshot(self.maker_target_buy_price, bid_order_id)
        if self.place_sell_order and self.maker_ask_order is None and not self.hedge_mode:
            if self.taker_result_buy_price == Decimal("0"):
                self.notify("info", "Waiting for taker result price (sell side)")
                return
            ask_order_id = await self.send_maker_order_to_exchange(side=TradeType.SELL, amount=self.config.base_amount, price=self.maker_target_sell_price)
            if ask_order_id:
                self.maker_ask_order = TrackedOrder(order_id=ask_order_id)
                self.current_maker_order_ids.add(ask_order_id)
                self.last_maker_order_timestamp = time.time()
                self.last_activity_timestamp = time.time()

    async def send_maker_order_to_exchange(self, side: TradeType, amount: Decimal, price: Decimal):
        """
        Create and send a maker limit order to the exchange.
        """
        order_candidate = OrderCandidate(
            trading_pair=self.config.maker_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=side,
            amount=amount,
            price=price)

        adjusted_candidate = self.connectors[self.config.connector_name].budget_checker.adjust_candidate(order_candidate, all_or_none=False)
        quantized_amount = self.connectors[self.config.connector_name].quantize_order_amount(self.config.maker_pair, adjusted_candidate.amount)
        
        if quantized_amount < self.trading_rules_maker.min_order_size:
            self.notify("warning", f"Not enough balance to place maker {side.name} order amount {amount} (adjusted: {quantized_amount}) at price {price} on {self.config.maker_pair}")
            return None

        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.maker_pair,
            order_type=OrderType.LIMIT,
            side=side,
            amount=quantized_amount,
            price=price)
        self.notify("info", f"Sent maker {side.name} order amount {amount} (adjusted: {quantized_amount}) at price {price} on {self.config.maker_pair}, id = {order_id}")
        return order_id


    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        """
        Handles order created events from the exchange.
        Updates the maker order tracking and resets retry counter when maker order is created.
        """
        if self.maker_bid_order and event.order_id == self.maker_bid_order.order_id:
            self.notify("info", f"Maker bid order created, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_bid_order.order = self.get_in_flight_order(self.config.connector_name, event.order_id)
        if self.maker_ask_order and event.order_id == self.maker_ask_order.order_id:
            self.notify("info", f"Maker ask order created, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_ask_order.order = self.get_in_flight_order(self.config.connector_name, event.order_id)

    def process_order_canceled_event(self,
                                     event_tag: int,
                                     market: ConnectorBase,
                                     event: OrderCancelledEvent):
        """
        Handles order cancelled events from the exchange.
        Clears the maker order tracking when the maker order is cancelled.
        
        IMPORTANT: We do NOT remove order_id from current_maker_order_ids here!
        This is to handle the race condition where cancellation arrives before a fill event.
        The fill may have happened on the exchange before cancellation, but the fill event
        arrives to our system after the cancel event. We need to keep the order_id tracked
        so we can still start a hedge for that fill.
        """
        if self.maker_bid_order and event.order_id == self.maker_bid_order.order_id:
            self.notify("info", f"Maker bid order canceled, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_bid_order = None
            self.maker_bid_cancellation_in_progress = False
            # Do NOT remove from current_maker_order_ids - late fills may still arrive
        elif self.maker_ask_order and event.order_id == self.maker_ask_order.order_id:
            self.notify("info", f"Maker ask order canceled, id = {event.order_id} on {self.config.maker_pair}")
            self.maker_ask_order = None
            self.maker_ask_cancellation_in_progress = False
            # Do NOT remove from current_maker_order_ids - late fills may still arrive

    def _handle_failed_maker_order(self, order_type: str, order_id: str):
        """
        Helper method to handle failed maker order cleanup.
        
        :param order_type: "bid" or "ask"
        :param order_id: The failed order ID
        """
        self.notify(
            "warning",
            f"Maker {order_type} order {order_id} failed to be placed on {self.config.maker_pair}",
            to_app=True,
        )
        # Cancel the order if it exists
        try:
            self._strategy.cancel(self.config.connector_name, self.config.maker_pair, order_id)
        except Exception:
            pass  # Order may not exist, ignore cancellation errors
        
        # Clear order tracking and reset cancellation flag
        if order_type == "bid":
            self.maker_bid_order = None
            self.maker_bid_cancellation_in_progress = False
        else:  # ask
            self.maker_ask_order = None
            self.maker_ask_cancellation_in_progress = False
        
        # Remove from tracking set since this order is truly dead
        self.current_maker_order_ids.discard(order_id)

    def process_order_failed_event(self,
                                   event_tag: int,
                                   market: ConnectorBase,
                                   event: MarketOrderFailureEvent):
        """
        Handles order failed events from the exchange.
        Clears the maker order tracking so new orders can be placed.
        """
        # Check if this is a maker order failure
        if self.maker_bid_order and event.order_id == self.maker_bid_order.order_id:
            self._handle_failed_maker_order("bid", event.order_id)
        elif self.maker_ask_order and event.order_id == self.maker_ask_order.order_id:
            self._handle_failed_maker_order("ask", event.order_id)

    def process_order_filled_event(self,
                                   event_tag: int,
                                   market: ConnectorBase,
                                   event: OrderFilledEvent):
        """
        Handles order filled events from the exchange.
        When maker order is filled, creates HedgingState and places taker orders.
        When taker orders are filled, tracks them in the corresponding HedgingState.
        
        Uses current_maker_order_ids set to handle race condition where cancellation
        event arrives before fill event (order object may be None but ID still tracked).
        """
        # Maker order filled - check trading pair, then check if order ID is tracked
        # This handles race condition: cancel event arrives first, sets order to None,
        # then fill event arrives and we still need to hedge the partial fill
        is_maker_fill = (
            event.trading_pair == self.config.maker_pair and
            event.order_id in self.current_maker_order_ids
        )
        
        if is_maker_fill:
            self.notify(
                "info",
                f"Maker {event.trade_type.name} {event.amount} {event.trading_pair} filled at {event.price}",
                to_app=True,
            )
            # Reset cancellation flags - determine which side based on order_id
            # (order object might be None if cancellation arrived first)
            if self.maker_bid_order and event.order_id == self.maker_bid_order.order_id:
                self.maker_bid_cancellation_in_progress = False
            elif self.maker_ask_order and event.order_id == self.maker_ask_order.order_id:
                self.maker_ask_cancellation_in_progress = False
            else:
                # Order was already cancelled (object is None), reset both flags to be safe
                self.maker_bid_cancellation_in_progress = False
                self.maker_ask_cancellation_in_progress = False
            if self.is_order_size_less_than_min(event.amount):
                self.notify("info", f"Filled order amount {event.amount} is less than the minimum usdt amount. Continue")
            else:
                if not self.hedge_mode:
                    self.hedge_mode = True
                    self.notify("info", "---- Hedge mode enabled ----")

                # Create HedgingState
                taker_1_side = TradeType.SELL if event.trade_type == TradeType.BUY else TradeType.BUY
                maker_quote_amount = event.amount * event.price

                if self.triangle_type == "TYPE_A":
                    taker_2_side = event.trade_type
                    taker_2_amount = maker_quote_amount
                else:  # TYPE_B
                    taker_2_side = taker_1_side
                    t2_depth_price = self.taker_2_depth.best_bid_price if taker_1_side == TradeType.SELL else self.taker_2_depth.best_ask_price
                    t2_price_type = PriceType.BestBid if taker_1_side == TradeType.SELL else PriceType.BestAsk
                    taker_2_price = t2_depth_price or self.get_price(self.config.connector_name, self.config.taker_2_pair, price_type=t2_price_type)
                    if not taker_2_price:
                        self.notify("error", "Taker 2 price unavailable, skipping hedge.")
                        return
                    taker_2_amount = maker_quote_amount / taker_2_price

                # Compute exchange-side maker fill timestamp from in-flight order fills
                maker_fill_exchange_ts: Optional[float] = self._get_last_fill_exchange_timestamp(event.order_id)

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
                    maker_fill_server_ts=time.time(),
                    maker_fill_exchange_ts=maker_fill_exchange_ts,
                )
                self.active_hedging_states.append(hedging_state)
                self.notify("info", f"Created new HedgingState for maker order {event.order_id}")

                # Place taker orders
                self.place_taker_order(hedging_state.taker_1)
                self.place_taker_order(hedging_state.taker_2)
                self.last_taker_order_timestamp = time.time()
                self.last_activity_timestamp = time.time()
                self.cancel_maker_orders()

        # Taker order filled - find matching HedgingState
        else:
            for state in self.active_hedging_states:
                if event.order_id == state.taker_1.order_id:
                    state.taker_1.filled_events.append(event)
                    # Update last taker fill timestamps for hedge latency (per cycle)
                    now = time.time()
                    state.last_taker_fill_server_ts = now
                    last_exch_ts = self._get_last_fill_exchange_timestamp(event.order_id)
                    if last_exch_ts is not None:
                        state.last_taker_fill_exchange_ts = last_exch_ts
                    self.notify(
                        "info",
                        f"Taker 1 {event.trade_type.name} {event.amount} {event.trading_pair} filled at {event.price}",
                        to_app=True,
                    )
                    break
                elif event.order_id == state.taker_2.order_id:
                    state.taker_2.filled_events.append(event)
                    # Update last taker fill timestamps for hedge latency (per cycle)
                    now = time.time()
                    state.last_taker_fill_server_ts = now
                    last_exch_ts = self._get_last_fill_exchange_timestamp(event.order_id)
                    if last_exch_ts is not None:
                        state.last_taker_fill_exchange_ts = last_exch_ts
                    self.notify(
                        "info",
                        f"Taker 2 {event.trade_type.name} {event.amount} {event.trading_pair} filled at {event.price}",
                        to_app=True,
                    )
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
                self.notify("info", f"Taker 1 order completed, id = {event.order_id}, "
                                    f"base_amount = {event.base_asset_amount}, quote_amount = {event.quote_asset_amount}")
                break
            elif event.order_id == state.taker_2.order_id:
                state.taker_2.completed = event
                self.notify("info", f"Taker 2 order completed, id = {event.order_id}, "
                                    f"base_amount = {event.base_asset_amount}, quote_amount = {event.quote_asset_amount}")
                break

    def is_order_size_less_than_min(self, order_amount: Decimal):
        """
        Check if the order size is less than the minimum trading rule.
        """
        conversion_rate = self.get_price(self.config.connector_name, self.usdt_pair, price_type=PriceType.MidPrice)
        self.notify("info", f"conversion_rate: {conversion_rate}")
        self.notify("info", f"order_amount in usdt: {order_amount * conversion_rate}")
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
            self.last_taker_order_timestamp = time.time()
            self.last_activity_timestamp = time.time()
        taker_info.trials += 1

        if log_retry:
            if order_id:
                self.notify("info", f"Retried taker order on {taker_info.trading_pair}, new order_id = {order_id}, trial = {taker_info.trials}")
            else:
                self.notify("warning", f"Failed to retry taker order on {taker_info.trading_pair}, trial = {taker_info.trials}")

        return order_id

    def send_taker_order_to_exchange(self, trading_pair: str, side: TradeType, amount: Decimal):
        """
        Create and send a taker market order to the exchange.
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
        self.notify("info", f"Opening taker {side.name} order on {trading_pair}, balance_base: {balance_base}, balance_quote: {balance_quote}")
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
        self.notify("info", f"Sent taker {side.name} order amount {amount} on {trading_pair}, id = {order_id}")

        return order_id

    def cancel_maker_orders(self):
        """
        Cancels the maker orders.
        """
        if self.maker_bid_order:
            self.notify("info", f"Cancelling maker bid order id = {self.maker_bid_order.order_id} on {self.config.maker_pair}")
            self._strategy.cancel(self.config.connector_name, self.config.maker_pair, self.maker_bid_order.order_id)
            # self.maker_bid_order = None
        if self.maker_ask_order:
            self.notify("info", f"Cancelling maker ask order id = {self.maker_ask_order.order_id} on {self.config.maker_pair}")
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

        # Trade 3: Taker2
        # TYPE_A: taker2 price denominates the bridge (e.g. BTC-USDT bid when selling BTC) → divide
        # TYPE_B: taker2 price converts bridge to terminal asset in same direction as taker1 → multiply
        if self.triangle_type == "TYPE_A":
            amount = amount / taker2_vwap if is_buy else amount * taker2_vwap
        else:  # TYPE_B
            amount = amount * taker2_vwap if is_buy else amount / taker2_vwap

        return Decimal("100") * (amount - Decimal("1")) - self.total_fee_pct

    def get_net_pnl_quote(self) -> Decimal:
        """
        Get the net profit and loss in quote currency.

        :return: The net profit and loss in quote currency.
        """
        # Sum maker amounts from all completed hedging states
        # total_maker_amount = Decimal("0")
        # for state in self.active_hedging_states:
        #     if state.is_complete():
        #         total_maker_amount += state.maker_fill.amount

        # if not total_maker_amount:
        #     return Decimal("0")

        # conversion_rate = self.get_price(self.config.connector_name, self.usdt_pair, price_type=PriceType.MidPrice)
        # total_maker_amount_in_usdt = total_maker_amount * conversion_rate
        pnl = self.get_net_pnl_pct() / Decimal("100")

        return self.filled_amount_quote * pnl

    @property
    def filled_amount_quote(self):
        total_maker_amount = Decimal("0")
        for state in self.active_hedging_states:
            if state.is_complete():
                total_maker_amount += state.maker_fill.amount

        if not total_maker_amount:
            return Decimal("0")

        conversion_rate = self.get_price(self.config.connector_name, self.usdt_pair, price_type=PriceType.MidPrice)

        return total_maker_amount * conversion_rate

    def get_cum_fees_quote(self) -> Decimal:
        """
        Get the cumulative fees in quote currency.

        :return: The cumulative fees in quote currency.
        """
        return Decimal("0")


    def _are_active_sides_ready(self) -> bool:
        """
        Check if depth is ready for the sides that are actually being used.

        TYPE_A:
          Sell side: taker_1 BUY, taker_2 SELL
          Buy side:  taker_1 SELL, taker_2 BUY

        TYPE_B:
          Sell side: taker_1 BUY, taker_2 BUY
          Buy side:  taker_1 SELL, taker_2 SELL
        """
        if self.triangle_type == "TYPE_A":
            if self.place_sell_order:
                if not (self.taker_1_depth.is_buy_side_ready(self.MIN_DEPTH_SAMPLES) and
                        self.taker_2_depth.is_sell_side_ready(self.MIN_DEPTH_SAMPLES)):
                    return False
            if self.place_buy_order:
                if not (self.taker_1_depth.is_sell_side_ready(self.MIN_DEPTH_SAMPLES) and
                        self.taker_2_depth.is_buy_side_ready(self.MIN_DEPTH_SAMPLES)):
                    return False
        else:  # TYPE_B
            if self.place_sell_order:
                if not (self.taker_1_depth.is_buy_side_ready(self.MIN_DEPTH_SAMPLES) and
                        self.taker_2_depth.is_buy_side_ready(self.MIN_DEPTH_SAMPLES)):
                    return False
            if self.place_buy_order:
                if not (self.taker_1_depth.is_sell_side_ready(self.MIN_DEPTH_SAMPLES) and
                        self.taker_2_depth.is_sell_side_ready(self.MIN_DEPTH_SAMPLES)):
                    return False

        return True

    def process_best_bidask_event(self, event_tag: int, market, event: OrderBookBestBidAskEvent):
        """
        Handle best bid/ask events from taker pairs.
        Updates depth tracker prices and recalculates triangular prices.
        """
        if event.trading_pair != self.config.taker_1_pair and event.trading_pair != self.config.taker_2_pair:
            return
        
        # Check if depth is ready for active sides before doing calculations
        if not self._are_active_sides_ready():
            return
        
        # Check if prices changed before updating and recalculating
        prices_changed = False
        if event.trading_pair == self.config.taker_1_pair:
            # Check if prices actually changed
            prices_changed = (
                self.taker_1_depth.best_bid_price != event.best_bid_price or
                self.taker_1_depth.best_ask_price != event.best_ask_price
            )
            if prices_changed:
                # self.logger().info(f"Bookticker: {event.trading_pair}, best_bid: {event.best_bid_price}, best_ask: {event.best_ask_price}, best_bid_size: {event.best_bid_size}, best_ask_size: {event.best_ask_size}")
                self.taker_1_depth.update_best_prices(event.best_bid_price, event.best_ask_price)

        elif event.trading_pair == self.config.taker_2_pair:
            # Check if prices actually changed
            prices_changed = (
                self.taker_2_depth.best_bid_price != event.best_bid_price or
                self.taker_2_depth.best_ask_price != event.best_ask_price
            )
            if prices_changed:
                # self.logger().info(f"Bookticker: {event.trading_pair}, best_bid: {event.best_bid_price}, best_ask: {event.best_ask_price}, best_bid_size: {event.best_bid_size}, best_ask_size: {event.best_ask_size}")
                self.taker_2_depth.update_best_prices(event.best_bid_price, event.best_ask_price)
        
        # Only recalculate if prices actually changed
        if prices_changed:
            self._recalculate_taker_prices()
            self._check_maker_orders_profitability()

    def _recalculate_taker_prices(self):
        """
        Recalculate triangular taker result prices using event-based prices + depth.
        This is the single source of truth for taker result prices.

        TYPE_A (e.g. ATOM-BTC ATOM-USDT BTC-USDT, bridge=USDT):
          taker_result_buy_price  = taker_1_buy  / taker_2_sell  (ATOM-USDT ask / BTC-USDT bid)
          taker_result_sell_price = taker_1_sell / taker_2_buy   (ATOM-USDT bid / BTC-USDT ask)

        TYPE_B (e.g. ATOM-USDT ATOM-BTC BTC-USDT, bridge=BTC):
          taker_result_buy_price  = taker_1_buy  * taker_2_buy   (ATOM-BTC ask * BTC-USDT ask)
          taker_result_sell_price = taker_1_sell * taker_2_sell  (ATOM-BTC bid * BTC-USDT bid)
        """
        if self.triangle_type == "TYPE_A":
            # Sell order on maker: buy on taker_1, sell on taker_2
            if self.place_sell_order:
                taker_1_target_buy = self.taker_1_depth.get_target_price_buy()
                taker_2_target_sell = self.taker_2_depth.get_target_price_sell()
                if taker_1_target_buy > Decimal("0") and taker_2_target_sell > Decimal("0"):
                    self.taker_result_buy_price = taker_1_target_buy / taker_2_target_sell

            # Buy order on maker: sell on taker_1, buy on taker_2
            if self.place_buy_order:
                taker_1_target_sell = self.taker_1_depth.get_target_price_sell()
                taker_2_target_buy = self.taker_2_depth.get_target_price_buy()
                if taker_1_target_sell > Decimal("0") and taker_2_target_buy > Decimal("0"):
                    self.taker_result_sell_price = taker_1_target_sell / taker_2_target_buy

        else:  # TYPE_B
            # Sell order on maker: buy on taker_1, buy on taker_2
            if self.place_sell_order:
                taker_1_target_buy = self.taker_1_depth.get_target_price_buy()
                taker_2_target_buy = self.taker_2_depth.get_target_price_buy()
                if taker_1_target_buy > Decimal("0") and taker_2_target_buy > Decimal("0"):
                    self.taker_result_buy_price = taker_1_target_buy * taker_2_target_buy

            # Buy order on maker: sell on taker_1, sell on taker_2
            if self.place_buy_order:
                taker_1_target_sell = self.taker_1_depth.get_target_price_sell()
                taker_2_target_sell = self.taker_2_depth.get_target_price_sell()
                if taker_1_target_sell > Decimal("0") and taker_2_target_sell > Decimal("0"):
                    self.taker_result_sell_price = taker_1_target_sell * taker_2_target_sell

    def _check_maker_orders_profitability(self):
        """
        Check profitability of all existing maker orders and cancel if out of range.
        """
        # Check sell side maker order (ask order)
        if self.place_sell_order and self.taker_result_buy_price > Decimal("0") and self.maker_ask_order:
            self._validate_and_cancel_maker_order(
                self.maker_ask_order, TradeType.SELL, 
                self.taker_result_buy_price, "Ask"
            )

        # Check buy side maker order (bid order)
        if self.place_buy_order and self.taker_result_sell_price > Decimal("0") and self.maker_bid_order:
            self._validate_and_cancel_maker_order(
                self.maker_bid_order, TradeType.BUY,
                self.taker_result_sell_price, "Bid"
            )

    def _validate_and_cancel_maker_order(self, order: Optional[TrackedOrder], trade_type: TradeType,
                                        taker_result_price: Decimal, order_type: str):
        """
        Validate profitability of a single maker order and cancel if out of range.
        
        :param order: The tracked order to check
        :param trade_type: TradeType.BUY or TradeType.SELL
        :param taker_result_price: The calculated taker result price
        :param order_type: String identifier for logging ("Bid" or "Ask")
        """
        if not order or not order.order or not order.order.is_open:
            return
        
        # Check if cancellation is already in progress to prevent duplicate attempts
        cancellation_flag = self.maker_bid_cancellation_in_progress if trade_type == TradeType.BUY else self.maker_ask_cancellation_in_progress
        if cancellation_flag:
            self.notify("info", f"{order_type} order {order.order_id} cancellation already in progress, skipping")
            return
        
        order_price = order.order.price
        
        if trade_type == TradeType.BUY:
            self.check_and_cancel_maker_order(order, order_price, taker_result_price, order_type)
        else:  # SELL
            self.check_and_cancel_maker_order(order, taker_result_price, order_price, order_type)

    def check_and_cancel_maker_order(self, order: TrackedOrder, buy_price: Decimal, sell_price: Decimal , type: str):
        profitability = round(Decimal("100") * (sell_price - buy_price) / buy_price - self.total_fee_pct, 3)    
        # self.logger().info(f"{type} {self.config.maker_pair} {order.order_id} profitability {profitability}")
        if profitability < self.config.min_profit or profitability > self.config.max_profit:
            # Double-check order is still open before cancelling
            if not order.order or not order.order.is_open:
                self.notify("info", f"{type} order {order.order_id} is no longer open, skipping cancellation")
                return
            
            # Set cancellation flag to prevent duplicate attempts
            if type == "Bid":
                self.maker_bid_cancellation_in_progress = True
            else:
                self.maker_ask_cancellation_in_progress = True
            
            self.notify("info", f"{type} order {order.order_id} profitability {profitability} on {self.config.maker_pair} is out of profitability range. Cancelling order.")
            self._strategy.cancel(self.config.connector_name, self.config.maker_pair, order.order_id)

    def get_custom_info(self) -> Dict:
        """
        Returns custom information about the executor including activity timestamps.
        Used for monitoring executor activity and detecting stuck executors.
        """
        maker_pair = self.config.maker_pair
        base, quote = maker_pair.split("-")
        
        return {
            "maker_pair": maker_pair,
            "base": base,
            "quote": quote,
            "hedge_mode": self.hedge_mode,
            "last_maker_order_timestamp": self.last_maker_order_timestamp,
            "last_taker_order_timestamp": self.last_taker_order_timestamp,
            "last_activity_timestamp": self.last_activity_timestamp,
            "delay_server": self.delay_server,
            "delay_exchange": self.delay_exchange,
        }