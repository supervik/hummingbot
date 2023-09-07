import csv
import logging
import os
import time
from enum import Enum

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderCancelledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, ScriptStrategyBase


class OrderState(Enum):
    PENDING_CREATE = 0
    CREATED = 1
    PENDING_CANCEL = 2
    CANCELED = 3


class LatencyTest(ScriptStrategyBase):
    """
    This script checks latency for order sent to the exchange
    """
    # Config params
    trading_pair = "SOL-USDT"
    connector_name = "kucoin"
    order_amount = Decimal("0.25")
    order_spread = Decimal("5")
    csv_file_id = "aws_japan"

    markets = {connector_name: {trading_pair}}

    delay_after_order = 30
    delay_after_cancel = 5
    next_order_timestamp = 0

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    @property
    def timestamp_now(self):
        return int(time.time() * 1e3)

    @property
    def filename(self):
        return f"data/{self.connector_name}_{self.csv_file_id}_latency_check.csv"

    def on_tick(self):
        """
        """
        if self.current_timestamp > self.next_order_timestamp:
            if len(self.get_active_orders(self.connector_name)) > 0:
                self.next_order_timestamp = self.current_timestamp + self.delay_after_cancel
                self.cancel_all_orders()
                return
            self.next_order_timestamp = self.current_timestamp + self.delay_after_order
            current_price = self.connector.get_price(self.trading_pair, False)
            order_price = current_price * Decimal(1 - self.order_spread / 100)
            candidate = OrderCandidate(trading_pair=self.trading_pair,
                                       is_maker=True,
                                       order_type=OrderType.LIMIT,
                                       order_side=TradeType.BUY,
                                       amount=self.order_amount,
                                       price=order_price)
            candidate_adjusted = self.connector.budget_checker.adjust_candidate(candidate, all_or_none=False)
            if candidate_adjusted.amount != Decimal("0"):
                self.send_order_to_exchange(candidate_adjusted)
            else:
                self.log_with_clock(logging.INFO, f"Can't create order. Not enough funds or amount is below threshold")

    def cancel_all_orders(self):
        self.log_with_clock(logging.INFO, f"cancel_all_orders on {self.trading_pair}")
        for order in self.get_active_orders(self.connector_name):
            self.save_to_csv(self.timestamp_now, order.client_order_id, OrderState.PENDING_CANCEL.name)
            self.cancel(self.connector_name, order.trading_pair, order.client_order_id)

    def send_order_to_exchange(self, candidate):
        self.log_with_clock(logging.INFO, f"send_order_to_exchange on {self.trading_pair}")
        time_before_order_sent = self.timestamp_now
        if candidate.order_side == TradeType.BUY:
            order_id = self.buy(self.connector_name, candidate.trading_pair, candidate.amount,
                                candidate.order_type, candidate.price)
        else:
            order_id = self.sell(self.connector_name, candidate.trading_pair, candidate.amount,
                                 candidate.order_type, candidate.price)
        self.save_to_csv(time_before_order_sent, order_id, OrderState.PENDING_CREATE.name)
        # self.log_with_clock(logging.INFO, f"{time_before_order_sent}, {order_id}, order_sent")

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        self.log_with_clock(logging.INFO, f"did_create_buy_order event")
        self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CREATED.name)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        self.log_with_clock(logging.INFO, f"did_create_sell_order event")
        self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CREATED.name)

    def did_cancel_order(self, event: OrderCancelledEvent):
        self.log_with_clock(logging.INFO, f"did_cancel_order event")
        self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CANCELED.name)

    def save_to_csv(self, timestamp, order_id, status):
        # Check if the file exists
        file_exists = os.path.exists(self.filename)

        # Open the file for appending ('a' mode) or create it if it doesn't exist
        with open(self.filename, 'a', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Order_ID', 'Status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # If the file did not exist, write the header
            if not file_exists:
                writer.writeheader()

            # Write the data
            writer.writerow({'Timestamp': timestamp, 'Order_ID': order_id, 'Status': status})
