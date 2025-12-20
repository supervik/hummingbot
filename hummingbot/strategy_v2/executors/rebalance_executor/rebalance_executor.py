import asyncio
import logging
from decimal import Decimal
from typing import Dict, Optional, Union

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
from hummingbot.strategy_v2.executors.rebalance_executor.data_types import RebalanceExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.connector.trading_rule import TradingRule


class RebalanceExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: RebalanceExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)
        self.config: RebalanceExecutorConfig = config
        self.assets_to_rebalance_info = []
        self.timestamp_to_rebalance = 0

    async def control_task(self):
        """
        Control the order execution process based on the execution strategy.
        """
        if self.status == RunnableStatus.RUNNING:
            if self._strategy.current_timestamp >= self.timestamp_to_rebalance:
                self.logger().info(f"RebalanceExecutor is running")
                self.rebalance()
                self._status = RunnableStatus.SHUTTING_DOWN
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            self.logger().info(f"RebalanceExecutor is shutting down")
            self.stop()

    def rebalance(self):
        self.logger().info(f"Start rebalancing assets")
        for rebalance_item in self.assets_to_rebalance_info:
            side = TradeType.SELL if rebalance_item['diff'] > 0 else TradeType.BUY
            self.send_order_to_exchange(rebalance_item['pair'], side, abs(rebalance_item['diff']))

    def send_order_to_exchange(self, trading_pair: str, side: TradeType, amount: Decimal):
        """
        Create the maker bid order.
        """
        price = self.get_price(self.config.connector_name, trading_pair, price_type=PriceType.MidPrice)
        order_candidate = OrderCandidate(
            trading_pair=trading_pair,
            is_maker=False,
            order_type=OrderType.MARKET,
            order_side=side,
            amount=amount,
            price=price)

        adjusted_candidate = self.connectors[self.config.connector_name].budget_checker.adjust_candidate(order_candidate, all_or_none=True)    
        if adjusted_candidate.amount == Decimal("0"):
            self.logger().info(f"Not enough balance to place {side.name} order amount {amount} on {trading_pair}")
            return None
        
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=trading_pair,
            order_type=OrderType.MARKET,
            side=side,
            amount=adjusted_candidate.amount,
            price=Decimal("0"))
        self.logger().info(f"Sent {side.name} order amount {amount} on {trading_pair}, id = {order_id} ")
        
    async def validate_sufficient_balance(self):
        """
        Validates that the executor has sufficient balance to place orders.
        """
        for asset, target_balance in self.config.balances.items():
            real_balance = self.get_balance(self.config.connector_name, asset)
            conersion_rate = self.get_price(self.config.connector_name, f"{asset}-{self.config.rebalance_asset}", price_type=PriceType.MidPrice)
            diff = real_balance - target_balance
            diff_in_rebalance_asset = diff * conersion_rate

            if abs(diff_in_rebalance_asset) > self.config.min_usdt:
                self.assets_to_rebalance_info.append({
                    "asset": asset,
                    "pair": f"{asset}-{self.config.rebalance_asset}",
                    "target_balance": target_balance,
                    "real_balance": real_balance,
                    "diff": diff,
                    "diff_in_rebalance_asset": diff_in_rebalance_asset,
                    "is_buy": diff_in_rebalance_asset > 0
                })

        if len(self.assets_to_rebalance_info) > 0:
            self.assets_to_rebalance_info.sort(key=lambda x: x['diff_in_rebalance_asset'], reverse=True)
            self.logger().info(f"!!! ATTENTION !!! Assets needed to be rebalanced:")
            for asset_info in self.assets_to_rebalance_info:
                side = 'sell' if asset_info['diff'] > 0 else 'buy'
                self.logger().info(f" {asset_info['asset']}: {side} {abs(asset_info['diff'])} {asset_info['pair']} ({round(asset_info['diff_in_rebalance_asset'], 2)} {self.config.rebalance_asset})")
            self.logger().info(f"Sleeping for 30 seconds before rebalance.")
            self.timestamp_to_rebalance = self._strategy.current_timestamp + 30
        else:
            self.logger().info(f"No assets needed to be rebalanced")
            self.close_type = CloseType.COMPLETED
            self.stop()

    def early_stop(self, keep_position: bool = False):
        """
        This method allows strategy to stop the executor early.
        """
        self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    def get_net_pnl_pct(self) -> Decimal:
        """
        Get the net profit and loss percentage.

        :return: The net profit and loss percentage.
        """
        return Decimal("0")

    def get_net_pnl_quote(self) -> Decimal:
        """
        Get the net profit and loss in quote currency.

        :return: The net profit and loss in quote currency.
        """
        return Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        """
        Get the cumulative fees in quote currency.

        :return: The cumulative fees in quote currency.
        """
        return Decimal("0")