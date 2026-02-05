import os
from decimal import Decimal
from typing import Dict, List, Set

import pandas as pd
from pydantic import Field

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class VikV2WithControllersConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    candles_config: List[CandlesConfig] = []
    markets: Dict[str, Set[str]] = {}
    executors_update_interval: float = 0.5
    
    # Global kill switch configuration
    kill_switch_enabled: bool = True
    kill_switch_asset: str = "USDT"
    kill_switch_rate_pct: Decimal = Decimal("-3")
    kill_switch_check_interval: int = 60
    kill_switch_counter_limit: int = 5


class VikV2WithControllers(StrategyV2Base):
    """
    This script runs a generic strategy with cash out feature. Will also check if the controllers configs have been
    updated and apply the new settings.
    The cash out of the script can be set by the time_to_cash_out parameter in the config file. If set, the script will
    stop the controllers after the specified time has passed, and wait until the active executors finalize their
    execution.
    The controllers will also have a parameter to manually cash out. In that scenario, the main strategy will stop the
    specific controller and wait until the active executors finalize their execution. The rest of the executors will
    wait until the main strategy stops them.
    """
    performance_report_interval: int = 1

    def __init__(self, connectors: Dict[str, ConnectorBase], config: VikV2WithControllersConfig):
        super().__init__(connectors, config)
        self.config = config
        self.closed_executors_buffer: int = 30
        self.executor_orchestrator = ExecutorOrchestrator(strategy=self, executors_update_interval=self.config.executors_update_interval)
        
        # Global kill switch state
        self.kill_switch_max_balance: Decimal = Decimal("0")
        self.kill_switch_counter: int = 0
        self._last_kill_switch_check_timestamp: float = 0.0


    async def on_stop(self):
        await super().on_stop()

    def on_tick(self):
        super().on_tick()
        if self.config.kill_switch_enabled:
            self.check_balance_kill_switch()

    # @staticmethod
    # def executors_info_to_df(executors_info: List[ExecutorInfo]) -> pd.DataFrame:
    #     """
    #     Convert a list of executor handler info to a dataframe.
    #     """
    #     df = pd.DataFrame([ei.to_dict() for ei in executors_info])
    #     # Convert the enum values to integers
    #     df['status'] = df['status'].apply(lambda x: x.value)

    #     # Sort the DataFrame
    #     df.sort_values(by='status', ascending=True, inplace=True)

    #     # Convert back to string representation without enum prefix
    #     df['status'] = df['status'].apply(lambda x: RunnableStatus(x).name)
    #     df['close_type'] = df['close_type'].apply(lambda x: CloseType(x).name if x is not None else None)
    #     return df
        
    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []
    
    def balance_warning(self, market_trading_pair_tuples: List[MarketTradingPairTuple]) -> List[str]:
        return []
    
    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Spread", "Age"]
        data = []
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                mid_price = connector.get_mid_price(order.trading_pair)
                if order.is_buy:
                    spread_mid = (mid_price - order.price) / mid_price * 100
                else:
                    spread_mid = (order.price - mid_price) / mid_price * 100

                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    connector_name,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    float(round(spread_mid, 2)),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Pair"], inplace=True)
        return df
    
    def check_balance_kill_switch(self) -> None:
        """
        Periodically checks the balance of kill_switch_asset (rebalance_asset) and
        stops Hummingbot if drawdown exceeds kill_switch_rate_pct for
        kill_switch_counter_limit consecutive checks.
        """
        # Check if enough time has passed since last check
        if self.current_timestamp - self._last_kill_switch_check_timestamp < self.config.kill_switch_check_interval:
            return
        
        self._last_kill_switch_check_timestamp = self.current_timestamp
        
        # Get connector - assume single connector for now
        if not self.connectors:
            return
        
        connector_name = list(self.connectors.keys())[0]
        connector = self.connectors[connector_name]
        
        try:
            current_balance = connector.get_balance(self.config.kill_switch_asset)
        except Exception as e:
            self.logger().warning(f"Failed to get balance for {self.config.kill_switch_asset}: {e}")
            return
        
        # Initialize max balance on first check
        if self.kill_switch_max_balance == Decimal("0"):
            self.kill_switch_max_balance = current_balance
            self.kill_switch_counter = 0
            self.logger().info(
                f"Kill switch initialized. {self.config.kill_switch_asset} balance: {current_balance}, "
                f"threshold: {self.config.kill_switch_rate_pct}%"
            )
            return
        
        # Check if balance increased (reset counter and update max)
        if current_balance >= self.kill_switch_max_balance:
            if current_balance > self.kill_switch_max_balance:
                self.kill_switch_max_balance = current_balance
            self.kill_switch_counter = 0
            return
        
        # Calculate drawdown percentage
        diff_pct = Decimal("100") * (current_balance / self.kill_switch_max_balance - Decimal("1"))
        
        # Check if drawdown exceeds threshold
        if diff_pct < self.config.kill_switch_rate_pct:
            self.kill_switch_counter += 1
            self.logger().warning(
                f"Kill switch check: {self.config.kill_switch_asset} balance drawdown {diff_pct:.2f}% "
                f"(current: {current_balance}, max: {self.kill_switch_max_balance}). "
                f"Counter: {self.kill_switch_counter}/{self.config.kill_switch_counter_limit}"
            )
            
            # Trigger kill switch if counter exceeds limit
            if self.kill_switch_counter > self.config.kill_switch_counter_limit:
                self.logger().error(
                    f"!!! Global kill switch triggered! {self.config.kill_switch_asset} balance drawdown "
                    f"{diff_pct:.2f}% exceeded threshold {self.config.kill_switch_rate_pct}% for "
                    f"{self.kill_switch_counter} consecutive checks. Stopping Hummingbot."
                )
                HummingbotApplication.main_application().stop()
        else:
            # Drawdown is within acceptable range, reset counter
            self.kill_switch_counter = 0
