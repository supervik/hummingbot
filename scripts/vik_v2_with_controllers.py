import os
from typing import Dict, List, Set

import pandas as pd
from pydantic import Field

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
    # executors_update_interval: float = 0.5


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
        # self.executor_orchestrator = ExecutorOrchestrator(strategy=self, executors_update_interval=self.config.executors_update_interval)


    async def on_stop(self):
        await super().on_stop()

    def on_tick(self):
        super().on_tick()

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
