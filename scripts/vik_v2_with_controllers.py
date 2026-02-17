import os
from decimal import Decimal
from typing import Dict, List, Set

import pandas as pd
from pydantic import Field

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.client.ui.interface_utils import format_df_for_printout
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

    def on_tick(self):
        super().on_tick()
        if self.config.kill_switch_enabled:
            self.check_balance_kill_switch()

    @staticmethod
    def executors_info_to_df(executors_info: List[ExecutorInfo]) -> pd.DataFrame:
        """
        Convert a list of executor handler info to a dataframe.
        """
        df = pd.DataFrame([ei.to_dict() for ei in executors_info])
        
        # Convert enum values
        df['status'] = df['status'].apply(lambda x: x.value)
        df.sort_values(by='status', ascending=True, inplace=True)
        df['status'] = df['status'].apply(lambda x: RunnableStatus(x).name)
        df['close_type'] = df['close_type'].apply(lambda x: CloseType(x).name if x is not None else None)
        
        # Extract custom_info fields for triangular executors
        custom_info_fields = ['maker_pair', 'delay_server', 'delay_exchange']
        for field in custom_info_fields:
            df[field] = df['custom_info'].apply(
                lambda x: x.get(field, None) if isinstance(x, dict) else None
            )
        
        # Format numeric columns: {column: (decimals, suffix)}
        format_specs = {
            'net_pnl_pct': (2, '%'),
            'net_pnl_quote': (2, ''),
            'filled_amount_quote': (2, ''),
            'delay_server': (3, ''),
            'delay_exchange': (3, ''),
        }
        
        for column, (decimals, suffix) in format_specs.items():
            if column in df.columns:
                df[column] = df[column].apply(
                    lambda x: f"{float(x):.{decimals}f}{suffix}" if x is not None else None
                )
        
        return df
        
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
    
    def format_status(self) -> str:
        """
        Override format_status to include maker_pair column in executor table.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        # Basic account info
        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            df = self.active_orders_df()
            lines.extend(["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        # Controller sections
        performance_data = []

        for controller_id, controller in self.controllers.items():
            lines.append(f"\n{'=' * 60}")
            lines.append(f"Controller: {controller_id}")
            lines.append(f"{'=' * 60}")

            # Controller status
            lines.extend(controller.to_format_status())

            # Last 20 executors table
            executors_list = self.get_executors_by_controller(controller_id)
            if executors_list:
                lines.append("\n  Recent Executors (Last 30):")
                # Sort by timestamp and take last 30
                recent_executors = sorted(executors_list, key=lambda x: x.timestamp, reverse=True)[:30]
                executors_df = self.executors_info_to_df(recent_executors)
                if not executors_df.empty:
                    executors_df["age"] = self.current_timestamp - executors_df["timestamp"]
                    # Include maker_pair and hedge latency metrics (if available) in the columns list
                    executor_columns = [
                        "type",
                        "maker_pair",
                        "side",
                        "status",
                        "net_pnl_pct",
                        "net_pnl_quote",
                        "filled_amount_quote",
                        "delay_server",
                        "delay_exchange",
                        "is_trading",
                        "close_type",
                        "age",
                    ]
                    available_columns = [col for col in executor_columns if col in executors_df.columns]
                    lines.append(format_df_for_printout(executors_df[available_columns],
                                                        table_format="psql", index=False))
            else:
                lines.append("  No executors found.")

            # Positions table
            positions = self.get_positions_by_controller(controller_id)
            if positions:
                lines.append("\n  Positions Held:")
                positions_data = []
                for pos in positions:
                    positions_data.append({
                        "Connector": pos.connector_name,
                        "Trading Pair": pos.trading_pair,
                        "Side": pos.side.name,
                        "Amount": f"{pos.amount:.4f}",
                        "Value (USD)": f"${pos.amount * pos.breakeven_price:.2f}",
                        "Breakeven Price": f"{pos.breakeven_price:.6f}",
                        "Unrealized PnL": f"${pos.unrealized_pnl_quote:+.2f}",
                        "Realized PnL": f"${pos.realized_pnl_quote:+.2f}",
                        "Fees": f"${pos.cum_fees_quote:.2f}"
                    })
                positions_df = pd.DataFrame(positions_data)
                lines.append(format_df_for_printout(positions_df, table_format="psql", index=False))
            else:
                lines.append("  No positions held.")

            # Collect performance data for summary table
            performance_report = self.get_performance_report(controller_id)
            if performance_report:
                performance_data.append({
                    "Controller": controller_id,
                    "Realized PnL": f"${performance_report.realized_pnl_quote:.2f}",
                    "Unrealized PnL": f"${performance_report.unrealized_pnl_quote:.2f}",
                    "Global PnL": f"${performance_report.global_pnl_quote:.2f}",
                    "Global PnL %": f"{performance_report.global_pnl_pct:.2f}%",
                    "Volume Traded": f"${performance_report.volume_traded:.2f}"
                })

        # Performance summary table
        if performance_data:
            lines.append(f"\n{'=' * 80}")
            lines.append("PERFORMANCE SUMMARY")
            lines.append(f"{'=' * 80}")

            # Calculate global totals
            global_realized = sum(Decimal(p["Realized PnL"].replace("$", "")) for p in performance_data)
            global_unrealized = sum(Decimal(p["Unrealized PnL"].replace("$", "")) for p in performance_data)
            global_total = global_realized + global_unrealized
            global_volume = sum(Decimal(p["Volume Traded"].replace("$", "")) for p in performance_data)
            global_pnl_pct = (global_total / global_volume) * 100 if global_volume > 0 else Decimal(0)

            # Add global row
            performance_data.append({
                "Controller": "GLOBAL TOTAL",
                "Realized PnL": f"${global_realized:.2f}",
                "Unrealized PnL": f"${global_unrealized:.2f}",
                "Global PnL": f"${global_total:.2f}",
                "Global PnL %": f"{global_pnl_pct:.2f}%",
                "Volume Traded": f"${global_volume:.2f}"
            })

            performance_df = pd.DataFrame(performance_data)
            lines.append(format_df_for_printout(performance_df, table_format="psql", index=False))

        return "\n".join(lines)
