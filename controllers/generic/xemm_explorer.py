import time
from decimal import Decimal
from typing import Dict, List, Set

import pandas as pd
from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.xemm_explorer_executor.data_types import XEMMExplorerExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class XEMMExplorerControllerConfig(ControllerConfigBase):
    controller_name: str = "xemm_explorer_controller"
    candles_config: List[CandlesConfig] = []
    maker_exchange: str = Field(
        default="kucoin",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maker connector: ",
            prompt_on_new=True
        ))
    taker_exchange: str = Field(
        default="okx",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the taker connector: ",
            prompt_on_new=True
        ))
    trading_pairs: List[str] = Field(
        default="BNB-USDT,ETH-USDT,BTC-USDT",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the trading pairs separated by commas. Example: BNB-USDT,ETH-USDT: ",
            prompt_on_new=True
        ))
    target_profitability: Decimal = Field(
        default=0.3,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the target profitability in percentage. Example: 0.2 for 0.2%: ",
            prompt_on_new=True
        ))
    min_profitability: Decimal = Field(
        default=0.2,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the minimum profitability in percentage. Example: 0.2 for 0.2%: ",
            prompt_on_new=True
        ))
    max_profitability: Decimal = Field(
        default=0.4,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maximum profitability in percentage. Example: 0.2 for 0.2%: ",
            prompt_on_new=True
        ))
    order_amount_in_quote: Decimal = Field(
        default=15,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the order amount in quote. Example: 20 for 20 USDT for BNB-USDT: ",
            prompt_on_new=True
        ))
    min_order_amount_in_quote: Decimal = Field(
        default=10,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the minimum order amount in quote. Typically the minimum allowed by the exchange. Example: 10 for 10 USDT for BNB-USDT: ",
            prompt_on_new=True
        ))
    paper_trade_amount_in_quote: Decimal = Field(
        default=1000,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the simulated order amount for calculating the profitability. Example: 1000 for 1000 USDT for BNB-USDT: ",
            prompt_on_new=True
        ))
    fee_maker_pct: Decimal = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the fee maker percentage. Example: 0.1 for 0.1%: ",
            prompt_on_new=True
        ))
    fee_taker_pct: Decimal = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the fee taker percentage. Example: 0.1 for 0.1%: ",
            prompt_on_new=True
        ))
    liquidate_base_assets: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda e: "Liquidate all remaining base assets on the maker exchange? (True/False): ",
            prompt_on_new=False
        ))
    
    @validator("trading_pairs", pre=True)
    def validate_trading_pairs(cls, v):
        if isinstance(v, str):
            return v.replace(" ", "").split(",")
        return v
    
    def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        if self.maker_exchange not in markets:
            markets[self.maker_exchange] = set()
        if self.taker_exchange not in markets:
            markets[self.taker_exchange] = set()
        for trading_pair in self.trading_pairs:
            markets[self.maker_exchange].add(trading_pair)
            markets[self.taker_exchange].add(trading_pair)
        return markets


class XEMMExplorerController(ControllerBase):

    def __init__(self, config: XEMMExplorerControllerConfig, *args, **kwargs):
        self.config = config
        self._last_executor_creation_attempts: Dict[str, float] = {}
        self._executor_creation_cooldown: int = 60
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        pass

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determines which executors should be created based on current market conditions and configuration.
        
        The method follows these steps for each trading pair:
        1. Checks if we can create a new executor (based on cooldown period)
        2. Verifies if there are any active executors for the trading pair
        3. Determines the trading side based on:
           - Available base asset balance for SELL orders
           - Available quote asset balance for BUY orders
        4. Creates executors while respecting:
           - Minimum order amounts
           - Maximum number of buy executors based on available quote balance
           - Liquidation mode settings
        
        Returns:
            List[ExecutorAction]: List of executor creation actions to be performed
        """
        executor_actions = []
        buy_executors_to_create = 0
        current_timestamp = self.market_data_provider.time()

        for trading_pair in self.config.trading_pairs:
            if not self._can_create_executor(trading_pair, current_timestamp):
                continue

            active_executors_on_pair = self.filter_executors(
                executors=self.executors_info,
                filter_func=lambda e: e.trading_pair == trading_pair and e.is_active
            )
            active_buy_executors = self.filter_executors(
                executors=self.executors_info,
                filter_func=lambda e: e.config.maker_side == TradeType.BUY and e.is_active
            )

            if len(active_executors_on_pair) == 0:
                self._last_executor_creation_attempts[trading_pair] = current_timestamp
                
                mid_price = self.market_data_provider.get_price_by_type(self.config.maker_exchange, trading_pair, PriceType.MidPrice)
                base_asset, quote_asset = split_hb_trading_pair(trading_pair)
                base_balance = self.market_data_provider.connectors[self.config.maker_exchange].get_available_balance(base_asset)
                total_quote_balance = self.market_data_provider.connectors[self.config.maker_exchange].get_balance(quote_asset)
                max_buy_executors = total_quote_balance / self.config.order_amount_in_quote
                
                if base_balance > self.config.min_order_amount_in_quote / mid_price:
                    maker_side = TradeType.SELL
                    order_amount = base_balance
                else:
                    maker_side = TradeType.BUY
                    order_amount = self.config.order_amount_in_quote / mid_price
                    buy_executors_to_create += 1
                
                # Check if we should create a new executor
                should_create_executor = (
                    # Always allow SELL orders
                    maker_side == TradeType.SELL
                    # For BUY orders, check two conditions:
                    or (
                        # 1. Not in liquidation mode
                        not self.config.liquidate_base_assets
                        # 2. Haven't exceeded maximum number of buy executors
                        and len(active_buy_executors) + buy_executors_to_create < max_buy_executors
                    )
                )

                if should_create_executor:
                    self.logger().info(f"Starting new {maker_side.name} executor for {trading_pair}")
                    config = XEMMExplorerExecutorConfig(
                        controller_id=self.config.id,
                        timestamp=self.market_data_provider.time(),
                        maker_exchange=self.config.maker_exchange,
                        taker_exchange=self.config.taker_exchange,
                        trading_pair=trading_pair,
                        maker_side=maker_side,
                        order_amount=order_amount,
                        paper_trade_amount=self.config.paper_trade_amount_in_quote / mid_price,
                        paper_trade_amount_in_quote=self.config.paper_trade_amount_in_quote,
                        target_profitability=self.config.target_profitability,
                        min_profitability=self.config.min_profitability,
                        max_profitability=self.config.max_profitability,
                        fee_maker_pct=self.config.fee_maker_pct,
                        fee_taker_pct=self.config.fee_taker_pct,
                        liquidate_base_assets=self.config.liquidate_base_assets
                    )
                    executor_actions.append(CreateExecutorAction(executor_config=config, controller_id=self.config.id))
                elif maker_side == TradeType.BUY and not self.config.liquidate_base_assets:
                    self.logger().info(f"Not enough {quote_asset} balance to open new {maker_side.name} executor for {trading_pair}")
        
        if len(executor_actions) == 0 and self.config.liquidate_base_assets:
            self.logger().info(f"Liquidating base assets finished")
            HummingbotApplication.main_application().stop()
        return executor_actions

    def to_format_status(self) -> List[str]:
        """
        Generate a formatted status report and export executor data to Excel.
        
        Returns:
            List[str]: Formatted status messages including controller configuration,
                      running time, and per-trading pair performance summary.
        """
        extra_info = []
        all_executors = self._get_all_executors()
        
        if not all_executors:
            return ["No executors found."]
        
        # Process executor data
        df = self._create_executor_dataframe(all_executors)
        
        # Export trade data to Excel
        self._export_trades_to_excel(df)
        
        start_timestamp = all_executors[0].timestamp
        hours_running = (self.market_data_provider.time() - start_timestamp) / 3600
        
        # Add controller configuration info
        extra_info.append(
            f"Controller config: {self.config.maker_exchange}, "
            f"{self.config.taker_exchange}, "
            f"{self.config.min_profitability}/"
            f"{self.config.target_profitability}/"
            f"{self.config.max_profitability}"
        )
        
        # Add running time info
        extra_info.append(f"Time running: {self._get_running_time_str(start_timestamp)}")
        extra_info.append("\nTrade details exported to Excel file")
        
        # Calculate and add trading pair statistics
        grouped_df = self._calculate_trading_pair_stats(df, hours_running)
        extra_info.extend([
            "\nPer Trading Pair Summary:",
            format_df_for_printout(grouped_df, table_format="psql")
        ])
        
        return extra_info

    def _can_create_executor(self, trading_pair: str, current_timestamp: float) -> bool:
        """
        Determines if a new executor can be created for a trading pair based on cooldown period.
        """
        last_attempt = self._last_executor_creation_attempts.get(trading_pair, 0)
        return current_timestamp - last_attempt >= self._executor_creation_cooldown    
    
    def _get_all_executors(self) -> List:
        """Get both historical and active executors for this controller."""
        historical_executors = MarketsRecorder.get_instance().get_executors_by_controller(self.config.id)
        active_executors = [
            executor for executor in self.executors_info 
            if not any(h.id == executor.config.id for h in historical_executors)
        ]
        return historical_executors + active_executors

    def _create_executor_dataframe(self, all_executors: List) -> pd.DataFrame:
        """Create and process DataFrame from executor data."""
        df = pd.DataFrame([executor.custom_info for executor in all_executors])
        for col in ['pnl_with_fees_pct', 'hedge_delay_seconds']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').apply(
                    lambda x: Decimal(str(x)) if pd.notnull(x) else Decimal('0')
                )
        return df
    
    def _export_trades_to_excel(self, df: pd.DataFrame) -> None:
        """
        Export executor trade data to an Excel file with a unique filename.
        """
        # Create unique filename using controller ID and exchanges
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"xemm_explorer_trades_{self.config.maker_exchange}_{self.config.taker_exchange}_{self.config.id[:8]}_{timestamp}.xlsx"
    
        export_df = df[['timestamp', 'trading_pair', 'maker_exchange', 'taker_exchange', 
                        'maker_side', 'maker_price', 'taker_price', 
                        'hedge_delay_seconds', 'pnl_with_fees_pct']].copy()
        export_df['date'] = pd.to_datetime(export_df['timestamp'], unit='s')
        export_df = export_df.drop('timestamp', axis=1)
        export_df = export_df.rename(columns={'maker_side': 'side'})
        export_df.to_excel(filename, index=False)

    def _get_running_time_str(self, start_timestamp: float) -> str:
        """Calculate and format the running time string."""
        hours_running = (self.market_data_provider.time() - start_timestamp) / 3600
        days = int(hours_running / 24)
        hours = int(hours_running % 24)
        return f"{days}d {hours}h" if days > 0 else f"{hours}h"

    def _calculate_trading_pair_stats(self, df: pd.DataFrame, hours_running: float) -> pd.DataFrame:
        """Calculate statistics for each trading pair."""
        stats = []
        for pair in df['trading_pair'].unique():
            pair_data = df[df['trading_pair'] == pair]
            filled = len(pair_data[pair_data['pnl_with_fees_pct'] != 0])
            avg_hedge_delay = pair_data[pair_data['hedge_delay_seconds'] != 0]['hedge_delay_seconds'].mean()
            avg_pnl = pair_data[pair_data['pnl_with_fees_pct'] != 0]['pnl_with_fees_pct'].mean()
            stats.append([pair, filled, avg_hedge_delay, avg_pnl])

        grouped_df = pd.DataFrame(stats, columns=['trading_pair', 'filled_orders', 'avg_hedge_delay_s', 'avg_pnl_pct'])
        grouped_df['total_pnl_pct'] = grouped_df['avg_pnl_pct'] * grouped_df['filled_orders']
        grouped_df['apy_pct'] = 365 * 24 * grouped_df['total_pnl_pct'] / hours_running
        return grouped_df.sort_values('apy_pct', ascending=False)
 