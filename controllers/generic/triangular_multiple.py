from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Set, Tuple, Union

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy_v2.executors.triangular_executor.data_types import TriangularExecutorConfig
from hummingbot.strategy_v2.executors.rebalance_executor.data_types import RebalanceExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.base import RunnableStatus


class TriangularMultipleConfig(ControllerConfigBase):
    controller_name: str = "triangular_multiple"
    candles_config: List[CandlesConfig] = []
    connector_name: str = "binance"

    # Triangular configuration
    triangles: List[str] = ["ATOM-BTC ATOM-USDT BTC-USDT", "XRP-BTC XRP-USDT BTC-USDT"]
    balances: Dict[str, Decimal] = {"ATOM": Decimal("5"), "BTC": Decimal("0.0002"), "XRP": Decimal("1000")}
    rebalance_asset: str = "USDT"

    # profitabilty parameters
    min_usdt: Decimal = Decimal("10")
    min_profit: Decimal = Decimal("0.2")
    max_profit: Decimal = Decimal("0.4")
    maker_fee: Decimal = Decimal("0.1")
    taker_fee: Decimal = Decimal("0.1")
    kill_switch_pnl_threshold: Decimal = Decimal("-0.2")

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for triangle in self.triangles:
            pairs = triangle.split()
            # First 3 tokens are always the pairs
            for pair in pairs[:3]:
                markets = markets.add_or_update(self.connector_name, pair)
        return markets

    def _parse_triangle_string(self, triangle_str: str) -> Dict[str, Union[str, bool, Optional[Decimal]]]:
        """
        Parse a triangle string to extract pairs, flags, and parameter overrides.
        
        Format: "MAKER TAKER1 TAKER2 [buy_only|sell_only] [min_X] [max_X]"
        Examples:
          - "ATOM-BTC ATOM-USDT BTC-USDT"
          - "ATOM-BTC ATOM-USDT BTC-USDT buy_only min_0.25 max_0.6"
          - "LSK-USDC LSK-USDT USDC-USDT sell_only"
        
        :param triangle_str: The triangle string to parse
        :return: Dict with parsed information
        """
        parts = triangle_str.split()
        if len(parts) < 3:
            raise ValueError(f"Invalid triangle string: {triangle_str}. Need at least 3 pairs.")
        
        maker_pair, taker_1_pair, taker_2_pair = parts[:3]
        buy_only = sell_only = False
        min_profit = max_profit = None
        
        for token in parts[3:]:
            if token == "buy_only":
                buy_only = True
            elif token == "sell_only":
                sell_only = True
            elif token.startswith("min_"):
                try:
                    min_profit = Decimal(token[4:])
                except (InvalidOperation, ValueError):
                    raise ValueError(f"Invalid min_profit value in '{triangle_str}': {token[4:]}")
            elif token.startswith("max_"):
                try:
                    max_profit = Decimal(token[4:])
                except (InvalidOperation, ValueError):
                    raise ValueError(f"Invalid max_profit value in '{triangle_str}': {token[4:]}")
        
        if buy_only and sell_only:
            raise ValueError(f"Triangle '{triangle_str}' cannot have both buy_only and sell_only flags.")
        
        return {
            "maker_pair": maker_pair,
            "taker_1_pair": taker_1_pair,
            "taker_2_pair": taker_2_pair,
            "buy_only": buy_only,
            "sell_only": sell_only,
            "min_profit": min_profit,
            "max_profit": max_profit,
        }

    def _parsed_triangles(self) -> List[Dict[str, Union[str, bool, Optional[Decimal]]]]:
        """Cache parsed triangle data to avoid re-parsing."""
        if not hasattr(self, '_cached_parsed_triangles'):
            self._cached_parsed_triangles = [self._parse_triangle_string(t) for t in self.triangles]
        return self._cached_parsed_triangles

    def _maker_asset_usage(self) -> Tuple[Dict[str, int], Dict[str, int]]:
        """
        Counts how many triangles use each asset as maker base and maker quote.
        Only counts assets that are actually needed based on buy_only/sell_only flags.
        """
        base_usage: Dict[str, int] = {}
        quote_usage: Dict[str, int] = {}

        for parsed in self._parsed_triangles():
            base, quote = parsed["maker_pair"].split("-")
            if not parsed["buy_only"]:
                base_usage[base] = base_usage.get(base, 0) + 1
            if not parsed["sell_only"]:
                quote_usage[quote] = quote_usage.get(quote, 0) + 1

        return base_usage, quote_usage

    def _allocate_amount(self, balance: Decimal, count: int) -> Decimal:
        """Allocate amount based on balance and usage count."""
        return balance / Decimal(count) if count > 0 else Decimal("0")

    @property
    def triangle_info(self) -> List[Dict[str, Union[str, Decimal, bool, Optional[Decimal]]]]:
        """
        Returns per-triangle info including maker/taker pairs and maker order sizes.
        - base_amount: maker base units allocated (0 if buy_only)
        - quote_amount: maker quote units allocated (0 if sell_only)
        - min_profit, max_profit: per-triangle overrides or global defaults
        Shared assets are split equally across triangles that use them.
        """
        base_usage, quote_usage = self._maker_asset_usage()
        triangle_dicts = []

        for parsed in self._parsed_triangles():
            maker_pair = parsed["maker_pair"]
            base, quote = maker_pair.split("-")
            
            base_amount = Decimal("0") if parsed["buy_only"] else \
                self._allocate_amount(self.balances.get(base, Decimal("0")), base_usage.get(base, 0))
            quote_amount = Decimal("0") if parsed["sell_only"] else \
                self._allocate_amount(self.balances.get(quote, Decimal("0")), quote_usage.get(quote, 0))

            triangle_dicts.append({
                "maker": maker_pair,
                "taker_1": parsed["taker_1_pair"],
                "taker_2": parsed["taker_2_pair"],
                "base": base,
                "quote": quote,
                "base_amount": base_amount,
                "quote_amount": quote_amount,
                "buy_only": parsed["buy_only"],
                "sell_only": parsed["sell_only"],
                "min_profit": parsed["min_profit"],
                "max_profit": parsed["max_profit"],
            })

        return triangle_dicts


class TriangularMultiple(ControllerBase):
    def __init__(self, config: TriangularMultipleConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.logger().info(f"Initializing TriangularMultiple controller with configuration: {self.config}")
        self.logger().info(f"Triangle info: {self.config.triangle_info}")
        self.first_run = True
        # Track per-triangle state: True = ready to create new executor, False = need rebalance first
        self.ready_for_new_triangle: Dict[str, bool] = {}  # maker_pair -> bool
        # Track triangles disabled due to PnL kill switch
        self.disabled_triangles: Set[str] = set()  # maker_pair -> disabled

    async def update_processed_data(self):
        pass

    def _has_failed_executor(self, maker_pair: str) -> bool:
        """
        Check if any terminated triangular_executor for this maker_pair has CloseType.STOP_LOSS.
        
        :param maker_pair: The maker pair to check
        :return: True if any failed executor found, False otherwise
        """
        failed_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: (
                e.type == "triangular_executor"
                and e.config.maker_pair == maker_pair
                and e.close_type == CloseType.STOP_LOSS
            )
        )
        return len(failed_executors) > 0
    
    def determine_executor_actions(self) -> List[ExecutorAction]:
        # self.logger().info(f"TriangularMultiple controller with triangle conf: {self.config.triangle_info}")
        executor_actions = []
        
        if self.first_run:
            # Start rebalance executor and exit loop
            rebalance_executor_config = RebalanceExecutorConfig(
                controller_id=self.config.id,
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                balances=self.config.balances,
                rebalance_asset=self.config.rebalance_asset,
                min_usdt=self.config.min_usdt,
                trigger_on_event=False
            )
            executor_actions.append(CreateExecutorAction(
                controller_id=self.config.id,
                executor_config=rebalance_executor_config
            ))
            self.first_run = False
            return executor_actions

        active_rebalance_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: e.is_active and e.type == "rebalance_executor"
        )

        # If any rebalance executor is active, wait
        if len(active_rebalance_executors) > 0:
            return executor_actions

        # Process each triangle
        for triangle in self.config.triangle_info:
            maker_pair = triangle["maker"]
            base = triangle["base"]
            quote = triangle["quote"]
            
            # Skip disabled triangles
            if maker_pair in self.disabled_triangles:
                continue
            
            # Initialize ready_for_new_triangle to True if not set (first time seeing this triangle)
            if maker_pair not in self.ready_for_new_triangle:
                self.ready_for_new_triangle[maker_pair] = True
            
            # Get active triangular executors for this triangle
            active_triangle_executors = self.filter_executors(
                executors=self.executors_info,
                filter_func=lambda e: e.is_active and e.type == "triangular_executor"
            )
            active_triangle_executors_on_pair = self.filter_executors(
                executors=active_triangle_executors,
                filter_func=lambda e: e.config.maker_pair == maker_pair
            )
            
            # If executor exists, do nothing (flag stays as is)
            if len(active_triangle_executors_on_pair) > 0:
                continue
            
            # No executor for this triangle
            if self.ready_for_new_triangle[maker_pair]:
                # Ready for new triangle - create triangular executor
                # Use per-triangle overrides if available, otherwise use global defaults
                min_profit = triangle.get("min_profit") if triangle.get("min_profit") is not None else self.config.min_profit
                max_profit = triangle.get("max_profit") if triangle.get("max_profit") is not None else self.config.max_profit
                
                self.logger().info(
                    f"Creating executor for triangle {maker_pair} "
                    f"(min_profit={min_profit}, max_profit={max_profit}, "
                    f"base_amount={triangle['base_amount']}, quote_amount={triangle['quote_amount']})"
                )
                config = TriangularExecutorConfig(
                    controller_id=self.config.id,
                    timestamp=self.market_data_provider.time(),
                    connector_name=self.config.connector_name,
                    maker_pair=maker_pair,
                    taker_1_pair=triangle["taker_1"],
                    taker_2_pair=triangle["taker_2"],
                    base_amount=triangle["base_amount"],
                    quote_amount=triangle["quote_amount"],
                    min_profit=min_profit,
                    max_profit=max_profit,
                    fee_maker=self.config.maker_fee,
                    fee_taker=self.config.taker_fee,
                    min_usdt=self.config.min_usdt,
                    kill_switch_pnl_threshold=self.config.kill_switch_pnl_threshold,
                )
                executor_actions.append(CreateExecutorAction(
                    controller_id=self.config.id,
                    executor_config=config
                ))
                # Mark as not ready (executor was created, if it stops we need rebalance)
                self.ready_for_new_triangle[maker_pair] = False
            else:
                # Not ready - executor stopped, need rebalance first
                # Create triangle-specific rebalance executor (only check base and quote assets)
                triangle_balances = {
                    base: self.config.balances.get(base, Decimal("0")),
                    quote: self.config.balances.get(quote, Decimal("0"))
                }
                
                self.logger().info(f"Executor stopped for triangle {maker_pair}, creating rebalance executor for assets: {list(triangle_balances.keys())}")
                rebalance_executor_config = RebalanceExecutorConfig(
                    controller_id=self.config.id,
                    timestamp=self.market_data_provider.time(),
                    connector_name=self.config.connector_name,
                    balances=triangle_balances,
                    rebalance_asset=self.config.rebalance_asset,
                    min_usdt=self.config.min_usdt,
                    trigger_on_event=True
                )
                executor_actions.append(CreateExecutorAction(
                    controller_id=self.config.id,
                    executor_config=rebalance_executor_config
                ))
                
                # Check for failed executor right after creating rebalance
                if self._has_failed_executor(maker_pair):
                    # Failed executor found - disable triangle
                    self.ready_for_new_triangle[maker_pair] = False
                    self.disabled_triangles.add(maker_pair)
                    self.logger().warning(
                        f"Triangle {maker_pair} disabled due to PnL kill switch. "
                        f"Rebalance will run but no new executor will be created."
                    )
                else:
                    # No failure - normal flow, re-enable after rebalance
                    self.ready_for_new_triangle[maker_pair] = True
                
                # Only create one rebalance at a time
                break
        
        return executor_actions
        
    def to_format_status(self) -> List[str]:
        status = []
        status.append(f"Triangular Multiple Controller: {self.config.id}")
        status.append(f"Controller status: {self._status}")
        # Add ready_for_new_triangle to triangle info for status display
        triangle_info_with_state = []
        for triangle in self.config.triangle_info:
            triangle_copy = triangle.copy()
            triangle_copy["ready_for_new_triangle"] = self.ready_for_new_triangle.get(triangle["maker"], True)
            triangle_info_with_state.append(triangle_copy)
        status.append(f"Triangle Info: {triangle_info_with_state}")
        # for executor in self.executors_info:
        #     status.append(f"\n{executor}")
        return status