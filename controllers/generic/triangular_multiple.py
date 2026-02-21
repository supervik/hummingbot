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
    # Format for triangles: "MAKER TAKER1 TAKER2 [buy_only|sell_only] [min_X] [max_X] [weight_X]
    triangles: List[str] = ["ATOM-BTC ATOM-USDT BTC-USDT", "XRP-BTC XRP-USDT BTC-USDT"]
    balances: Dict[str, Decimal] = {"ATOM": Decimal("5"), "BTC": Decimal("0.0002"), "XRP": Decimal("1000")}
    rebalance_asset: str = "USDT"
    fee_asset: Optional[str] = None  # Optional fee asset (e.g., BNB, KCS) to be rebalanced along with base/quote

    # profitabilty parameters
    min_usdt: Decimal = Decimal("10")
    min_profit: Decimal = Decimal("0.2")
    max_profit: Decimal = Decimal("0.4")
    maker_fee: Decimal = Decimal("0.1")
    taker_fee: Decimal = Decimal("0.1")
    kill_switch_pnl_threshold: Decimal = Decimal("-0.2")
    taker_fill_completion_ratio: Decimal = Decimal("0.99")
    maker_quote_buffer_inverse: Decimal = Decimal("0.005")  # For inverse triangles, need some quote asset for taker_2 orders 

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for triangle in self.triangles:
            pairs = triangle.split()
            # First 3 tokens are always the pairs
            for pair in pairs[:3]:
                markets = markets.add_or_update(self.connector_name, pair)
        
        # Add fee asset trading pair if configured
        if self.fee_asset:
            fee_pair = f"{self.fee_asset}-{self.rebalance_asset}"
            markets = markets.add_or_update(self.connector_name, fee_pair)
        
        return markets

    def _parse_triangle_string(self, triangle_str: str) -> Dict[str, Union[str, bool, Optional[Decimal]]]:
        """
        Parse a triangle string to extract pairs, flags, and parameter overrides.
        
        Format: "MAKER TAKER1 TAKER2 [buy_only|sell_only] [min_X] [max_X] [weight_X]"
        Examples:
          - "ATOM-BTC ATOM-USDT BTC-USDT"
          - "ATOM-BTC ATOM-USDT BTC-USDT buy_only min_0.25 max_0.6"
          - "LSK-USDC LSK-USDT USDC-USDT sell_only"
          - "ATOM-BTC ATOM-USDT BTC-USDT weight_2.0 min_0.5 max_1"
        
        :param triangle_str: The triangle string to parse
        :return: Dict with parsed information
        """
        parts = triangle_str.split()
        if len(parts) < 3:
            raise ValueError(f"Invalid triangle string: {triangle_str}. Need at least 3 pairs.")
        
        maker_pair, taker_1_pair, taker_2_pair = parts[:3]
        buy_only = sell_only = False
        min_profit = max_profit = None
        weight = Decimal("1.0")  # Default weight is 1.0
        
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
            elif token.startswith("weight_"):
                try:
                    weight = Decimal(token[7:])
                    if weight <= Decimal("0"):
                        raise ValueError(f"Weight must be positive in '{triangle_str}': {token[7:]}")
                except (InvalidOperation, ValueError) as e:
                    if isinstance(e, ValueError) and "must be positive" in str(e):
                        raise
                    raise ValueError(f"Invalid weight value in '{triangle_str}': {token[7:]}")
        
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
            "weight": weight,
        }

    def _parsed_triangles(self) -> List[Dict[str, Union[str, bool, Optional[Decimal]]]]:
        """Cache parsed triangle data to avoid re-parsing."""
        if not hasattr(self, '_cached_parsed_triangles'):
            self._cached_parsed_triangles = [self._parse_triangle_string(t) for t in self.triangles]
        return self._cached_parsed_triangles

    def _maker_asset_usages(self) -> Dict[str, List[Tuple[str, Decimal]]]:
        """
        Returns all usages of each asset across maker pairs, combining base and quote roles
        into a single pool per asset.

        This ensures that an asset appearing as base in some triangles and quote in others
        is allocated from the same shared balance rather than two independent pools.

        Returns:
            Dict mapping asset -> list of (maker_pair, weight) for every triangle that
            consumes that asset (whether as base or as quote).
        """
        usages: Dict[str, List[Tuple[str, Decimal]]] = {}

        for parsed in self._parsed_triangles():
            maker_pair = parsed["maker_pair"]
            base, quote = maker_pair.split("-")
            weight = parsed.get("weight", Decimal("1.0"))

            if not parsed["buy_only"]:   # triangle consumes base asset
                usages.setdefault(base, []).append((maker_pair, weight))

            if not parsed["sell_only"]:  # triangle consumes quote asset
                usages.setdefault(quote, []).append((maker_pair, weight))

        return usages

    def _allocate_amount_weighted(self, balance: Decimal, triangle_weight: Decimal, total_weight: Decimal) -> Decimal:
        """
        Allocate amount based on balance and weighted allocation.
        
        :param balance: Total balance to allocate
        :param triangle_weight: Weight for this specific triangle
        :param total_weight: Sum of all weights for triangles using this asset
        :return: Allocated amount for this triangle
        """
        if total_weight == Decimal("0"):
            return Decimal("0")
        return balance * triangle_weight / total_weight

    @property
    def triangle_info(self) -> List[Dict[str, Union[str, Decimal, bool, Optional[Decimal]]]]:
        """
        Returns per-triangle info including maker/taker pairs and maker order sizes.
        - base_amount: maker base units allocated (0 if buy_only)
        - quote_amount: maker quote units allocated (0 if sell_only)
        - min_profit, max_profit: per-triangle overrides or global defaults
        - weight: allocation weight for this triangle (default 1.0)
        Shared assets are split proportionally based on weights across triangles that use them.
        """
        usages = self._maker_asset_usages()
        triangle_dicts = []

        for parsed in self._parsed_triangles():
            maker_pair = parsed["maker_pair"]
            base, quote = maker_pair.split("-")
            weight = parsed.get("weight", Decimal("1.0"))

            # Allocate base: shared across all triangles that consume this asset (as base or quote)
            if parsed["buy_only"]:
                base_amount = Decimal("0")
            else:
                base_balance = self.balances.get(base, Decimal("0"))
                total_weight = sum(w for _, w in usages.get(base, []))
                base_amount = self._allocate_amount_weighted(base_balance, weight, total_weight)

            # Allocate quote: shared across all triangles that consume this asset (as base or quote)
            if parsed["sell_only"]:
                quote_amount = Decimal("0")
            else:
                quote_balance = self.balances.get(quote, Decimal("0"))
                total_weight = sum(w for _, w in usages.get(quote, []))
                quote_amount = self._allocate_amount_weighted(quote_balance, weight, total_weight)

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
                "weight": weight,
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
                    taker_fill_completion_ratio=self.config.taker_fill_completion_ratio,
                    maker_quote_buffer_inverse=self.config.maker_quote_buffer_inverse,
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
                # Create triangle-specific rebalance executor (check base, quote, and optionally fee asset)
                triangle_balances = {
                    base: self.config.balances.get(base, Decimal("0")),
                    quote: self.config.balances.get(quote, Decimal("0"))
                }
                
                # Add fee asset if configured
                if self.config.fee_asset and self.config.fee_asset in self.config.balances:
                    triangle_balances[self.config.fee_asset] = self.config.balances[self.config.fee_asset]
                
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
        # Show configured target balances for quick inspection
        try:
            balances_str = ", ".join(f"{asset}: {amount}" for asset, amount in self.config.balances.items())
        except Exception:
            balances_str = str(self.config.balances)
        status.append(f"Target balances: {balances_str}")
        # Add ready_for_new_triangle to triangle info for status display
        # triangle_info_with_state = []
        # for triangle in self.config.triangle_info:
        #     triangle_copy = triangle.copy()
        #     triangle_copy["ready_for_new_triangle"] = self.ready_for_new_triangle.get(triangle["maker"], True)
        #     triangle_info_with_state.append(triangle_copy)
        # status.append(f"Triangle Info: {triangle_info_with_state}")
        # for executor in self.executors_info:
        #     status.append(f"\n{executor}")
        return status