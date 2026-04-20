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
        - extra_base_amount: sum of base_amounts from cheaper levels on the same triangle (0 if solo)
        - extra_quote_amount: sum of quote_amounts from cheaper levels on the same triangle (0 if solo)
        - level_label: "L1", "L2", ... when multiple levels share the same triangle; None otherwise
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

            # Resolve effective min/max profit for this triangle (used as the level identity key)
            eff_min = parsed["min_profit"] if parsed["min_profit"] is not None else self.min_profit
            eff_max = parsed["max_profit"] if parsed["max_profit"] is not None else self.max_profit

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
                "eff_min_profit": eff_min,
                "eff_max_profit": eff_max,
                "weight": weight,
            })

        # For levels sharing the same triangle (same maker + taker pairs), compute extra depth
        # amounts and assign level labels. Sort by eff_min_profit ascending so cheaper levels
        # (closer to best bid/ask, fill first) accumulate first.
        def triangle_key(t):
            return (t["maker"], t["taker_1"], t["taker_2"])

        groups: Dict[tuple, List] = {}
        for t in triangle_dicts:
            groups.setdefault(triangle_key(t), []).append(t)

        for group in groups.values():
            group.sort(key=lambda t: t["eff_min_profit"])
            multi_level = len(group) > 1
            cum_base = Decimal("0")
            cum_quote = Decimal("0")
            for idx, t in enumerate(group):
                t["extra_base_amount"] = cum_base
                t["extra_quote_amount"] = cum_quote
                t["level_label"] = f"L{idx + 1}" if multi_level else None
                cum_base += t["base_amount"]
                cum_quote += t["quote_amount"]

        return triangle_dicts


class TriangularMultiple(ControllerBase):
    def __init__(self, config: TriangularMultipleConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.logger().info(f"Initializing TriangularMultiple controller with configuration: {self.config}")
        self.logger().info(f"Triangle info: {self.config.triangle_info}")
        self.first_run = True
        # Track per-level state: True = ready to create new executor, False = need rebalance first.
        # Key is (maker_pair, taker_1_pair, taker_2_pair, eff_min_profit, eff_max_profit) so that
        # multiple profit levels on the same triangle are tracked independently.
        self.ready_for_new_triangle: Dict[tuple, bool] = {}
        # Track levels disabled due to PnL kill switch (same key as above)
        self.disabled_triangles: Set[tuple] = set()

    async def update_processed_data(self):
        pass

    def _has_failed_executor(self, maker_pair: str, min_profit: Decimal, max_profit: Decimal) -> bool:
        """
        Check if any terminated triangular_executor for this specific level has CloseType.STOP_LOSS.

        :param maker_pair: The maker pair to check
        :param min_profit: Effective min_profit for this level
        :param max_profit: Effective max_profit for this level
        :return: True if any failed executor found for this level, False otherwise
        """
        failed_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: (
                e.type == "triangular_executor"
                and e.config.maker_pair == maker_pair
                and e.config.min_profit == min_profit
                and e.config.max_profit == max_profit
                and e.close_type == CloseType.STOP_LOSS
            )
        )
        return len(failed_executors) > 0
    
    def determine_executor_actions(self) -> List[ExecutorAction]:
        executor_actions = []

        if self.first_run:
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

        # Process each triangle level
        for triangle in self.config.triangle_info:
            maker_pair = triangle["maker"]
            taker_1 = triangle["taker_1"]
            taker_2 = triangle["taker_2"]
            base = triangle["base"]
            quote = triangle["quote"]
            min_profit = triangle["eff_min_profit"]
            max_profit = triangle["eff_max_profit"]

            # Unique key per profit level on the same triangle
            level_key = (maker_pair, taker_1, taker_2, min_profit, max_profit)

            # Skip levels disabled due to kill switch
            if level_key in self.disabled_triangles:
                continue

            # Initialize state the first time we see this level
            if level_key not in self.ready_for_new_triangle:
                self.ready_for_new_triangle[level_key] = True

            # Find the active executor for this specific level (same pairs + same profit band)
            active_executors_for_level = self.filter_executors(
                executors=self.executors_info,
                filter_func=lambda e: (
                    e.is_active
                    and e.type == "triangular_executor"
                    and e.config.maker_pair == maker_pair
                    and e.config.min_profit == min_profit
                    and e.config.max_profit == max_profit
                )
            )

            # Executor for this level is already running — nothing to do
            if len(active_executors_for_level) > 0:
                continue

            if self.ready_for_new_triangle[level_key]:
                # Create a new executor for this level
                self.logger().info(
                    f"Creating executor for triangle {maker_pair} level [{min_profit}-{max_profit}] "
                    f"label={triangle['level_label']} "
                    f"(base={triangle['base_amount']} +extra={triangle['extra_base_amount']}, "
                    f"quote={triangle['quote_amount']} +extra={triangle['extra_quote_amount']})"
                )
                config = TriangularExecutorConfig(
                    controller_id=self.config.id,
                    timestamp=self.market_data_provider.time(),
                    connector_name=self.config.connector_name,
                    maker_pair=maker_pair,
                    taker_1_pair=taker_1,
                    taker_2_pair=taker_2,
                    base_amount=triangle["base_amount"],
                    quote_amount=triangle["quote_amount"],
                    extra_base_amount=triangle["extra_base_amount"],
                    extra_quote_amount=triangle["extra_quote_amount"],
                    level_label=triangle["level_label"],
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
                # After creation mark not-ready; restart needs rebalance
                self.ready_for_new_triangle[level_key] = False
            else:
                # Executor stopped — rebalance this level's allocated assets.
                # Sibling levels are unaffected since assets are split proportionally per level.
                triangle_balances = {
                    base: self.config.balances.get(base, Decimal("0")),
                    quote: self.config.balances.get(quote, Decimal("0"))
                }
                if self.config.fee_asset and self.config.fee_asset in self.config.balances:
                    triangle_balances[self.config.fee_asset] = self.config.balances[self.config.fee_asset]

                self.logger().info(
                    f"Level [{min_profit}-{max_profit}] on {maker_pair} stopped, "
                    f"creating rebalance for assets: {list(triangle_balances.keys())}"
                )
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

                if self._has_failed_executor(maker_pair, min_profit, max_profit):
                    self.ready_for_new_triangle[level_key] = False
                    self.disabled_triangles.add(level_key)
                    self.logger().warning(
                        f"Level [{min_profit}-{max_profit}] on {maker_pair} disabled due to PnL kill switch. "
                        f"Rebalance will run but no new executor will be created."
                    )
                else:
                    self.ready_for_new_triangle[level_key] = True

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
        #     level_key = (triangle["maker"], triangle["taker_1"], triangle["taker_2"],
        #                  triangle["eff_min_profit"], triangle["eff_max_profit"])
        #     triangle_copy = triangle.copy()
        #     triangle_copy["ready_for_new_triangle"] = self.ready_for_new_triangle.get(level_key, True)
        #     triangle_info_with_state.append(triangle_copy)
        # status.append(f"Triangle Info: {triangle_info_with_state}")
        # for executor in self.executors_info:
        #     status.append(f"\n{executor}")
        return status