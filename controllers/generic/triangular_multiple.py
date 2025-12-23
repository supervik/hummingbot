from decimal import Decimal
from typing import Dict, List, Tuple, Union

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy_v2.executors.triangular_executor.data_types import TriangularExecutorConfig
from hummingbot.strategy_v2.executors.rebalance_executor.data_types import RebalanceExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


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

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for triangle in self.triangles:
            pairs = triangle.split()
            for pair in pairs:
                markets = markets.add_or_update(self.connector_name, pair)
        return markets

    def _maker_asset_usage(self) -> Tuple[Dict[str, int], Dict[str, int]]:
        """
        Counts how many triangles use each asset as maker base and maker quote.
        Shared assets are split evenly when computing allocations.
        """
        base_usage: Dict[str, int] = {}
        quote_usage: Dict[str, int] = {}

        for triangle in self.triangles:
            pairs = triangle.split()
            maker_pair = pairs[0]
            base, quote = maker_pair.split("-")
            base_usage[base] = base_usage.get(base, 0) + 1
            quote_usage[quote] = quote_usage.get(quote, 0) + 1

        return base_usage, quote_usage

    @property
    def triangle_info(self) -> List[Dict[str, Union[str, Decimal]]]:
        """
        Returns per-triangle info including maker/taker pairs and maker order sizes.
        - order_size_ask: maker base units allocated to asks.
        - order_size_bid: maker quote units allocated to bids (later converted to base when buying).
        Shared assets are split equally across triangles that use them.
        """
        base_usage, quote_usage = self._maker_asset_usage()
        triangle_dicts: List[Dict[str, Union[str, Decimal]]] = []

        for triangle in self.triangles:
            pairs = triangle.split()
            maker_pair = pairs[0]
            base, quote = maker_pair.split("-")

            base_balance = self.balances.get(base, Decimal("0"))
            quote_balance = self.balances.get(quote, Decimal("0"))
            base_count = base_usage.get(base, 0)
            quote_count = quote_usage.get(quote, 0)

            # if base_balance <= 0:
            #     raise ValueError(f"Missing or zero balance for maker base asset {base} in triangle {triangle}")
            # if quote_balance <= 0:
            #     raise ValueError(f"Missing or zero balance for maker quote asset {quote} in triangle {triangle}")

            base_share = base_balance / Decimal(base_count)
            quote_share = quote_balance / Decimal(quote_count)

            triangle_dicts.append({
                "maker": maker_pair,
                "taker_1": pairs[1],
                "taker_2": pairs[2],
                "base_amount": base_share,
                "quote_amount": quote_share,
            })

        return triangle_dicts


class TriangularMultiple(ControllerBase):
    def __init__(self, config: TriangularMultipleConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.logger().info(f"Initializing TriangularMultiple controller with configuration: {self.config}")
        self.logger().info(f"Triangle info: {self.config.triangle_info}")
        self.first_run = True

    async def update_processed_data(self):
        pass

    
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

        if len(active_rebalance_executors) == 0:
            for triangle in self.config.triangle_info:
                active_triangle_executors = self.filter_executors(
                    executors=self.executors_info,
                    filter_func=lambda e: e.is_active and e.type == "triangular_executor"
                )
                active_triangle_executors_on_pair = self.filter_executors(
                    executors=active_triangle_executors,
                    filter_func=lambda e: e.config.maker_pair == triangle["maker"]
                )
                
                if len(active_triangle_executors_on_pair) == 0:
                    self.logger().info(f"Creating executor for triangle {triangle['maker']}")
                    config = TriangularExecutorConfig(
                        controller_id=self.config.id,
                        timestamp=self.market_data_provider.time(),
                        connector_name=self.config.connector_name,
                        maker_pair=triangle["maker"],
                        taker_1_pair=triangle["taker_1"],
                        taker_2_pair=triangle["taker_2"],
                        base_amount=triangle["base_amount"],
                        quote_amount=triangle["quote_amount"],
                        min_profit=self.config.min_profit,
                        max_profit=self.config.max_profit,
                        fee_maker=self.config.maker_fee,
                        fee_taker=self.config.taker_fee,
                        min_usdt=self.config.min_usdt,
                    )
                    executor_actions.append(CreateExecutorAction(
                        controller_id=self.config.id,
                        executor_config=config
                    ))
        return executor_actions
        
    def to_format_status(self) -> List[str]:
        status = []
        status.append(f"Triangular Multiple Controller: {self.config.id}")
        status.append(f"Controller status: {self._status}")
        status.append(f"Triangle Info: {self.config.triangle_info}")
        # for executor in self.executors_info:
        #     status.append(f"\n{executor}")
        return status