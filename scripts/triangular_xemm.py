import logging
import math

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class TriangularXEMM(ScriptStrategyBase):
    # Config params
    connector_name: str = "kucoin_paper_trade"
    maker_pair: str = "ADA-BTC"
    taker_pair_1: str = "BTC-USDT"
    taker_pair_2: str = "ADA-USDT"

    min_spread: Decimal = Decimal("1")
    max_spread: Decimal = Decimal("2")
    order_amount: Decimal = Decimal("10")
    target_amount = Decimal("100")
    # min_order_amount = Decimal("1")
    # slippage_buffer = Decimal("1")

    # kill_switch_enabled: bool = True
    # kill_switch_rate = Decimal("-2")

    # Class params
    status: str = "NOT_INIT"
    taker_pairs: tuple = ()
    taker_order_sides: dict = {}

    has_open_bid = False
    has_open_ask = False
    taker_candidates: list = []
    # profit: dict = {}
    # order_amount: dict = {}
    # profitable_direction: str = ""
    # place_order_trials_count: int = 0
    # place_order_trials_limit: int = 10
    # place_order_failure: bool = False
    # order_candidate = None
    # initial_spent_amount = Decimal("0")
    # total_profit = Decimal("0")
    # total_profit_pct = Decimal("0")

    markets = {connector_name: {maker_pair, taker_pair_1, taker_pair_2}}

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    def on_tick(self):
        """

        """
        # do some checks
        if self.status == "NOT_INIT":
            self.init_strategy()
        elif self.status == "NOT_ACTIVE":
            return

        # check for balances
        self.check_target_balance()

        # open maker orders
        self.check_and_cancel_open_orders()
        self.place_maker_orders()
        
    def init_strategy(self):
        """
        Initializes strategy once before the start.
        """
        self.status = "ACTIVE"
        self.set_trading_pair()
        self.set_order_side()
        
    # def check_trading_pair(self):
    #     """
    #     Checks if the pairs specified in the config are suitable for the triangular xemm.
    #     They should have only 3 common assets.
    #     """
    #     self.maker_base, self.maker_quote = split_hb_trading_pair(self.maker_pair)
    #     self.taker_1_base, self.taker_1_quote = split_hb_trading_pair(self.taker_pair_1)
    #     self.taker_2_base, self.taker_2_quote = split_hb_trading_pair(self.taker_pair_2)
    #     all_assets = {self.maker_base, self.maker_quote,
    #                   self.taker_1_base, self.taker_1_quote,
    #                   self.taker_2_base, self.taker_2_quote}
    #     if len(all_assets) != 3:
    #         self.status = "NOT_ACTIVE"
    #         self.log_with_clock(logging.WARNING, f"Pairs {self.maker_pair}, {self.taker_pair_1}, {self.taker_pair_2} "
    #                                              f"are not suited for triangular arbitrage!")

    def set_trading_pair(self):
        """
        Rearrange taker trading pairs so when ask or bid on maker market filled we start hedging by selling the
        asset we have.
        Makes 2 tuples for "bid_filled" and "ask_filled" directions and assigns them to the corresponding dictionary.
        """
        maker_base, maker_quote = split_hb_trading_pair(self.maker_pair)
        if maker_base in self.taker_pair_1:
            self.taker_pairs = (self.taker_pair_1, self.taker_pair_2)
        else:
            self.taker_pairs = (self.taker_pair_2, self.taker_pair_1)

    def set_order_side(self):
        """
        Sets order sides (1 = buy, 0 = sell) for already ordered trading pairs.
        Makes 2 tuples for "bid_filled" and "ask_filled" directions and assigns them to the corresponding dictionary.
        """
        maker_base, maker_quote = split_hb_trading_pair(self.maker_pair)
        taker_1_base, taker_1_quote = split_hb_trading_pair(self.taker_pairs[0])
        taker_2_base, taker_2_quote = split_hb_trading_pair(self.taker_pairs[1])

        order_side_1 = 0 if maker_base == taker_1_base else 1
        order_side_2 = 0 if maker_quote == taker_2_quote else 1

        self.taker_order_sides["maker_bid_filled"] = (order_side_1, order_side_2)
        self.taker_order_sides["maker_ask_filled"] = (1 - order_side_1, 1 - order_side_2)

    def check_target_balance(self):
        pass
    #     current_maker_base_balance = self.connector.get_available_balance(self.maker_base)
    #     amount_diff = current_maker_base_balance - self.target_amount
    #     amount_diff_quantize = self.connector.quantize_order_amount(self.maker_pair, Decimal(amount_diff))
    #     if amount_diff_quantize == Decimal("0"):
    #         return True
    #     else:
    #         # get min_order_size on taker market
    #         min_order_size_taker_1 = self.connector()
    #         min_order_size_taker_2 = self.connector()
    #         if both > 0:
    #             filled_side = "bid_filled" if amount_diff > 0 else "ask_filled"
    #             self.log_with_clock(logging.INFO, f"Hedging mode started")
    #             self.status = "HEDGE_MODE"
    #             self.create_and_process_taker_order_candidates(filled_side, amount_diff_quantize)
    #
    def place_maker_orders(self):
        pass
        # if not self.has_open_bid:
        #     # Calculate price
        #     order_sides = self.taker_order_sides["maker_bid_filled"]
        #     taker_1_price = self.connector.get_price(self.taker_pair_1, order_sides[0])
        #     taker_2_price = self.connector.get_price(self.taker_pair_1, order_sides[1])
        #     if order_sides[0] == order_sides[1]:
        #         taker_price = taker_1_price * taker_2_price
        #     else:
        #         if order_sides[0]:
        #             taker_price = taker_2_price / taker_1_price
        #         else:
        #             taker_price = taker_1_price / taker_2_price
        #     result_price = taker_price -
        #
        # if not self.has_open_ask:
        #     pass


    # def create_and_process_taker_order_candidates(self, filled_side, filled_amount):
    #     exchanged_amount = filled_amount
    #
    #
    #     for i in range(2):
    #         amount = self.get_order_amount_from_exchanged_amount(self.taker_pairs[filled_side][i],
    #                                                              self.taker_order_side[filled_side][i],
    #                                                              exchanged_amount)
    #
    #         self.taker_candidates[i] = self.create_order_candidate(self.taker_pairs[filled_side][i],
    #                                                                self.taker_order_side[filled_side][i],
    #                                                                amount)
    #         # update amount for next cycle
    #         if self.taker_order_side[filled_side][i]:
    #             exchanged_amount = amount
    #         else:
    #             exchanged_amount = self.connector.get_quote_volume_for_base_amount(self.taker_pairs[filled_side][i],
    #                                                                                self.taker_order_side[filled_side][i],
    #                                                                                amount).result_volume
    # def get_order_amount_from_exchanged_amount(self, pair, side, exchanged_amount) -> Decimal:
    #     """
    #     Calculates order amount using the amount that we want to exchange.
    #     - If the side is buy then exchanged asset is a quote asset. Get base amount using the orderbook
    #     - If the side is sell then exchanged asset is a base asset.
    #     """
    #     if side:
    #         orderbook = self.connector.get_order_book(pair)
    #         order_amount = self.get_base_amount_for_quote_volume(orderbook.ask_entries(), exchanged_amount)
    #     else:
    #         order_amount = exchanged_amount
    #
    #     return order_amount
    #
    # def create_order_candidate(self, pair, side, amount):
    #     """
    #     Creates order candidate. Checks the quantized amount
    #     """
    #     side = TradeType.BUY if side else TradeType.SELL
    #     price = self.connector.get_price_for_volume(pair, side, amount).result_price
    #     price_quantize = self.connector.quantize_order_price(pair, Decimal(price))
    #     amount_quantize = self.connector.quantize_order_amount(pair, Decimal(amount))
    #
    #     if amount_quantize == Decimal("0"):
    #         self.log_with_clock(logging.INFO, f"Order amount on {pair} is too low to place an order")
    #         return None
    #
    #     return OrderCandidate(
    #         trading_pair=pair,
    #         is_maker=False,
    #         order_type=OrderType.MARKET,
    #         order_side=side,
    #         amount=amount_quantize,
    #         price=price_quantize)
    #
    # def adjust_order_candidate(self, order_candidate, multiple_trials_enabled) -> bool:
    #     """
    #     Checks order candidate balance and either places an order or sets a failure for the next trials
    #     """
    #     order_candidate_adjusted = self.connector.budget_checker.adjust_candidate(order_candidate, all_or_none=True)
    #     if math.isclose(order_candidate.amount, Decimal("0"), rel_tol=1E-6):
    #         self.logger().info(f"Order adjusted amount: {order_candidate.amount} on {order_candidate.trading_pair}, "
    #                            f"too low to place an order")
    #         if multiple_trials_enabled:
    #             self.place_order_trials_count += 1
    #             self.place_order_failure = True
    #         return False
    #     else:
    #         is_buy = True if order_candidate.order_side == TradeType.BUY else False
    #         self.place_order(self.connector_name,
    #                          order_candidate.trading_pair,
    #                          is_buy,
    #                          order_candidate_adjusted.amount,
    #                          order_candidate.order_type,
    #                          order_candidate_adjusted.price)
    #         return True
    #
    # def place_order(self,
    #                 connector_name: str,
    #                 trading_pair: str,
    #                 is_buy: bool,
    #                 amount: Decimal,
    #                 order_type: OrderType,
    #                 price=Decimal("NaN"),
    #                 ):
    #     if is_buy:
    #         self.buy(connector_name, trading_pair, amount, order_type, price)
    #     else:
    #         self.sell(connector_name, trading_pair, amount, order_type, price)
    #
    #
    # def check_and_cancel_open_orders(self):
    #     for order in self.get_active_orders(connector_name=self.connector_name):
    #         if order.is_buy:
    #             if self.check_price_of_open_order():
    #                 self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
    #                 self.has_open_bid = False
    #         else:
    #             if self.check_price_of_open_order():
    #                 self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
    #                 self.has_open_ask = False
    #
