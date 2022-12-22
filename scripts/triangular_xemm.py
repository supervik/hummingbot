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
    connector_name: str = "kucoin"
    maker_pair: str = "XRP-USDT"
    taker_pair_1: str = "XRP-BTC"
    taker_pair_2: str = "BTC-USDT"

    min_spread: Decimal = Decimal("1")
    max_spread: Decimal = Decimal("2")
    order_amount: Decimal = Decimal("1")
    target_amount = Decimal("0")
    # min_order_amount = Decimal("1")
    # slippage_buffer = Decimal("1")

    # kill_switch_enabled: bool = True
    # kill_switch_rate = Decimal("-2")

    # Class params
    status: str = "NOT_INIT"
    taker_pairs: tuple = ()
    taker_order_sides: dict = {}
    taker_price_calculation_method: int = 0
    taker_sell_price = 0
    taker_buy_price = 0
    spread = 0

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
        self.taker_sell_price = self.calculate_taker_price(self.taker_order_sides["maker_bid_filled"])
        self.taker_buy_price = self.calculate_taker_price(self.taker_order_sides["maker_ask_filled"])

        self.check_and_cancel_open_orders()
        self.place_maker_orders()
        
    def init_strategy(self):
        """
        Initializes strategy once before the start.
        """
        self.status = "ACTIVE"
        self.set_trading_pair()
        self.set_order_side()
        self.set_taker_price_calculation_method()
        self.set_spread()
        
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

    def set_taker_price_calculation_method(self):
        taker_side_1, taker_side_2 = self.taker_order_sides["maker_bid_filled"][0], self.taker_order_sides["maker_bid_filled"][1]
        if taker_side_1 == taker_side_2:
            self.taker_price_calculation_method = 1
        else:
            self.taker_price_calculation_method = 2 if taker_side_2 else 3

    def set_spread(self):
        self.spread = (self.min_spread + self.max_spread) / 2

    def check_target_balance(self):
        pass
        # current_maker_base_balance = self.connector.get_available_balance(self.maker_base)
        # amount_diff = current_maker_base_balance - self.target_amount
        # amount_diff_quantize = self.connector.quantize_order_amount(self.maker_pair, Decimal(amount_diff))
        # self.log_with_clock(logging.INFO, f"Current balance: {current_maker_base_balance}, "
        #                                   f"Target balance: {self.target_amount}, "
        #                                   f"amount_diff_quantize: {amount_diff_quantize}")
        # if amount_diff_quantize == Decimal("0"):
        #     return True
        # else:
        #     # get min_order_size on taker market
        #     min_order_size_taker_1 = self.connector()
        #     min_order_size_taker_2 = self.connector()
        #     if both > 0:
        #         filled_side = "bid_filled" if amount_diff > 0 else "ask_filled"
        #         self.log_with_clock(logging.INFO, f"Hedging mode started")
        #         self.status = "HEDGE_MODE"
        #         self.create_and_process_taker_order_candidates(filled_side, amount_diff_quantize)

    def place_maker_orders(self):
        if not self.has_open_bid:
            order_price = self.taker_sell_price * Decimal(1 - self.spread / 100)
            buy_candidate = OrderCandidate(
                                trading_pair=self.maker_pair,
                                is_maker=True,
                                order_type=OrderType.LIMIT,
                                order_side=TradeType.BUY,
                                amount=self.order_amount,
                                price=order_price)
            buy_candidate_adjusted = self.connector.budget_checker.adjust_candidate(buy_candidate, all_or_none=False)
            if buy_candidate_adjusted.amount > 0:
                self.log_with_clock(logging.INFO, f"Buy {buy_candidate_adjusted.amount} {self.maker_pair} "
                                                  f"with the price {buy_candidate_adjusted.price}")
                self.buy(
                    self.connector_name,
                    self.maker_pair,
                    buy_candidate_adjusted.amount,
                    buy_candidate_adjusted.order_type,
                    buy_candidate_adjusted.price)
                self.has_open_bid = True

        if not self.has_open_ask:
            order_price = self.taker_buy_price * Decimal(1 + self.spread / 100)
            sell_candidate = OrderCandidate(
                trading_pair=self.maker_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=self.order_amount,
                price=order_price)
            sell_candidate_adjusted = self.connector.budget_checker.adjust_candidate(sell_candidate, all_or_none=False)
            if sell_candidate_adjusted.amount > 0:
                self.log_with_clock(logging.INFO, f"Sell {sell_candidate_adjusted.amount} {self.maker_pair} "
                                                  f"with the price {sell_candidate_adjusted.price}")
                self.sell(
                    self.connector_name,
                    self.maker_pair,
                    sell_candidate_adjusted.amount,
                    sell_candidate_adjusted.order_type,
                    sell_candidate_adjusted.price)
                self.has_open_ask = True

    def calculate_taker_price(self, order_sides):
        taker_1_price = self.connector.get_price(self.taker_pair_1, order_sides[0])
        taker_1_price_ = self.connector.get_price(self.taker_pair_1, 1 - order_sides[0])
        taker_2_price = self.connector.get_price(self.taker_pair_2, order_sides[1])
        taker_2_price_ = self.connector.get_price(self.taker_pair_2, 1 - order_sides[1])
        maker_price_bid_ = self.connector.get_price(self.maker_pair, False)
        maker_price_ask_ = self.connector.get_price(self.maker_pair, True)
        if self.taker_price_calculation_method == 1:
            taker_price = taker_1_price * taker_2_price
        elif self.taker_price_calculation_method == 2:
            taker_price = taker_1_price / taker_2_price
        else:
            taker_price = taker_2_price / taker_1_price
        return taker_price

    def check_and_cancel_open_orders(self):
        self.has_open_bid = False
        self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.connector_name):
            if order.is_buy:
                upper_price = self.taker_sell_price * Decimal(1 - self.min_spread / 100)
                lower_price = self.taker_sell_price * Decimal(1 - self.max_spread / 100)
                self.log_with_clock(logging.INFO, f"BUY order price {order.price} Range:"
                                                  f"{lower_price} - {upper_price}")
                if order.price > upper_price or order.price < lower_price:
                    self.log_with_clock(logging.INFO, f"BUY order price {order.price} is not in the range "
                                                      f"{lower_price} - {upper_price}. Cancelling order.")
                    self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
                else:
                    self.has_open_bid = True
            else:
                upper_price = self.taker_buy_price * Decimal(1 + self.max_spread / 100)
                lower_price = self.taker_buy_price * Decimal(1 + self.min_spread / 100)
                self.log_with_clock(logging.INFO, f"SELL order price {order.price} Range:"
                                                  f"{lower_price} - {upper_price}")
                if order.price > upper_price or order.price < lower_price:
                    self.log_with_clock(logging.INFO, f"SELL order price {order.price} is not in the range "
                                                      f"{lower_price} - {upper_price}. Cancelling order.")
                    self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
                else:
                    self.has_open_ask = True
