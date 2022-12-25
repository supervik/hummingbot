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
    maker_pair: str = "XMR-BTC"
    taker_pair_1: str = "XMR-USDT"
    taker_pair_2: str = "BTC-USDT"

    min_spread: Decimal = Decimal("0.2")
    max_spread: Decimal = Decimal("0.5")
    order_amount: Decimal = Decimal("0.1")
    target_base_amount = Decimal("0")
    target_quote_amount = Decimal("1")
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
    maker_base = ""
    maker_quote = ""
    taker_1_base = ""
    taker_1_quote = ""
    taker_2_base = ""
    taker_2_quote = ""

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
        balance_diff_base = self.get_target_balance_diff(self.maker_base, self.target_base_amount)
        balance_diff_base_quantize = self.connector.quantize_order_amount(self.maker_pair, abs(balance_diff_base))

        if balance_diff_base_quantize != Decimal("0"):
            self.status = "HEDGE_MODE"
            self.log_with_clock(logging.INFO, f"Hedging mode started")
            balance_diff_quote = self.get_target_balance_diff(self.maker_quote, self.target_quote_amount)
            if balance_diff_base > 0:
                # bid was filled
                taker_orders = self.get_taker_order_data(True, abs(balance_diff_base), abs(balance_diff_quote))
            else:
                # ask was filled
                taker_orders = self.get_taker_order_data(False, abs(balance_diff_base), abs(balance_diff_quote))
            self.place_taker_orders(taker_orders)

        # open maker orders
        self.taker_sell_price = self.calculate_taker_price(is_maker_bid=True)
        self.log_with_clock(logging.INFO, f"self.taker_sell_price = {self.taker_sell_price }")
        self.taker_buy_price = self.calculate_taker_price(is_maker_bid=False)
        self.log_with_clock(logging.INFO, f"self.taker_buy_price = {self.taker_buy_price}")

        self.check_and_cancel_open_orders()
        self.place_maker_orders()
        
    def init_strategy(self):
        """
        Initializes strategy once before the start.
        """
        self.status = "ACTIVE"
        self.set_base_quote_assets()
        self.set_spread()

    def set_base_quote_assets(self):
        """
        """
        self.maker_base, self.maker_quote = split_hb_trading_pair(self.maker_pair)
        self.taker_1_base, self.taker_1_quote = split_hb_trading_pair(self.taker_pair_1)
        self.taker_2_base, self.taker_2_quote = split_hb_trading_pair(self.taker_pair_2)

    def set_spread(self):
        self.spread = (self.min_spread + self.max_spread) / 2

    def get_target_balance_diff(self, asset, target_amount):
        current_balance = self.connector.get_available_balance(asset)
        amount_diff = current_balance - target_amount
        self.log_with_clock(logging.INFO, f"Current balance {asset}: {current_balance}, "
                                          f"Target balance: {target_amount}, "
                                          f"Amount_diff: {amount_diff}")
        return amount_diff

    def get_taker_order_data(self, is_maker_bid, balances_diff_base, balances_diff_quote):
        taker_side_1 = not is_maker_bid
        taker_amount_1 = balances_diff_base
        taker_price_1 = self.connector.get_price_for_volume(self.taker_pair_1, taker_side_1, taker_amount_1).result_price
        if self.taker_1_quote == self.taker_2_quote:
            taker_side_2 = True
            taker_amount_2 = self.connector.quantize_order_amount(self.taker_pair_2, balances_diff_quote)
        else:
            taker_side_2 = False
            taker_amount_2 = self.get_base_volume_for_quote_amount(self.taker_pair_2, False, balances_diff_quote)
        taker_price_2 = self.connector.get_price_for_volume(self.taker_pair_2, taker_side_2, taker_amount_2).result_price

        return {"pair": [self.taker_pair_1, self.taker_pair_2],
                "side": [taker_side_1, taker_side_2],
                "amount": [taker_amount_1, taker_amount_2],
                "price": [taker_price_1, taker_price_2]}

    def place_taker_orders(self, taker_order):
        order_candidate_adjusted = []
        for i in range(2):
            side = TradeType.BUY if taker_order["side"][i] else TradeType.SELL
            order_candidate = OrderCandidate(
                                    trading_pair=taker_order["pair"][i],
                                    is_maker=False,
                                    order_type=OrderType.MARKET,
                                    order_side=side,
                                    amount=taker_order["amount"][i],
                                    price=taker_order["price"][i])
            order_candidate_adjusted.append(self.connector.budget_checker.adjust_candidate(order_candidate[i],
                                                                                           all_or_none=False))
            if order_candidate_adjusted[i].amount < 0:
                self.log_with_clock(logging.INFO, f"Order candidate amount is less than allowed on the market: "
                                                  f" {order_candidate}")
                return

        self.log_with_clock(logging.INFO, f"Placing taker orders {order_candidate_adjusted}")
        for order_candidate in order_candidate_adjusted:
            self.place_order(order_candidate)

    def place_maker_orders(self):
        if not self.has_open_bid:
            order_price = self.taker_sell_price * Decimal(1 - self.spread / 100)
            buy_candidate = OrderCandidate(trading_pair=self.maker_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=self.order_amount,
                                           price=order_price)
            buy_candidate_adjusted = self.connector.budget_checker.adjust_candidate(buy_candidate, all_or_none=False)
            if buy_candidate_adjusted.amount > 0:
                self.log_with_clock(logging.INFO, f"Buy {buy_candidate_adjusted.amount} {self.maker_pair} "
                                                  f"with the price {buy_candidate_adjusted.price}")
                self.place_order(buy_candidate_adjusted)

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
                self.place_order(sell_candidate_adjusted)

    def place_order(self, candidate):
        if candidate.order_side == TradeType.BUY:
            self.buy(
                self.connector_name, candidate.trading_pair, candidate.amount, candidate.order_type, candidate.price)
        else:
            self.sell(
                self.connector_name, candidate.trading_pair, candidate.amount, candidate.order_type, candidate.price)

    def calculate_taker_price(self, is_maker_bid):
        side_taker_1 = not is_maker_bid
        exchanged_amount_1 = self.connector.get_quote_volume_for_base_amount(self.taker_pair_1, side_taker_1, self.order_amount).result_volume
        if self.taker_1_quote == self.taker_2_quote:
            side_taker_2 = not side_taker_1
            exchanged_amount_2 = self.get_base_amount_for_quote_volume(self.taker_pair_2, side_taker_2, exchanged_amount_1)
        else:
            side_taker_2 = side_taker_1
            exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(self.taker_pair_2, side_taker_2, exchanged_amount_1).result_volume
        final_price = exchanged_amount_2 / self.order_amount
        return final_price

    def check_and_cancel_open_orders(self):
        self.has_open_bid = False
        self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.connector_name):
            if order.is_buy:
                self.has_open_bid = True
                upper_price = self.taker_sell_price * Decimal(1 - self.min_spread / 100)
                lower_price = self.taker_sell_price * Decimal(1 - self.max_spread / 100)
                self.log_with_clock(logging.INFO, f"BUY order price {order.price} Range:"
                                                  f"{lower_price} - {upper_price}")
                if order.price > upper_price or order.price < lower_price:
                    self.log_with_clock(logging.INFO, f"BUY order price {order.price} is not in the range "
                                                      f"{lower_price} - {upper_price}. Cancelling order.")
                    self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
                    self.has_open_bid = False
            else:
                self.has_open_ask = True
                upper_price = self.taker_buy_price * Decimal(1 + self.max_spread / 100)
                lower_price = self.taker_buy_price * Decimal(1 + self.min_spread / 100)
                self.log_with_clock(logging.INFO, f"SELL order price {order.price} Range:"
                                                  f"{lower_price} - {upper_price}")
                if order.price > upper_price or order.price < lower_price:
                    self.log_with_clock(logging.INFO, f"SELL order price {order.price} is not in the range "
                                                      f"{lower_price} - {upper_price}. Cancelling order.")
                    self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
                    self.has_open_ask = False

    def get_base_amount_for_quote_volume(self, pair, side, quote_volume) -> Decimal:
        """
        Calculates base amount that you get for the quote volume using the orderbook entries
        """
        orderbook = self.connector.get_order_book(pair)
        orderbook_entries = orderbook.ask_entries() if side else orderbook.bid_entries()

        cumulative_volume = 0.
        cumulative_base_amount = 0.
        quote_volume = float(quote_volume)

        for order_book_row in orderbook_entries:
            row_amount = order_book_row.amount
            row_price = order_book_row.price
            row_volume = row_amount * row_price
            if row_volume + cumulative_volume >= quote_volume:
                row_volume = quote_volume - cumulative_volume
                row_amount = row_volume / row_price
            cumulative_volume += row_volume
            cumulative_base_amount += row_amount
            if cumulative_volume >= quote_volume:
                break

        return Decimal(cumulative_base_amount)