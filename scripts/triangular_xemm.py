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
    SellOrderCreatedEvent, OrderFilledEvent,
)
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class TriangularXEMM(ScriptStrategyBase):
    # Config params
    connector_name: str = "kucoin"
    maker_pair: str = "FRONT-BTC"
    taker_pair_1: str = "FRONT-USDT"
    taker_pair_2: str = "BTC-USDT"

    min_spread: Decimal = Decimal("0.1")
    max_spread: Decimal = Decimal("0.5")

    order_amount: Decimal = Decimal("800")
    target_base_amount = Decimal("0")
    target_quote_amount = Decimal("0.016893")
    order_delay = 60
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
    last_order_timestamp = 0
    place_order_trials_delay = 5
    place_order_trials_limit = 10

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
        elif self.status == "HEDGE_MODE":
            self.process_hedge()
            return

        if self.current_timestamp < self.last_order_timestamp + self.order_delay:
            return

        # check for balances
        balance_diff_base = self.get_target_balance_diff(self.maker_base, self.target_base_amount)
        balance_diff_base_quantize = self.connector.quantize_order_amount(self.taker_pair_1, abs(balance_diff_base))

        if balance_diff_base_quantize != Decimal("0"):
            balance_diff_quote = self.get_target_balance_diff(self.maker_quote, self.target_quote_amount)
            if balance_diff_base > 0:
                # bid was filled
                taker_orders = self.get_taker_order_data(True, abs(balance_diff_base), abs(balance_diff_quote))
            else:
                # ask was filled
                taker_orders = self.get_taker_order_data(False, abs(balance_diff_base), abs(balance_diff_quote))
            self.place_taker_orders(taker_orders)
            return

        # open maker orders
        self.taker_sell_price = self.calculate_taker_price(is_maker_bid=True)
        # self.log_with_clock(logging.INFO, f"self.taker_sell_price = {self.taker_sell_price }")

        self.taker_buy_price = self.calculate_taker_price(is_maker_bid=False)
        # self.log_with_clock(logging.INFO, f"self.taker_buy_price = {self.taker_buy_price}")

        if self.check_and_cancel_maker_orders():
            return True
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

    def process_hedge(self):
        if len(self.taker_candidates) > 0:
            for i, candidate in enumerate(self.taker_candidates):
                if self.current_timestamp > candidate["sent_timestamp"] + self.place_order_trials_delay:
                    if candidate["trials"] <= self.place_order_trials_limit:
                        self.log_with_clock(logging.INFO, f"Failed to place {candidate['order_candidate'].trading_pair}"
                                                          f" {candidate['order_candidate'].order_side} order. "
                                                          f"Trial number {candidate['trials']}")
                        sent_result = self.adjust_and_place_order(candidate=candidate['order_candidate'],
                                                                  all_or_none=True)
                        if sent_result:
                            self.taker_candidates[i]["sent_timestamp"] = self.current_timestamp
                        self.taker_candidates[i]["trials"] += 1
                    else:
                        msg = f"Error placing {candidate['order_candidate'].trading_pair} " \
                              f"{candidate['order_candidate'].order_side} order. Stop trading"
                        self.notify_hb_app_with_timestamp(msg)
                        self.log_with_clock(logging.WARNING, msg)
                        self.status = "NOT_ACTIVE"
                else:
                    delay = candidate['sent_timestamp'] + self.place_order_trials_delay - self.current_timestamp
                    self.log_with_clock(logging.INFO, f"Too early to place an order. Try again. {delay} sec left.")
        else:
            msg = f"Arbitrage round completed"
            self.notify_hb_app_with_timestamp(msg)
            self.log_with_clock(logging.WARNING, msg)
            self.status = "ACTIVE"

    def get_target_balance_diff(self, asset, target_amount):
        current_balance = self.connector.get_balance(asset)
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
            taker_amount_2 = self.get_base_amount_for_quote_volume(self.taker_pair_2, False, balances_diff_quote)
        taker_price_2 = self.connector.get_price_for_volume(self.taker_pair_2, taker_side_2, taker_amount_2).result_price

        return {"pair": [self.taker_pair_1, self.taker_pair_2],
                "side": [taker_side_1, taker_side_2],
                "amount": [taker_amount_1, taker_amount_2],
                "price": [taker_price_1, taker_price_2]}

    def place_taker_orders(self, taker_order):
        self.status = "HEDGE_MODE"
        self.log_with_clock(logging.INFO, f"Hedging mode started")
        self.last_order_timestamp = self.current_timestamp
        self.cancel_all_orders()
        for i in range(2):
            side = TradeType.BUY if taker_order["side"][i] else TradeType.SELL
            if taker_order["amount"][i] <= Decimal("0"):
                self.log_with_clock(logging.INFO, f"Can't add taker candidate {side} {taker_order['pair'][i]} "
                                                  f"to the list. Too low amount")
                continue
            taker_candidate = OrderCandidate(
                                    trading_pair=taker_order["pair"][i],
                                    is_maker=False,
                                    order_type=OrderType.MARKET,
                                    order_side=side,
                                    amount=taker_order["amount"][i],
                                    price=taker_order["price"][i])
            self.taker_candidates.append({"order_candidate": taker_candidate, "sent_timestamp": 0, "trials": 0})
            sent_result = self.adjust_and_place_order(candidate=taker_candidate, all_or_none=True)
            if sent_result:
                self.taker_candidates[-1]["sent_timestamp"] = self.current_timestamp
            self.log_with_clock(logging.INFO, f"New taker candidate added to the list: {self.taker_candidates[-1]}")

            # order_candidate_adjusted = self.connector.budget_checker.adjust_candidate(order_candidate, all_or_none=True)
            # if order_candidate_adjusted.amount == Decimal("0"):
            #     self.log_with_clock(logging.INFO, f"Order candidate amount is less than allowed on the market: "
            #                                       f" {order_candidate_adjusted.trading_pair}. Can't create"
            #                                       f" {order_candidate_adjusted.order_side.name}"
            #                                       f" {order_candidate_adjusted.order_type.name} order")
            #     self.taker_candidates.append({"order_candidate": order_candidate,
            #                                   "sent_timestamp": 0,
            #                                   "trials": 0})
            # else:
            #     self.taker_candidates.append({"order_candidate": order_candidate,
            #                                   "sent_timestamp": self.current_timestamp,
            #                                   "trials": 0})
            #     self.place_order(order_candidate_adjusted)

    def place_maker_orders(self):
        if not self.has_open_bid:
            self.log_with_clock(logging.INFO, f"Placing maker BUY order")
            order_price = self.taker_sell_price * Decimal(1 - self.spread / 100)
            buy_candidate = OrderCandidate(trading_pair=self.maker_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=self.order_amount,
                                           price=order_price)
            self.adjust_and_place_order(candidate=buy_candidate, all_or_none=False)
            # buy_candidate_adjusted = self.connector.budget_checker.adjust_candidate(buy_candidate, all_or_none=False)
            # if buy_candidate_adjusted.amount > Decimal("0"):
            #     self.place_order(buy_candidate_adjusted)
            # else:
            #     self.log_with_clock(logging.INFO, f"BUY amount is less than allowed on the maker market"
            #                                       f"{self.maker_pair} Can't place order.")

        if not self.has_open_ask:
            self.log_with_clock(logging.INFO, f"Placing maker SELL order")
            order_price = self.taker_buy_price * Decimal(1 + self.spread / 100)
            sell_candidate = OrderCandidate(
                trading_pair=self.maker_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=self.order_amount,
                price=order_price)
            self.adjust_and_place_order(candidate=sell_candidate, all_or_none=False)
            # sell_candidate_adjusted = self.connector.budget_checker.adjust_candidate(sell_candidate, all_or_none=False)
            # if sell_candidate_adjusted.amount > Decimal("0"):
            #     self.place_order(sell_candidate_adjusted)
            # else:
            #     self.log_with_clock(logging.INFO, f"SELL amount is less than allowed on the maker market"
            #                                       f"{self.maker_pair} Can't place order.")

    def adjust_and_place_order(self, candidate, all_or_none):
        candidate_adjusted = self.connector.budget_checker.adjust_candidate(candidate, all_or_none=all_or_none)
        if candidate_adjusted.amount == Decimal("0"):
            self.log_with_clock(logging.INFO,
                                f"Order candidate amount is less than allowed on the market: "
                                f" {candidate_adjusted.trading_pair}. Can't create"
                                f" {candidate_adjusted.order_side.name}"
                                f" {candidate_adjusted.order_type.name} order")
            return False
        else:
            self.place_order(candidate_adjusted)
            return True

    def place_order(self, candidate_adjusted):
        if candidate_adjusted.order_side == TradeType.BUY:
            self.buy(
                self.connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price)
        else:
            self.sell(
                self.connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price)

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.connector_name):
            self.cancel(self.connector_name, order.trading_pair, order.client_order_id)

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
        # tests
        # test_price_taker_1_ask = self.connector.get_vwap_for_volume(self.taker_pair_1, True, self.order_amount).result_price
        # test_price_taker_1_bid = self.connector.get_vwap_for_volume(self.taker_pair_1, False, self.order_amount).result_price
        # order_amount_in_cross = self.connector.get_quote_volume_for_base_amount(self.maker_pair, True, self.order_amount).result_volume
        # test_price_taker_2_ask = self.connector.get_vwap_for_volume(self.taker_pair_2, True, order_amount_in_cross).result_price
        # test_price_taker_2_bid = self.connector.get_vwap_for_volume(self.taker_pair_2, False, order_amount_in_cross).result_price

        return final_price

    def check_and_cancel_maker_orders(self):
        self.has_open_bid = False
        self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.connector_name):
            if order.is_buy:
                self.has_open_bid = True
                upper_price = self.taker_sell_price * Decimal(1 - self.min_spread / 100)
                lower_price = self.taker_sell_price * Decimal(1 - self.max_spread / 100)
                if order.price > upper_price or order.price < lower_price:
                    self.log_with_clock(logging.INFO, f"BUY order price {order.price} is not in the range "
                                                      f"{lower_price} - {upper_price}. Cancelling order.")
                    self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
                    return True
            else:
                self.has_open_ask = True
                upper_price = self.taker_buy_price * Decimal(1 + self.max_spread / 100)
                lower_price = self.taker_buy_price * Decimal(1 + self.min_spread / 100)
                if order.price > upper_price or order.price < lower_price:
                    self.log_with_clock(logging.INFO, f"SELL order price {order.price} is not in the range "
                                                      f"{lower_price} - {upper_price}. Cancelling order.")
                    self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
                    return True
        return False

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

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        self.log_with_clock(logging.INFO, f"Buy order is created on the market {event.trading_pair}")
        self.check_and_remove_taker_candidates(event, TradeType.BUY)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        self.log_with_clock(logging.INFO, f"Sell order is created on the market {event.trading_pair}")
        self.check_and_remove_taker_candidates(event, TradeType.SELL)

    def did_fill_order(self, event: OrderFilledEvent):
        self.check_and_remove_taker_candidates(event, event.trade_type)
        msg = (f"{event.trade_type.name} {round(event.amount, 5)} {event.trading_pair} {self.connector_name} "
               f"at {round(event.price, 5)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def check_and_remove_taker_candidates(self, filled_event, trade_type):
        candidates = self.taker_candidates.copy()
        for i, candidate in enumerate(candidates):
            if candidate["order_candidate"].trading_pair == filled_event.trading_pair \
                    and candidate["order_candidate"].order_side == trade_type:
                self.log_with_clock(logging.INFO, f"Remove order candidate {candidate}")
                self.taker_candidates.pop(i)
                break
