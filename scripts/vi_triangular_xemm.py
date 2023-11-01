import csv
import logging
import os
import time
from enum import Enum

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent, \
    MarketOrderFailureEvent, BuyOrderCompletedEvent, SellOrderCompletedEvent, OrderCancelledEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class OrderState(Enum):
    PENDING_CREATE = 0
    CREATED = 1
    PENDING_CANCEL = 2
    CANCELED = 3
    PENDING_EXECUTE = 4
    EXECUTED = 5


class TriangularXEMM(ScriptStrategyBase):
    """
    The script that performs triangular XEMM on a single exchange.
    The script based on balances check. If it finds the imbalance it opens 2 other orders
    The script has kill_switch and fee asset check and rebalance
    """
    # Config params
    connector_name: str = "kucoin"
    maker_pair: str = "ETH-DAI"
    taker_pair_1: str = "ETH-USDT"
    taker_pair_2: str = "USDT-DAI"

    min_spread: Decimal = Decimal("0.5")
    max_spread: Decimal = Decimal("1")

    order_amount: Decimal = Decimal("0.4")
    min_maker_order_amount = Decimal("0.2")
    min_taker_order_amount = Decimal("0.002")
    leftover_bid_pct = Decimal("0")
    leftover_ask_pct = Decimal("0")

    trigger_arbitrage_on_base_change = False
    set_target_from_config = False
    target_base_amount = Decimal("0.01")
    target_quote_amount = Decimal("20")

    place_bid = True
    place_ask = True

    fee_tracking_enabled = True
    fee_asset = "KCS"
    fee_asset_target_amount = Decimal("2")
    fee_asset_min_order_amount = Decimal("0.2")
    fee_pair = "KCS-USDT"
    fee_asset_check_interval = 300

    kill_switch_enabled: bool = True
    kill_switch_asset = "USDT"
    kill_switch_rate = Decimal("-3")
    kill_switch_check_interval = 60
    kill_switch_counter_limit = 5

    order_delay = 15
    slippage_buffer = Decimal("1")
    taker_order_type = OrderType.MARKET
    test_latency = True
    max_order_age = 300

    # Class params
    status: str = "NOT_INIT"
    taker_sell_price = 0
    taker_buy_price = 0
    spread = 0
    assets = {}

    has_open_bid = False
    has_open_ask = False
    maker_order_filled = False
    taker_candidates: list = []
    last_order_timestamp = 0
    place_order_trials_delay = 10
    place_order_trials_limit = 10
    last_fee_asset_check_timestamp = 0
    kill_switch_check_timestamp = 0
    kill_switch_counter = 0
    kill_switch_max_balance = Decimal("0")

    if fee_tracking_enabled:
        markets = {connector_name: {maker_pair, taker_pair_1, taker_pair_2, fee_pair}}
    else:
        markets = {connector_name: {maker_pair, taker_pair_1, taker_pair_2}}

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    @property
    def timestamp_now(self):
        """Returns the current timestamp in milliseconds."""
        return int(time.time() * 1e3)

    @property
    def filename(self):
        """Generates the filename for the CSV based on the connector name and trading_pair."""
        return f"data/latency_test_tri_xemm_{self.connector_name}_{self.maker_pair}.csv"

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

        if self.fee_tracking_enabled:
            if self.current_timestamp > self.last_fee_asset_check_timestamp + self.fee_asset_check_interval:
                self.check_fee_asset()
                self.last_fee_asset_check_timestamp = self.current_timestamp
                return

        if self.kill_switch_enabled:
            if self.current_timestamp > self.kill_switch_check_timestamp + self.kill_switch_check_interval:
                self.check_kill_switch()
                self.kill_switch_check_timestamp = self.current_timestamp
                return

        # check for balances
        balance_diff_base = self.get_target_balance_diff(self.assets["maker_base"], self.target_base_amount)
        balance_diff_quote = self.get_target_balance_diff(self.assets["maker_quote"], self.target_quote_amount)

        if self.trigger_arbitrage_on_base_change:
            amount_base_quantized = self.connector.quantize_order_amount(self.taker_pair_1, abs(balance_diff_base))
        else:
            amount_base = self.get_base_amount_for_quote_volume(self.maker_pair, True, abs(balance_diff_quote))
            amount_base_quantized = self.connector.quantize_order_amount(self.taker_pair_1, amount_base)

        if amount_base_quantized > self.min_taker_order_amount:
            # Maker order is filled start arbitrage
            taker_orders = self.get_taker_order_data(balance_diff_base, balance_diff_quote)
            self.place_taker_orders(taker_orders)
            return

        if self.maker_order_filled:
            return

        # open maker orders
        self.taker_sell_price = self.calculate_taker_price(is_maker_bid=True)
        # self.log_with_clock(logging.INFO, f"self.taker_sell_price = {self.taker_sell_price }")

        self.taker_buy_price = self.calculate_taker_price(is_maker_bid=False)
        # self.log_with_clock(logging.INFO, f"self.taker_buy_price = {self.taker_buy_price}")

        if self.check_and_cancel_maker_orders():
            return
        self.place_maker_orders()

    def init_strategy(self):
        """
        Initializes strategy once before the start
        """
        self.notify_hb_app_with_timestamp("Strategy started")
        self.status = "ACTIVE"
        self.set_base_quote_assets()
        self.set_spread()
        self.set_target_amounts()

    def set_base_quote_assets(self):
        """
        """
        self.assets["maker_base"], self.assets["maker_quote"] = split_hb_trading_pair(self.maker_pair)
        self.assets["taker_1_base"], self.assets["taker_1_quote"] = split_hb_trading_pair(self.taker_pair_1)
        self.assets["taker_2_base"], self.assets["taker_2_quote"] = split_hb_trading_pair(self.taker_pair_2)

    def set_spread(self):
        self.spread = (self.min_spread + self.max_spread) / 2

    def set_target_amounts(self):
        if not self.set_target_from_config:
            self.notify_hb_app_with_timestamp(f"Setting target amounts from balances")
            self.target_base_amount = self.connector.get_balance(self.assets["maker_base"])
            self.target_quote_amount = self.connector.get_balance(self.assets["maker_quote"])
        else:
            self.notify_hb_app_with_timestamp(f"Setting target amounts from config")
            balance_diff_base = self.get_target_balance_diff(self.assets["maker_base"], self.target_base_amount)
            balance_diff_base_quantize = self.connector.quantize_order_amount(self.taker_pair_1, abs(balance_diff_base))
            if balance_diff_base_quantize != Decimal("0"):
                self.notify_hb_app_with_timestamp(f"Target balances doesn't match. Rebalance in {self.order_delay} sec")
                self.last_order_timestamp = self.current_timestamp
        msg = f"Target base amount: {self.target_base_amount} {self.assets['maker_base']}, " \
              f"Target quote amount: {self.target_quote_amount} {self.assets['maker_quote']}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def check_fee_asset(self):
        fee_asset_diff = self.get_target_balance_diff(self.fee_asset, self.fee_asset_target_amount)
        fee_asset_diff_quantize = self.connector.quantize_order_amount(self.fee_pair, abs(fee_asset_diff))

        if fee_asset_diff_quantize > self.fee_asset_min_order_amount:
            if fee_asset_diff < 0:
                order_price = self.connector.get_price(self.fee_pair, True) * Decimal(1 + self.slippage_buffer / 100)
                buy_fee_asset_candidate = OrderCandidate(trading_pair=self.fee_pair,
                                                         is_maker=True,
                                                         order_type=self.taker_order_type,
                                                         order_side=TradeType.BUY,
                                                         amount=fee_asset_diff_quantize,
                                                         price=order_price)
                place_result = self.adjust_and_place_order(candidate=buy_fee_asset_candidate, all_or_none=True)
                if place_result:
                    self.log_with_clock(logging.INFO, f"{fee_asset_diff_quantize} {self.fee_asset} "
                                                      f"on the {self.fee_pair} market was bought to adjust fees assets")
            if fee_asset_diff > 0:
                order_price = self.connector.get_price(self.fee_pair, False) * Decimal(1 - self.slippage_buffer / 100)
                sell_fee_asset_candidate = OrderCandidate(trading_pair=self.fee_pair,
                                                          is_maker=True,
                                                          order_type=self.taker_order_type,
                                                          order_side=TradeType.SELL,
                                                          amount=fee_asset_diff_quantize,
                                                          price=order_price)
                place_result = self.adjust_and_place_order(candidate=sell_fee_asset_candidate, all_or_none=True)
                if place_result:
                    self.log_with_clock(logging.INFO, f"{fee_asset_diff_quantize} {self.fee_asset} "
                                                      f"on the {self.fee_pair} market was sold to adjust fees assets")

    def check_kill_switch(self):
        kill_switch_current_balance = self.connector.get_balance(self.kill_switch_asset)
        # self.log_with_clock(logging.WARNING, f"kill_switch_current_balance = {kill_switch_current_balance},"
        #                                      f"kill_switch_max_balance = {self.kill_switch_max_balance}")
        if kill_switch_current_balance < self.kill_switch_max_balance:
            diff_pct = Decimal("100") * (kill_switch_current_balance / self.kill_switch_max_balance - Decimal("1"))
            if diff_pct < self.kill_switch_rate:
                if self.kill_switch_counter > self.kill_switch_counter_limit:
                    msg = f"!!! Kill switch threshold reached. Stop trading!"
                    self.cancel_all_orders()
                    self.notify_hb_app_with_timestamp(msg)
                    self.log_with_clock(logging.WARNING, msg)
                    self.status = "NOT_ACTIVE"
                else:
                    self.log_with_clock(logging.WARNING, f"diff_pct = {round(diff_pct, 2)}% less than "
                                                         f"{self.kill_switch_rate}%. Counter = {self.kill_switch_counter}")
                    self.kill_switch_counter += 1
        else:
            self.kill_switch_max_balance = kill_switch_current_balance
            self.kill_switch_counter = 0

    def process_hedge(self):
        if len(self.taker_candidates) > 0:
            for i, candidate in enumerate(self.taker_candidates):
                if self.current_timestamp > candidate["sent_timestamp"] + self.place_order_trials_delay:
                    if candidate["trials"] <= self.place_order_trials_limit:
                        self.log_with_clock(logging.INFO, f"Failed to place {candidate['order_candidate'].trading_pair}"
                                                          f" {candidate['order_candidate'].order_side} order. "
                                                          f"Trial number {candidate['trials']}")
                        updated_price = self.connector.get_price_for_volume(candidate['order_candidate'].trading_pair,
                                                                            candidate['order_candidate'].order_side,
                                                                            candidate[
                                                                                'order_candidate'].amount).result_price
                        if candidate['order_candidate'].order_side == TradeType.BUY:
                            candidate['order_candidate'].price = updated_price * Decimal(1 + self.slippage_buffer / 100)
                        else:
                            candidate['order_candidate'].price = updated_price * Decimal(1 - self.slippage_buffer / 100)

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
            self.finalize_arbitrage()

    def finalize_arbitrage(self):
        msg = f"--- Arbitrage round completed"
        self.notify_hb_app_with_timestamp(msg)
        self.log_with_clock(logging.WARNING, msg)
        self.status = "ACTIVE"
        self.maker_order_filled = False

    def get_target_balance_diff(self, asset, target_amount):
        current_balance = self.connector.get_balance(asset)
        amount_diff = current_balance - target_amount
        return amount_diff

    def get_taker_order_data(self, balances_diff_base, balances_diff_quote):
        taker_side_1 = False if balances_diff_base > 0 else True
        taker_amount_1 = abs(balances_diff_base)

        if self.assets["maker_quote"] == self.assets["taker_2_base"]:
            taker_side_2 = False if balances_diff_quote > 0 else True
            taker_amount_2 = abs(balances_diff_quote)
        else:
            taker_side_2 = True if balances_diff_quote > 0 else False
            taker_amount_2 = self.get_base_amount_for_quote_volume(self.taker_pair_2, taker_side_2,
                                                                   abs(balances_diff_quote))

        taker_price_1 = self.connector.get_price_for_volume(self.taker_pair_1, taker_side_1,
                                                            taker_amount_1).result_price
        taker_price_2 = self.connector.get_price_for_volume(self.taker_pair_2, taker_side_2,
                                                            taker_amount_2).result_price

        taker_orders_data = {"pair": [self.taker_pair_1, self.taker_pair_2],
                             "side": [taker_side_1, taker_side_2],
                             "amount": [taker_amount_1, taker_amount_2],
                             "price": [taker_price_1, taker_price_2]}
        self.log_with_clock(logging.INFO, f"taker_orders_data = {taker_orders_data}")
        return taker_orders_data

    def place_taker_orders(self, taker_order):
        self.status = "HEDGE_MODE"
        self.log_with_clock(logging.INFO, "Hedging mode started")
        self.last_order_timestamp = self.current_timestamp
        self.cancel_all_orders()

        for i in range(2):
            amount = self.connector.quantize_order_amount(taker_order["pair"][i], taker_order["amount"][i])
            if amount <= Decimal("0"):
                self.log_with_clock(logging.INFO, f"Can't add taker candidate {taker_order['pair'][i]} "
                                                  f"to the list. Too low amount")
                continue
            if taker_order["side"][i]:
                side = TradeType.BUY
                price = taker_order["price"][i] * Decimal(1 + self.slippage_buffer / 100)
            else:
                side = TradeType.SELL
                price = taker_order["price"][i] * Decimal(1 - self.slippage_buffer / 100)

            taker_candidate = OrderCandidate(
                trading_pair=taker_order["pair"][i],
                is_maker=False,
                order_type=self.taker_order_type,
                order_side=side,
                amount=amount,
                price=price)
            self.taker_candidates.append({"order_candidate": taker_candidate, "sent_timestamp": 0, "trials": 0})
            sent_result = self.adjust_and_place_order(candidate=taker_candidate, all_or_none=True)
            if sent_result:
                self.taker_candidates[-1]["sent_timestamp"] = self.current_timestamp
            self.log_with_clock(logging.INFO, f"New taker candidate added to the list: {self.taker_candidates[-1]}")

    def place_maker_orders(self):
        if not self.has_open_bid and self.place_bid:
            order_price = self.taker_sell_price * Decimal(1 - self.spread / 100)
            buy_candidate = OrderCandidate(trading_pair=self.maker_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=self.order_amount,
                                           price=order_price)
            self.adjust_and_place_order(candidate=buy_candidate, all_or_none=False)

        if not self.has_open_ask and self.place_ask:
            order_price = self.taker_buy_price * Decimal(1 + self.spread / 100)
            sell_candidate = OrderCandidate(
                trading_pair=self.maker_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=self.order_amount,
                price=order_price)
            self.adjust_and_place_order(candidate=sell_candidate, all_or_none=False)

    def adjust_and_place_order(self, candidate, all_or_none):
        candidate_adjusted = self.connector.budget_checker.adjust_candidate(candidate, all_or_none=all_or_none)
        if candidate_adjusted.amount == Decimal("0"):
            if candidate_adjusted.trading_pair != self.maker_pair:
                self.log_with_clock(logging.INFO,
                                    f"Order candidate amount is less than allowed on the market: "
                                    f" {candidate_adjusted.trading_pair}. Can't create"
                                    f" {candidate_adjusted.order_side.name}"
                                    f" {candidate_adjusted.order_type.name} order")
            return False
        if candidate_adjusted.trading_pair == self.maker_pair and candidate_adjusted.amount < Decimal(
                self.min_maker_order_amount):
            self.log_with_clock(logging.INFO,
                                f"Order candidate maker amount = {candidate_adjusted.amount} is less "
                                f"than min_maker_order_amount {self.min_maker_order_amount}. "
                                f"Can't create {candidate_adjusted.order_side.name}"
                                f" {candidate_adjusted.order_type.name} order")
            return False

        if candidate_adjusted.trading_pair == self.maker_pair:
            leftover_pct = self.leftover_bid_pct if candidate_adjusted.order_side == TradeType.BUY else self.leftover_ask_pct
            candidate_adjusted.amount *= Decimal("1") - leftover_pct / Decimal("100")
            candidate_adjusted.amount = self.connector.quantize_order_amount(candidate_adjusted.trading_pair, candidate_adjusted.amount)

        self.place_order(candidate_adjusted)
        return True

    def place_order(self, candidate):
        time_before_order_sent = self.timestamp_now

        if candidate.order_side == TradeType.BUY:
            order_id = self.buy(
                self.connector_name, candidate.trading_pair, candidate.amount,
                candidate.order_type, candidate.price)
        else:
            order_id = self.sell(
                self.connector_name, candidate.trading_pair, candidate.amount,
                candidate.order_type, candidate.price)

        status = OrderState.PENDING_CREATE if candidate.order_type == OrderType.LIMIT else OrderState.PENDING_EXECUTE
        self.save_to_csv(time_before_order_sent, order_id, status.name)

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.connector_name):
            self.cancel_order_by_id(self.connector_name, order.trading_pair, order.client_order_id)

    def cancel_order_by_id(self, connector, pair, order_id):
        self.save_to_csv(self.timestamp_now, order_id, OrderState.PENDING_CANCEL.name)
        self.cancel(connector, pair, order_id)

    def calculate_taker_price(self, is_maker_bid):
        if self.assets["taker_1_base"] == self.assets["taker_2_base"]:
            taker_side_1 = not is_maker_bid
            taker_side_2 = is_maker_bid
            exchanged_amount_1 = self.get_base_amount_for_quote_volume(self.taker_pair_2, taker_side_2,
                                                                       self.order_amount)
            exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(self.taker_pair_1, taker_side_1,
                                                                                 exchanged_amount_1).result_volume
        else:
            taker_side_1 = not is_maker_bid
            exchanged_amount_1 = self.connector.get_quote_volume_for_base_amount(self.taker_pair_1, taker_side_1,
                                                                                 self.order_amount).result_volume
            if self.assets["taker_1_quote"] == self.assets["taker_2_quote"]:
                taker_side_2 = not taker_side_1
                exchanged_amount_2 = self.get_base_amount_for_quote_volume(self.taker_pair_2, taker_side_2,
                                                                           exchanged_amount_1)
            else:
                taker_side_2 = taker_side_1
                exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(self.taker_pair_2, taker_side_2,
                                                                                     exchanged_amount_1).result_volume
        final_price = exchanged_amount_2 / self.order_amount
        return final_price

    def check_and_cancel_maker_orders(self):
        self.has_open_bid = False
        self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.connector_name):
            cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
            if order.is_buy:
                self.has_open_bid = True
                upper_price = self.taker_sell_price * Decimal(1 - self.min_spread / 100)
                lower_price = self.taker_sell_price * Decimal(1 - self.max_spread / 100)
                if order.price > upper_price or order.price < lower_price or self.current_timestamp > cancel_timestamp:
                    self.log_with_clock(logging.INFO, f"BUY Order {order.client_order_id} is out of price range or too old")
                    self.cancel_order_by_id(self.connector_name, order.trading_pair, order.client_order_id)
                    return True
            else:
                self.has_open_ask = True
                upper_price = self.taker_buy_price * Decimal(1 + self.max_spread / 100)
                lower_price = self.taker_buy_price * Decimal(1 + self.min_spread / 100)
                if order.price > upper_price or order.price < lower_price or self.current_timestamp > cancel_timestamp:
                    self.log_with_clock(logging.INFO, f"SELL Order {order.client_order_id} is out of price range or too old")
                    self.cancel_order_by_id(self.connector_name, order.trading_pair, order.client_order_id)
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
        if event.trading_pair == self.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CREATED.name)
        else:
            self.check_and_remove_taker_candidates(event, TradeType.BUY)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        if event.trading_pair == self.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CREATED.name)
        else:
            self.check_and_remove_taker_candidates(event, TradeType.SELL)

    def did_fill_order(self, event: OrderFilledEvent):
        if event.trading_pair == self.maker_pair:
            self.maker_order_filled = True
            self.cancel_all_orders()
        else:
            self.check_and_remove_taker_candidates(event, event.trade_type)
        msg = (f"fill {event.trade_type.name} {round(event.amount, 5)} {event.trading_pair} {self.connector_name} "
               f"at {round(event.price, 5)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        if f"{event.base_asset}-{event.quote_asset}" != self.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.EXECUTED.name)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        if f"{event.base_asset}-{event.quote_asset}" != self.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.EXECUTED.name)

    def did_cancel_order(self, event: OrderCancelledEvent):
        """Logs the post-transmission timestamp when a confirmation of order cancelled is received."""
        self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CANCELED.name)

    def check_and_remove_taker_candidates(self, filled_event, trade_type):
        candidates = self.taker_candidates.copy()
        for i, candidate in enumerate(candidates):
            if candidate["order_candidate"].trading_pair == filled_event.trading_pair \
                    and candidate["order_candidate"].order_side == trade_type:
                self.log_with_clock(logging.INFO, f"Remove order candidate {candidate}")
                self.taker_candidates.pop(i)
                break

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Min price", "Max price", "Spread", "Age"]
        data = []
        mid_price = self.connector.get_mid_price(self.maker_pair)
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                if order.is_buy:
                    upper_price = self.taker_sell_price * Decimal(1 - self.min_spread / 100)
                    lower_price = self.taker_sell_price * Decimal(1 - self.max_spread / 100)
                    spread_mid = (mid_price - order.price) / mid_price * 100
                else:
                    upper_price = self.taker_buy_price * Decimal(1 + self.max_spread / 100)
                    lower_price = self.taker_buy_price * Decimal(1 + self.min_spread / 100)
                    spread_mid = (order.price - mid_price) / mid_price * 100

                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    self.connector_name,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    float(lower_price),
                    float(upper_price),
                    float(round(spread_mid, 2)),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend([f"  Strategy status:  {self.status}"])
        lines.extend([f"  Trading pairs:    {self.maker_pair}, {self.taker_pair_1}, {self.taker_pair_2}"])
        lines.extend([f"  Target amounts:   {self.target_base_amount} {self.assets['maker_base']}, "
                      f"{self.target_quote_amount} {self.assets['maker_quote']}"])

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)

    def save_to_csv(self, timestamp, order_id, status):
        """Appends the provided data to the CSV file. If the file doesn't exist, it creates one."""
        if self.test_latency:
            file_exists = os.path.exists(self.filename)

            with open(self.filename, 'a', newline='') as csvfile:
                fieldnames = ['Timestamp', 'Order_ID', 'Status']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()

                writer.writerow({'Timestamp': timestamp, 'Order_ID': order_id, 'Status': status})