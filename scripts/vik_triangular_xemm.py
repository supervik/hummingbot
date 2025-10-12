import csv
import logging
import os
import time
from decimal import Decimal
from enum import Enum
from typing import Dict

import pandas as pd
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class OrderState(Enum):
    PENDING_CREATE = 0
    CREATED = 1
    PENDING_CANCEL = 2
    CANCELED = 3
    PENDING_EXECUTE = 4
    EXECUTED = 5


class TriangularXEMMConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    
    # ===== Exchange & Trading Pairs =====
    connector_name: str = Field("htx", client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Exchange name"))
    maker_pair: str = Field("TRX-BTC", client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Maker trading pair"))
    taker_pair_1: str = Field("TRX-USDT", client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Taker trading pair 1"))
    taker_pair_2: str = Field("BTC-USDT", client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Taker trading pair 2"))
    
    # ===== Spread Configuration =====
    min_spread: Decimal = Field(Decimal("1"), client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Minimum spread (%)"))
    max_spread: Decimal = Field(Decimal("2"), client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Maximum spread (%)"))
    
    # ===== Order Amount Configuration =====
    order_amount: Decimal = Field(Decimal("600"), client_data=ClientFieldData(
        prompt_on_new=True, prompt=lambda mi: "Order amount"))
    min_maker_order_amount: Decimal = Field(Decimal("400"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Minimum maker order amount"))
    min_taker_order_amount: Decimal = Field(Decimal("100"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Minimum taker order amount"))
    leftover_bid_pct: Decimal = Field(Decimal("0"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Leftover bid percentage"))
    leftover_ask_pct: Decimal = Field(Decimal("0"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Leftover ask percentage"))
    
    # ===== Rebalancing Configuration =====
    trigger_arbitrage_on_base_change: bool = Field(True, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Trigger arbitrage on base change"))
    set_target_from_config: bool = Field(True, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Set target amounts from config"))
    target_base_amount: Decimal = Field(Decimal("600"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Target base amount"))
    target_quote_amount: Decimal = Field(Decimal("0.0015"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Target quote amount"))
    place_bid: bool = Field(True, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Place bid orders"))
    place_ask: bool = Field(True, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Place ask orders"))
    
    # ===== Fee Tracking Configuration =====
    fee_tracking_enabled: bool = Field(False, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Enable fee tracking"))
    fee_asset: str = Field("KCS", client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Fee asset"))
    fee_asset_target_amount: Decimal = Field(Decimal("2"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Fee asset target amount"))
    fee_asset_min_order_amount: Decimal = Field(Decimal("0.2"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Fee asset minimum order amount"))
    fee_pair: str = Field("KCS-USDT", client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Fee trading pair"))
    fee_asset_check_interval: int = Field(300, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Fee asset check interval (seconds)"))
    
    # ===== Kill Switch Configuration =====
    kill_switch_enabled: bool = Field(True, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Enable kill switch"))
    kill_switch_asset: str = Field("USDT", client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Kill switch asset"))
    kill_switch_rate: Decimal = Field(Decimal("-3"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Kill switch rate (%)"))
    kill_switch_check_interval: int = Field(60, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Kill switch check interval (seconds)"))
    kill_switch_counter_limit: int = Field(5, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Kill switch counter limit"))
    kill_switch_arb_max_loss_pct: Decimal = Field(Decimal("-0.2"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Kill switch max loss percentage"))
    
    # ===== Order Management Configuration =====
    order_delay: int = Field(15, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Order delay (seconds)"))
    slippage_buffer: Decimal = Field(Decimal("1"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Slippage buffer (%)"))
    slippage_buffer_third_asset: Decimal = Field(Decimal("1.2"), client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Slippage buffer third asset (%)"))
    taker_order_type: OrderType = Field(OrderType.MARKET, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Taker order type"))
    test_latency: bool = Field(False, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Test latency"))
    max_order_age: int = Field(1800, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Max order age (seconds)"))
    place_maker_delay: int = Field(5, client_data=ClientFieldData(
        prompt_on_new=False, prompt=lambda mi: "Place maker delay (seconds)"))


class TriangularXEMM(ScriptStrategyBase):
    """
    The script that performs triangular XEMM on a single exchange.
    The script based on balances check. If it finds the imbalance it opens 2 other orders
    The script has kill_switch and fee asset check and rebalance
    """

    # State variables
    status: str = "NOT_INIT"
    taker_sell_price = 0
    taker_buy_price = 0
    spread = 0
    assets = {}

    open_maker_bid_id = None
    open_maker_ask_id = None
    maker_order_filled = False
    taker1_order_filled = False
    taker2_order_filled = False
    maker_filled_order_price = 0
    taker_1_filled_order_price = 0
    taker_2_filled_order_price = 0
    maker_filled_order_side = None
    taker_candidates: list = []
    last_order_timestamp = 0
    maker_filled_timestamp = 0
    place_order_trials_delay = 10
    place_order_trials_limit = 10
    last_fee_asset_check_timestamp = 0
    kill_switch_check_timestamp = 0
    kill_switch_counter = 0
    kill_switch_max_balance = Decimal("0")
    place_maker_delay_timestamp = 0

    @classmethod
    def init_markets(cls, config: TriangularXEMMConfig):
        if config.fee_tracking_enabled:
            cls.markets = {config.connector_name: {config.maker_pair, config.taker_pair_1, config.taker_pair_2, config.fee_pair}}
        else:
            cls.markets = {config.connector_name: {config.maker_pair, config.taker_pair_1, config.taker_pair_2}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: TriangularXEMMConfig):
        super().__init__(connectors)
        self.config = config

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.config.connector_name]

    @property
    def timestamp_now(self):
        """Returns the current timestamp in milliseconds."""
        return int(time.time() * 1e3)

    @property
    def filename(self):
        """Generates the filename for the CSV based on the connector name and trading_pair."""
        return f"data/latency_test_tri_xemm_{self.config.connector_name}_{self.config.maker_pair}.csv"

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

        if self.current_timestamp < self.last_order_timestamp + self.config.order_delay:
            return

        if self.config.fee_tracking_enabled:
            if self.current_timestamp > self.last_fee_asset_check_timestamp + self.config.fee_asset_check_interval:
                self.check_fee_asset()
                self.last_fee_asset_check_timestamp = self.current_timestamp
                return

        if self.config.kill_switch_enabled:
            if self.current_timestamp > self.kill_switch_check_timestamp + self.config.kill_switch_check_interval:
                self.check_kill_switch()
                self.kill_switch_check_timestamp = self.current_timestamp
                return

        # check for balances
        balance_diff_base = self.get_target_balance_diff(self.assets["maker_base"], self.config.target_base_amount)
        balance_diff_quote = self.get_target_balance_diff(self.assets["maker_quote"], self.config.target_quote_amount)

        if self.config.trigger_arbitrage_on_base_change:
            amount_base_quantized = self.connector.quantize_order_amount(self.config.taker_pair_1, abs(balance_diff_base))
        else:
            amount_base = self.get_base_amount_for_quote_volume(self.config.maker_pair, True, abs(balance_diff_quote))
            amount_base_quantized = self.connector.quantize_order_amount(self.config.taker_pair_1, amount_base)

        if amount_base_quantized > self.config.min_taker_order_amount:
            # Maker order is filled start arbitrage
            self.log_with_clock(logging.INFO, "<< Hedging mode started! >>")
            taker_orders = self.get_taker_order_data(balance_diff_base, balance_diff_quote)
            self.place_taker_orders(taker_orders)
            self.cancel_all_orders()
            return

        if self.maker_order_filled:
            if self.current_timestamp > self.maker_filled_timestamp + self.config.order_delay:
                self.maker_order_filled = False
            return

        # open maker orders
        self.taker_sell_price = self.calculate_taker_price(is_maker_bid=True)
        # self.log_with_clock(logging.INFO, f"self.taker_sell_price = {self.taker_sell_price }")

        self.taker_buy_price = self.calculate_taker_price(is_maker_bid=False)
        # self.log_with_clock(logging.INFO, f"self.taker_buy_price = {self.taker_buy_price}")

        if self.check_and_cancel_maker_orders():
            return

        if self.current_timestamp < self.place_maker_delay_timestamp:
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
        self.assets["maker_base"], self.assets["maker_quote"] = split_hb_trading_pair(self.config.maker_pair)
        self.assets["taker_1_base"], self.assets["taker_1_quote"] = split_hb_trading_pair(self.config.taker_pair_1)
        self.assets["taker_2_base"], self.assets["taker_2_quote"] = split_hb_trading_pair(self.config.taker_pair_2)

    def set_spread(self):
        self.spread = (self.config.min_spread + self.config.max_spread) / 2

    def set_target_amounts(self):
        if not self.config.set_target_from_config:
            self.notify_hb_app_with_timestamp(f"Setting target amounts from balances")
            self.config.target_base_amount = self.connector.get_balance(self.assets["maker_base"])
            self.config.target_quote_amount = self.connector.get_balance(self.assets["maker_quote"])
        else:
            self.notify_hb_app_with_timestamp(f"Setting target amounts from config")
            balance_diff_base = self.get_target_balance_diff(self.assets["maker_base"], self.config.target_base_amount)
            balance_diff_base_quantize = self.connector.quantize_order_amount(self.config.taker_pair_1, abs(balance_diff_base))
            if balance_diff_base_quantize != Decimal("0"):
                self.notify_hb_app_with_timestamp(f"Target balances don't match. Rebalance in {self.config.order_delay} sec")
                self.last_order_timestamp = self.current_timestamp
        msg = f"Target base amount: {self.config.target_base_amount} {self.assets['maker_base']}, " \
              f"Target quote amount: {self.config.target_quote_amount} {self.assets['maker_quote']}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def check_fee_asset(self):
        fee_asset_diff = self.get_target_balance_diff(self.config.fee_asset, self.config.fee_asset_target_amount)
        fee_asset_diff_quantize = self.connector.quantize_order_amount(self.config.fee_pair, abs(fee_asset_diff))

        if fee_asset_diff_quantize > self.config.fee_asset_min_order_amount:
            if fee_asset_diff < 0:
                order_price = self.connector.get_price(self.config.fee_pair, True) * Decimal(1 + self.config.slippage_buffer / 100)
                buy_fee_asset_candidate = OrderCandidate(trading_pair=self.config.fee_pair,
                                                         is_maker=True,
                                                         order_type=self.config.taker_order_type,
                                                         order_side=TradeType.BUY,
                                                         amount=fee_asset_diff_quantize,
                                                         price=order_price)
                place_result = self.adjust_and_place_order(candidate=buy_fee_asset_candidate, all_or_none=True)
                if place_result:
                    self.log_with_clock(logging.INFO, f"{fee_asset_diff_quantize} {self.config.fee_asset} "
                                                      f"on the {self.config.fee_pair} market was bought to adjust fees assets")
            if fee_asset_diff > 0:
                order_price = self.connector.get_price(self.config.fee_pair, False) * Decimal(1 - self.config.slippage_buffer / 100)
                sell_fee_asset_candidate = OrderCandidate(trading_pair=self.config.fee_pair,
                                                          is_maker=True,
                                                          order_type=self.config.taker_order_type,
                                                          order_side=TradeType.SELL,
                                                          amount=fee_asset_diff_quantize,
                                                          price=order_price)
                place_result = self.adjust_and_place_order(candidate=sell_fee_asset_candidate, all_or_none=True)
                if place_result:
                    self.log_with_clock(logging.INFO, f"{fee_asset_diff_quantize} {self.config.fee_asset} "
                                                      f"on the {self.config.fee_pair} market was sold to adjust fees assets")

    def check_kill_switch(self):
        kill_switch_current_balance = self.connector.get_balance(self.config.kill_switch_asset)
        # self.log_with_clock(logging.WARNING, f"kill_switch_current_balance = {kill_switch_current_balance},"
        #                                      f"kill_switch_max_balance = {self.kill_switch_max_balance}")
        if kill_switch_current_balance < self.kill_switch_max_balance:
            diff_pct = Decimal("100") * (kill_switch_current_balance / self.kill_switch_max_balance - Decimal("1"))
            if diff_pct < self.config.kill_switch_rate:
                if self.kill_switch_counter > self.config.kill_switch_counter_limit:
                    msg = f"!!! Kill switch threshold reached. Stop trading!"
                    self.cancel_all_orders()
                    self.notify_hb_app_with_timestamp(msg)
                    self.log_with_clock(logging.WARNING, msg)
                    self.status = "NOT_ACTIVE"
                else:
                    self.log_with_clock(logging.WARNING, f"diff_pct = {round(diff_pct, 2)}% less than "
                                                         f"{self.config.kill_switch_rate}%. Counter = {self.kill_switch_counter}")
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
                            candidate['order_candidate'].price = updated_price * Decimal(1 + self.config.slippage_buffer / 100)
                        else:
                            candidate['order_candidate'].price = updated_price * Decimal(1 - self.config.slippage_buffer / 100)

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
        elif self.taker1_order_filled and self.taker2_order_filled:
            self.finalize_arbitrage()

    def finalize_arbitrage(self):
        profit = self.calculate_arb_profit()
        msg = f"--- Arbitrage round completed. Profit: {profit}%"
        self.notify_hb_app_with_timestamp(msg)
        self.log_with_clock(logging.WARNING, msg)

        if profit < self.config.kill_switch_arb_max_loss_pct:
            msg = f"Arbitrage profit is less than {self.config.kill_switch_arb_max_loss_pct}%. Stop trading"
            self.log_with_clock(logging.WARNING, msg)
            self.notify_hb_app_with_timestamp(msg)
            self.status = "NOT_ACTIVE"
            return

        self.status = "ACTIVE"
        self.maker_order_filled = False
        self.taker1_order_filled = False
        self.taker2_order_filled = False
        self.open_maker_bid_id = None
        self.open_maker_ask_id = None
        self.maker_filled_order_price = 0
        self.taker_1_filled_order_price = 0
        self.taker_2_filled_order_price = 0

    def calculate_arb_profit(self):
        if self.assets["taker_1_quote"] == self.assets["taker_2_quote"]:
            open_price = self.maker_filled_order_price / self.taker_1_filled_order_price
        else:
            open_price = self.taker_1_filled_order_price / self.maker_filled_order_price

        close_price = self.taker_2_filled_order_price
        if self.maker_filled_order_side == TradeType.BUY:
            buy_price = open_price
            sell_price = close_price
        else:
            buy_price = close_price
            sell_price = open_price

        profit = Decimal("100") * (sell_price - buy_price) / buy_price 
        return profit

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
            taker_amount_2 = self.get_base_amount_for_quote_volume(self.config.taker_pair_2, taker_side_2,
                                                                   abs(balances_diff_quote))

        taker_price_1 = self.connector.get_price_for_volume(self.config.taker_pair_1, taker_side_1,
                                                            taker_amount_1).result_price
        taker_price_2 = self.connector.get_price_for_volume(self.config.taker_pair_2, taker_side_2,
                                                            taker_amount_2).result_price

        taker_orders_data = {"pair": [self.config.taker_pair_1, self.config.taker_pair_2],
                             "side": [taker_side_1, taker_side_2],
                             "amount": [taker_amount_1, taker_amount_2],
                             "price": [taker_price_1, taker_price_2]}
        self.log_with_clock(logging.INFO, f"taker_orders_data = {taker_orders_data}")
        return taker_orders_data

    def place_taker_orders(self, taker_order):
        self.status = "HEDGE_MODE"
        self.last_order_timestamp = self.current_timestamp

        for i in range(2):
            amount = self.connector.quantize_order_amount(taker_order["pair"][i], taker_order["amount"][i])
            if amount <= Decimal("0"):
                self.log_with_clock(logging.INFO, f"Can't add taker candidate {taker_order['pair'][i]} "
                                                  f"to the list. Too low amount")
                continue
            if taker_order["side"][i]:
                side = TradeType.BUY
                price = taker_order["price"][i] * Decimal(1 + self.config.slippage_buffer / 100)
            else:
                side = TradeType.SELL
                price = taker_order["price"][i] * Decimal(1 - self.config.slippage_buffer / 100)

            taker_candidate = OrderCandidate(
                trading_pair=taker_order["pair"][i],
                is_maker=False,
                order_type=self.config.taker_order_type,
                order_side=side,
                amount=amount,
                price=price)
            self.taker_candidates.append({"order_candidate": taker_candidate, "sent_timestamp": 0, "trials": 0})
            sent_result = self.adjust_and_place_order(candidate=taker_candidate, all_or_none=True)
            if sent_result:
                self.taker_candidates[-1]["sent_timestamp"] = self.current_timestamp
            self.log_with_clock(logging.INFO, f"New taker candidate added to the list: {self.taker_candidates[-1]}")

    def place_maker_orders(self):
        if not self.open_maker_bid_id and self.config.place_bid:
            order_price = self.taker_sell_price * Decimal(1 - self.spread / 100)
            amount = self.get_order_amount_considering_third_asset_balance()
            buy_candidate = OrderCandidate(trading_pair=self.config.maker_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=amount,
                                           price=order_price)
            maker_buy_result = self.adjust_and_place_order(candidate=buy_candidate, all_or_none=False)
            if maker_buy_result:
                self.open_maker_bid_id = maker_buy_result

        if not self.open_maker_ask_id and self.config.place_ask:
            order_price = self.taker_buy_price * Decimal(1 + self.spread / 100)
            amount = self.get_order_amount_considering_third_asset_balance()
            sell_candidate = OrderCandidate(
                trading_pair=self.config.maker_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=amount,
                price=order_price)
            maker_sell_result = self.adjust_and_place_order(candidate=sell_candidate, all_or_none=False)
            if maker_sell_result:
                self.open_maker_ask_id = maker_sell_result

    def get_order_amount_considering_third_asset_balance(self):
        third_asset = self.assets["taker_1_quote"]
        third_asset_balance = self.connector.get_balance(third_asset)
        base_amount_in_third_asset = self.get_base_amount_for_quote_volume(self.config.taker_pair_1, True, third_asset_balance)
        base_amount_in_third_asset *= Decimal(1 - self.config.slippage_buffer_third_asset / 100)
        amount = min(self.config.order_amount, base_amount_in_third_asset)
        return amount

    def adjust_and_place_order(self, candidate, all_or_none):
        candidate_adjusted = self.connector.budget_checker.adjust_candidate(candidate, all_or_none=all_or_none)
        if candidate_adjusted.amount == Decimal("0"):
            if candidate_adjusted.trading_pair != self.config.maker_pair:
                self.log_with_clock(logging.INFO,
                                    f"Order candidate amount is less than allowed on the market: "
                                    f" {candidate_adjusted.trading_pair}. Can't create"
                                    f" {candidate_adjusted.order_side.name}"
                                    f" {candidate_adjusted.order_type.name} order")
            return False
        if candidate_adjusted.trading_pair == self.config.maker_pair and candidate_adjusted.amount < Decimal(
                self.config.min_maker_order_amount):
            self.log_with_clock(logging.INFO,
                                f"Order candidate maker amount = {candidate_adjusted.amount} is less "
                                f"than min_maker_order_amount {self.config.min_maker_order_amount}. "
                                f"Can't create {candidate_adjusted.order_side.name}"
                                f" {candidate_adjusted.order_type.name} order")
            return False

        if candidate_adjusted.trading_pair == self.config.maker_pair:
            leftover_pct = self.config.leftover_bid_pct if candidate_adjusted.order_side == TradeType.BUY else self.config.leftover_ask_pct
            candidate_adjusted.amount *= Decimal("1") - leftover_pct / Decimal("100")
            candidate_adjusted.amount = self.connector.quantize_order_amount(candidate_adjusted.trading_pair, candidate_adjusted.amount)

        order_id = self.place_order(candidate_adjusted)
        return order_id

    def place_order(self, candidate):
        time_before_order_sent = self.timestamp_now

        if candidate.order_side == TradeType.BUY:
            order_id = self.buy(
                self.config.connector_name, candidate.trading_pair, candidate.amount,
                candidate.order_type, candidate.price)
        else:
            order_id = self.sell(
                self.config.connector_name, candidate.trading_pair, candidate.amount,
                candidate.order_type, candidate.price)

        status = OrderState.PENDING_CREATE if candidate.trading_pair == self.config.maker_pair else OrderState.PENDING_EXECUTE
        self.save_to_csv(time_before_order_sent, order_id, status.name)
        return order_id

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.config.connector_name):
            self.cancel_order_by_id(self.config.connector_name, order.trading_pair, order.client_order_id)

    def cancel_order_by_id(self, connector, pair, order_id):
        self.save_to_csv(self.timestamp_now, order_id, OrderState.PENDING_CANCEL.name)
        self.cancel(connector, pair, order_id)

    def calculate_taker_price(self, is_maker_bid):
        if self.assets["taker_1_base"] == self.assets["taker_2_base"]:
            taker_side_1 = not is_maker_bid
            taker_side_2 = is_maker_bid
            exchanged_amount_1 = self.get_base_amount_for_quote_volume(self.config.taker_pair_2, taker_side_2,
                                                                       self.config.order_amount)
            exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(self.config.taker_pair_1, taker_side_1,
                                                                                 exchanged_amount_1).result_volume
        else:
            taker_side_1 = not is_maker_bid
            exchanged_amount_1 = self.connector.get_quote_volume_for_base_amount(self.config.taker_pair_1, taker_side_1,
                                                                                 self.config.order_amount).result_volume
            if self.assets["taker_1_quote"] == self.assets["taker_2_quote"]:
                taker_side_2 = not taker_side_1
                exchanged_amount_2 = self.get_base_amount_for_quote_volume(self.config.taker_pair_2, taker_side_2,
                                                                           exchanged_amount_1)
            else:
                taker_side_2 = taker_side_1
                exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(self.config.taker_pair_2, taker_side_2,
                                                                                     exchanged_amount_1).result_volume
        final_price = exchanged_amount_2 / self.config.order_amount
        return final_price

    def check_and_cancel_maker_orders(self):
        # self.has_open_bid = False
        # self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.config.connector_name):
            cancel_timestamp = order.creation_timestamp / 1000000 + self.config.max_order_age
            if order.is_buy:
                # self.has_open_bid = True
                upper_price = self.taker_sell_price * Decimal(1 - self.config.min_spread / 100)
                lower_price = self.taker_sell_price * Decimal(1 - self.config.max_spread / 100)
                if order.price > upper_price or order.price < lower_price or self.current_timestamp > cancel_timestamp:
                    self.log_with_clock(logging.INFO, f"BUY Order {order.client_order_id} is out of price range or too old")
                    self.cancel_order_by_id(self.config.connector_name, order.trading_pair, order.client_order_id)
                    return True
            else:
                # self.has_open_ask = True
                upper_price = self.taker_buy_price * Decimal(1 + self.config.max_spread / 100)
                lower_price = self.taker_buy_price * Decimal(1 + self.config.min_spread / 100)
                if order.price > upper_price or order.price < lower_price or self.current_timestamp > cancel_timestamp:
                    self.log_with_clock(logging.INFO, f"SELL Order {order.client_order_id} is out of price range or too old")
                    self.cancel_order_by_id(self.config.connector_name, order.trading_pair, order.client_order_id)
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

    def did_fail_order(self, event: MarketOrderFailureEvent):
        self.log_with_clock(logging.INFO, f"Order {event.order_id} was failed to be placed")
        self.place_maker_delay_timestamp = self.current_timestamp + self.config.place_maker_delay
        if event.order_id == self.open_maker_bid_id:
            self.open_maker_bid_id = None
        elif event.order_id == self.open_maker_ask_id:
            self.open_maker_ask_id = None

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        if event.trading_pair == self.config.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CREATED.name)
        else:
            self.check_and_remove_taker_candidates(event, TradeType.BUY)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        if event.trading_pair == self.config.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CREATED.name)
        else:
            self.check_and_remove_taker_candidates(event, TradeType.SELL)

    def did_fill_order(self, event: OrderFilledEvent):
        if event.trading_pair == self.config.maker_pair:
            self.maker_order_filled = True
            self.maker_filled_timestamp = self.current_timestamp
            self.maker_filled_order_price = event.price
            self.maker_filled_order_side = event.trade_type
            if self.status != "HEDGE_MODE":
                self.cancel_all_orders()
        else:
            if event.trading_pair == self.config.taker_pair_1:
                self.taker1_order_filled = True
                self.taker_1_filled_order_price = event.price
            if event.trading_pair == self.config.taker_pair_2:
                self.taker2_order_filled = True
                self.taker_2_filled_order_price = event.price
            self.check_and_remove_taker_candidates(event, event.trade_type)
        msg = (f"fill {event.trade_type.name} {round(event.amount, 5)} {event.trading_pair} {self.config.connector_name} "
               f"at {round(event.price, 5)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        if f"{event.base_asset}-{event.quote_asset}" != self.config.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.EXECUTED.name)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        if f"{event.base_asset}-{event.quote_asset}" != self.config.maker_pair:
            self.save_to_csv(self.timestamp_now, event.order_id, OrderState.EXECUTED.name)

    def did_cancel_order(self, event: OrderCancelledEvent):
        """Logs the post-transmission timestamp when a confirmation of order cancelled is received."""
        self.save_to_csv(self.timestamp_now, event.order_id, OrderState.CANCELED.name)
        self.log_with_clock(logging.INFO, f"Order {event.order_id} was cancelled. bid_order_id = {self.open_maker_bid_id} ask_order_id = {self.open_maker_ask_id}")
        if event.order_id == self.open_maker_bid_id:
            self.log_with_clock(logging.INFO, f"Maker bid order {event.order_id} was cancelled")
            self.open_maker_bid_id = None
        elif event.order_id == self.open_maker_ask_id:
            self.log_with_clock(logging.INFO, f"Maker ask order {event.order_id} was cancelled")
            self.open_maker_ask_id = None

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
        mid_price = self.connector.get_mid_price(self.config.maker_pair)
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                if order.is_buy:
                    upper_price = self.taker_sell_price * Decimal(1 - self.config.min_spread / 100)
                    lower_price = self.taker_sell_price * Decimal(1 - self.config.max_spread / 100)
                    spread_mid = (mid_price - order.price) / mid_price * 100
                else:
                    upper_price = self.taker_buy_price * Decimal(1 + self.config.max_spread / 100)
                    lower_price = self.taker_buy_price * Decimal(1 + self.config.min_spread / 100)
                    spread_mid = (order.price - mid_price) / mid_price * 100

                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    self.config.connector_name,
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
        lines.extend([f"  Trading pairs:    {self.config.maker_pair}, {self.config.taker_pair_1}, {self.config.taker_pair_2}"])
        lines.extend([f"  Target amounts:   {self.config.target_base_amount} {self.assets['maker_base']}, "
                      f"{self.config.target_quote_amount} {self.assets['maker_quote']}"])

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
        if self.config.test_latency:
            file_exists = os.path.exists(self.filename)

            with open(self.filename, 'a', newline='') as csvfile:
                fieldnames = ['Timestamp', 'Order_ID', 'Status']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()

                writer.writerow({'Timestamp': timestamp, 'Order_ID': order_id, 'Status': status})