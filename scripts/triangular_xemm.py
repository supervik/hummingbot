import logging

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class TriangularXEMM(ScriptStrategyBase):
    # Config params
    connector_name: str = "kucoin"
    maker_pair: str = "ADA-BTC"
    taker_pair_1: str = "ADA-USDT"
    taker_pair_2: str = "BTC-USDT"

    min_spread: Decimal = Decimal("0.8")
    max_spread: Decimal = Decimal("1.2")

    order_amount: Decimal = Decimal("100")
    set_target_from_balances = True
    target_base_amount = Decimal("3")
    target_quote_amount = Decimal("0.4")
    order_delay = 60
    min_order_amount = Decimal("20")
    slippage_buffer = Decimal("1")

    fee_asset = "KCS"
    fee_asset_target_amount = 2
    fee_pair = "KCS-USDT"
    fee_asset_check_interval = 300

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
    assets = {}

    has_open_bid = False
    has_open_ask = False
    taker_candidates: list = []
    last_order_timestamp = 0
    place_order_trials_delay = 5
    place_order_trials_limit = 10
    last_fee_asset_check_timestamp = 0

    markets = {connector_name: {maker_pair, taker_pair_1, taker_pair_2, fee_pair}}

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

        if self.current_timestamp > self.last_fee_asset_check_timestamp + self.fee_asset_check_interval:
            self.check_fee_asset()
            self.last_fee_asset_check_timestamp = self.current_timestamp
            return

        # check for balances
        balance_diff_base = self.get_target_balance_diff(self.assets["maker_base"], self.target_base_amount)
        balance_diff_base_quantize = self.connector.quantize_order_amount(self.taker_pair_1, abs(balance_diff_base))

        if balance_diff_base_quantize != Decimal("0"):
            balance_diff_quote = self.get_target_balance_diff(self.assets["maker_quote"], self.target_quote_amount)
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
        if self.set_target_from_balances:
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

        if fee_asset_diff_quantize != Decimal("0"):
            if fee_asset_diff < 0:
                order_price = self.connector.get_price(self.fee_pair, True) * Decimal(1 + self.slippage_buffer / 100)
                buy_fee_asset_candidate = OrderCandidate(trading_pair=self.fee_pair,
                                                         is_maker=True,
                                                         order_type=OrderType.LIMIT,
                                                         order_side=TradeType.BUY,
                                                         amount=fee_asset_diff_quantize,
                                                         price=order_price)
            place_result = self.adjust_and_place_order(candidate=buy_fee_asset_candidate, all_or_none=True)
            if place_result:
                self.log_with_clock(logging.INFO, f"{fee_asset_diff_quantize} {self.fee_asset} "
                                                  f"on the {self.fee_pair} market was bought to adjust fees assets")

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
            msg = "Arbitrage round completed"
            self.notify_hb_app_with_timestamp(msg)
            self.log_with_clock(logging.WARNING, msg)
            self.status = "ACTIVE"

    def get_target_balance_diff(self, asset, target_amount):
        current_balance = self.connector.get_balance(asset)
        amount_diff = current_balance - target_amount
        # self.log_with_clock(logging.INFO, f"Current balance {asset}: {current_balance}, "
        #                                   f"Target balance: {target_amount}, "
        #                                   f"Amount_diff: {amount_diff}")
        return amount_diff

    def get_taker_order_data(self, is_maker_bid, balances_diff_base, balances_diff_quote):
        taker_side_1 = not is_maker_bid
        taker_amount_1 = balances_diff_base
        taker_price_1 = self.connector.get_price_for_volume(self.taker_pair_1, taker_side_1,
                                                            taker_amount_1).result_price

        if self.assets["taker_1_quote"] == self.assets["taker_2_quote"]:
            taker_side_2 = True if is_maker_bid else False
            taker_amount_2 = balances_diff_quote
        else:
            taker_side_2 = False if is_maker_bid else True
            taker_amount_2 = self.get_base_amount_for_quote_volume(self.taker_pair_2, False, balances_diff_quote)

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
                self.log_with_clock(logging.INFO, f"Can't add taker candidate {side} {taker_order['pair'][i]} "
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
                order_type=OrderType.LIMIT,
                order_side=side,
                amount=amount,
                price=price)
            self.taker_candidates.append({"order_candidate": taker_candidate, "sent_timestamp": 0, "trials": 0})
            sent_result = self.adjust_and_place_order(candidate=taker_candidate, all_or_none=True)
            if sent_result:
                self.taker_candidates[-1]["sent_timestamp"] = self.current_timestamp
            self.log_with_clock(logging.INFO, f"New taker candidate added to the list: {self.taker_candidates[-1]}")

    def place_maker_orders(self):
        if not self.has_open_bid:
            order_price = self.taker_sell_price * Decimal(1 - self.spread / 100)
            buy_candidate = OrderCandidate(trading_pair=self.maker_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=self.order_amount,
                                           price=order_price)
            place_result = self.adjust_and_place_order(candidate=buy_candidate, all_or_none=False)
            if place_result:
                self.log_with_clock(logging.INFO, "Placed maker BUY order")

        if not self.has_open_ask:
            order_price = self.taker_buy_price * Decimal(1 + self.spread / 100)
            sell_candidate = OrderCandidate(
                trading_pair=self.maker_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=self.order_amount,
                price=order_price)
            place_result = self.adjust_and_place_order(candidate=sell_candidate, all_or_none=False)
            if place_result:
                self.log_with_clock(logging.INFO, "Placed maker SELL order")
            # sell_candidate_adjusted = self.connector.budget_checker.adjust_candidate(sell_candidate, all_or_none=False)
            # if sell_candidate_adjusted.amount > Decimal("0"):
            #     self.place_order(sell_candidate_adjusted)
            # else:
            #     self.log_with_clock(logging.INFO, f"SELL amount is less than allowed on the maker market"
            #                                       f"{self.maker_pair} Can't place order.")

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
                self.min_order_amount):
            self.log_with_clock(logging.INFO,
                                f"Order candidate maker amount = {candidate_adjusted.amount} is less "
                                f"than min_order_amount {self.min_order_amount}. "
                                f"Can't create {candidate_adjusted.order_side.name}"
                                f" {candidate_adjusted.order_type.name} order")
            return False

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
        exchanged_amount_1 = self.connector.get_quote_volume_for_base_amount(self.taker_pair_1, side_taker_1,
                                                                             self.order_amount).result_volume
        if self.assets["taker_1_quote"] == self.assets["taker_2_quote"]:
            side_taker_2 = not side_taker_1
            exchanged_amount_2 = self.get_base_amount_for_quote_volume(self.taker_pair_2, side_taker_2,
                                                                       exchanged_amount_1)
        else:
            side_taker_2 = side_taker_1
            exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(self.taker_pair_2, side_taker_2,
                                                                                 exchanged_amount_1).result_volume
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

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])
        lines.extend(["", "  Target amounts:"] + ["    " + f"{self.target_base_amount} {self.assets['maker_base']} "
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
