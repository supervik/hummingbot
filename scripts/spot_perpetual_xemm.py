import logging

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType, PositionAction
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class SpotPerpetualXEMM(ScriptStrategyBase):
    # Config params
    spot_connector_name: str = "kucoin"
    perp_connector_name: str = "binance_perpetual"
    spot_pair: str = "RLC-USDT"
    perp_pair: str = "RLC-USDT"

    spot_order_amount = Decimal("10")
    perp_order_amount = Decimal("10")
    min_spread = Decimal("0.3")
    start_spread = Decimal("0.5")
    max_spread = Decimal("0.7")

    target_spot_base_amount = Decimal("0")

    slippage_buffer = Decimal("1")
    leverage = Decimal("20")

    # class parameters
    status = "NOT_INIT"
    last_spot_base_amount = 0
    has_open_bid = False
    has_open_ask = False
    perp_order_sell_price = 0
    perp_breakeven_price_diff = 0
    spot_base_asset = ""
    spot_quote_asset = ""
    perp_base_asset = ""
    perp_quote_asset = ""
    upper_price = 0
    lower_price = 0
    place_bid = True
    place_ask = True
    maker_order_open_price = 0
    filled_spot_order_amount = 0
    perp_last_order_timestamp = 0
    next_spot_order_delay = 60
    taker_candidate = {}

    markets = {spot_connector_name: {spot_pair},
               perp_connector_name: {perp_pair}}

    @property
    def spot_connector(self):
        """
        The spot connector in this strategy, define it here for easy access
        """
        return self.connectors[self.spot_connector_name]

    @property
    def perp_connector(self):
        """
        The perpetuals connector in this strategy, define it here for easy access
        """
        return self.connectors[self.perp_connector_name]

    def init_strategy(self):
        """
        Initializes strategy once before the start
        """
        self.notify_hb_app_with_timestamp("Strategy started")
        self.set_base_quote_assets()
        self.status = "OPEN"

    def set_base_quote_assets(self):
        """
        """
        self.spot_base_asset, self.spot_quote_asset = split_hb_trading_pair(self.spot_pair)
        self.perp_base_asset, self.perp_quote_asset = split_hb_trading_pair(self.perp_pair)

    def on_tick(self):
        """
        """
        if self.status == "NOT_INIT":
            self.init_strategy()

        if self.status == "NOT_ACTIVE":
            return

        if self.status == "OPEN":
            if self.check_filled_orders(prev_balance=self.target_spot_base_amount):
                self.log_with_clock(logging.INFO,
                                    f"Spot order BUY was filled. Amount = {self.filled_spot_order_amount}. "
                                    f"Start hedging on perpetuals! Last hedge price = {self.perp_order_sell_price}")
                self.cancel_all_orders()
                self.status = "OPEN_HEDGE"
            else:
                if self.current_timestamp > self.perp_last_order_timestamp + self.next_spot_order_delay:
                    self.manage_spot_limit_order(is_buy=True)

        if self.status == "OPEN_HEDGE":
            self.open_perp_hedging_order(is_long=False)
            self.status = "CLOSE"
            self.perp_last_order_timestamp = self.current_timestamp

        if self.status == "CLOSE":
            if self.check_filled_orders(prev_balance=self.last_spot_base_amount):
                self.log_with_clock(logging.INFO,
                                    f"Spot order SELL was filled. Amount = {self.filled_spot_order_amount}. "
                                    f"Start hedging on perpetuals!")
                self.cancel_all_orders()
                self.status = "CLOSE_HEDGE"
            else:
                if self.current_timestamp > self.perp_last_order_timestamp + self.next_spot_order_delay:
                    self.manage_spot_limit_order(is_buy=False)

        if self.status == "CLOSE_HEDGE":
            self.open_perp_hedging_order(is_long=True)
            self.status = "OPEN"
            self.perp_last_order_timestamp = self.current_timestamp

    def check_filled_orders(self, prev_balance):
        current_balance = self.spot_connector.get_balance(self.spot_base_asset)
        amount_diff = current_balance - prev_balance
        balance_diff_base_quantize = self.perp_connector.quantize_order_amount(self.perp_pair, abs(amount_diff))
        if balance_diff_base_quantize != Decimal("0"):
            self.last_spot_base_amount = current_balance
            self.filled_spot_order_amount = balance_diff_base_quantize
            return True
        return False

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.spot_connector_name):
            self.cancel(self.spot_connector_name, order.trading_pair, order.client_order_id)

    def manage_spot_limit_order(self, is_buy):
        if is_buy:
            self.calculate_perp_order_sell_price()
        else:
            self.calculate_perp_profit()

        if self.check_and_cancel_maker_orders(is_buy):
            return

        if is_buy:
            if not self.has_open_bid and self.place_bid:
                self.place_spot_limit_order(is_buy)
        else:
            if not self.has_open_ask and self.place_ask:
                self.place_spot_limit_order(is_buy)

    def calculate_perp_order_sell_price(self):
        self.perp_order_sell_price = self.perp_connector.get_price_for_volume(self.perp_pair, False,
                                                                         self.perp_order_amount).result_price

    def calculate_perp_profit(self):
        order_buy_price = self.perp_connector.get_price_for_volume(self.perp_pair, True,
                                                                   self.perp_order_amount).result_price
        active_positions = self.perp_connector.account_positions
        for position in active_positions.values():
            self.perp_breakeven_price_diff = position.entry_price - order_buy_price

    def check_and_cancel_maker_orders(self, is_buy):
        self.has_open_bid = False
        self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.spot_connector_name):
            if order.is_buy:
                if is_buy:
                    self.has_open_bid = True
                    # max_spread_adj = self.perp_spread + self.max_spread
                    # min_spread_adj = self.perp_spread + self.min_spread
                    # ask_price = self.spot_connector.get_price(self.spot_pair, True)

                    self.upper_price = self.perp_order_sell_price * Decimal(1 - self.min_spread / 100)
                    self.lower_price = self.perp_order_sell_price * Decimal(1 - self.max_spread / 100)

                    if order.price > self.upper_price or order.price < self.lower_price:
                        self.log_with_clock(logging.INFO, f"BUY order price {order.price} is not in the range "
                                                          f"{self.lower_price} - {self.upper_price}. Cancelling order.")
                        self.cancel(self.spot_connector_name, order.trading_pair, order.client_order_id)
                        return True
            else:
                if not is_buy:
                    self.has_open_ask = True
                    bid_price_adj = self.maker_order_open_price - self.perp_breakeven_price_diff
                    self.upper_price = bid_price_adj * Decimal(1 + self.max_spread / 100)
                    self.lower_price = bid_price_adj * Decimal(1 + self.min_spread / 100)
                    if order.price > self.upper_price or order.price < self.lower_price:
                        self.log_with_clock(logging.INFO, f"SELL order price {order.price} is not in the range "
                                                          f"{self.lower_price} - {self.upper_price}. Cancelling order.")
                        self.cancel(self.spot_connector_name, order.trading_pair, order.client_order_id)
                        return True
        return False

    def place_spot_limit_order(self, is_buy):
        if is_buy:
            order_price = self.perp_order_sell_price * Decimal(1 - self.start_spread / 100)
            buy_candidate = OrderCandidate(trading_pair=self.spot_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=self.spot_order_amount,
                                           price=order_price)
            place_result = self.adjust_and_place_order(candidate=buy_candidate, all_or_none=False, place_spot=True)
            if place_result:
                self.log_with_clock(logging.INFO, f"Placed maker BUY order {place_result} at {order_price}")
                self.maker_order_open_price = order_price
        else:
            order_amount = self.last_spot_base_amount
            bid_price = self.spot_connector.get_price(self.spot_pair, False)
            bid_price_adj = self.maker_order_open_price - self.perp_breakeven_price_diff
            self.log_with_clock(logging.INFO, f"bid_price = {bid_price}")
            self.log_with_clock(logging.INFO, f"bid_price_adj = {bid_price_adj}")
            order_price = bid_price_adj * Decimal(1 + self.start_spread / 100)
            sell_candidate = OrderCandidate(trading_pair=self.spot_pair,
                                            is_maker=True,
                                            order_type=OrderType.LIMIT,
                                            order_side=TradeType.SELL,
                                            amount=order_amount,
                                            price=order_price)
            place_result = self.adjust_and_place_order(candidate=sell_candidate, all_or_none=False, place_spot=True)
            if place_result:
                self.log_with_clock(logging.INFO, f"Placed maker SELL order at {order_price}")

    def adjust_and_place_order(self, candidate, all_or_none, place_spot):
        connector = self.spot_connector if place_spot else self.perp_connector
        candidate_adjusted = connector.budget_checker.adjust_candidate(candidate, all_or_none=all_or_none)
        if candidate_adjusted.amount == Decimal("0"):
            self.log_with_clock(logging.INFO,
                                f"Order candidate amount is less than allowed on the market: "
                                f" {candidate_adjusted.trading_pair}. Can't create"
                                f" {candidate_adjusted.order_side.name}"
                                f" {candidate_adjusted.order_type.name} order")
            return False

        self.log_with_clock(logging.INFO, f"candidate = {candidate}")
        self.log_with_clock(logging.INFO, f"candidate_adjusted = {candidate_adjusted}")
        if place_spot:
            self.place_spot_order(candidate_adjusted)
        else:
            self.place_perp_order(candidate_adjusted)
        return True

    def place_spot_order(self, candidate_adjusted):
        if candidate_adjusted.order_side == TradeType.BUY:
            self.buy(
                self.spot_connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price)
        else:
            self.sell(
                self.spot_connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price)

    def place_perp_order(self, candidate_adjusted):
        if candidate_adjusted.order_side == TradeType.BUY:
            self.buy(
                self.perp_connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price, PositionAction.OPEN)
        else:
            self.sell(
                self.perp_connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price, PositionAction.OPEN)

    def open_perp_hedging_order(self, is_long):
        if is_long:
            best_ask = self.perp_connector.get_price(self.perp_pair, True)
            order_price = best_ask * (1 + self.slippage_buffer / Decimal("100"))
        else:
            best_bid = self.perp_connector.get_price(self.perp_pair, False)
            order_price = best_bid * (1 - self.slippage_buffer / Decimal("100"))

        order_side = TradeType.BUY if is_long else TradeType.SELL

        perp_candidate = PerpetualOrderCandidate(
            trading_pair=self.perp_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=order_side,
            amount=self.filled_spot_order_amount,
            price=order_price,
            leverage=self.leverage,
        )
        self.log_with_clock(logging.INFO, f"candidate = {perp_candidate}")
        self.taker_candidate = {"perp_candidate": perp_candidate, "sent_timestamp": 0, "trials": 0}

        place_result = self.adjust_and_place_order(candidate=perp_candidate, all_or_none=True, place_spot=False)
        if place_result:
            self.taker_candidate["sent_timestamp"] = self.current_timestamp
            self.log_with_clock(logging.INFO, f"Placed Perpetuals order")


        # adjusted_candidate = self.perp_connector.budget_checker.adjust_candidate(candidate, all_or_none=True)
        # self.log_with_clock(logging.INFO, f"adjusted_candidate = {adjusted_candidate}")
        #
        # if adjusted_candidate.amount > Decimal("0"):
        #     self.taker_candidate["sent_timestamp"] = self.current_timestamp
        #     self.log_with_clock(logging.INFO, f"Opening order")
        #     if is_long:
        #         self.buy(self.perp_connector_name,
        #                  self.perp_pair,
        #                  adjusted_candidate.amount,
        #                  adjusted_candidate.order_type,
        #                  adjusted_candidate.price,
        #                  PositionAction.OPEN)
        #     else:
        #         self.sell(self.perp_connector_name,
        #                   self.perp_pair,
        #                   adjusted_candidate.amount,
        #                   adjusted_candidate.order_type,
        #                   adjusted_candidate.price,
        #                   PositionAction.OPEN)

    def is_open_orders_on_perps(self):
        active_positions = self.perp_connector.account_positions
        return True if len(active_positions) > 0 else False

    def did_fill_order(self, event: OrderFilledEvent):
        msg = (f"{event.trade_type.name} {round(event.amount, 5)} {event.trading_pair} "
               f"at {round(event.price, 5)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])

        lines.extend(["", "  Spot order buy price:"] + ["    " + str(self.maker_order_open_price)])
        lines.extend(["", "  Perp order sell price:"] + ["    " + str(self.perp_order_sell_price)])
        lines.extend(["", "  Spot base balance:"] + ["    " + str(self.last_spot_base_amount)])
        lines.extend(["", "  Perp price pnl:"] + ["    " + str(self.perp_breakeven_price_diff)])
        # lines.extend(["", "  Target amounts:"] + ["    " + f"{self.target_base_amount} {self.assets['maker_base']} "
        #                                                    f"{self.target_quote_amount} {self.assets['maker_quote']}"])
        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)

    def active_orders_df(self) -> pd.DataFrame:
        """
        Returns a custom data frame of all active maker orders for display purposes
        """
        columns = ["Market", "Pair", "Side", "Price", "Size", "Min price", "Max price", "Spread", "Age"]
        data = []
        mid_price = self.spot_connector.get_mid_price(self.spot_pair)
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                if order.is_buy:
                    spread_mid = (mid_price - order.price) / mid_price * 100
                else:
                    spread_mid = (order.price - mid_price) / mid_price * 100

                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    connector_name,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    float(self.lower_price),
                    float(self.upper_price),
                    float(round(spread_mid, 2)),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df
