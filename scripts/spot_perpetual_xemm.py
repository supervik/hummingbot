import logging

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType, PositionAction
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class SpotPerpetualXEMM(ScriptStrategyBase):
    # Config params
    spot_connector_name: str = "binance"
    perp_connector_name: str = "binance_perpetual"
    spot_pair: str = "RLC-USDT"
    perp_pair: str = "RLC-USDT"

    spot_order_amount = Decimal("10")
    perp_order_amount = Decimal("10")
    min_spread = Decimal("0.6")
    start_spread = Decimal("0.8")
    max_spread = Decimal("1")

    target_spot_base_amount = Decimal("10")

    slippage_buffer = Decimal("1")
    leverage = Decimal("20")

    # class parameters
    status = "NOT_INIT"
    has_open_bid = False
    has_open_ask = False
    perp_spread = 0
    perp_price_pnl = 0
    spot_base_asset = ""
    spot_quote_asset = ""
    perp_base_asset = ""
    perp_quote_asset = ""
    upper_price = 0
    lower_price = 0
    place_bid = False
    place_ask = True

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
        self.status = "ACTIVE"

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

        # check for balances
        balance_diff_base = self.get_target_balance_diff(self.spot_base_asset, self.target_spot_base_amount)
        balance_diff_base_quantize = self.perp_connector.quantize_order_amount(self.perp_pair, abs(balance_diff_base))

        if balance_diff_base_quantize != Decimal("0"):
            self.log_with_clock(logging.INFO, f"Order was filled. Start hedging on perpetuals!")
            self.place_perp_orders(balance_diff_base_quantize)
            self.status = "NOT_ACTIVE"
            return

        self.calculate_perp_profit()

        if self.check_and_cancel_maker_orders():
            return

        if self.place_ask:
            self.place_maker_orders()

        # balance_diff_base = self.get_target_balance_diff(self.spot_base_asset, self.target_spot_base_amount)
        # balance_diff_base_quantize = self.perp_connector.quantize_order_amount(self.perp_pair, abs(balance_diff_base))
        #
        # if balance_diff_base_quantize != Decimal("0"):
        #     self.log_with_clock(logging.INFO, f"Order was filled. Start hedging on perpetuals!")
        #     self.place_perp_orders(balance_diff_base_quantize)
        #     self.status = "NOT_ACTIVE"
        #     return

        # self.calculate_perp_spread()

        # self.log_with_clock(logging.INFO, f"perp_spread = {self.perp_spread}")
        # if self.check_and_cancel_maker_orders():
        #     return

        #
        # if self.place_bid:
        #     self.place_maker_orders()

    def get_target_balance_diff(self, asset, target_amount):
        current_balance = self.spot_connector.get_balance(asset)
        amount_diff = current_balance - target_amount
        return amount_diff

    def place_perp_orders(self, order_amount):
        # best_bid = self.perp_connector.get_price(self.perp_pair, False)
        # order_price = best_bid * (1 - self.slippage_buffer / Decimal("100"))
        # candidate = PerpetualOrderCandidate(
        #     trading_pair=self.perp_pair,
        #     is_maker=True,
        #     order_type=OrderType.LIMIT,
        #     order_side=TradeType.SELL,
        #     amount=order_amount,
        #     price=order_price,
        #     leverage=self.leverage,
        # )
        # self.log_with_clock(logging.INFO, f"candidate = {candidate}")
        #
        # adjusted_candidate = self.perp_connector.budget_checker.adjust_candidate(candidate, all_or_none=True)
        # self.log_with_clock(logging.INFO, f"adjusted_candidate = {adjusted_candidate}")
        #
        # if adjusted_candidate.amount > Decimal("0"):
        #     self.log_with_clock(logging.INFO, f"Opening order")
        #     self.sell(self.perp_connector_name,
        #               self.perp_pair,
        #               adjusted_candidate.amount,
        #               adjusted_candidate.order_type,
        #               adjusted_candidate.price,
        #               PositionAction.OPEN)
        #     self.place_bid = False

        best_ask = self.perp_connector.get_price(self.perp_pair, True)
        order_price = best_ask * (1 + self.slippage_buffer / Decimal("100"))
        candidate = PerpetualOrderCandidate(
            trading_pair=self.perp_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=order_amount,
            price=order_price,
            leverage=self.leverage,
        )
        self.log_with_clock(logging.INFO, f"candidate = {candidate}")

        adjusted_candidate = self.perp_connector.budget_checker.adjust_candidate(candidate, all_or_none=True)
        self.log_with_clock(logging.INFO, f"adjusted_candidate = {adjusted_candidate}")

        if adjusted_candidate.amount > Decimal("0"):
            self.log_with_clock(logging.INFO, f"Opening order")
            self.buy(self.perp_connector_name,
                     self.perp_pair,
                     adjusted_candidate.amount,
                     adjusted_candidate.order_type,
                     adjusted_candidate.price,
                     PositionAction.OPEN)
            self.place_ask = False

    def calculate_perp_spread(self):
        order_buy_price = self.perp_connector.get_price_for_volume(self.perp_pair, True,
                                                                   self.perp_order_amount).result_price
        order_sell_price = self.perp_connector.get_price_for_volume(self.perp_pair, False,
                                                                    self.perp_order_amount).result_price
        # self.log_with_clock(logging.INFO, f"order_buy_price = {order_buy_price}, order_sell_price = {order_sell_price}")
        self.perp_spread = Decimal("100") * (order_buy_price - order_sell_price) / order_buy_price

    def calculate_perp_profit(self):
        order_buy_price = self.perp_connector.get_price_for_volume(self.perp_pair, True,
                                                                   self.perp_order_amount).result_price
        active_positions = self.perp_connector.account_positions
        # self.log_with_clock(logging.INFO, f"active_positions: {active_positions}")
        for position in active_positions.values():
            self.perp_price_pnl = position.entry_price - order_buy_price
            # self.log_with_clock(logging.INFO, f"order_buy_price: {order_buy_price},"
            #                                   f"perp_price_pnl: {self.perp_price_pnl}")

    def check_and_cancel_maker_orders(self):
        self.has_open_bid = False
        self.has_open_ask = False
        for order in self.get_active_orders(connector_name=self.spot_connector_name):
            if order.is_buy:
                self.has_open_bid = True
                max_spread_adj = self.perp_spread + self.max_spread
                min_spread_adj = self.perp_spread + self.min_spread
                ask_price = self.spot_connector.get_price(self.spot_pair, True)

                self.upper_price = ask_price * Decimal(1 - min_spread_adj / 100)
                self.lower_price = ask_price * Decimal(1 - max_spread_adj / 100)

                if order.price > self.upper_price or order.price < self.lower_price:
                    self.log_with_clock(logging.INFO, f"BUY order price {order.price} is not in the range "
                                                      f"{self.lower_price} - {self.upper_price}. Cancelling order.")
                    self.cancel(self.spot_connector_name, order.trading_pair, order.client_order_id)
                    return True
            else:
                self.has_open_ask = True
                open_price = Decimal("1.629")
                bid_price_adj = open_price - self.perp_price_pnl

                self.upper_price = bid_price_adj * Decimal(1 + self.max_spread / 100)
                self.lower_price = bid_price_adj * Decimal(1 + self.min_spread / 100)
                if order.price > self.upper_price or order.price < self.lower_price:
                    self.log_with_clock(logging.INFO, f"SELL order price {order.price} is not in the range "
                                                      f"{self.lower_price} - {self.upper_price}. Cancelling order.")
                    self.cancel(self.spot_connector_name, order.trading_pair, order.client_order_id)
                    return True
        return False

    def place_maker_orders(self):
        if not self.has_open_bid and self.place_bid:
            ask_price = self.spot_connector.get_price(self.spot_pair, True)
            spread = self.start_spread + self.perp_spread
            order_price = ask_price * Decimal(1 - spread / 100)
            buy_candidate = OrderCandidate(trading_pair=self.spot_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=self.spot_order_amount,
                                           price=order_price)
            place_result = self.adjust_and_place_order(candidate=buy_candidate, all_or_none=False)
            if place_result:
                self.log_with_clock(logging.INFO, "Placed maker BUY order")

        if not self.has_open_ask:
            open_price = Decimal("1.629")
            bid_price = self.spot_connector.get_price(self.spot_pair, False)
            bid_price_adj = open_price - self.perp_price_pnl
            self.log_with_clock(logging.INFO, f"bid_price = {bid_price}")
            self.log_with_clock(logging.INFO, f"bid_price_adj = {bid_price_adj}")
            order_price = bid_price_adj * Decimal(1 + self.start_spread / 100)
            sell_candidate = OrderCandidate(trading_pair=self.spot_pair,
                                            is_maker=True,
                                            order_type=OrderType.LIMIT,
                                            order_side=TradeType.SELL,
                                            amount=self.spot_order_amount,
                                            price=order_price)
            place_result = self.adjust_and_place_order(candidate=sell_candidate, all_or_none=False)
            if place_result:
                self.log_with_clock(logging.INFO, "Placed maker SELL order")

    def adjust_and_place_order(self, candidate, all_or_none):
        candidate_adjusted = self.spot_connector.budget_checker.adjust_candidate(candidate, all_or_none=all_or_none)
        if candidate_adjusted.amount == Decimal("0"):
            self.log_with_clock(logging.INFO,
                                f"Order candidate amount is less than allowed on the market: "
                                f" {candidate_adjusted.trading_pair}. Can't create"
                                f" {candidate_adjusted.order_side.name}"
                                f" {candidate_adjusted.order_type.name} order")
            return False

        self.log_with_clock(logging.INFO, f"candidate = {candidate}")
        self.log_with_clock(logging.INFO, f"candidate_adjusted = {candidate_adjusted}")
        self.place_order(candidate_adjusted)
        return True

    def place_order(self, candidate_adjusted):
        if candidate_adjusted.order_side == TradeType.BUY:
            self.buy(
                self.spot_connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price)
        else:
            self.sell(
                self.spot_connector_name, candidate_adjusted.trading_pair, candidate_adjusted.amount,
                candidate_adjusted.order_type, candidate_adjusted.price)

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])
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
