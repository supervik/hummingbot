import logging

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType, PositionAction, PositionSide
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class SpotPerpetualXEMM(ScriptStrategyBase):
    """
    This script defines a cross-exchange market making strategy between spot and perpetual markets
    - During each "tick", the bot checks if it's initialized and active.
    - At appropriate times, the bot cleans up old maker order IDs.
    - Bot calculates trade amounts based on the balance and the middle price.
    - Checks if existing orders exceed the cancellation threshold and cancels them.
    - Bot determines whether to place a buy or sell order based on base asset balance.
    - The bot places maker orders with amounts that fit the budget.
    - Upon successful order fulfillment, logs the event, and places a taker order.
    - Taker orders are placed with adjusted amounts. If unable, the bot goes inactive.
    """
    # Config params
    maker_connector_name: str = "kucoin"
    taker_connector_name: str = "gate_io_perpetual"

    trading_pairs = {"KAVA-USDT", "FTT-USDT", "SQUAD-USDT", "VET-USDT", "VELO-USDT"}

    order_amount_in_quote = Decimal("20")  # order amount for buying denominated in the quote currency
    buy_spread_bps = 200  # profitability of the buy order
    min_buy_spread_bps = 150  # the min threshold after which the buy maker order is cancelled
    sell_spread_bps = 100  # profitability of the sell order
    min_sell_spread_bps = 70  # the min threshold after which the sell maker order is cancelled
    max_order_age = 120  # the maximum order age after which the maker order is cancelled

    slippage_buffer_spread_bps = 300
    leverage = Decimal("20")

    dry_run = False
    close_all_positions = False
    add_safety_mid_prices_dif_for_hedge = False

    # class parameters
    status = "NOT_INIT"
    close_all_position_timestamp = 0
    pair = ""
    maker_order_ids = {}
    maker_order_ids_clean_interval = 30 * 60
    maker_order_ids_clean_timestamp = 0
    order_amount_in_base = 0
    sell_order_amount = 0
    buy_order_amount = 0
    taker_sell_hedging_price = 0
    taker_buy_hedging_price = 0
    maker_side = "BUY"
    buy_order_placed = False
    sell_order_placed = False
    filled_event_buffer = {}
    order_delay = 20
    next_maker_order_timestamp = {}

    markets = {maker_connector_name: trading_pairs, taker_connector_name: trading_pairs}

    @property
    def maker_connector(self):
        """
        The maker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.maker_connector_name]

    @property
    def taker_connector(self):
        """
        The taker connector in this strategy, define it here for easy access
        """
        return self.connectors[self.taker_connector_name]

    def on_tick(self):
        """
        Manages operations performed during each strategy cycle
        """
        if self.status == "NOT_INIT":
            self.init_strategy()

        if self.status != "ACTIVE":
            return

        if self.current_timestamp > self.maker_order_ids_clean_timestamp:
            self.clean_maker_order_ids()

        for self.pair in self.trading_pairs:
            self.calculate_maker_order_amount()

            self.calculate_taker_hedging_price()

            self.check_existing_orders_for_cancellation()

            if self.current_timestamp < self.next_maker_order_timestamp[self.pair]:
                return

            self.get_maker_order_side()

            self.place_maker_orders()

        if self.dry_run:
            self.status = "NOT_ACTIVE"

    def init_strategy(self):
        """
        Initializes the strategy, defines the assets, and sets the bot to active.
        """
        if self.close_all_positions:
            self.sell_all_leftovers_and_close_positions()
            return

        if self.dry_run:
            self.notify_hb_app_with_timestamp("Attention! Dry run mode!")

        self.next_maker_order_timestamp = {pair: 0 for pair in self.trading_pairs}
        self.filled_event_buffer = {pair: [] for pair in self.trading_pairs}
        self.status = "ACTIVE"
        self.notify_hb_app_with_timestamp("Strategy started")

    def sell_all_leftovers_and_close_positions(self):
        if not self.close_all_position_timestamp:
            if self.dry_run:
                self.notify_hb_app_with_timestamp("Attention! Dry run mode!")
            delay = 30
            self.notify_hb_app_with_timestamp(f"Attention! Close all positions in {delay} sec!")
            self.close_all_position_timestamp = self.current_timestamp + delay
            return

        if self.current_timestamp < self.close_all_position_timestamp:
            return

        self.logger().info(f"Close all positions and sell leftovers started")
        self.sell_all_leftovers_on_maker()
        self.close_positions_on_taker()
        self.notify_hb_app_with_timestamp("Finished! Strategy not active")
        self.status = "NOT_ACTIVE"

    def sell_all_leftovers_on_maker(self):
        for pair in self.trading_pairs:
            base_asset, quote_asset = split_hb_trading_pair(pair)
            base_amount = self.maker_connector.get_balance(base_asset)
            sell_amount = self.maker_connector.quantize_order_amount(pair, base_amount)
            if sell_amount != Decimal("0"):
                price = self.maker_connector.get_price_for_volume(pair, False, base_amount).result_price
                price_with_slippage = price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
                sell_order = OrderCandidate(trading_pair=pair, is_maker=True, order_type=OrderType.LIMIT,
                                            order_side=TradeType.SELL, amount=base_amount,
                                            price=price_with_slippage)

                sell_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(sell_order,
                                                                                           all_or_none=False)
                if sell_order_adjusted.amount != Decimal("0"):
                    self.notify_app_and_log(f"Sell leftover {pair} amount {sell_order_adjusted.amount}")
                    self.send_order_to_exchange(candidate=sell_order_adjusted,
                                                connector_name=self.maker_connector_name)
        self.notify_app_and_log(f"Selling leftovers on maker finished")

    def close_positions_on_taker(self):
        open_positions = self.taker_connector.account_positions

        for key, position in open_positions.items():
            if position.trading_pair in self.trading_pairs:
                amount = abs(position.amount)
                if position.position_side == PositionSide.SHORT:
                    side = TradeType.BUY
                    price = self.taker_connector.get_price_for_volume(position.trading_pair, True, amount).result_price
                    price_with_slippage = price * Decimal(1 + self.slippage_buffer_spread_bps / 10000)
                else:
                    side = TradeType.SELL
                    price = self.taker_connector.get_price_for_volume(position.trading_pair, False, amount).result_price
                    price_with_slippage = price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)

                self.logger().info(
                    f"Sending {side.name} {position.trading_pair} with amount = {amount}"
                    f" and price (with slippage) = {price_with_slippage}")

                position_candidate = PerpetualOrderCandidate(trading_pair=position.trading_pair, is_maker=False,
                                                             order_type=OrderType.LIMIT,
                                                             order_side=side, amount=amount,
                                                             price=price_with_slippage, leverage=self.leverage)
                position_candidate_adjusted = self.taker_connector.budget_checker.adjust_candidate(
                    position_candidate, all_or_none=True)
                if position_candidate_adjusted.amount != Decimal("0"):
                    self.notify_app_and_log(f"Close position {position.position_side.name} {amount} {position.trading_pair}")
                    self.send_order_to_exchange(candidate=position_candidate_adjusted, connector_name=self.taker_connector_name)

                else:
                    self.logger().info(f"Position can't be closed. Adjusted candidate = {position_candidate_adjusted}")

    def clean_maker_order_ids(self):
        """
        Cleans up old maker order IDs to avoid using expired ones
        """
        updated_maker_order_ids = {}
        for order_id, timestamp in self.maker_order_ids.items():
            if timestamp > self.current_timestamp - self.maker_order_ids_clean_interval:
                updated_maker_order_ids[order_id] = timestamp

        self.maker_order_ids = updated_maker_order_ids
        self.maker_order_ids_clean_timestamp = self.current_timestamp + self.maker_order_ids_clean_interval

    def cancel_all_orders_on_trading_pair(self, trading_pair):
        """
        Cancels all active orders on the maker connector
        """
        for order in self.get_active_orders(self.maker_connector_name):
            if trading_pair == order.trading_pair:
                self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)

    def cancel_all_orders(self):
        """
        Cancels all active orders on the maker connector
        """
        for order in self.get_active_orders(self.maker_connector_name):
            self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)

    def calculate_maker_order_amount(self):
        """
        Calculates the maker order amount based on balance and order_amount_in_quote defined in the configuration
        """
        # get sell amount
        base_asset, quote_asset = split_hb_trading_pair(self.pair)
        base_amount = self.maker_connector.get_balance(base_asset)
        self.sell_order_amount = self.quantize_amount_on_maker_and_taker(base_amount)

        # get buy amount
        mid_price = self.maker_connector.get_mid_price(self.pair)
        self.order_amount_in_base = self.order_amount_in_quote / mid_price
        self.buy_order_amount = self.quantize_amount_on_maker_and_taker(self.order_amount_in_base)
        if self.dry_run and self.buy_order_amount == Decimal("0"):
            self.logger().info(f"Pair {self.pair} minimum requirements don't met. Do not open order")
            self.logger().info(f"Pair {self.pair} mid_price {mid_price}, "
                               f"order_amount_in_base = {self.order_amount_in_base}, buy_order_amount = {self.buy_order_amount}")

    def quantize_amount_on_maker_and_taker(self, amount):
        """
        Quantizes amounts for both maker and taker to match minimal requirements.
        """
        amount_quantized_on_maker = self.maker_connector.quantize_order_amount(self.pair, amount)
        amount_quantized_on_taker = self.taker_connector.quantize_order_amount(self.pair, amount)
        return min(amount_quantized_on_maker, amount_quantized_on_taker)

    def calculate_taker_hedging_price(self):
        """
        Calculates the taker hedging price to be used for placing orders
        """
        self.taker_sell_hedging_price = self.taker_connector.get_price_for_volume(self.pair, False,
                                                                                  self.order_amount_in_base).result_price
        self.taker_buy_hedging_price = self.taker_connector.get_price_for_volume(self.pair, True,
                                                                                 self.order_amount_in_base).result_price
        if self.add_safety_mid_prices_dif_for_hedge:
            mid_taker_price = (self.taker_sell_hedging_price + self.taker_sell_hedging_price) / 2
            mid_maker_price = self.maker_connector.get_mid_price(self.pair)

            mid_dif = mid_taker_price - mid_maker_price
            try:
                if mid_dif > Decimal("0"):
                    self.taker_sell_hedging_price -= mid_dif
            except:
                self.logger().info(f"Can't calculate mid_dif for {self.pair} "
                                   f"mid_taker_price = {mid_taker_price}, mid_maker_price = {mid_maker_price}, "
                                   f"taker_sell_hedging_price = {self.taker_sell_hedging_price}, "
                                   f"taker_buy_hedging_price = {self.taker_buy_hedging_price}")

    def check_existing_orders_for_cancellation(self):
        """
        Checks and cancels existing orders that exceed a threshold.
        """
        self.buy_order_placed = False
        self.sell_order_placed = False
        for order in self.get_active_orders(connector_name=self.maker_connector_name):
            if order.trading_pair == self.pair:
                cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
                if order.is_buy:
                    self.buy_order_placed = True
                    buy_cancel_threshold = self.taker_sell_hedging_price * Decimal(1 - self.min_buy_spread_bps / 10000)
                    if order.price > buy_cancel_threshold or cancel_timestamp < self.current_timestamp:
                        self.logger().info(f"Cancelling {order.trading_pair} buy order {order.client_order_id}")
                        self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)
                else:
                    self.sell_order_placed = True
                    sell_cancel_threshold = self.taker_buy_hedging_price * Decimal(1 + self.min_sell_spread_bps / 10000)
                    if order.price < sell_cancel_threshold or cancel_timestamp < self.current_timestamp:
                        self.logger().info(f"Cancelling {order.trading_pair} sell order: {order.client_order_id}")
                        self.cancel(self.maker_connector_name, order.trading_pair, order.client_order_id)

    def get_maker_order_side(self):
        """
        Determines if the bot should place a buy or sell order
        If there is enough base amount to sell it places a sell order
        """
        self.maker_side = TradeType.BUY if self.sell_order_amount == Decimal("0") else TradeType.SELL

    def place_maker_orders(self):
        """
        Places maker orders based on the calculated parameters
        """
        if self.buy_order_placed or self.sell_order_placed:
            return

        if self.maker_side == TradeType.BUY:
            maker_price = self.taker_sell_hedging_price * Decimal(1 - self.buy_spread_bps / 10000)
            maker_order_amount = self.buy_order_amount
        else:
            maker_price = self.taker_buy_hedging_price * Decimal(1 + self.sell_spread_bps / 10000)
            maker_order_amount = self.sell_order_amount

        maker_order = OrderCandidate(trading_pair=self.pair, is_maker=True, order_type=OrderType.LIMIT,
                                     order_side=self.maker_side, amount=maker_order_amount, price=maker_price)

        maker_order_adjusted = self.maker_connector.budget_checker.adjust_candidate(maker_order, all_or_none=False)
        if maker_order_adjusted.amount != Decimal("0"):
            order_id = self.send_order_to_exchange(candidate=maker_order_adjusted,
                                                   connector_name=self.maker_connector_name)
            if order_id:
                self.maker_order_ids[order_id] = self.current_timestamp

    def send_order_to_exchange(self, candidate, connector_name):
        """
        Sends a given order candidate to the exchange
        """
        if self.dry_run:
            return
        if candidate.order_side == TradeType.SELL:
            order_id = self.sell(connector_name, candidate.trading_pair, candidate.amount, candidate.order_type,
                                 candidate.price, PositionAction.OPEN)
        else:
            order_id = self.buy(connector_name, candidate.trading_pair, candidate.amount, candidate.order_type,
                                candidate.price, PositionAction.OPEN)

        return order_id

    def is_active_maker_order(self, event: OrderFilledEvent):
        """
        Helper function that checks if order is an active order on the maker exchange
        """
        return True if event.order_id in self.maker_order_ids else False

    def did_fill_order(self, event: OrderFilledEvent):
        """
        Handles order fill events, logs them, and places corresponding taker orders
        """
        filled_maker = True if self.is_active_maker_order(event) else False
        self.notify_app_and_log(f"{'--- Maker' if filled_maker else 'taker'} {event.trade_type.name} "
                                f"{round(event.amount, 8)} {event.trading_pair} at {round(event.price, 8)}")
        self.next_maker_order_timestamp[event.trading_pair] = self.current_timestamp + self.order_delay
        if filled_maker:
            filled_pair = event.trading_pair
            self.filled_event_buffer[filled_pair].append(event)
            self.logger().info(
                f"New filled event added to filled_event_buffer = {self.filled_event_buffer[filled_pair]}")
            self.place_taker_orders(filled_pair)

    def place_taker_orders(self, trading_pair):
        """
        Places taker orders based on the filled orders from the maker side
        """
        amount_total = Decimal("0")
        for event in self.filled_event_buffer[trading_pair]:
            amount_total += event.amount
            event_side = event.trade_type

        self.logger().info(f"amount_total = {amount_total}")
        quantized_amount_total = self.taker_connector.quantize_order_amount(trading_pair, amount_total)
        if quantized_amount_total == Decimal("0"):
            msg = f"Not enough amount filled to open a hedge order on {trading_pair}. Current total amount = {amount_total}"
            self.logger().info(msg)
            self.notify_hb_app_with_timestamp(msg)
            return

        if event_side == TradeType.BUY:
            taker_side = TradeType.SELL
            taker_amount = quantized_amount_total
            taker_price = self.taker_connector.get_price_for_volume(trading_pair, False, taker_amount).result_price
            taker_price_with_slippage = taker_price * Decimal(1 - self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending sell on taker with price {taker_price}. "
                               f"Price with slippage {taker_price_with_slippage}")

        else:
            # get amount of short positions to close them
            open_positions = self.taker_connector.account_positions
            short_position_amount = Decimal("0")
            self.logger().info(f"open_positions = {open_positions}")
            for key, position in open_positions.items():
                if position.position_side == PositionSide.SHORT and position.trading_pair == trading_pair:
                    short_position_amount = abs(position.amount)
                    self.logger().info(f"Current short_position_amount on {trading_pair} = {short_position_amount}")

            if short_position_amount == Decimal("0"):
                self.logger().info(f"There is no open short positions on {trading_pair}")
                return
            taker_side = TradeType.BUY
            taker_amount = min(short_position_amount, amount_total)
            taker_price = self.taker_connector.get_price_for_volume(trading_pair, True, taker_amount).result_price
            taker_price_with_slippage = taker_price * Decimal(1 + self.slippage_buffer_spread_bps / 10000)
            self.logger().info(f"Sending buy on taker {trading_pair} with price {taker_price}."
                               f" Price with slippage {taker_price_with_slippage}")

        taker_candidate = PerpetualOrderCandidate(trading_pair=trading_pair, is_maker=False,
                                                  order_type=OrderType.LIMIT,
                                                  order_side=taker_side, amount=taker_amount,
                                                  price=taker_price_with_slippage, leverage=self.leverage)

        taker_candidate_adjusted = self.taker_connector.budget_checker.adjust_candidate(taker_candidate,
                                                                                        all_or_none=True)
        self.logger().info(f"Delete all events from filled_event_buffer")
        self.filled_event_buffer[trading_pair] = []
        if taker_candidate_adjusted.amount != Decimal("0"):
            self.send_order_to_exchange(candidate=taker_candidate_adjusted, connector_name=self.taker_connector_name)
            self.logger().info(f"send_order_to_exchange. Candidate adjusted {taker_candidate_adjusted}")
        else:
            self.notify_app_and_log(f"Can't create taker order with {taker_candidate.amount} amount on {trading_pair} "
                                    f"Check minimum amount requirement or balance")
            self.cancel_all_orders()
            self.status = "NOT_ACTIVE"

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])
        # lines.extend(["", "  order_ids:"] + ["    " + str(self.maker_order_ids)])

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
        columns = ["Market", "Pair", "Side", "Price", "Size", "Spread", "Age"]
        data = []
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                mid_price = connector.get_mid_price(order.trading_pair)
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
                    float(round(spread_mid, 2)),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", "Side"], inplace=True)
        return df

    def notify_app_and_log(self, msg):
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)
