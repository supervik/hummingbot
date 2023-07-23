import logging
from math import ceil

import pandas as pd

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class TriangularXEMM(ScriptStrategyBase):
    # Config params
    connector_name: str = "kucoin"

    symbols_config = [{'asset': 'ALGO', 'min_spread': Decimal('0.5'), 'amount': Decimal('0.006')},
                      {'asset': 'DAG', 'min_spread': Decimal('2.5'), 'amount': Decimal('0.006')},
                      {'asset': 'KCS', 'min_spread': Decimal('0.5'), 'amount': Decimal('0.006')},
                      {'asset': 'XDC', 'min_spread': Decimal('1'), 'amount': Decimal('0.003')},
                      {'asset': 'DFI', 'min_spread': Decimal('1'), 'amount': Decimal('0.003')},
                      {'asset': 'FLUX', 'min_spread': Decimal('1'), 'amount': Decimal('0.003')},
                      {'asset': 'CRPT', 'min_spread': Decimal('1'), 'amount': Decimal('0.0006')},
                      {'asset': 'VEED', 'min_spread': Decimal('3'), 'amount': Decimal('0.0003')},
                      {'asset': 'ETN', 'min_spread': Decimal('2'), 'amount': Decimal('0.0006')},
                      {'asset': 'POND', 'min_spread': Decimal('1'), 'amount': Decimal('0.0006')},
                      {'asset': 'ABBC', 'min_spread': Decimal('4'), 'amount': Decimal('0.0006')}
                      ]

    cross_pair = 'BTC-USDT'

    spread_distance = Decimal("0.25")
    max_order_age = 120
    set_target_from_config = False
    target_base_amount = Decimal("0")
    target_quote_amount = Decimal("0.03")

    order_delay = 30
    slippage_buffer = Decimal("2")
    # minimum order amount allowed for maker orders. Calculated from the order_amount minus this pct
    order_amount_min_pct = Decimal("0.6")
    dry_run = False
    taker_order_type = OrderType.MARKET
    rebalance_at_start = False

    maker_pairs = [f"{item['asset']}-BTC" for item in symbols_config]
    taker_pairs = [f"{item['asset']}-USDT" for item in symbols_config]
    min_spread_list = [item['min_spread'] for item in symbols_config]
    order_amount_in_quote_list = [item['amount'] for item in symbols_config]
    # Define here all spreads and amounts the same and comment the lines above that calculate data from symbols_config
    # maker_pairs = ['ADA-BTC', 'XRP-BTC']
    # taker_pairs = ['ADA-USDT', 'XRP-USDT']
    # cross_pair = 'BTC-USDT'
    # min_spread_list = [Decimal("0.25")] * len(maker_pairs)
    # order_amount_in_quote_list = [Decimal("0.01")] * len(maker_pairs)

    # Class params
    status: str = "NOT_INIT"
    maker_pair = ""
    taker_pair = ""
    order_amount = 0
    taker_sell_price = 0
    taker_buy_price = 0
    config = {}
    assets = {}
    arbitrage_round = {}

    has_open_bid = False
    has_open_ask = False
    taker_candidates: list = []
    last_order_timestamp = 0
    place_order_trials_delay = 5
    place_order_trials_limit = 10
    last_fee_asset_check_timestamp = 0

    markets = {connector_name: set(maker_pairs + taker_pairs + [cross_pair])}

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

        if self.status == "NOT_ACTIVE":
            return

        if self.status == "HEDGE_MODE":
            self.process_hedge()
            return

        for self.maker_pair, self.taker_pair in zip(self.maker_pairs, self.taker_pairs):
            # check for balances
            self.set_base_quote_assets()
            balance_diff_base = self.get_target_balance_diff(self.assets["maker_base"], self.target_base_amount)
            balance_diff_base_quantize = self.connector.quantize_order_amount(self.taker_pair, abs(balance_diff_base))

            if balance_diff_base_quantize != Decimal("0"):
                self.log_with_clock(logging.INFO, "The maker balances doesn't match. Open hedge orders.")
                balance_diff_quote = self.get_target_balance_diff(self.assets["maker_quote"], self.target_quote_amount)
                if balance_diff_base > 0:
                    # bid was filled
                    taker_orders = self.get_taker_order_data(True, abs(balance_diff_base), abs(balance_diff_quote))
                else:
                    # ask was filled
                    taker_orders = self.get_taker_order_data(False, abs(balance_diff_base), abs(balance_diff_quote))
                self.place_taker_orders(taker_orders)
                continue

            # open maker orders
            self.calculate_order_amount()

            self.taker_sell_price = self.calculate_taker_price(is_maker_bid=True)

            if self.check_and_cancel_maker_orders():
                continue

            if self.current_timestamp < self.last_order_timestamp + self.order_delay:
                continue

            self.place_maker_orders()

    def init_strategy(self):
        """
        Initializes strategy once before the start
        """
        self.notify_hb_app_with_timestamp("Strategy started")
        self.status = "ACTIVE"
        self.set_spread()
        if not self.set_target_amounts():
            self.status = "NOT_ACTIVE"
        if not self.check_maker_taker_pairs():
            self.notify_hb_app_with_timestamp(f"Check maker and taker failed. Set status NOT_ACTIVE")
            self.status = "NOT_ACTIVE"

    def set_base_quote_assets(self):
        """
        """
        self.assets["maker_base"], self.assets["maker_quote"] = split_hb_trading_pair(self.maker_pair)
        self.assets["taker_base"], self.assets["taker_quote"] = split_hb_trading_pair(self.taker_pair)
        self.assets["cross_base"], self.assets["cross_quote"] = split_hb_trading_pair(self.cross_pair)

    def set_spread(self):
        for i, maker in enumerate(self.maker_pairs):
            min_spread = self.min_spread_list[i]
            trade_spread = min_spread + self.spread_distance
            order_amount = self.order_amount_in_quote_list[i]
            self.config[maker] = {"min_spread": min_spread,
                                  "trade_spread": trade_spread,
                                  "order_amount_in_quote": order_amount}

    def set_target_amounts(self):
        self.notify_hb_app_with_timestamp(f"Setting target amounts from config")
        for self.maker_pair, self.taker_pair in zip(self.maker_pairs, self.taker_pairs):
            self.set_base_quote_assets()
            balance_diff_base = self.get_target_balance_diff(self.assets["maker_base"], self.target_base_amount)
            balance_diff_base_quantize = self.connector.quantize_order_amount(self.taker_pair, abs(balance_diff_base))
            if balance_diff_base_quantize != Decimal("0"):
                self.notify_hb_app_with_timestamp(f"Target balances of {self.maker_pair} doesn't match.")
                if self.rebalance_at_start:
                    self.notify_hb_app_with_timestamp(f"Starting rebalance")
                    self.last_order_timestamp = self.current_timestamp
                else:
                    self.notify_hb_app_with_timestamp(f"status NOT_ACTIVE. Activate rebalance_at_start for rebalance")
                    return False
        msg = f"Target base amount: {self.target_base_amount} base asset, " \
              f"Target quote amount: {self.target_quote_amount} {self.assets['maker_quote']}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        return True

    def check_maker_taker_pairs(self):
        if len(self.maker_pairs) != len(self.taker_pairs):
            self.log_with_clock(logging.INFO, f"!!! Length of maker and taker pairs doesnt match")
            return False

        if len(self.maker_pairs) != len(self.maker_pairs) != len(self.min_spread_list):
            self.log_with_clock(logging.INFO, f"!!! Length of maker and spread doesnt match")
            return False

        for maker, taker in zip(self.maker_pairs, self.taker_pairs):
            maker_base, maker_quote = split_hb_trading_pair(maker)
            taker_base, taker_quote = split_hb_trading_pair(taker)
            if maker_base != taker_base:
                self.log_with_clock(logging.INFO, f"!!! Base asset of {maker} dont match {taker}")
                return False
        return True

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
                                                                            candidate['order_candidate'].amount).result_price
                        if candidate['order_candidate'].order_side == TradeType.BUY:
                            candidate['order_candidate'].price = updated_price * Decimal(1 + self.slippage_buffer / 100)
                        else:
                            candidate['order_candidate'].price = updated_price * Decimal(1 - self.slippage_buffer / 100)

                        candidate_adjusted = self.connector.budget_checker.adjust_candidate(candidate['order_candidate'],
                                                                                            all_or_none=True)
                        if candidate_adjusted.amount != Decimal("0"):
                            self.taker_candidates[i]["sent_timestamp"] = self.current_timestamp
                            self.send_order_to_exchange(candidate_adjusted)
                        self.taker_candidates[i]["trials"] += 1
                    else:
                        msg = f"Error placing {candidate['order_candidate'].trading_pair} " \
                              f"{candidate['order_candidate'].order_side} order. Stop trading"
                        self.notify_hb_app_with_timestamp(msg)
                        self.log_with_clock(logging.WARNING, msg)
                        self.cancel_all_maker_orders()
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

    def get_target_balance_diff(self, asset, target_amount):
        current_balance = self.connector.get_balance(asset)
        amount_diff = current_balance - target_amount
        return amount_diff

    def get_taker_order_data(self, is_maker_bid, balances_diff_base, balances_diff_quote):
        # if self.assets["taker_base"] == self.assets["cross_base"]:
        #     if self.assets["taker_quote"] == self.assets["maker_quote"]:
        #         taker_side_1 = not is_maker_bid
        #         taker_side_2 = is_maker_bid
        #         taker_amount_1 = self.get_base_amount_for_quote_volume(self.taker_pair, taker_side_1,
        #                                                                balances_diff_quote)
        #         taker_amount_2 = self.get_base_amount_for_quote_volume(self.cross_pair, taker_side_2,
        #                                                                balances_diff_base)
        #     else:
        #         taker_side_1 = is_maker_bid
        #         taker_side_2 = not is_maker_bid
        #         taker_amount_1 = self.get_base_amount_for_quote_volume(self.taker_pair, taker_side_1,
        #                                                                balances_diff_base)
        #         taker_amount_2 = self.get_base_amount_for_quote_volume(self.cross_pair, taker_side_2,
        #                                                                balances_diff_quote)
        # else:
        taker_side_1 = not is_maker_bid
        taker_amount_1 = balances_diff_base

        if self.assets["taker_quote"] == self.assets["cross_quote"]:
            taker_side_2 = True if is_maker_bid else False
            taker_amount_2 = balances_diff_quote
        else:
            taker_side_2 = False if is_maker_bid else True
            taker_amount_2 = self.get_base_amount_for_quote_volume(self.cross_pair, taker_side_2,
                                                                   balances_diff_quote)

        taker_price_1 = self.connector.get_price_for_volume(self.taker_pair, taker_side_1, taker_amount_1).result_price
        taker_price_2 = self.connector.get_price_for_volume(self.cross_pair, taker_side_2, taker_amount_2).result_price

        taker_orders_data = {"pair": [self.taker_pair, self.cross_pair],
                             "side": [taker_side_1, taker_side_2],
                             "amount": [taker_amount_1, taker_amount_2],
                             "price": [taker_price_1, taker_price_2]}
        self.log_with_clock(logging.INFO, f"taker_orders_data = {taker_orders_data}")
        return taker_orders_data

    def place_taker_orders(self, taker_order):
        self.status = "HEDGE_MODE"
        self.log_with_clock(logging.INFO, "Hedging mode started")
        self.last_order_timestamp = self.current_timestamp
        # self.cancel_all_maker_orders()

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
            taker_candidate_adjusted = self.connector.budget_checker.adjust_candidate(taker_candidate, all_or_none=True)
            if taker_candidate_adjusted.amount != Decimal("0"):
                self.taker_candidates[-1]["sent_timestamp"] = self.current_timestamp
                self.send_order_to_exchange(taker_candidate_adjusted)
            self.log_with_clock(logging.INFO, f"New taker candidate added to the list: {self.taker_candidates[-1]}")

    def place_maker_orders(self):
        if not self.has_open_bid:
            suggested_price = self.taker_sell_price * Decimal(1 - self.config[self.maker_pair]["trade_spread"] / 100)
            order_price = self.get_closest_ob_bid_price(self.maker_pair, True, suggested_price)
            buy_candidate = OrderCandidate(trading_pair=self.maker_pair,
                                           is_maker=True,
                                           order_type=OrderType.LIMIT,
                                           order_side=TradeType.BUY,
                                           amount=self.order_amount,
                                           price=order_price)
            buy_candidate_adjusted = self.connector.budget_checker.adjust_candidate(buy_candidate, all_or_none=False)
            if buy_candidate_adjusted.amount < self.order_amount * self.order_amount_min_pct:
                self.log_with_clock(logging.INFO, f"Not enough funds to open maker order on {self.maker_pair}")
            if buy_candidate_adjusted.amount != Decimal("0"):
                self.send_order_to_exchange(buy_candidate_adjusted)
                self.log_with_clock(logging.INFO, f"Placed maker BUY order {self.maker_pair}")

    def send_order_to_exchange(self, candidate):
        if self.dry_run:
            return

        if candidate.order_side == TradeType.BUY:
            self.buy(self.connector_name, candidate.trading_pair, candidate.amount,
                     candidate.order_type, candidate.price)
        else:
            self.sell(self.connector_name, candidate.trading_pair, candidate.amount,
                      candidate.order_type, candidate.price)

    def cancel_all_maker_orders(self):
        for order in self.get_active_orders(self.connector_name):
            self.cancel(self.connector_name, order.trading_pair, order.client_order_id)

    def calculate_order_amount(self):
        self.order_amount = self.get_base_amount_for_quote_volume(self.maker_pair, True, self.config[self.maker_pair]['order_amount_in_quote'])

    def calculate_taker_price(self, is_maker_bid):
        taker_side_1 = not is_maker_bid
        exchanged_amount_1 = self.connector.get_quote_volume_for_base_amount(self.taker_pair, taker_side_1,
                                                                             self.order_amount).result_volume
        if self.assets["taker_quote"] == self.assets["cross_quote"]:
            taker_side_2 = not taker_side_1
            exchanged_amount_2 = self.get_base_amount_for_quote_volume(self.cross_pair, taker_side_2,
                                                                       exchanged_amount_1)
        else:
            taker_side_2 = taker_side_1
            exchanged_amount_2 = self.connector.get_quote_volume_for_base_amount(self.cross_pair, taker_side_2,
                                                                                 exchanged_amount_1).result_volume
        final_price = exchanged_amount_2 / self.order_amount
        return final_price

    def check_and_cancel_maker_orders(self):
        self.has_open_bid = False
        for order in self.get_active_orders(connector_name=self.connector_name):
            if order.trading_pair == self.maker_pair:
                if order.is_buy:
                    self.has_open_bid = True
                    buy_cancel_threshold = self.taker_sell_price * Decimal(1 - self.config[self.maker_pair]["min_spread"] / 100)
                    if order.price > buy_cancel_threshold:
                        self.log_with_clock(logging.INFO, f"{order.trading_pair} BUY order price {order.price} "
                                                          f"is higher than {buy_cancel_threshold}. Cancel order.")
                        self.cancel(self.connector_name, order.trading_pair, order.client_order_id)
                        return True
                    cancel_timestamp = order.creation_timestamp / 1000000 + self.max_order_age
                    if cancel_timestamp < self.current_timestamp:
                        self.log_with_clock(logging.INFO, f"{order.trading_pair} order age exceeds max. Cancel order.")
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

    def get_closest_ob_bid_price(self, pair, is_bid, price) -> Decimal:
        """
        Calculates the next tick best price in the orderbook
        """
        orderbook = self.connector.get_order_book(pair)
        orderbook_entries = orderbook.bid_entries() if is_bid else orderbook.ask_entries()

        next_price = price
        for order_book_row in orderbook_entries:
            row_price = Decimal(order_book_row.price)
            if row_price > price:
                continue

            if row_price < price:
                price_quantum = self.connector.get_order_price_quantum(pair, Decimal("1"))
                next_price = (ceil(row_price / price_quantum) + 1) * price_quantum
                if next_price > price:
                    next_price = price
                break

        return next_price

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        self.log_with_clock(logging.INFO, f"Buy order is created on the market {event.trading_pair}")
        self.check_and_remove_taker_candidates(event, TradeType.BUY)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        self.log_with_clock(logging.INFO, f"Sell order is created on the market {event.trading_pair}")
        self.check_and_remove_taker_candidates(event, TradeType.SELL)

    def did_fill_order(self, event: OrderFilledEvent):
        self.check_and_remove_taker_candidates(event, event.trade_type)
        msg = (f"{event.trade_type.name} {round(event.amount, 8)} {event.trading_pair} {self.connector_name} "
               f"at {round(event.price, 8)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        # if event.trading_pair in self.maker_pairs:
        #     self.arbitrage_round[event.trading_pair].append(event)

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
        columns = ["Market", "Pair", "Side", "Price", "Size", "Spread", "Age"]
        data = []
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                mid_price = self.connector.get_mid_price(order.trading_pair)
                if order.is_buy:
                    spread_mid = (mid_price - order.price) / mid_price * 100
                else:
                    spread_mid = (order.price - mid_price) / mid_price * 100

                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    self.connector_name,
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

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])
        lines.extend(["", "  Target amounts:"] + ["    " + f"{self.target_base_amount} base asset "
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
