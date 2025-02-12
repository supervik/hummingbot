import logging

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class BalanceMonitor(ScriptStrategyBase):
    """
    The script monitor balances and rebalance assets if there balances doesn't match.
    Used together with the triangular XEMM
    """
    # Config params
    connector_name: str = "kucoin"
    assets_config = {
        "BTC": {"target": Decimal("0.105"), "min_diff": Decimal("0.00005"), "counter": 0, "pair": "BTC-USDT"},
        "USDT": {"target": Decimal("800"), "min_diff": Decimal("2"), "counter": 0, "pair": "USDT-DAI"},
    }

    trading_pairs = set([item["pair"] for item in assets_config.values()])
    markets = {connector_name: trading_pairs}

    balance_check_counter_limit = 5
    balance_check_delay = 30
    rebalance_sell_finished = False
    status = "NOT_INIT"
    dry_run = False
    rebalance_delay = 5
    delay_timestamp = 0

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    def init_strategy(self):
        self.status = "ACTIVE"

    def on_tick(self):
        """
        """
        if self.status == "NOT_INIT":
            self.init_strategy()

        if self.current_timestamp < self.delay_timestamp:
            return

        if self.status == "REBALANCE":
            self.delay_timestamp = self.current_timestamp + self.rebalance_delay
            if not self.rebalance_sell_finished:
                self.start_rebalance(is_rebalance_buy=False)
                self.rebalance_sell_finished = True
            else:
                self.start_rebalance(is_rebalance_buy=True)
                self.update_params_after_rebalance()
            return

        if self.is_rebalance_needed():
            self.status = "REBALANCE"
            return

        self.delay_timestamp = self.current_timestamp + self.balance_check_delay

    def is_rebalance_needed(self):
        return_result = False
        for asset, diff in self.get_assets_diff().items():
            if abs(diff) > self.assets_config[asset]["min_diff"]:
                if self.assets_config[asset]["counter"] >= self.balance_check_counter_limit:
                    self.log_with_clock(logging.INFO, f">> The balance of {asset} is unbalanced. "
                                                      f"The counter is over")
                    self.log_with_clock(logging.INFO, f">> Start rebalancing!")
                    self.notify_hb_app_with_timestamp(f"The balance of {asset} is unbalanced. Diff = {diff}")
                    return_result = True
                else:
                    self.log_with_clock(logging.INFO, f">> The balance of {asset} is unbalanced. "
                                                      f" The difference is {diff}. "
                                                      f"Counter {self.assets_config[asset]['counter']}")
                    self.assets_config[asset]["counter"] += 1
            else:
                self.assets_config[asset]["counter"] = 0
        return return_result

    def get_assets_diff(self):
        assets_diff = {}
        for asset, config in self.assets_config.items():
            current_balance = self.connector.get_balance(asset)
            diff_from_target = current_balance - config["target"]
            assets_diff[asset] = diff_from_target if abs(diff_from_target) > config["min_diff"] else 0
        return assets_diff

    def start_rebalance(self, is_rebalance_buy):
        assets_diff = self.get_assets_diff()

        for asset, diff in assets_diff.items():
            if self.assets_config[asset]["counter"] >= self.balance_check_counter_limit:
                if (not is_rebalance_buy and diff > Decimal("0")) or (is_rebalance_buy and diff < Decimal("0")):
                    pair = self.assets_config[asset]["pair"]
                    base, quote = split_hb_trading_pair(pair)
                    if asset == base:
                        amount = abs(diff)
                        trade_type = TradeType.BUY if is_rebalance_buy else TradeType.SELL
                    else:
                        amount = self.get_base_amount_for_quote_volume(pair, False if is_rebalance_buy else True, abs(diff))
                        trade_type = TradeType.SELL if is_rebalance_buy else TradeType.BUY
                    self.create_candidate(pair, trade_type, amount)
                else:
                    continue

    def update_params_after_rebalance(self):
        self.status = "ACTIVE"
        self.rebalance_sell_finished = False
        for asset in self.assets_config:
            self.assets_config[asset]["counter"] = 0

    def create_candidate(self, trading_pair, side, amount):
        price = self.connector.get_price(trading_pair, True)
        candidate = OrderCandidate(trading_pair=trading_pair,
                                   is_maker=False,
                                   order_type=OrderType.MARKET,
                                   order_side=side,
                                   amount=amount,
                                   price=price)
        candidate_adjusted = self.connector.budget_checker.adjust_candidate(candidate, all_or_none=False)
        if candidate_adjusted.amount != Decimal("0"):
            self.send_order_to_exchange(candidate_adjusted)
            self.log_with_clock(logging.INFO, f">> Placed maker {side} order {trading_pair}")
            self.log_with_clock(logging.INFO, f">> Candidate = {candidate_adjusted}")
        else:
            self.log_with_clock(logging.INFO, f">> Not enough funds to open {side} order on {trading_pair}")

    def send_order_to_exchange(self, candidate):
        if self.dry_run:
            return

        if candidate.order_side == TradeType.BUY:
            self.buy(self.connector_name, candidate.trading_pair, candidate.amount,
                     candidate.order_type, candidate.price)
        else:
            self.sell(self.connector_name, candidate.trading_pair, candidate.amount,
                      candidate.order_type, candidate.price)

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

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []

        lines.extend([f"  Strategy status:  {self.status}"])
        # target_amounts = ""
        # for asset in self.assets_config:
        #     target_amounts += f"{asset} : {self.assets_config[asset]['target']} \n"
        target_amounts = [f"{asset} : {self.assets_config[asset]['target']}" for asset in self.assets_config]
        target_diff = [f"{asset} : {diff}" for asset, diff in self.get_assets_diff().items()]
        # assets_diff = self.get_assets_diff()

        lines.extend([f"  Target amounts:   {target_amounts}"])
        lines.extend([f"  Diff amounts:     {target_diff}"])

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            orders_df = self.active_orders_df()
            lines.extend(
                ["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        return "\n".join(lines)