from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class TriangularArbitrage(ScriptStrategyBase):
    """
    Test triangular arbitrage on multiple pairs
    This script doesn't open any order but just print the information about found opportunity
    TODO: Save opportunities to CSV file
    """
    # Config params
    connector_name: str = "kucoin"

    first_pairs = ['ABBC-BTC', 'ADA-BTC', 'AGIX-BTC', 'AVAX-BTC', 'BAX-BTC', 'BCHSV-BTC', 'BDX-BTC', 'BNB-BTC', 'CAS-BTC', 'CRPT-BTC', 'DASH-BTC', 'DFI-BTC', 'DOGE-BTC', 'DOT-BTC', 'ETH-BTC', 'EWT-BTC', 'HAI-BTC', 'INJ-BTC', 'KCS-BTC', 'LINK-BTC', 'LOKI-BTC', 'LTC-BTC', 'MATIC-BTC', 'OGN-BTC', 'OUSD-BTC', 'SHA-BTC', 'STX-BTC', 'TRVL-BTC', 'TRX-BTC', 'VET-BTC', 'VID-BTC', 'VRA-BTC', 'WAX-BTC', 'XCUR-BTC', 'XDC-BTC', 'XLM-BTC', 'XMR-BTC', 'XRP-BTC', 'ZEC-BTC']
    second_pairs = ['ABBC-USDT', 'ADA-USDT', 'AGIX-USDT', 'AVAX-USDT', 'BAX-USDT', 'BCHSV-USDT', 'BDX-USDT', 'BNB-USDT', 'CAS-USDT', 'CRPT-USDT', 'DASH-USDT', 'DFI-USDT', 'DOGE-USDT', 'DOT-USDT', 'ETH-USDT', 'EWT-USDT', 'HAI-USDT', 'INJ-USDT', 'KCS-USDT', 'LINK-USDT', 'LOKI-USDT', 'LTC-USDT', 'MATIC-USDT', 'OGN-USDT', 'OUSD-USDT', 'SHA-USDT', 'STX-USDT', 'TRVL-USDT', 'TRX-USDT', 'VET-USDT', 'VID-USDT', 'VRA-USDT', 'WAX-USDT', 'XCUR-USDT', 'XDC-USDT', 'XLM-USDT', 'XMR-USDT', 'XRP-USDT', 'ZEC-USDT']
    third_pair = 'BTC-USDT'
    holding_asset: str = "USDT"

    min_profitability: Decimal = Decimal("0.4")
    order_amount_in_holding_asset: Decimal = Decimal("100")

    # Class params
    status: str = "NOT_INIT"
    trading_pairs = []
    order_sides = []
    trading_pair = {}
    order_side = {}
    profit = []
    order_amount: dict = {}
    profitable_direction: str = ""

    markets = {connector_name: set(first_pairs + second_pairs + [third_pair])}

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    def on_tick(self):
        """
        Every tick the strategy calculates the profitability of both direct and reverse direction.
        If the profitability of any direction is large enough it starts the arbitrage by creating and processing
        the first order candidate.
        """
        if self.status == "NOT_INIT":
            self.init_strategy()
        for num, (self.trading_pair, self.order_side) in enumerate(zip(self.trading_pairs, self.order_sides)):
            self.profit[num]["direct"] = self.calculate_profit(self.trading_pair["direct"], self.order_side["direct"])
            self.profit[num]["reverse"] = self.calculate_profit(self.trading_pair["reverse"], self.order_side["reverse"])
            # self.log_with_clock(logging.INFO, f"{self.trading_pair['direct']} "
            #                                   f"Profit direct: {round(self.profit[num]['direct'], 3)}, "
            #                                   f"Profit reverse: {round(self.profit[num]['reverse'], 3)}")
            if self.profit[num]["direct"] < self.min_profitability and self.profit[num]["reverse"] < self.min_profitability:
                continue

            self.profitable_direction = "direct" if self.profit[num]["direct"] > self.profit[num]["reverse"] else "reverse"
            self.notify_app_and_log(f"Arbitrage opportunity {self.trading_pair[self.profitable_direction]}. "
                                    f"Profit = {round(self.profit[num][self.profitable_direction],3)}%")

    def init_strategy(self):
        """
        Initializes strategy once before the start.
        """
        self.status = "ACTIVE"
        self.set_trading_pair()
        self.set_order_side()
        self.notify_app_and_log("Strategy is initialized")
        for pair in self.trading_pairs:
            self.notify_app_and_log(f"{pair}")
        for side in self.order_sides:
            self.notify_app_and_log(f"{side}")

    def set_trading_pair(self):
        """
        Rearrange trading pairs so that the first and last pair contains holding asset.
        We start trading round by selling holding asset and finish by buying it.
        Makes 2 tuples for "direct" and "reverse" directions and assigns them to the corresponding dictionary.
        """
        for first_pair, second_pair in zip(self.first_pairs, self.second_pairs):
            if self.holding_asset not in first_pair:
                pairs_ordered = (second_pair, first_pair, self.third_pair)
            elif self.holding_asset not in second_pair:
                pairs_ordered = (first_pair, second_pair, self.third_pair)
            else:
                pairs_ordered = (first_pair, self.third_pair, second_pair)

            self.trading_pairs.append({"direct": pairs_ordered, "reverse": pairs_ordered[::-1]})
            self.profit.append({"direct": 0, "reverse": 0})

    def set_order_side(self):
        """
        Sets order sides (1 = buy, 0 = sell) for already ordered trading pairs.
        Makes 2 tuples for "direct" and "reverse" directions and assigns them to the corresponding dictionary.
        """
        for trading_pair in self.trading_pairs:
            base_1, quote_1 = split_hb_trading_pair(trading_pair["direct"][0])
            base_2, quote_2 = split_hb_trading_pair(trading_pair["direct"][1])
            base_3, quote_3 = split_hb_trading_pair(trading_pair["direct"][2])

            order_side_1 = 0 if base_1 == self.holding_asset else 1
            order_side_2 = 0 if base_1 == base_2 else 1
            order_side_3 = 1 if base_3 == self.holding_asset else 0

            self.order_sides.append({"direct": (order_side_1, order_side_2, order_side_3),
                                     "reverse": (1 - order_side_3, 1 - order_side_2, 1 - order_side_1)})

    def calculate_profit(self, trading_pair, order_side):
        """
        Calculates profitability and order amounts for 3 trading pairs based on the orderbook depth.
        """
        exchanged_amount = self.order_amount_in_holding_asset
        order_amount = [0, 0, 0]

        for i in range(3):
            order_amount[i] = self.get_order_amount_from_exchanged_amount(trading_pair[i], order_side[i],
                                                                          exchanged_amount)
            # Update exchanged_amount for the next cycle
            if order_side[i]:
                exchanged_amount = order_amount[i]
            else:
                exchanged_amount = self.connector.get_quote_volume_for_base_amount(trading_pair[i], order_side[i],
                                                                                   order_amount[i]).result_volume
        start_amount = self.order_amount_in_holding_asset
        end_amount = exchanged_amount
        profit = (end_amount / start_amount - 1) * 100

        return profit

    def get_order_amount_from_exchanged_amount(self, pair, side, exchanged_amount) -> Decimal:
        """
        Calculates order amount using the amount that we want to exchange.
        - If the side is buy then exchanged asset is a quote asset. Get base amount using the orderbook
        - If the side is sell then exchanged asset is a base asset.
        """
        if side:
            orderbook = self.connector.get_order_book(pair)
            order_amount = self.get_base_amount_for_quote_volume(orderbook.ask_entries(), exchanged_amount)
        else:
            order_amount = exchanged_amount

        return order_amount

    def get_base_amount_for_quote_volume(self, orderbook_entries, quote_volume) -> Decimal:
        """
        Calculates base amount that you get for the quote volume using the orderbook entries
        """
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

    def notify_app_and_log(self, msg):
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def format_status(self) -> str:
        """
        Returns status of the current strategy, total profit, current profitability of possible trades and balances.
        This function is called when status command is issued.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        lines.extend(["", "  Strategy status:"] + ["    " + self.status])

        for k, (trading_pair, order_side) in enumerate(zip(self.trading_pairs, self.order_sides)):
            for direction in trading_pair:
                pairs_str = [f"{'buy' if side else 'sell'} {pair}"
                             for side, pair in zip(order_side[direction], trading_pair[direction])]
                pairs_str = " > ".join(pairs_str)
                profit_str = str(round(self.profit[k][direction], 3))
                lines.extend([f"  profitability: {profit_str}% {pairs_str}    "])

        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)
