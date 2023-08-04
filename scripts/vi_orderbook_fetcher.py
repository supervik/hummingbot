import os.path
from datetime import datetime
import pandas as pd

from hummingbot import data_path
from hummingbot.strategy.script_strategy_base import Decimal, ScriptStrategyBase


class OrderBookFetcher(ScriptStrategyBase):
    """
    This strategy gets orderbook data (spread and volume for a certain depth)
    and save it to a csv file.
    """
    markets = {
        "binance_paper_trade": {"XMR-ETH", "FRONT-BUSD"},
        "gate_io_paper_trade": {"AVAX-USDT"},
        "kucoin_paper_trade": {"ALGO-BTC"},

    }
    # Orderbook depth for calculating orders volume (in %, i.e use 2 for 2%)
    depth: Decimal = Decimal("2")

    # Time (in seconds) for updating and saving the orderbook data
    update_time: float = 60

    # Whether to save to a csv file (files will be saved to hummingbot/data folder)
    save_to_file_enabled: bool = True

    last_updated_timestamp: float = 0
    columns = ["Exchange", "Pair", "Mid Price", "Spread", f"Vol {depth}%", f"Vol -{depth}%"]
    orderbook_data_df = pd.DataFrame(columns=columns)

    def on_tick(self):
        if self.current_timestamp >= self.last_updated_timestamp + self.update_time:
            self.last_updated_timestamp = self.current_timestamp
            self.orderbook_data_df = pd.DataFrame(columns=self.columns)
            for connector_name, connector in self.connectors.items():
                for trading_pair in self.markets[connector_name]:
                    mid_price = connector.get_mid_price(trading_pair)
                    spread = self.get_spread(connector, trading_pair)
                    asks_volume, bids_volume = self.get_depth_volume(connector, trading_pair, mid_price)

                    data_df = pd.DataFrame(
                        [[connector_name, trading_pair, mid_price, spread, asks_volume, bids_volume]],
                        columns=self.columns)

                    # Save to file separately for each trading pair
                    if self.save_to_file_enabled:
                        self.export_to_csv(connector_name, trading_pair, data_df)

                    # Get cumulative dataframe for all pairs to display in status
                    self.orderbook_data_df = pd.concat([self.orderbook_data_df, data_df])

    def get_spread(self, connector, trading_pair):
        """
        Calculates current spread
        """
        best_bid = connector.get_price(trading_pair, False)
        best_ask = connector.get_price(trading_pair, True)
        return round(Decimal("100") * (best_ask - best_bid) / best_ask, 2)

    def get_depth_volume(self, connector, trading_pair, mid_price):
        """
        Get certain depth orderbook volume denominated in a base currency
        """
        price_upper = mid_price * (Decimal("1") + self.depth / Decimal("100"))
        price_lower = mid_price * (Decimal("1") - self.depth / Decimal("100"))
        asks_volume = connector.get_volume_for_price(trading_pair, True, price_upper).result_volume
        bids_volume = connector.get_volume_for_price(trading_pair, False, price_lower).result_volume
        # base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        # usd_pair = f"{base_asset}-USD"
        # usd_conversion_rate = RateOracle.get_instance().rate(usd_pair)
        # self.log_with_clock(logging.INFO, f"{usd_pair=} {usd_conversion_rate=}")
        # asks_volume * = usd_conversion_rate
        # bids_volume * = usd_conversion_rate
        return round(asks_volume, 2), round(bids_volume, 2)

    def export_to_csv(self, connector_name, trading_pair, data_df):
        """
        Appends new data to a csv file, separate for each trading pair.
        """
        csv_filename = f"orderbook_{connector_name}_{trading_pair}.csv"
        csv_path = os.path.join(data_path(), csv_filename)
        csv_df = data_df
        csv_df["Time"] = datetime.fromtimestamp(self.last_updated_timestamp).strftime('%Y-%m-%d %H:%M:%S')

        add_header = False if os.path.exists(csv_path) else True
        csv_df.to_csv(csv_path, mode='a', header=add_header, index=False,
                      columns=["Time", "Mid Price", "Spread", f"Vol {self.depth}%",
                               f"Vol -{self.depth}%"])

    def format_status(self) -> str:
        """
        Method called by the `status` command. Generates the real time update for the orderbook data.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        date = datetime.fromtimestamp(self.last_updated_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        lines.extend(["", "  Last updated timestamp:", "    " + date])
        lines.extend(["", "  OrderBook Data:"])
        lines.extend(["    " + line for line in self.orderbook_data_df.to_string(index=False).split("\n")])

        return "\n".join(lines)
