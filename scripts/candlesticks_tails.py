import logging
import os

import pandas as pd
import requests

from hummingbot import data_path
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCreatedEvent, OrderFilledEvent, SellOrderCreatedEvent
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase


class CandleSticksTails(ScriptStrategyBase):
    # Config params
    connector: str = "kucoin"
    trading_pair = "SOL-USDT"
    start_candles = True

    if start_candles:
        candles = []
        # Open file with all pairs to fetch

        for pair in trading_pairs:
            candles = CandlesFactory.get_candle(connector=connector,
                                               trading_pair=pair,
                                               interval="5m", max_records=100)

    markets = {connector: {trading_pair}}

    @property
    def connector(self):
        """
        The only connector in this strategy, define it here for easy access
        """
        return self.connectors[self.connector_name]

    def get_all_symbols(self):
        """
        Fetches data and returns a list daily close
        This is the API response data structure:
        {
            "time":1602832092060,
            "ticker":[
                {
                    "symbol": "BTC-USDT",   // symbol
                    "symbolName":"BTC-USDT", // Name of trading pairs, it would change after renaming
                    "buy": "11328.9",   // bestAsk
                    "sell": "11329",    // bestBid
                    "changeRate": "-0.0055",    // 24h change rate
                    "changePrice": "-63.6", // 24h change price
                    "high": "11610",    // 24h highest price
                    "low": "11200", // 24h lowest price
                    "vol": "2282.70993217", // 24h volume，the aggregated trading volume in BTC
                    "volValue": "25984946.157790431",   // 24h total, the trading volume in quote currency of last 24 hours
                    "last": "11328.9",  // last price
                    "averagePrice": "11360.66065903",   // 24h average transaction price yesterday
                    "takerFeeRate": "0.001",    // Basic Taker Fee
                    "makerFeeRate": "0.001",    // Basic Maker Fee
                    "takerCoefficient": "1",    // Taker Fee Coefficient
                    "makerCoefficient": "1" // Maker Fee Coefficient
                }
            ]
        }
        returns ticker list
        """
        records = requests.get(url=self.url).json()
        if records["code"] != "200000":
            return None
        return records["data"]["ticker"]