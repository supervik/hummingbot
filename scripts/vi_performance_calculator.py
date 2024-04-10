import logging
import os

import numpy as np
import pandas as pd
import os

from hummingbot import data_path
from hummingbot.strategy.script_strategy_base import Decimal, ScriptStrategyBase
from hummingbot.client.hummingbot_application import HummingbotApplication


class PerformanceCalculator(ScriptStrategyBase):
    """
    Old Performance calculator script. Leave it only for ed purpose
    """
    # Config params
    connector_name: str = "kucoin_paper_trade"
    fake_pair = "SOL-USDT"
    trades_folder = "test"
    ignore_asset = "KCS"

    # group by base asset and id (for multi pairs) or id only (trade round for simple/tri xemm)
    group_by_base_asset = False
    group_by_day = False

    # time in seconds between trades to combine them into 1 round
    timestamp_threshold = 10
    fee_pct = 0.08

    markets = {connector_name: {fake_pair}}

    def on_tick(self):
        work_folder = os.path.join(data_path(), self.trades_folder)
        trades_files = [x for x in os.listdir(work_folder) if x.startswith("trades")]
        self.log_with_clock(logging.INFO, f"trades_files = {trades_files}")
        for csv_file in trades_files:
            csv_path = os.path.join(work_folder, csv_file)
            df = pd.read_csv(csv_path)
            df = df[['market', 'symbol', 'base_asset', 'quote_asset', 'timestamp', 'trade_type', 'amount', 'price']]
            df = df[~df['symbol'].str.contains(self.ignore_asset)]
            df["ts_dif"] = df.timestamp - df.timestamp.shift(1)
            df['id'] = np.where((df['ts_dif'] >= self.timestamp_threshold * 1000) | np.isnan(df['ts_dif']), df.timestamp, 0)
            df['id'] = df['id'].replace(to_replace=0, method='ffill')
            base_assets = df['base_asset'].unique().tolist()
            quote_assets = df['quote_asset'].unique().tolist()
            all_assets = set(base_assets + quote_assets)

            for asset in all_assets:
                conditions = [(df['base_asset'] == asset) & (df['trade_type'] == "BUY"),
                              (df['base_asset'] == asset) & (df['trade_type'] == "SELL"),
                              (df['quote_asset'] == asset) & (df['trade_type'] == "BUY"),
                              (df['quote_asset'] == asset) & (df['trade_type'] == "SELL")]
                choices = [df.amount, -df.amount,
                           -df.amount * df.price * (1 + self.fee_pct / 100),
                           df.amount * df.price * (1 - self.fee_pct / 100)]
                df[asset] = np.select(conditions, choices, default=0)

            if self.group_by_base_asset:
                df = df.groupby(['base_asset', 'id'], as_index=False).sum()
                columns = ["id", "base_asset"] + list(all_assets)
            else:
                numeric_cols = df.select_dtypes(include=['float']).columns
                self.log_with_clock(logging.INFO, f"numeric_cols = {numeric_cols}")
                df = df.groupby('id', as_index=False).agg({**{col: 'sum' for col in numeric_cols}, **{'symbol': 'first'}})
                # df = df.groupby('id', as_index=False).sum()
                df['time'] = pd.to_datetime(df['id'], unit='ms')
                df['time'] = df['time'].dt.floor('S')
                columns = ["id", "time", "symbol"] + list(all_assets)

            self.log_with_clock(logging.INFO, f"df = {df}")
            self.log_with_clock(logging.INFO, f"columns = {columns}")
            df = df[columns]

            if not self.group_by_base_asset and self.group_by_day:
                if len(columns) == 5:
                    df = df.groupby([df['time'].dt.date]).agg({columns[1]: 'count', columns[2]: 'sum',
                                                               columns[3]: 'sum', columns[4]: 'sum'})
                if len(columns) == 4:
                    df = df.groupby([df['time'].dt.date]).agg({columns[1]: 'count', columns[2]: 'sum',
                                                               columns[3]: 'sum'})

            self.log_with_clock(logging.INFO, f"df = {df}")

            filename = f"profitability_{csv_file}.xlsx"
            df.to_excel(os.path.join(work_folder, filename))

        HummingbotApplication.main_application().stop()
