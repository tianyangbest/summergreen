# -*- coding: utf-8 -*-
# Author: Steven Field

import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar
import cudf

ProgressBar().register()


class StockAnalyzer(object):
    def __init__(self, if_gpu=False):
        self.std_col = [
            "current",
            "high",
            "low",
            "volume",
            "money",
            "a1_p",
            "a2_p",
            "a3_p",
            "a4_p",
            "a5_p",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
            "b1_p",
            "b2_p",
            "b3_p",
            "b4_p",
            "b5_p",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
        ]
        self.std_index = ["code", "time"]
        self.all_col = [*self.std_col, *self.std_index]
        self.sina2std = {
            "ask1": "a1_p",
            "ask1_volume": "a1_v",
            "ask2": "a2_p",
            "ask2_volume": "a2_v",
            "ask3": "a3_p",
            "ask3_volume": "a3_v",
            "ask4": "a4_p",
            "ask4_volume": "a4_v",
            "ask5": "a5_p",
            "ask5_volume": "a5_v",
            "bid1": "b1_p",
            "bid1_volume": "b1_v",
            "bid2": "b2_p",
            "bid2_volume": "b2_v",
            "bid3": "b3_p",
            "bid3_volume": "b3_v",
            "bid4": "b4_p",
            "bid4_volume": "b4_v",
            "bid5": "b5_p",
            "bid5_volume": "b5_v",
            "buy": "buy",
            "curr_time_str": "curr_time_str",
            "date": "date",
            "high": "high",
            "low": "low",
            "now": "current",
            "sell": "sell",
            "time": "time",
            "turnover": "volume",
            "volume": "money",
        }
        self.if_gpu = if_gpu
        # self.stock_df = cudf.from_pandas(pd.DataFrame(columns=self.std_col)) \
        #     if if_gpu else pd.DataFrame(columns=self.std_col)
        self.stock_df_dict = {}

    def merge_from_csv_path(self, file_path):
        merge_df = pd.read_csv(file_path)
        merge_df = merge_df.rename(columns=self.sina2std).sort_values("curr_time_str")
        merge_df["time"] = pd.to_datetime(merge_df["date"] + " " + merge_df["time"])

        # for i in merge_df.code.to_list():
        #     code = str(i)
        #     if code not in self.stock_df_dict.keys():
        #         self.stock_df_dict[code] = pd.DataFrame(columns=self.std_col)
        #     self.stock_df_dict[code] = pd.concat([self.stock_df_dict[code],
        #                                           merge_df[merge_df.code == i]]).drop_duplicates(keep='last')

        # merge_df['code_time'] = list(zip(merge_df['code'], merge_df['time']))
        # merge_df = merge_df.set_index('code_time')
        # merge_df = merge_df[self.std_col]

        # idx_new = merge_df.index.difference(self.stock_df.index)
        # return merge_df.loc[idx_new]
        # self.stock_df = self.stock_df.append(merge_df)
        # if self.if_gpu:
        #     merge_df = cudf.from_pandas(merge_df)
        #     self.stock_df = cudf.concat([self.stock_df, merge_df]).drop_duplicates(keep='last')
        # else:
        #     self.stock_df = pd.concat([self.stock_df, merge_df]).drop_duplicates(keep='last')
