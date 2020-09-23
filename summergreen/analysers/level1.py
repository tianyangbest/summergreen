# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
import ray


@ray.remote
class Level1(object):
    def __init__(self, stock_codes):
        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.json"""
        ) as f:
            self.stock_config = json.load(f)
        self.stock_codes = stock_codes
        self.columns_names = list(self.stock_config["sina_datatype"].keys())
        self.level1_dict = {
            c: pd.DataFrame(columns=self.columns_names)
            .astype(self.stock_config["sina_datatype"])
            .set_index("time")
            for c in self.stock_codes
        }

    def merge_new_level1_dict(self, new_dict_level1_lists):
        for k in self.stock_codes:
            if len(new_dict_level1_lists[k]) >= 1:
                new_df = pd.DataFrame(
                    new_dict_level1_lists[k], columns=self.columns_names
                )
                new_df = new_df.astype(self.stock_config["sina_datatype"]).set_index(
                    "time"
                )
                self.level1_dict[k] = pd.concat([self.level1_dict[k], new_df])
                self.level1_dict[k]["volume_diff"] = self.level1_dict[k][
                    "volume"
                ].diff()
                self.level1_dict[k]["money_diff"] = self.level1_dict[k]["money"].diff()

    def get_level1_dict_range(self, time_range_start_time, time_range_end_time):
        return {
            i: self.level1_dict[i][
                (self.level1_dict[i].index >= time_range_start_time)
                & (self.level1_dict[i].index < time_range_end_time)
            ]
            for i in self.level1_dict
        }

    def get_level1_dict_range_ticks2bar(
        self, time_range_start_time, time_range_end_time
    ):
        level1_dict_range = self.get_level1_dict_range(
            time_range_start_time, time_range_end_time
        )

        return {
            i: level1_dict_range[i]
            .groupby("code")
            .aggregate(
                open=pd.NamedAgg(column="current", aggfunc="first"),
                close=pd.NamedAgg(column="current", aggfunc="last"),
                high=pd.NamedAgg(column="high", aggfunc="max"),
                low=pd.NamedAgg(column="low", aggfunc="min"),
                volume_range=pd.NamedAgg(column="volume_diff", aggfunc="sum"),
                money_range=pd.NamedAgg(column="money_diff", aggfunc="sum"),
            )
            .assign(time_range=f"{time_range_start_time},{time_range_end_time}")
            if level1_dict_range[i].shape[0] > 0
            else None
            for i in level1_dict_range
        }
