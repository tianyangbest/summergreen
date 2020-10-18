# -*- coding: utf-8 -*-
import datetime

import numpy as np
import pandas as pd
import redis
import tqdm
from sqlalchemy import create_engine
from summergreen.operators.base_operator import BaseOperator
from summergreen.utils.logging_mixin import LoggingMixin
from summergreen.utils.redis_util import redis_value2list
from summergreen.utils.time_util import get_all_timestamp_list


class TickOperator(LoggingMixin, BaseOperator):
    def __init__(self):
        super().__init__()
        self._r = redis.Redis(
            host=self._base_config["tick_redis_config"]["host"],
            port=self._base_config["tick_redis_config"]["port"],
            db=self._base_config["tick_redis_config"]["db"],
            decode_responses=self._base_config["tick_redis_config"]["decode_responses"],
        )

        self._stock_dtypes = [
            (k, v) for k, v in self._stock_config["tick_dtypes"].items()
        ]
        init_arr = np.array([], dtype=self._stock_dtypes)
        self._stock_codes_arr_dict = {
            code: init_arr.copy() for code in self._stock_codes["stock"]
        }
        self._base_postgres_engine = create_engine(
            self._base_config["base_postgres_engine_str"]
        )
        print(
            self._base_postgres_engine.execute(
                """SELECT max(time), min(time) FROM base_info.bar_1day"""
            ).fetchall()
        )

        self._stock_codes_last_close_dict = {}
        self._last_update_time_stamp = 0

    def mirror_from_df(self, df):
        df["volume_increased"] = df.groupby(level=[0])["volume"].shift()
        df["volume_increased"] = df["volume_increased"].fillna(df["volume"]).astype(int)
        df["money_increased"] = df.groupby(level=[0])["money"].shift()
        df["money_increased"] = df["money_increased"].fillna(df["money"])
        for c in tqdm.tqdm(set(df.index.get_level_values(0).to_list())):
            self._stock_codes_arr_dict[c] = (
                df[df.index.get_level_values(0) == c]
                .reset_index()
                .to_records(index=False)
            )

    def get_stock_codes_list(self):
        return self._stock_codes["stock"]

    def update_stock_codes_arr_dict(self, tmp_time_str: str):
        tmp_time_stamp = datetime.datetime.strptime(
            tmp_time_str, "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        if tmp_time_stamp <= self._last_update_time_stamp:
            raise ValueError(
                f"tmp_time_stamp:{datetime.datetime.fromtimestamp(tmp_time_stamp)} "
                f"is smaller than or equal to  "
                f"_last_update_time_stamp:{datetime.datetime.fromtimestamp(self._last_update_time_stamp)}"
            )
        tmp_dict = self._r.hgetall(tmp_time_str)
        for k, v in tmp_dict.items():
            try:
                redis_value_list = redis_value2list(v)
                if (
                    redis_value_list[0] <= 0
                    or redis_value_list[1] <= 0
                    or redis_value_list[2] <= 0
                ):
                    pass
                else:
                    tmp_arr = np.hstack(
                        (
                            self._stock_codes_arr_dict[k],
                            np.array(
                                [
                                    tuple(
                                        [k, tmp_time_stamp] + redis_value_list + [0, 0]
                                    )
                                ],
                                dtype=self._stock_dtypes,
                            ),
                        ),
                    )
                    tmp_arr["volume_increased"] = np.diff(
                        np.hstack((0, tmp_arr["volume"]))
                    )
                    tmp_arr["money_increased"] = np.diff(
                        np.hstack((0, tmp_arr["money"]))
                    )
                    self._stock_codes_arr_dict[k] = tmp_arr
            except Exception as e:
                self.log.error(e)

        self._last_update_time_stamp = tmp_time_stamp

    def get_arr_by_time_code(
        self,
        code: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ):
        arr_by_time_code = self._stock_codes_arr_dict[code]
        return arr_by_time_code[
            (arr_by_time_code["time"] >= np.datetime64(start_time))
            & (arr_by_time_code["time"] < np.datetime64(end_time))
        ]

    def get_bar_by_time_code(self, code, bar_start_time, bar_end_time):
        arr_by_time_code = self.get_arr_by_time_code(code, bar_start_time, bar_end_time)
        try:
            open = arr_by_time_code["current"][0]
            close = arr_by_time_code["current"][-1]
            high = arr_by_time_code["high"].max()
            low = arr_by_time_code["low"].min()
            volume = arr_by_time_code["volume_increased"].sum()
            money = arr_by_time_code["money_increased"].sum()
            return [code, bar_start_time, open, close, high, low, volume, money]
        except Exception as e:
            # self.log.error([code, bar_start_time_stamp, bar_end_time_stamp, e])
            return None

    def get_bar_list_by_time(self, bar_start_time, bar_end_time):
        bar_list = [
            b
            for b in [
                self.get_bar_by_time_code(c, bar_start_time, bar_end_time)
                for c in self.get_stock_codes_list()
            ]
            if b is not None
        ]
        return bar_list

    def get_bar_df_by_time_range(self, bar_start_time, bar_end_time, seconds_interval):

        bar_list = []
        for t in tqdm.tqdm(
            get_all_timestamp_list(bar_start_time, bar_end_time, seconds_interval)
        ):
            bar_list = bar_list + self.get_bar_list_by_time(
                t, t + datetime.timedelta(seconds=seconds_interval)
            )
        return pd.DataFrame(bar_list, columns=self._stock_config["bar_dtypes"].keys())


if __name__ == "__main__":
    pass
