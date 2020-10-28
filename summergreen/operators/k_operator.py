# -*- coding: utf-8 -*-
import datetime
import itertools

import numpy as np
import pandas as pd
import redis
import tqdm
from sqlalchemy import create_engine
from summergreen.operators.base_operator import BaseOperator
from summergreen.utils.logging_mixin import LoggingMixin
from summergreen.utils.redis_util import redis_value2list
from summergreen.utils.time_util import get_all_timestamp_list


class KOperator(LoggingMixin, BaseOperator):
    def __init__(self):
        super().__init__()
        self._tr = redis.Redis(
            host=self._base_config["tick_redis_config"]["host"],
            port=self._base_config["tick_redis_config"]["port"],
            db=self._base_config["tick_redis_config"]["db"],
            decode_responses=self._base_config["tick_redis_config"]["decode_responses"],
        )
        self._k15r = redis.Redis(
            host=self._base_config["k15s_redis_config"]["host"],
            port=self._base_config["k15s_redis_config"]["port"],
            db=self._base_config["k15s_redis_config"]["db"],
            decode_responses=self._base_config["k15s_redis_config"]["decode_responses"],
        )

        self._stock_dtypes = [
            (k, v) for k, v in self._stock_config["tick_dtypes"].items()
        ] + [(k, v) for k, v in self._stock_config["tick_increased_dtypes"].items()]
        init_arr = np.array([], dtype=self._stock_dtypes)
        self._stock_codes_arr_dict = {
            code: init_arr.copy() for code in self._stock_codes["stock"]
        }
        self._base_postgres_engine = create_engine(
            self._base_config["base_postgres_engine_str"]
        )

        self._stock_codes_last_close_dict = {}
        self._last_update_time = datetime.datetime(1900, 1, 1)

    def redis2df(self, match_time):
        redis_list = [
            [[k] + [i] + v.split(",") for k, v in self._tr.hgetall(i).items()]
            for i in self._tr.keys(match_time)
        ]
        redis_list = list(itertools.chain(*redis_list))
        df = pd.DataFrame(redis_list)
        df.columns = list(self._stock_config["tick_dtypes"].keys())
        df = df.astype(self._stock_config["tick_dtypes"])
        df = df.set_index(["code", "time"])
        return df

    def redis2json(self, match_time):
        redis_dict = {c: [] for c in self._stock_codes["stock"]}
        for i in sorted(self._tr.keys(match_time)):
            for k, v in self._tr.hgetall(i).items():
                try:
                    redis_dict[k].append([k] + [i] + v.split(","))
                except:
                    pass
        return redis_dict

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

    def update_stock_codes_arr_dict(self, tmp_time: datetime.datetime):
        if tmp_time <= self._last_update_time:
            raise ValueError(
                f"tmp_time_stamp:{tmp_time} "
                f"is smaller than or equal to  "
                f"_last_update_time_stamp:{self._last_update_time}"
            )
        tmp_dict = self._tr.hgetall(
            datetime.datetime.strftime(tmp_time, "%Y-%m-%d %H:%M:%S")
        )
        for k, v in tmp_dict.items():
            redis_value_list = redis_value2list(v)
            if (
                redis_value_list[0] <= 0
                or redis_value_list[1] <= 0
                or redis_value_list[2] <= 0
                or redis_value_list[3] <= 0
                or redis_value_list[4] <= 0
            ):
                pass
            else:
                tmp_arr = np.hstack(
                    (
                        self._stock_codes_arr_dict[k],
                        np.array(
                            [tuple([k, tmp_time] + redis_value_list + [0, 0])],
                            dtype=self._stock_dtypes,
                        ),
                    ),
                )
                tmp_arr["volume_increased"] = np.diff(np.hstack((0, tmp_arr["volume"])))
                tmp_arr["money_increased"] = np.diff(np.hstack((0, tmp_arr["money"])))
                self._stock_codes_arr_dict[k] = tmp_arr

        self._last_update_time = tmp_time
        self.log.info(f"从redis更新Tick数据，时间:{tmp_time}")

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

    def get_k_by_time_code(self, code, k_start_time, k_end_time):
        arr_by_time_code = self.get_arr_by_time_code(code, k_start_time, k_end_time)
        try:
            open = arr_by_time_code["current"][0]
            close = arr_by_time_code["current"][-1]
            high = arr_by_time_code["high"].max()
            low = arr_by_time_code["low"].min()
            volume = arr_by_time_code["volume_increased"].sum()
            money = arr_by_time_code["money_increased"].sum()
            return [code, k_start_time, open, close, high, low, volume, money]
        except Exception as e:
            return None

    def get_k_list_by_time(self, k_start_time, k_end_time):
        k_list = [
            b
            for b in [
                self.get_k_by_time_code(c, k_start_time, k_end_time)
                for c in self.get_stock_codes_list()
            ]
            if b is not None
        ]
        return k_list

    def update_k_list_by_time(self, k_start_time, k_end_time):
        k_list_by_time = self.get_k_list_by_time(k_start_time, k_end_time)
        snap_pipe = self._k15r.pipeline()

        for k in k_list_by_time:
            k = [str(i) for i in k]
            snap_pipe.hset(k[1], k[0], ",".join(k[2:]))
        snap_pipe.execute()
        self.log.info(f"K15数据更新到redis, 时间范围:{k_start_time}-{k_end_time}")

    def get_k_df_by_time_range(self, k_start_time, k_end_time, seconds_interval):

        k_list = []
        for t in tqdm.tqdm(
            get_all_timestamp_list(k_start_time, k_end_time, seconds_interval)
        ):
            k_list = k_list + self.get_k_list_by_time(
                t, t + datetime.timedelta(seconds=seconds_interval)
            )
        return pd.DataFrame(k_list, columns=self._stock_config["k_dtypes"].keys())

    def redis2df2parquet(self, date_str, parquet_dir):
        self.redis2df(date_str).to_parquet(parquet_dir)
