# -*- coding: utf-8 -*-
import datetime
import json
import os

import numpy as np
import redis

from summergreen.utils.logging_mixin import LoggingMixin


class TickOperator(LoggingMixin):
    def __init__(self, redis_host, redis_port, redis_db):
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.json"""
        ) as f:
            self.stock_config = json.load(f)
        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/fetchers/quotation/stock_codes.conf"""
        ) as f:
            self.stock_codes = json.load(f)["stock"]

        self.stock_dtypes = [
            (k, v) for k, v in self.stock_config["sina_numpy_datatype"].items()
        ]
        init_arr = np.array([], dtype=self.stock_dtypes)
        self.stock_codes_arr_dict = {code: init_arr.copy() for code in self.stock_codes}
        # init_df = (
        #     pd.DataFrame(columns=self.stock_config["sina_datatype"].keys())
        #     .astype(self.stock_config["sina_datatype"])
        #     .set_index("time")
        # )
        # ray.init()
        # self.stock_codes_df_dict = {
        #     code: ray.put(init_df.copy()) for code in self.stock_codes
        # }
        # self.stock_codes_df_dict = {code: init_df.copy() for code in self.stock_codes}

    def update_stock_codes_arr_dict(self, time_str):
        tmp_time_stamp = datetime.datetime.strptime(
            time_str, "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        tmp_dict = self.r.hgetall(time_str)
        for k, v in tmp_dict.items():
            try:
                self.stock_codes_arr_dict[k] = np.hstack(
                    (
                        self.stock_codes_arr_dict[k],
                        np.array(
                            [
                                tuple(
                                    [k, tmp_time_stamp]
                                    + [float(x) for x in v.split(",")]
                                )
                            ],
                            dtype=self.stock_dtypes,
                        ),
                    ),
                )
            except KeyError as e:
                self.log.exception(e)
        return self

    # def update_stock_codes_df_dict(self, time):
    #     tmp_dict = self.r.hgetall(time)
    #     for k, v in tmp_dict.items():
    #         self.stock_codes_df_dict[k] = update_df.remote(
    #             self.stock_codes_df_dict[k], time, k, v
    #         )

    # def update_stock_codes_df_dict(self, time):
    #     tmp_dict = self.r.hgetall(time)
    #     for k, v in tmp_dict.items():
    #         self.stock_codes_df_dict[k].loc[time] = [k] + v.split(",")


# @ray.remote
# def update_df(obj_ref_df, new_time_index, code, new_line):
#     obj_ref_df.loc[new_time_index] = [code] + new_line.split(",")
#     return obj_ref_df


if __name__ == "__main__":
    with open(
        f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.json"""
    ) as f:
        redis_config = json.load(f)["redis_config"]
    to = TickOperator(
        redis_host=redis_config["redis_host"],
        redis_port=redis_config["redis_port"],
        redis_db=redis_config["redis_db"],
    )
    # to.update_stock_codes_arr_dict("2020-09-30 11:11:12")
    start_time = datetime.datetime(2020, 9, 30, 9, 0, 0)
    end_time = datetime.datetime(2020, 9, 30, 15, 1, 0)
    time_list = [
        str(start_time + datetime.timedelta(seconds=i))
        for i in range(0, (end_time - start_time).seconds, 1)
    ]
    for t in time_list[:10]:
        to.update_stock_codes_arr_dict(t)
