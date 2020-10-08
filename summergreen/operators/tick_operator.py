# -*- coding: utf-8 -*-
import datetime

import numpy as np
import redis

from summergreen.operators.base_operator import BaseOperator
from summergreen.utils.logging_mixin import LoggingMixin
from summergreen.utils.redis_util import redis_value2list


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
            (k, v) for k, v in self._stock_config["tick_numpy_datatype"].items()
        ]
        init_arr = np.array([], dtype=self._stock_dtypes)
        self._stock_codes_arr_dict = {
            code: init_arr.copy() for code in self._stock_codes["stock"]
        }
        self._last_update_time_stamp = 0

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
                    tmp_arr["volume"] = np.diff(np.hstack((0, tmp_arr["volume_incr"])))
                    tmp_arr["money"] = np.diff(np.hstack((0, tmp_arr["money_incr"])))
                    self._stock_codes_arr_dict[k] = tmp_arr
            except Exception as e:
                self.log.error(e)

        self._last_update_time_stamp = tmp_time_stamp
        print(tmp_time_str, ":", len(tmp_dict))

    def get_arr_by_time_code(
        self, code: str, start_time_stamp: float, end_time_stamp: float
    ):
        arr_by_time_code = self._stock_codes_arr_dict[code]
        return arr_by_time_code[
            (arr_by_time_code["time"] >= start_time_stamp)
            & (arr_by_time_code["time"] < end_time_stamp)
        ]

    def get_bar_by_time_code(self, code, bar_start_time_stamp, bar_end_time_stamp):
        arr_by_time_code = self.get_arr_by_time_code(
            code, bar_start_time_stamp, bar_end_time_stamp
        )
        try:
            open = arr_by_time_code["current"][0]
            close = arr_by_time_code["current"][-1]
            high = arr_by_time_code["high"].max()
            low = arr_by_time_code["low"].min()
            volume = arr_by_time_code["volume"].sum()
            money = arr_by_time_code["money"].sum()
            return [code, bar_start_time_stamp, open, close, high, low, volume, money]
        except Exception as e:
            # self.log.error([code, bar_start_time_stamp, bar_end_time_stamp, e])
            return None

    def get_bar_list_by_time(self, bar_start_time_stamp, bar_end_time_stamp):
        bar_list = [
            b
            for b in [
                self.get_bar_by_time_code(c, bar_start_time_stamp, bar_end_time_stamp)
                for c in self.get_stock_codes_list()
            ]
            if b is not None
        ]
        print(bar_list)
        return bar_list


if __name__ == "__main__":
    pass
