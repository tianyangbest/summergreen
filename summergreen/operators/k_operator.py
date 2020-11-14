# -*- coding: utf-8 -*-
import datetime

import pandas as pd
import redis
from sqlalchemy import create_engine

from summergreen.operators.base_operator import BaseOperator
from summergreen.utils.logging_mixin import LoggingMixin
from summergreen.utils.time_util import get_all_timestamp_list


class KOperator(LoggingMixin, BaseOperator):
    def __init__(
        self,
        today_datetime=datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ),
    ):
        super().__init__()
        self._tr = redis.Redis(
            host=self._base_config["tick_redis_config"]["host"],
            port=self._base_config["tick_redis_config"]["port"],
            db=self._base_config["tick_redis_config"]["db"],
            decode_responses=self._base_config["tick_redis_config"]["decode_responses"],
        )
        self._kr = redis.Redis(
            host=self._base_config["k15s_redis_config"]["host"],
            port=self._base_config["k15s_redis_config"]["port"],
            db=self._base_config["k15s_redis_config"]["db"],
            decode_responses=self._base_config["k15s_redis_config"]["decode_responses"],
        )
        self._base_postgres_engine = create_engine(
            self._base_config["base_postgres_engine_str"]
        )
        self._last_k_end_time = datetime.datetime(1900, 1, 1)
        self._last_update_vm_codes_dict = {
            code: [0, 0, 0, 0, 0, 0] for code in self._stock_codes["stock"]
        }
        pre_close_k_day_df = pd.read_sql_query(
            f"""SELECT code, close pre_close FROM
            (SELECT code, close, time, ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) rn
            FROM base_info.k_1day WHERE time < '{today_datetime}') k
            WHERE rn = 1""",
            self._base_postgres_engine,
        )
        self._pre_close_dict = pre_close_k_day_df.set_index("code")[
            "pre_close"
        ].to_dict()
        self._pre_close_dict = {
            **{code: 0 for code in self._stock_codes["stock"]},
            **self._pre_close_dict,
        }

    def uptime_update2redis(self, k_start_time, k_end_time):
        if self._last_k_end_time > k_end_time:
            self.log.error(
                f"时间范围:结束时间（{k_end_time}）早于上次更新结束时间（{self._last_k_end_time}）"
            )
            raise
        timestamp_list = get_all_timestamp_list(k_start_time, k_end_time, 1)
        # open, close, high, low, volume, money in tuple, set all value to 0 on all tuple
        tmp_k_codes_dict = {
            code: [0, 0, 0, 0, 0, 0] for code in self._stock_codes["stock"]
        }

        for t in timestamp_list:
            for k, v in self._tr.hgetall(str(t)).items():
                v_list = [float(i) for i in v.split(",")]
                if v_list[0] > 0 and v_list[3] > 0 and v_list[4] > 0:
                    # get first current value for open
                    if tmp_k_codes_dict[k][0] <= 0:
                        tmp_k_codes_dict[k][0] = v_list[0]

                    # get last current value for close
                    tmp_k_codes_dict[k][1] = v_list[0]

                    # get max current value for high
                    if v_list[0] > tmp_k_codes_dict[k][2]:
                        tmp_k_codes_dict[k][2] = v_list[0]

                    # get min current value for low
                    if (
                        v_list[0] < tmp_k_codes_dict[k][3]
                        or tmp_k_codes_dict[k][3] == 0
                    ):
                        tmp_k_codes_dict[k][3] = v_list[0]

                    # get last value for volume
                    tmp_k_codes_dict[k][4] = int(v_list[3])

                    # get last value for money
                    tmp_k_codes_dict[k][5] = v_list[4]

        snap_pipe = self._kr.pipeline()
        for k in tmp_k_codes_dict.keys():
            if tmp_k_codes_dict[k][4] > 0 and tmp_k_codes_dict[k][5] > 0:
                volume_increased = (
                    tmp_k_codes_dict[k][4] - self._last_update_vm_codes_dict[k][4]
                )
                money_increased = (
                    tmp_k_codes_dict[k][5] - self._last_update_vm_codes_dict[k][5]
                )
                snap_pipe.hset(
                    k,
                    str(k_end_time),
                    ",".join(
                        [
                            str(i)
                            for i in tmp_k_codes_dict[k][:4]
                            + [
                                volume_increased,
                                money_increased,
                                self._pre_close_dict[k],
                            ]
                        ]
                    ),
                )
                if self._last_k_end_time <= k_start_time:
                    self._last_update_vm_codes_dict[k] = tmp_k_codes_dict[k]

        snap_pipe.execute()
        if self._last_k_end_time <= k_start_time:
            self._last_k_end_time = k_end_time
            self.log.info("K数据更新了结束时间。")
        self.log.info(f"K数据更新到redis, 时间范围:{k_start_time}-{k_end_time}")
