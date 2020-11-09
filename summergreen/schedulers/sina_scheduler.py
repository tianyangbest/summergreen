# -*- coding: utf-8 -*-
import datetime
import itertools

import pandas as pd
import redis

from summergreen import quotation
from summergreen.schedulers.base_scheduler import BaseScheduler


class SinaScheduler(BaseScheduler):
    def __init__(self):
        super().__init__()
        self._scheduler_name = "新浪数据调度"
        self._r = redis.Redis(
            host=self._base_config["tick_redis_config"]["host"],
            port=self._base_config["tick_redis_config"]["port"],
            db=self._base_config["tick_redis_config"]["db"],
            decode_responses=1,
        )
        self._today = datetime.datetime.now().date()
        self._today_str = self._today.strftime("%Y-%m-%d")
        self._sq = quotation.use("sina")
        self._bs.add_job(
            self.day_initialization,
            "cron",
            hour="8",
            minute="59",
            second="0",
            max_instances=1,
        )

        self._bs.add_job(
            self.snap2redis,
            "cron",
            hour="9-10,13-14",
            max_instances=10,
            second="*",
        )
        self._bs.add_job(
            self.snap2redis,
            "cron",
            hour="11",
            minute="0-31",
            max_instances=10,
            second="*",
        )
        self._bs.add_job(
            self.snap2redis,
            "cron",
            hour="15",
            minute="0-5",
            max_instances=10,
            second="*",
        )
        self._bs.add_job(
            self.redis2parquet,
            "cron",
            hour="15",
            minute="15",
            second="0",
            max_instances=1,
            args=[
                f"""{self._today_str}*""",
                f"""{self._base_config['to_tick_day_parquet_dir']}/{self._today_str}.parquet""",
            ],
            misfire_grace_time=1 * 60 * 60,
        )
        self.log.info("K线数据调度今日调度已经初始化")

    def day_initialization(self):
        quotation.update_stock_codes()
        self._r.flushdb(self._base_config["tick_redis_config"]["db"])
        self._today = datetime.datetime.now().date()
        self._today_str = self._today.strftime("%Y-%m-%d")
        self.log.info("Tick股票代码已经初始化")

    def snap2redis(self):
        try:
            snap_pipe = self._r.pipeline()
            snap = self._sq.market_snapshot()
            for k, v in snap.items():
                if k[1][:10] == self._today_str:
                    snap_pipe.hset(k[1], k[0], v)
            snap_pipe.execute()
            self.log.info("Tick数据更新到redis")
        except Exception as e:
            print(e)

    def redis2parquet(self, match_time, to_path_dir):
        redis_list = [
            [[k] + [i] + v.split(",") for k, v in self._r.hgetall(i).items()]
            for i in self._r.keys(match_time)
        ]
        redis_list = list(itertools.chain(*redis_list))
        df = pd.DataFrame(redis_list)
        df.columns = list(self._stock_config["tick_dtypes"].keys())
        df = df.astype(self._stock_config["tick_dtypes"])
        df = df.set_index(["code", "time"])
        df.to_parquet(to_path_dir)
