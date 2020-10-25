# -*- coding: utf-8 -*-
import datetime

import redis

from summergreen import quotation
from summergreen.schedulers.base_scheduler import BaseScheduler


class FetcherScheduler(BaseScheduler):
    def __init__(self):
        super().__init__()
        redis_host = self._base_config["tick_redis_config"]["redis_host"]
        redis_port = self._base_config["tick_redis_config"]["redis_port"]
        redis_db = self._base_config["tick_redis_config"]["redis_db"]

        self.redis_db = redis_db
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )
        self.today = datetime.datetime.now().date()
        self.today_str = self.today.strftime("%Y-%m-%d")
        self.sq = quotation.use("sina")
        self._bs.add_job(
            self.day_initialization,
            "cron",
            hour="8",
            minute="59",
            second="0",
            max_instances=1,
        )

        self._bs.add_job(
            self.snap2redis, "cron", hour="9-10,13-14", max_instances=10, second="*"
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

    def day_initialization(self):
        quotation.update_stock_codes()
        self.r.flushdb(self.redis_db)
        self.today = datetime.datetime.now().date()
        self.today_str = self.today.strftime("%Y-%m-%d")
        self.log.info("Tick股票代码已经初始化")

    def snap2redis(self):
        try:
            snap_pipe = self.r.pipeline()
            snap = self.sq.market_snapshot()
            for k, v in snap.items():
                if k[1][:10] == self.today_str:
                    snap_pipe.hset(k[1], k[0], v)
            snap_pipe.execute()
            self.log.info("Tick数据更新到redis")
        except Exception as e:
            print(e)
