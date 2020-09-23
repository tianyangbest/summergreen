# -*- coding: utf-8 -*-
# Author: Steven Field

import datetime
import redis
from summergreen.fetchers import quotation


class SinaStockFetcher:
    def __init__(self, redis_host, redis_port, redis_db):
        self.redis_db = redis_db
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.today = datetime.datetime.now().date()
        self.today_str = self.today.strftime("%Y-%m-%d")
        self.sq = quotation.use("sina")

    def day_initialization(self):
        quotation.update_stock_codes()
        self.r.flushdb(self.redis_db)
        self.today = datetime.datetime.now().date()
        self.today_str = self.today.strftime("%Y-%m-%d")

    def snap2redis(self):
        try:
            snap_pipe = self.r.pipeline()
            snap = self.sq.market_snapshot()
            for k, v in snap.items():
                if k[1][:10] == self.today_str:
                    snap_pipe.hset(k[1], k[0], v)
            snap_pipe.execute()
        except Exception as e:
            print(e)
