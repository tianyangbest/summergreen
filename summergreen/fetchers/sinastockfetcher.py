# -*- coding: utf-8 -*-
# Author: Steven Field

import json
import os

import datetime
import pandas as pd
import redis

from summergreen.fetchers import quotation


class SinaStockFetcher:
    def __init__(self, redis_host, redis_port, redis_db, parquet_dir):
        self.redis_db = redis_db
        self.parquet_dir = parquet_dir
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.stock_hash_name = "stock_tick_current_day"
        self.today = datetime.datetime.now().date()
        self.sq = quotation.use("sina")
        with open(f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.json""") as f:
            self.stock_config = json.load(f)

    def day_initialization(self):
        quotation.update_stock_codes()
        self.r.flushdb(self.redis_db)
        self.today = datetime.datetime.now().date()

    def snap2redis(self):
        try:
            print("start to fetch.")
            snap_pipe = self.r.pipeline()
            snap = self.sq.market_snapshot()
            for k, v in snap.items():
                snap_pipe.hset(self.stock_hash_name, k, v)
            snap_pipe.execute()
        except Exception as e:
            print(e)

    def get_redis_all(self):
        return self.r.hgetall(self.stock_hash_name)

    def redis2parquet(self):
        df = pd.DataFrame([k.split("#") + v.split(",") for k, v in self.get_redis_all().items()])
        df.columns = self.stock_config['sina_columns']
        df = df.astype(self.stock_config['sina_datatype'])
        df = df.set_index(['code', 'time'])
        df.to_parquet(f"""{self.parquet_dir}/{self.today}.parquet""")
