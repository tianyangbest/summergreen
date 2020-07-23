# -*- coding: utf-8 -*-
# Author: Steven Field

import json
import os

import datetime
import pandas as pd
import redis
import itertools
from summergreen.fetchers import quotation
import tqdm


class SinaStockFetcher:
    def __init__(self, redis_host, redis_port, redis_db, parquet_dir):
        self.redis_db = redis_db
        self.parquet_dir = parquet_dir
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.today = datetime.datetime.now().date()
        self.today_str = self.today.strftime("%Y-%m-%d")
        self.sq = quotation.use("sina")
        with open(f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.json""") as f:
            self.stock_config = json.load(f)

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
            
    def redis2df(self, match_time):
        redis_list = [[[k] + [i] + v.split(",") for k, v in self.r.hgetall(i).items()] for i in
                      self.r.keys(match_time)]
        redis_list = list(itertools.chain(*redis_list))
        df = pd.DataFrame(redis_list)
        df.columns = list(self.stock_config['sina_datatype'].keys())
        df = df.astype(self.stock_config['sina_datatype'])
        df = df.set_index(['code', 'time'])
        return df        

    def redis2parquet(self):
        df = self.redis2df(f"{self.today}*")
        df.to_parquet(f"""{self.parquet_dir}/{self.today}.parquet""")

    def df2codesplitjson(self, df):
        return {
            c: df[df.index.get_level_values("code") == c].reset_index(level=0, drop=True).sort_index()
            for c in tqdm.tqdm(df.index.levels[0].to_list())
        }
