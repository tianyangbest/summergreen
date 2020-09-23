# -*- coding: utf-8 -*-
# Author: Steven Field

import json
import os
import datetime
import pandas as pd
import redis
import itertools


class SinaStockLoader(object):
    def __init__(self, redis_host, redis_port, redis_db):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        with open(f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.json""") as f:
            self.stock_config = json.load(f)
        with open(f"""{os.path.dirname(os.path.dirname(__file__))}/fetchers/quotation/stock_codes.conf""") as f:
            self.stock_codes = json.load(f)

    def redis2df(self, match_time):
        redis_list = [[[k] + [i] + v.split(",") for k, v in self.r.hgetall(i).items()] for i in
                      self.r.keys(match_time)]
        redis_list = list(itertools.chain(*redis_list))
        df = pd.DataFrame(redis_list)
        df.columns = list(self.stock_config['sina_datatype'].keys())
        df = df.astype(self.stock_config['sina_datatype'])
        df = df.set_index(['code', 'time'])
        return df

    def redis2df_today(self):
        today_str = datetime.datetime.now().date()
        return self.redis2df(f"{today_str}*")

    def redis2json(self, match_time):
        redis_dict = {c: [] for c in self.stock_codes['stock']}
        for i in sorted(self.r.keys(match_time)):
            for k, v in self.r.hgetall(i).items():
                redis_dict[k].append([k] + [i] + v.split(","))
        return redis_dict
