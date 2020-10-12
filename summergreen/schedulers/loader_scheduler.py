# -*- coding: utf-8 -*-
import os
import datetime

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.loaders import sinastockloader


with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.yaml"""
) as f:
    redis_config = yaml.full_load(f)["tick_redis_config"]

sl = sinastockloader.SinaStockLoader(
    redis_host=redis_config["redis_host"],
    redis_port=redis_config["redis_port"],
    redis_db=redis_config["redis_db"],
)


def redis2df2parquet(date_str, parquet_dir):
    sl.redis2df(date_str).to_parquet(parquet_dir)


today_str = datetime.datetime.now().date()
bs = BlockingScheduler()
bs.add_job(
    redis2df2parquet,
    "cron",
    [today_str, "/mnt/stock_data/stock_tick_current_day/"],
    hour="15",
    minute="1",
    second="1",
    max_instances=1,
)

print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))
try:
    bs.start()
except (KeyboardInterrupt, SystemExit):
    pass
