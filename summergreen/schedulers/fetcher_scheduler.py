# -*- coding: utf-8 -*-
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.fetchers.sinastockfetcher import SinaStockFetcher
import yaml

with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.yaml"""
) as f:
    redis_config = yaml.full_load(f)["tick_redis_config"]
sf = SinaStockFetcher(
    redis_host=redis_config["redis_host"],
    redis_port=redis_config["redis_port"],
    redis_db=redis_config["redis_db"],
)

bs = BlockingScheduler()

bs.add_job(
    sf.day_initialization,
    "cron",
    hour="9",
    minute="14",
    max_instances=1,
    second="30",
)

bs.add_job(sf.snap2redis, "cron", hour="9-10,13-14", max_instances=10, second="*")
bs.add_job(
    sf.snap2redis,
    "cron",
    hour="11",
    minute="0-31",
    max_instances=10,
    second="*",
)
bs.add_job(
    sf.snap2redis,
    "cron",
    hour="15",
    minute="0",
    max_instances=10,
    second="*",
)

# test samples
# bs.add_job(sf.snap2redis, 'cron', max_instances=10, second='*')

print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))

try:
    bs.start()
except (KeyboardInterrupt, SystemExit):
    pass
