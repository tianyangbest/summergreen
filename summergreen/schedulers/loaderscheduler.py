# -*- coding: utf-8 -*-
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.loaders import sinastockloader
import os
import datetime

sl = sinastockloader.SinaStockLoader(
    redis_host="localhost",
    redis_port=6377,
    redis_db=1,
)


def redis2df2parquet(date_str, parquet_dir):
    sl.redis2df(date_str).to_parquet(parquet_dir)


if __name__ == "__main__":
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
