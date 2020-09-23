# -*- coding: utf-8 -*-
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.loaders import sinastockloader
import os

sl = sinastockloader.SinaStockLoader(
    redis_host="localhost",
    redis_port=6377,
    redis_db=1,
)


def redis2df_today2parquet(parquet_dir):
    sl.redis2df_today().to_parquet(parquet_dir)


if __name__ == "__main__":

    bs = BlockingScheduler()
    bs.add_job(
        redis2df_today2parquet,
        "cron",
        ["/mnt/stock_data/stock_tick_current_day/"],
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
