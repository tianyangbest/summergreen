# -*- coding: utf-8 -*-
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.loaders import sinastockloader
import os

if __name__ == "__main__":
    sl = sinastockloader.SinaStockLoader(
        redis_host="localhost",
        redis_port=6377,
        redis_db=1,
    )
    bs = BlockingScheduler()

    bs.add_job(
        sl.redis2df_today,
        "cron",
        hour="23",
        minute="1",
        second="1",
        max_instances=1,
    )

    print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        bs.start()
    except (KeyboardInterrupt, SystemExit):
        pass
