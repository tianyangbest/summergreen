# -*- coding: utf-8 -*-
# Author: Steven Field
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.fetchers import sinastockfetcher
import os

if __name__ == "__main__":
    sf = sinastockfetcher.SinaStockFetcher(
        redis_host="localhost",
        redis_port=6378,
        redis_db=1,
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
