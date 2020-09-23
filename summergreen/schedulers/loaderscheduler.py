# -*- coding: utf-8 -*-
# Author: Steven Field
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from summergreen.loaders import sinastockloader


class LoaderScheduler:
    def __init__(self):
        self.sl = sinastockloader.SinaStockLoader(
            redis_host="localhost",
            redis_port=6377,
            redis_db=1,
        )
        self.bs = BackgroundScheduler()

        self.bs.add_job(
            self.sl.redis2df_today().to_parquet,
            "cron",
            ["/mnt/stock_data/"],
            hour="23",
            minute="1",
            second="1",
            max_instances=1,
        )

        self.bs.start()
        # shut down the scheduler when exiting the app
        atexit.register(lambda: self.bs.shutdown())
