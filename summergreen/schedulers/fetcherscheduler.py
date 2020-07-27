# -*- coding: utf-8 -*-
# Author: Steven Field
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from summergreen.fetchers import sinastockfetcher


class FetcherScheduler:
    def __init__(self):
        self.sf = sinastockfetcher.SinaStockFetcher(redis_host="localhost",
                                                    redis_port=6378,
                                                    redis_db=1,
                                                    parquet_dir="/mnt/stock_data/stock_tick_current_day/", )
        self.bs = BackgroundScheduler()

        self.bs.add_job(self.sf.day_initialization, 'cron', hour='9', minute='14', max_instances=1, second='30')
        self.bs.add_job(self.sf.snap2redis, 'cron',
                        hour='9', minute='15-59', max_instances=10, second='*')
        self.bs.add_job(self.sf.snap2redis, 'cron',
                        hour='10,13-14', max_instances=10, second='*')
        self.bs.add_job(self.sf.snap2redis, 'cron',
                        hour='11', minute='0-31', max_instances=10, second='*')
        self.bs.add_job(self.sf.snap2redis, 'cron',
                        hour='15', minute='0', max_instances=10, second='*')

        self.bs.add_job(self.sf.redis2parquet, 'cron',
                        hour='15', minute='1', max_instances=1, second='5')

        # test samples
        # self.bs.add_job(self.sf.snap2redis, 'cron', max_instances=10, second='*')

        self.bs.start()
        # shut down the scheduler when exiting the app
        atexit.register(lambda: self.bs.shutdown())
