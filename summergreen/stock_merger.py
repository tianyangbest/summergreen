# -*- coding: utf-8 -*-
# Author: Steven Field

import cudf
import datetime
import pandas as pd
from queue import Queue
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import quotation
import threading

cudf.set_allocator("managed")


class StockMerger(object):
    def __init__(self, parquet_target_dir):
        self.ordered_columns = [
            "current", "high", "low", "volume", "money",
            "a1_p", "a2_p", "a3_p", "a4_p", "a5_p", "a1_v", "a2_v", "a3_v", "a4_v", "a5_v",
            "b1_p", "b2_p", "b3_p", "b4_p", "b5_p", "b1_v", "b2_v", "b3_v", "b4_v", "b5_v",
        ]
        self.ordered_index = ['code', 'time']
        self.parquet_target_dir = parquet_target_dir
        self.shift_timedelta = datetime.timedelta(seconds=15)

        self.stock_scheduler = BackgroundScheduler()
        self.stock_queue = Queue()
        self.stock_threading = threading.Thread(target=self.stock_dict2tmp_dict, daemon=True)
        self.set_merger_plan()

        # shut down the scheduler when exiting the app
        atexit.register(lambda: self.stop_merger_plan)

    def initialize_one_day_job(self, trade_date):
        print(f"""started initialize_one_day_job at {datetime.datetime.now()}""")
        quotation.update_stock_codes()
        self.fetcher = quotation.Sina()
        self.trade_date = trade_date
        self.cut_off_time = self.trade_date
        self.persistent_cdf = cudf.DataFrame(columns=self.ordered_index + self.ordered_columns)
        self.persistent_cdf = self.persistent_cdf.set_index(self.ordered_index)
        self.tmp_df = pd.DataFrame(columns=self.ordered_columns)
        if self.stock_scheduler.state == 2:
            self.stock_scheduler.resume()
        elif self.stock_scheduler.state == 0:
            print("plan is already stopped")
        else:
            print("plan is already running")
        print(f"""finished initialize_one_day_job at {datetime.datetime.now()}""")

    def estimate_trading(self):
        """
        Get snapshot of stock and determine if is trading today.
        :rtype: Boolean
        """
        stock_dict = self.fetcher.market_snapshot()
        trade_date_list = list(set([i[1].date() for i in stock_dict.keys()]))
        if self.trade_date not in trade_date_list:
            return False
        else:
            return True

    def cache_stock_dict(self):
        """
        Get a snapshot of stock by fetcher to stock queue
        """
        print(f"""started cache stock dict at {datetime.datetime.now()}""")
        stock_dict = self.fetcher.market_snapshot()
        self.stock_queue.put(stock_dict)
        print(f"""finished cache stock dict at {datetime.datetime.now()}""")

    def stock_dict2tmp_dict(self):
        """
        Merge snapshot of stock in queue to tmp_df if exiting in queue
        """
        while True:
            try:
                print(f"""started cache to tmp_df stock dict at {datetime.datetime.now()}""")
                stock_df_list = [self.tmp_df, pd.DataFrame.from_dict(self.stock_queue.get(), orient='index')]
                tmp_df = pd.concat(stock_df_list)
                tmp_df = tmp_df[tmp_df.index.map(lambda x: x[1] >= self.cut_off_time)]
                tmp_df = tmp_df.loc[~tmp_df.index.duplicated(keep='first')]
                tmp_df.index = pd.MultiIndex.from_tuples(tmp_df.index)
                self.tmp_df = tmp_df
                self.stock_queue.task_done()
                print(f"""finished cache to tmp_df stock dict at {datetime.datetime.now()}""")
            except Exception as e:
                print(e)

    def tmp2persistent_delayed(self):
        """
        Merge tmp_df to cudf frame and update cut off time
        """
        print(f"""started tmp_df to cudf at {datetime.datetime.now()}""")
        tmp_df = self.tmp_df.copy()
        cut_off_time = tmp_df.index.map(lambda x: x[1]).max() - self.shift_timedelta
        tmp_df = tmp_df[tmp_df.index.map(lambda x: x[1] < cut_off_time)]
        tmp_cdf = cudf.from_pandas(tmp_df)
        self.persistent_cdf = cudf.concat([self.persistent_cdf, tmp_cdf])
        self.cut_off_time = cut_off_time
        print(f"""finished tmp_df to cudf at {datetime.datetime.now()}""")

    def save_persistent2parquet(self):
        """
        save the tmp_cdf to parquet_path
        """
        print(f"""started to save cudf at {datetime.datetime.now()}""")
        tmp_cdf = cudf.from_pandas(self.tmp_df)
        self.persistent_cdf = cudf.concat([self.persistent_cdf, tmp_cdf])
        self.persistent_cdf.to_parquet(f"{self.parquet_target_dir}/{self.trade_date}.parquet")
        print(f"""finished to save cudf at {datetime.datetime.now()}""")

    def set_merger_plan(self):
        # self.stock_scheduler.add_job(self.cache_stock_dict, 'cron',
        #                              max_instances=10, second='*')
        # self.stock_scheduler.add_job(self.tmp2persistent_delayed, 'cron',
        #                              max_instances=1, second='*/30')

        # fetcher scheduler
        self.stock_scheduler.add_job(self.cache_stock_dict, 'cron',
                                     hour='9', minute='15-59', max_instances=10, second='*')
        self.stock_scheduler.add_job(self.cache_stock_dict, 'cron',
                                     hour='10,13-14', max_instances=10, second='*')
        self.stock_scheduler.add_job(self.cache_stock_dict, 'cron',
                                     hour='11', minute='0-31', max_instances=10, second='*')
        self.stock_scheduler.add_job(self.cache_stock_dict, 'cron',
                                     hour='15', minute='0', max_instances=10, second='*')

        # merger scheduler
        self.stock_scheduler.add_job(self.tmp2persistent_delayed, 'cron',
                                     hour='9', minute='15-59', max_instances=1, second='*/30')
        self.stock_scheduler.add_job(self.tmp2persistent_delayed, 'cron',
                                     hour='10,13-14', max_instances=1, second='*/30')
        self.stock_scheduler.add_job(self.tmp2persistent_delayed, 'cron',
                                     hour='11', minute='0-30', max_instances=1, second='*/30')
        self.stock_scheduler.add_job(self.tmp2persistent_delayed, 'cron',
                                     hour='15', minute='0', max_instances=1, second='*/30')

        # save scheduler
        self.stock_scheduler.add_job(self.save_persistent2parquet, 'cron',
                                     hour='15', minute='1', max_instances=1, second='30')

    def start_plan(self):
        self.stock_threading.start()
        self.stock_scheduler.start()

    def pause_plan(self):
        if self.stock_scheduler.state == 2:
            print("plan already paused.")
        elif self.stock_scheduler.state == 0:
            print("plan already stopped")
        else:
            self.stock_scheduler.pause()

    def stop_merger_plan(self):
        self.stock_scheduler.shutdown()
        self.stock_threading.join()
