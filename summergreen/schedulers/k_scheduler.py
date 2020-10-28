# -*- coding: utf-8 -*-
import datetime
import queue
import threading

from summergreen.operators.k_operator import KOperator
from summergreen.schedulers.base_scheduler import BaseScheduler
from summergreen.utils.time_util import get_all_timestamp_list


class KScheduler(BaseScheduler):
    def __init__(self):
        super().__init__()
        self._scheduler_name = "K线数据调度"
        self._q = queue.Queue()
        self._ko = KOperator()
        threading.Thread(target=self.q_worker, daemon=True).start()
        self._bs.add_job(
            self.initial_today_job,
            "cron",
            hour="8",
            minute="30",
            max_instances=1,
            next_run_time=datetime.datetime.now().replace(hour=0),
            misfire_grace_time=15 * 60 * 60,
        )

    def q_worker(self):
        while True:
            i = self._q.get()
            i[0](*i[1:])
            self._q.task_done()

    def initial_today_job(self):
        self._q.empty()
        self._ko.__init__()
        today_datetime = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        for t in get_all_timestamp_list(
            today_datetime + datetime.timedelta(hours=9),
            today_datetime + datetime.timedelta(hours=15, minutes=10),
            3,
        ):
            tmp_time = t - datetime.timedelta(seconds=15)
            self._bs.add_job(
                self._q.put,
                "date",
                run_date=t,
                args=[[self._ko.update_stock_codes_arr_dict, tmp_time]],
                misfire_grace_time=10 * 60 * 60,
            )

        for t in get_all_timestamp_list(
            today_datetime + datetime.timedelta(hours=9),
            today_datetime + datetime.timedelta(hours=15, minutes=10),
            15,
        ):
            bar_start_time = t - datetime.timedelta(seconds=30)
            bar_end_time = t - datetime.timedelta(seconds=15)
            self._bs.add_job(
                self._q.put,
                "date",
                run_date=t,
                args=[
                    [
                        self._ko.update_k_list_by_time,
                        bar_start_time,
                        bar_end_time,
                    ]
                ],
                misfire_grace_time=10 * 60 * 60,
            )
        self.log.info("K线数据调度今日调度已经初始化")

        self._bs.add_job(
            self._ko.redis2df2parquet,
            "date",
            run_date=today_datetime + datetime.timedelta(hours=15, minutes=11),
            args=[
                f"""{today_datetime.strftime("%Y-%m-%d")}*""",
                f"""{self._base_config['to_tick_day_parquet_dir']}/{today_datetime.strftime("%Y-%m-%d")}.parquet""",
            ],
            misfire_grace_time=10 * 60 * 60,
        )
        self.log.info("K线数据调度今日调度已经初始化")
