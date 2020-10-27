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
            misfire_grace_time=60 * 60 * 23,
        )

    def q_worker(self):
        while True:
            i = self._q.get()
            i[0](*i[1:])
            self._q.task_done()

    def initial_today_job(self):
        self.log.info("K线数据调度今日调度已经初始化")
        self._q.empty()
        self._ko.__init__()
        today_datetime = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        for t in get_all_timestamp_list(
            today_datetime + datetime.timedelta(hours=9),
            today_datetime + datetime.timedelta(hours=15, minutes=10),
            1,
        ):
            tmp_time = t - datetime.timedelta(seconds=14)
            self._bs.add_job(
                self._q.put,
                "date",
                run_date=t,
                args=[[self._ko.update_stock_codes_arr_dict, tmp_time]],
                misfire_grace_time=8 * 60 * 60,
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
                misfire_grace_time=8 * 60 * 60,
            )
