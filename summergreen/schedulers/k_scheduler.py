# -*- coding: utf-8 -*-
import datetime
import queue
import threading

from summergreen.operators.k_operator import KOperator
from summergreen.schedulers.base_scheduler import BaseScheduler
from summergreen.utils.time_util import get_all_timestamp_list


class KScheduler(BaseScheduler):
    def __init__(self, interval_seconds, lag_seconds, frequency_seconds):
        """
        interval_seconds 必须大于等于frequency_seconds，且可以被interval_seconds整除。
        :param interval_seconds: K线数据interval_seconds汇总一次
        :param lag_seconds: 延迟时间，基于frequency_seconds
        :param frequency_seconds: 每frequency_seconds触发一次计算。
        """

        if (
            interval_seconds < frequency_seconds
            or interval_seconds % frequency_seconds != 0
        ):
            self.log.error(
                f"interval_seconds: {interval_seconds} 必须大于等于 frequency_seconds：{frequency_seconds}"
                f"，且frequency_seconds可以被interval_seconds整除"
            )
            raise

        super().__init__()
        self._scheduler_name = "K线数据调度"
        self._q = queue.Queue()
        self._ko = KOperator()
        self._interval_seconds = interval_seconds
        self._lag_seconds = lag_seconds
        self._frequency_seconds = frequency_seconds
        self._today_datetime = None
        threading.Thread(target=self.q_worker, daemon=True).start()
        self._bs.add_job(
            self.initial_date_job,
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

    def initial_date_job(self, today_datetime=None):
        self._q.empty()

        if today_datetime is None:
            self._today_datetime = datetime.datetime.now()
        else:
            self._today_datetime = today_datetime
        self._today_datetime = self._today_datetime.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        self._ko.__init__(self._today_datetime)
        for t in get_all_timestamp_list(
            self._today_datetime + datetime.timedelta(hours=9, minutes=30),
            self._today_datetime + datetime.timedelta(hours=11, minutes=30),
            self._frequency_seconds,
        ) + get_all_timestamp_list(
            self._today_datetime + datetime.timedelta(hours=13),
            self._today_datetime + datetime.timedelta(hours=15),
            self._frequency_seconds,
        ):
            k_start_time = datetime.datetime.fromtimestamp(
                t.timestamp() // self._interval_seconds * self._interval_seconds
            )
            k_end_time = k_start_time + datetime.timedelta(
                seconds=self._interval_seconds
            )
            run_time = (
                t
                + datetime.timedelta(seconds=self._lag_seconds)
                + datetime.timedelta(seconds=self._frequency_seconds)
            )
            self.log.info(
                f"""\n k_start_time:{k_start_time}"""
                f"""\n k_end_time: {k_end_time}"""
                f"""\n run_time:{run_time}"""
            )

            self._bs.add_job(
                self._q.put,
                "date",
                run_date=run_time,
                args=[[self._ko.uptime_update2redis, k_start_time, k_end_time]],
                misfire_grace_time=15 * 60 * 60,
            )
