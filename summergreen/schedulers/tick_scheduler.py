# -*- coding: utf-8 -*-
import datetime
import queue
import threading

from apscheduler.schedulers.blocking import BlockingScheduler

from summergreen import TickOperator
from summergreen.utils.time_util import get_all_timestamp_list


bs = BlockingScheduler()
q = queue.Queue()
tio = TickOperator()


def q_worker():
    while True:
        i = q.get()
        i[0](*i[1:])
        q.task_done()


threading.Thread(target=q_worker, daemon=True).start()


def initial_today_job():
    q.empty()
    tio.__init__()
    today_datetime = datetime.datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    for t in get_all_timestamp_list(
        today_datetime + datetime.timedelta(hours=11),
        today_datetime + datetime.timedelta(hours=11, minutes=1, seconds=15),
        # today_datetime + datetime.timedelta(hours=9),
        # today_datetime + datetime.timedelta(hours=15, minutes=1, seconds=15),
        1,
    ):
        tmp_time_str = str(t.replace(month=9, day=30) - datetime.timedelta(seconds=15))
        bs.add_job(
            q.put,
            "date",
            run_date=t,
            args=[[tio.update_stock_codes_arr_dict, tmp_time_str]],
            misfire_grace_time=16 * 60 * 60,
        )
    for t in get_all_timestamp_list(
        today_datetime + datetime.timedelta(hours=11),
        today_datetime + datetime.timedelta(hours=11, minutes=1, seconds=15),
        # today_datetime + datetime.timedelta(hours=9),
        # today_datetime + datetime.timedelta(hours=15, minutes=1, seconds=15),
        15,
    ):
        bar_start_time_stamp = (
            t.replace(month=9, day=30) - datetime.timedelta(seconds=30)
        ).timestamp()
        bar_end_time_stamp = (
            t.replace(month=9, day=30) - datetime.timedelta(seconds=15)
        ).timestamp()
        bs.add_job(
            q.put,
            "date",
            run_date=t,
            args=[
                [
                    tio.get_bar_list_by_time,
                    bar_start_time_stamp,
                    bar_end_time_stamp,
                ]
            ],
            misfire_grace_time=16 * 60 * 60,
        )


# bs.add_job(
#     initial_one_day_job,
#     "cron",
#     hour="8",
# )

bs.add_job(
    initial_today_job,
    "date",
    run_date=datetime.datetime.now(),
    misfire_grace_time=24 * 60 * 60,
)

try:
    bs.start()
except (KeyboardInterrupt, SystemExit):
    pass
