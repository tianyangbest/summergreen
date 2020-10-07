# -*- coding: utf-8 -*-
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.utils.time_util import get_all_timestamp_list


def test(x, t):
    print("{0}~{1}~{2}".format(x, t, datetime.datetime.now()))


bs = BlockingScheduler()


def initial_one_day_job():
    today_date = datetime.datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    merger_time_list = get_all_timestamp_list(
        today_date + datetime.timedelta(hours=9, minutes=10),
        today_date + datetime.timedelta(hours=15, minutes=1),
        1,
    )
    for t in merger_time_list:
        bs.add_job(
            test,
            "date",
            run_date=t,
            args=["merger", t],
            misfire_grace_time=6 * 60 * 60,
        )

    ranger15_time_list = get_all_timestamp_list(
        today_date + datetime.timedelta(hours=9, minutes=30),
        today_date + datetime.timedelta(hours=15),
        15,
    )
    for t in ranger15_time_list:
        bs.add_job(
            test,
            "date",
            run_date=t,
            args=["ranger15", t],
            misfire_grace_time=6 * 60 * 60,
        )


# bs.add_job(
#     initial_one_day_job,
#     "cron",
#     hour="9",
# )
bs.add_job(
    initial_one_day_job,
    "date",
    run_date=datetime.datetime.now(),
    misfire_grace_time=24 * 60 * 60,
)

try:
    bs.start()
except (KeyboardInterrupt, SystemExit):
    pass
