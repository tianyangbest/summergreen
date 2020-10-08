# -*- coding: utf-8 -*-
import datetime


def get_all_timestamp_list(
    start_time: datetime.datetime, end_time: datetime.datetime, seconds_interval: int
):
    return [
        start_time + datetime.timedelta(seconds=i)
        for i in range(0, (end_time - start_time).seconds, seconds_interval)
    ]
