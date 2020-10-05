# -*- coding: utf-8 -*-
import datetime


def get_all_timestamp_list(start_time, end_time, seconds_interval):
    return [
        start_time + datetime.timedelta(seconds=i)
        for i in range(0, (end_time - start_time).seconds, seconds_interval)
    ]
