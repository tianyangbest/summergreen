# -*- coding: utf-8 -*-
import json
import os

import yaml
from apscheduler.schedulers.background import BackgroundScheduler

from summergreen.utils.logging_mixin import LoggingMixin


class BaseScheduler(LoggingMixin):
    def __init__(self):
        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.yml"""
        ) as f:
            self._base_config = yaml.full_load(f)

        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.yml"""
        ) as tmp_f:
            self._stock_config = yaml.full_load(tmp_f)

        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/quotation/stock_codes.conf"""
        ) as tmp_f:
            self._stock_codes = json.load(tmp_f)

        self._bs = BackgroundScheduler()
        self._scheduler_name = "基本数据调度"

    def start_now(self):
        self._bs.start()
        self.log.info(f"《---{self._scheduler_name}---》已经初始化")
