# -*- coding: utf-8 -*-
import json
import os


class BaseOperator:
    def __init__(self):
        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.json"""
        ) as f:
            self._base_config = json.load(f)
        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.json"""
        ) as tmp_f:
            self._stock_config = json.load(tmp_f)
        with open(
            f"""{os.path.dirname(os.path.dirname(__file__))}/fetchers/quotation/stock_codes.conf"""
        ) as tmp_f:
            self._stock_codes = json.load(tmp_f)
