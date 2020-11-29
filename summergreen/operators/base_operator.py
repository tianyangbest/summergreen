# -*- coding: utf-8 -*-
import json
import os
import yaml
from sqlalchemy import create_engine


class BaseOperator:
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
        self._base_postgres_engine = create_engine(
            self._base_config["base_postgres_engine_str"]
        )
