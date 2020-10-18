# -*- coding: utf-8 -*-
import datetime

import pandas as pd
from sqlalchemy import create_engine

from summergreen.schedulers.base_scheduler import BaseScheduler
import jqdatasdk as jq

from summergreen.utils.logging_mixin import LoggingMixin
from summergreen.utils.stock_util import get_last_trade_date


class JointScheduler(LoggingMixin, BaseScheduler):
    def __init__(self):
        super().__init__()
        jq.auth(
            self._base_config["joint_auth"]["username"],
            self._base_config["joint_auth"]["password"],
        )
        self._base_postgres_engine = create_engine(
            self._base_config["base_postgres_engine_str"]
        )
        self._bs.add_job(
            self.update_billboard,
            "cron",
            hour="20",
            minute="1",
        )
        self._bs.add_job(
            self.update_margin_trading_short_selling,
            "cron",
            hour="10",
            minute="1",
        )
        self._bs.add_job(
            self.update_money_flow,
            "cron",
            hour="20",
            minute="1",
        )
        self._bs.add_job(
            self.update_stock,
            "cron",
            hour="8",
            minute="1",
        )

    def update_billboard(self, dt=None):
        if dt is None:
            dt = str(datetime.datetime.now().date())
        df = jq.get_billboard_list(None, dt, dt)
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.billboard WHERE day = '{dt}'"""
        )
        df.to_sql(
            "billboard",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )

    def update_margin_trading_short_selling(self, dt=None):
        if dt is None:
            dt = str(get_last_trade_date(str(datetime.datetime.now().date())))
        stock_codes = pd.read_sql_query(
            """
        SELECT full_code FROM base_info.stock
        """,
            self._base_postgres_engine,
        )["full_code"].to_list()
        df: pd.DataFrame = jq.get_mtss(stock_codes, dt, dt)
        df = df.rename(columns={"sec_code": "code"})
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.margin_trading_short_selling WHERE date = '{dt}'"""
        )
        df.to_sql(
            "margin_trading_short_selling",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )

    def update_money_flow(self, dt=None):
        if dt is None:
            dt = str(datetime.datetime.now().date())
        stock_codes = pd.read_sql_query(
            """
        SELECT full_code FROM base_info.stock WHERE type = 'stock'
        """,
            self._base_postgres_engine,
        )["full_code"].to_list()
        df: pd.DataFrame = jq.get_money_flow(stock_codes, dt, dt)
        df = df.rename(columns={"sec_code": "code"})
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.money_flow WHERE date = '{dt}'"""
        )
        df.to_sql(
            "money_flow",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )

    def update_k_1day(self, dt=None):
        if dt is None:
            dt = str(datetime.datetime.now().date())
        stock_codes = pd.read_sql_query(
            """
        SELECT full_code FROM base_info.stock
        """,
            self._base_postgres_engine,
        )["full_code"].to_list()
        df: pd.DataFrame = jq.get_price(
            stock_codes,
            start_date=dt,
            end_date=dt,
            frequency="daily",
            fields=[
                "open",
                "close",
                "high",
                "low",
                "volume",
                "money",
                "high_limit",
                "low_limit",
                "avg",
                "pre_close",
            ],
            panel=False,
        )
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.k_1day WHERE time = '{dt}'"""
        )
        df.to_sql(
            "k_1day",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )

    def update_stock(self):
        df: pd.DataFrame = jq.get_all_securities(
            types=["stock", "fund", "stock", "etf", "lof", "fja", "fjb"], date=None
        )
        df["full_code"] = df.index
        df["market"] = df.full_code.map(lambda x: x.split(".")[1])
        df["code"] = df.full_code.map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(f"""DELETE FROM base_info.stock""")
        df.to_sql(
            "stock",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )

    def start_now(self):
        try:
            self._bs.start()
        except (KeyboardInterrupt, SystemExit):
            pass


if __name__ == "__main__":
    jts = JointScheduler()
    dt = "2020-10-15"

    jts.update_stock()
    jts.update_k_1day(dt)
    jts.update_billboard(dt)
    jts.update_money_flow(dt)
    jts.update_margin_trading_short_selling(dt)
