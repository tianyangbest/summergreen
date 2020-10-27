# -*- coding: utf-8 -*-
import datetime

import jqdatasdk as jq
import pandas as pd
from sqlalchemy import create_engine

from summergreen.schedulers.base_scheduler import BaseScheduler


class JointScheduler(BaseScheduler):
    def __init__(self):
        super().__init__()
        self._jq = jq
        self._jq.auth(
            self._base_config["joint_auth"]["username"],
            self._base_config["joint_auth"]["password"],
        )
        self._base_postgres_engine = create_engine(
            self._base_config["base_postgres_engine_str"]
        )
        self._bs.add_job(
            self.update_k_1day,
            "cron",
            hour="15",
            minute="10",
        )
        self._bs.add_job(
            self.update_billboard,
            "cron",
            hour="20",
            minute="1",
        )
        self._bs.add_job(
            self.update_money_flow,
            "cron",
            hour="20",
            minute="2",
        )
        self._bs.add_job(
            self.update_margin_trading_short_selling,
            "cron",
            hour="10",
            minute="1",
        )
        self._bs.add_job(
            self.update_stock,
            "cron",
            hour="8",
            minute="1",
        )
        self._bs.add_job(
            self.update_concepts,
            "cron",
            hour="8",
            minute="2",
        )

    def update_billboard(self):
        start_date = datetime.datetime.strftime(
            self._base_postgres_engine.execute(
                """SELECT max(day) FROM base_info.billboard"""
            ).fetchall()[0][0]
            + datetime.timedelta(days=1),
            "%Y-%m-%d",
        )
        end_date = str(datetime.datetime.now().date())
        df = jq.get_billboard_list(None, start_date, end_date)
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        if df.shape[0] == 0:
            self.log.warning(f"龙虎榜无更新")
            return
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.billboard 
                WHERE day between '{start_date}' AND '{end_date}'"""
        )
        df.to_sql(
            "billboard",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )
        self.log.info(f"龙虎榜已更新")

    def update_margin_trading_short_selling(self):
        start_date = datetime.datetime.strftime(
            self._base_postgres_engine.execute(
                """SELECT max(date) FROM base_info.margin_trading_short_selling"""
            ).fetchall()[0][0]
            + datetime.timedelta(days=1),
            "%Y-%m-%d",
        )
        end_date = str(datetime.datetime.now().date())
        stock_codes = pd.read_sql_query(
            """
            SELECT full_code FROM base_info.stock""",
            self._base_postgres_engine,
        )["full_code"].to_list()
        df: pd.DataFrame = jq.get_mtss(stock_codes, start_date, end_date)
        if df.shape[0] == 0:
            self.log.warning(f"融资融券信息无更新")
            return
        df = df.rename(columns={"sec_code": "code"})
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.margin_trading_short_selling 
                WHERE date between '{start_date}' AND '{end_date}'"""
        )
        df.to_sql(
            "margin_trading_short_selling",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )
        self.log.info("融资融券信息已更新")

    def update_money_flow(self):
        start_date = datetime.datetime.strftime(
            self._base_postgres_engine.execute(
                """SELECT max(date) FROM base_info.money_flow"""
            ).fetchall()[0][0]
            + datetime.timedelta(days=1),
            "%Y-%m-%d",
        )
        end_date = str(datetime.datetime.now().date())
        stock_codes = pd.read_sql_query(
            """
        SELECT full_code FROM base_info.stock WHERE type = 'stock'
        """,
            self._base_postgres_engine,
        )["full_code"].to_list()
        df: pd.DataFrame = jq.get_money_flow(stock_codes, start_date, end_date)
        if df.shape[0] == 0:
            self.log.warning(f"股票资金流向信息无更新")
            return
        df = df.rename(columns={"sec_code": "code"})
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.money_flow WHERE date between '{start_date}' AND '{end_date}'"""
        )
        df.to_sql(
            "money_flow",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )
        self.log.info("股票资金流向信息已更新")

    def update_k_1day(self):
        start_date = datetime.datetime.strftime(
            self._base_postgres_engine.execute(
                """SELECT max(date) FROM base_info.money_flow"""
            ).fetchall()[0][0]
            + datetime.timedelta(days=1),
            "%Y-%m-%d",
        )
        end_date = str(datetime.datetime.now().date())
        stock_codes = pd.read_sql_query(
            """
        SELECT full_code FROM base_info.stock
        """,
            self._base_postgres_engine,
        )["full_code"].to_list()
        df: pd.DataFrame = jq.get_price(
            stock_codes,
            start_date=start_date,
            end_date=end_date,
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
        if df.shape[0] == 0:
            self.log.warning(f"日k线信息无更新")
            return
        df["code"] = df["code"].map(lambda x: x.split(".")[0])
        df["update_time"] = datetime.datetime.now()
        self._base_postgres_engine.execute(
            f"""DELETE FROM base_info.k_1day WHERE time between '{start_date}' AND '{end_date}'"""
        )
        df = df[(~df.open.isna()) & (df.volume > 0)]
        df.to_sql(
            "k_1day",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )
        self.log.info("日k线信息已更新")

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
        self.log.info("股票名单信息已更新")

    def update_concepts(self):
        latest_date = datetime.datetime.strftime(
            self._base_postgres_engine.execute(
                "SELECT max(concept_start_date) FROM base_info.concepts"
            ).fetchall()[0][0],
            "%Y-%m-%d",
        )

        category_df_list = []
        for ca, v in {
            "sw_l1": "申万一级行业",
            "sw_l2": "申万二级行业",
            "sw_l3": "申万三级行业",
            "jq_l1": "聚宽一级行业",
            "jq_l2": "聚宽二级行业",
            "zjw": "证监会行业",
        }.items():
            zjw_df = (
                self._jq.get_industries(name=ca, date=None)
                .sort_values("start_date")
                .reset_index()
                .rename(
                    columns={
                        "index": "concept_code",
                        "start_date": "concept_start_date",
                        "name": "concept_name",
                    }
                )
            )
            zjw_df["category"] = ca
            zjw_df["category_name"] = v
            category_df_list.append(zjw_df)

        gn_df = (
            self._jq.get_concepts()
            .sort_values("start_date")
            .reset_index()
            .rename(
                columns={
                    "index": "concept_code",
                    "start_date": "concept_start_date",
                    "name": "concept_name",
                }
            )
        )
        gn_df["category"] = "gn"
        gn_df["category_name"] = "概念板块"
        category_df_list.append(gn_df)
        category_df = pd.concat(category_df_list, ignore_index=True, sort=False)
        category_df = category_df[category_df.concept_start_date > latest_date].copy()
        code_df_list = []
        for cc in category_df[category_df.category == "gn"].concept_code.tolist():
            code_df_list.append(
                pd.DataFrame(
                    [[cc, c.split(".")[0]] for c in self._jq.get_concept_stocks(cc)],
                    columns=["concept_code", "code"],
                )
            )
        for cc in category_df[category_df.category != "gn"].concept_code.tolist():
            code_df_list.append(
                pd.DataFrame(
                    [[cc, c.split(".")[0]] for c in self._jq.get_industry_stocks(cc)],
                    columns=["concept_code", "code"],
                )
            )
        if len(code_df_list) == 0:
            self.log.warning(f"概念信息无更新内容")
            return
        category_code_rel_df: pd.DataFrame = pd.concat(code_df_list)
        for concept_code in category_df.concept_code.to_list():
            self._base_postgres_engine.execute(
                f"DELETE FROM base_info.concepts WHERE concept_code = '{concept_code}'"
            )

        new_concepts_df: pd.DataFrame = category_code_rel_df.merge(
            category_df, left_on="concept_code", right_on="concept_code"
        )
        new_concepts_df["update_time"] = datetime.datetime.now()
        new_concepts_df.to_sql(
            "concepts",
            self._base_postgres_engine,
            schema="base_info",
            index=False,
            if_exists="append",
        )
        self.log.info("概念信息已更新")

    def update_all(self):
        self.update_stock()
        self.update_k_1day()
        self.update_billboard()
        self.update_margin_trading_short_selling()
        self.update_money_flow()
        self.update_concepts()


if __name__ == "__main__":
    js = JointScheduler()
    js.start_now()
