# -*- coding: utf-8 -*-
import datetime
import json
import os

import pandas as pd
import redis
import yaml
from dask import dataframe as dd
from sqlalchemy import create_engine

with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.yml"""
) as f:
    stock_config = yaml.full_load(f)

    joint_stock_config = stock_config["tick_dtypes"].copy()
    joint_stock_config["time"] = "object"

with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.yml"""
) as f:
    base_config = yaml.full_load(f)


def fix_redis_df_bug(original_df: pd.DataFrame):
    original_df.columns = list(stock_config["tick_dtypes"].keys())[2:]
    original_df = original_df.astype(
        {
            k: stock_config["tick_dtypes"][k]
            for k in list(stock_config["tick_dtypes"].keys())[2:]
        }
    )
    return original_df


def concat_joint_parquet_list(one_day_parquet_path_list):
    """
    Parameters
    --------
    :param one_day_parquet_path_list: path list
    :return: pd.DataFrame()
    Example:
    --------
    >>> df = concat_joint_parquet_list(
    >>> glob.glob("/mnt/stock_data/tmp_data/2020-10-09_*.csv.gz")
    >>> )
    """
    df = dd.read_csv(
        one_day_parquet_path_list,
        compression="gzip",
        blocksize=None,
        dtype=joint_stock_config,
        parse_dates=["time"],
    ).compute()
    df = df[stock_config["tick_dtypes"].keys()]
    df = df.set_index(["code", "time"], drop=True)
    return df


def json_file2yaml_file(json_file_path, yaml_file_path):
    """
    Parameters
    --------
    :param json_file_path:
    :param yaml_file_path:
    Example:
    --------
    >>> json_file2yaml_file(
    >>> "/mnt/project_data/projects/summergreen/summergreen/config/base_config.json",
    >>> "/mnt/project_data/projects/summergreen/summergreen/config/base_config.yml")
    """
    with open(json_file_path) as f:
        _stock_config = json.load(f)
    with open(
        yaml_file_path,
        "w",
    ) as f:
        yaml.safe_dump(_stock_config, f)


def mirror_df2redis(df: pd.DataFrame):
    r = redis.Redis(
        host=base_config["tick_redis_config"]["host"],
        port=base_config["tick_redis_config"]["port"],
        db=base_config["tick_redis_config"]["db"],
        decode_responses=base_config["tick_redis_config"]["decode_responses"],
    )
    with r.pipeline(transaction=False) as p:
        i = 0
        for k, v in df.iterrows():
            p.hset(str(k[1]), k[0], ",".join([str(i) for i in v.tolist()]))
            i = i + 1
            if i > 10000:
                p.execute()
                i = 0
        p.execute()


def get_last_trade_date(dt):
    base_postgres_engine = create_engine(base_config["base_postgres_engine_str"])
    df = pd.read_sql_query(
        f"""SELECT * FROM base_info.calendar 
        WHERE trade_date < '{dt}' ORDER BY trade_date DESC LIMIT 1""",
        base_postgres_engine,
    )
    return df.loc[0]["trade_date"]
    # last_date = df.iloc[-1]["trade_date"]
    # return last_date


def tick_df2k_df(tick_df: pd.DataFrame, interval_freq, tick_date):
    tick_date_str = str(tick_date)
    base_postgres_engine = create_engine(base_config["base_postgres_engine_str"])
    pre_close_k_day_df = pd.read_sql_query(
        f"""SELECT code, close pre_close FROM
        (SELECT code, close, time, ROW_NUMBER() OVER (PARTITION BY code ORDER BY time DESC) rn
        FROM base_info.k_1day WHERE time < '{tick_date_str}') k
        WHERE rn = 1""",
        base_postgres_engine,
    )
    tick_df.index = tick_df.index.set_levels(
        tick_df.index.levels[1].astype("datetime64[ns]"), level=1
    )
    tick_df = (
        tick_df[
            (tick_df.current > 0)
            & (tick_df.volume > 0)
            & (
                tick_df.index.get_level_values(1)
                >= tick_date.replace(hour=9, minute=30)
            )
        ]
        .sort_index()
        .copy()
    )

    k_df = tick_df.groupby(["code", pd.Grouper(level=1, freq=interval_freq)]).agg(
        open=("current", "first"),
        close=("current", "last"),
        high=("current", "max"),
        low=("current", "min"),
        volume=("volume", "last"),
        money=("money", "last"),
    )
    k_df["volume"] = k_df.groupby("code").volume.diff().fillna(k_df.volume).astype(int)
    k_df["money"] = k_df.groupby("code").money.diff().fillna(k_df.money)
    k_df = k_df.reset_index()
    k_df["time"] = k_df.time.astype(str)
    k_df = k_df.merge(pre_close_k_day_df, on="code", how="left")
    k_df["pre_close"] = k_df.pre_close.fillna(k_df.open)
    return k_df
