# -*- coding: utf-8 -*-
import datetime
import json
import os

import pandas as pd
import redis
import yaml
from dask import dataframe as dd

with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.yaml"""
) as f:
    stock_config = yaml.full_load(f)
    joint_stock_config = stock_config["tick_dtypes"].copy()
    joint_stock_config["time"] = "object"

with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.yaml"""
) as f:
    base_config = yaml.full_load(f)


def fix_redis_df_bug(original_df: pd.DataFrame):
    original_df.columns = stock_config["tick_dtypes"].keys()[2:]
    original_df = original_df.astype(
        {
            k: stock_config["tick_dtypes"][k]
            for k in stock_config["tick_dtypes"].keys()[2:]
        }
    )
    return original_df


def concat_joint_parquet_list(one_day_parquet_path_list):
    """

    :param one_day_parquet_path_list: path list
    :return: pd.DataFrame()
    Example:
    --------
    >>> st_date = "2020-10-09"
    >>> df = concat_joint_parquet_list(
    >>> glob.glob(f"/mnt/stock_data/tmp_data/{st_date}_*.csv.gz")
    >>> )
    >>> df.to_parquet(f"/mnt/stock_data/stock_tick_current_day/{st_date}.parquet")
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
    ----------
    :param json_file_path:
    :param yaml_file_path:
    Example:
    --------
    >>> json_file2yaml_file(
    >>> "/mnt/project_data/projects/summergreen/summergreen/config/base_config.json",
    >>> "/mnt/project_data/projects/summergreen/summergreen/config/base_config.yaml")
    """
    with open(json_file_path) as f:
        stock_config = json.load(f)
    with open(
        yaml_file_path,
        "w",
    ) as f:
        yaml.dump(stock_config, f, Dumper=yaml.SafeDumper)


def mirror_df2redis(df: pd.DataFrame):
    r = redis.Redis(
        host=base_config["mirror_redis_config"]["host"],
        port=base_config["mirror_redis_config"]["port"],
        db=base_config["mirror_redis_config"]["db"],
        decode_responses=base_config["mirror_redis_config"]["decode_responses"],
    )
    with r.pipeline(transaction=False) as p:
        for k, v in df.iterrows():
            p.hset(str(k[1]), k[0], v.tolist())
            p.execute()
