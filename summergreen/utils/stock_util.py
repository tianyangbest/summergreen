# -*- coding: utf-8 -*-
import datetime

import json
import pandas as pd
import yaml
from dask import dataframe as dd


def fix_redis_df_bug(original_df: pd.DataFrame):
    fixed_columns = [
        "current",
        "high",
        "low",
        "volume",
        "money",
        "b1_v",
        "b1_p",
        "b2_v",
        "b2_p",
        "b3_v",
        "b3_p",
        "b4_v",
        "b4_p",
        "b5_v",
        "b5_p",
        "a1_v",
        "a1_p",
        "a2_v",
        "a2_p",
        "a3_v",
        "a3_p",
        "a4_v",
        "a4_p",
        "a5_v",
        "a5_p",
    ]
    original_df.columns = fixed_columns
    original_df = original_df[
        [
            "current",
            "high",
            "low",
            "volume",
            "money",
            "a1_p",
            "a2_p",
            "a3_p",
            "a4_p",
            "a5_p",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
            "b1_p",
            "b2_p",
            "b3_p",
            "b4_p",
            "b5_p",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
        ]
    ]
    return original_df


def concat_joint_parquet_list(one_day_parquet_path_list):
    df = dd.read_csv(
        one_day_parquet_path_list,
        compression="gzip",
        dtype={"code": str},
        blocksize=None,
    ).compute()
    df = df[
        [
            "code",
            "time",
            "current",
            "high",
            "low",
            "volume",
            "money",
            "a1_p",
            "a2_p",
            "a3_p",
            "a4_p",
            "a5_p",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
            "b1_p",
            "b2_p",
            "b3_p",
            "b4_p",
            "b5_p",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
        ]
    ]
    df["time"] = (
        df["time"]
        .astype(str)
        .apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d%H%M%S"))
    )
    df = df.set_index(["code", "time"])
    return df


def json_file2yaml_file(json_file_path, yaml_file_path):
    with open(json_file_path) as f:
        stock_config = json.load(f)
    with open(
        yaml_file_path,
        "w",
    ) as f:
        yaml.dump(stock_config, f, Dumper=yaml.SafeDumper)
