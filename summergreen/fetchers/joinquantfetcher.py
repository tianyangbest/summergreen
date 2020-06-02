# -*- coding: utf-8 -*-
# Author: Steven Field


import json
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os

ProgressBar().register()


def joinquant_csv2parquet(csv_source_dir, parquet_dir, date_str):
    ddf = dd.read_csv(f"""{csv_source_dir}/{date_str}/*.csv""")
    df = ddf.compute(scheduler="processes")
    df['code'] = df.code.map(lambda x: x.split(".")[0])
    with open(f"""{os.path.dirname(os.path.dirname(__file__))}/config/stock_config.json""") as f:
        sina_datatype = json.load(f)["sina_datatype"]
        sina_columns = list(sina_datatype.keys())
    df = df[sina_columns]
    df = df.astype(sina_datatype)
    df = df.set_index(["code", "time"])
    df = df.loc[~df.index.duplicated(keep="first")]
    df.to_parquet(f"{parquet_dir}/{date_str}.parquet")
