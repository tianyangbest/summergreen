# -*- coding: utf-8 -*-
# Author: Steven Field

from dask import dataframe as dd
import pandas as pd
import os


def merge_from_csvs2parquet(code, csv_source_dir, parquet_target_dir):
    print(f"{code} start to process!")
    file_list = [
        f"{csv_source_dir}{f}"
        for f in os.listdir(csv_source_dir)
        if code in f and f.endswith(".csv")
    ]
    if len(file_list) <= 0:
        print(f"{code} no related csv files!")
        return
    df = dd.read_csv(file_list).compute()
    if df.shape[0] > 0:
        df = df.astype(
            {
                "time": "int",
                "current": "float",
                "high": "float",
                "low": "float",
                "volume": "int",
                "money": "int",
                "a1_p": "float",
                "a2_p": "float",
                "a3_p": "float",
                "a4_p": "float",
                "a5_p": "float",
                "a1_v": "int",
                "a2_v": "int",
                "a3_v": "int",
                "a4_v": "int",
                "a5_v": "int",
                "b1_p": "float",
                "b2_p": "float",
                "b3_p": "float",
                "b4_p": "float",
                "b5_p": "float",
                "b1_v": "int",
                "b2_v": "int",
                "b3_v": "int",
                "b4_v": "int",
                "b5_v": "int",
            }
        )
        df["code"] = code
        df["time"] = pd.to_datetime(df.time.astype(int), format="%Y%m%d%H%M%S")
        df.set_index("time", inplace=True)

        ddf = dd.from_pandas(df, npartitions=1).repartition(freq="Y")
        ddf.to_parquet(f"{parquet_target_dir}{code}")
        print(f"{code} process finished!")
    else:
        print(f"{code} process cancel caused by null data!")


class JoinQuant:
    def __init__(self):
        pass
