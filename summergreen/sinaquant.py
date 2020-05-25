from dask import dataframe as dd
import pandas as pd


def merge_from_sina2parquet(tmp_dir, to_dir, date_str, code_str):
    parquet_columns = [
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
        "code",
    ]

    rename_columns = {
        "ask1": "a1_p",
        "ask1_volume": "a1_v",
        "ask2": "a2_p",
        "ask2_volume": "a2_v",
        "ask3": "a3_p",
        "ask3_volume": "a3_v",
        "ask4": "a4_p",
        "ask4_volume": "a4_v",
        "ask5": "a5_p",
        "ask5_volume": "a5_v",
        "bid1": "b1_p",
        "bid1_volume": "b1_v",
        "bid2": "b2_p",
        "bid2_volume": "b2_v",
        "bid3": "b3_p",
        "bid3_volume": "b3_v",
        "bid4": "b4_p",
        "bid4_volume": "b4_v",
        "bid5": "b5_p",
        "bid5_volume": "b5_v",
        "buy": "buy",
        "curr_time_str": "curr_time_str",
        "date": "date",
        "high": "high",
        "low": "low",
        "now": "current",
        "sell": "sell",
        "time": "time",
        "turnover": "volume",
        "volume": "money",
    }
    df = dd.read_parquet(f"{tmp_dir}/{date_str}/code={code_str}/*/*.parquet").compute()
    df = df[df.date == date_str]

    if df.shape[0] > 0:
        df = df.rename(columns=rename_columns).sort_values("curr_time_str")
        df["time"] = pd.to_datetime(df["date"] + " " + df["time"])
        df = df.drop_duplicates(subset=["time"], keep="last")
        df = df.set_index("time")
        df["code"] = code_str
        df = df[parquet_columns]
        # df.to_parquet(f"""{to_dir}/{code_str}/part.{date_str}.parquet""")
        ddf = dd.from_pandas(df, npartitions=1)
        ddf.to_parquet(f"""{to_dir}/{code_str}_{date_str}/""")
