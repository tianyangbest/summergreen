# -*- coding: utf-8 -*-
import pandas as pd


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
