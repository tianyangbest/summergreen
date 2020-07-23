# -*- coding: utf-8 -*-


def cleanup_ticks(df):
    return df[df.current > 0]


def ticks2bar(df):
    return [
        df.current.iloc[0],
        df.current.iloc[-1],
        df.high.max(),
        df.low.min(),
        round(df.money.max() - df.money.min(), 2),
        (df.volume.max() - df.volume.min()),
        round(
            (df.money.max() - df.money.min()) / (df.volume.max() - df.volume.min()), 2,
        ),
    ]


def time_range_cutter_df(df, start, end):
    left_indexer = df.index.searchsorted(start, side="right") - 1
    right_indexer = df.index.searchsorted(end) + 1
    return df.iloc[left_indexer:right_indexer]
