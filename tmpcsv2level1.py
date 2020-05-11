# -*- coding: utf-8 -*-
# Author: Steven Field

import summergreen
import glob
import multiprocessing


def smfs(z):
    summergreen.merge_from_sina2parquet(*z)


if __name__ == '__main__':
    print('start to run!!!')
    smfs_list = []
    for date_path in sorted(glob.glob("""/mnt/project_data/sina_tmpdata/*""")):
        date_str = date_path.split("/")[-1]
        for code_path in glob.glob(f"""/mnt/project_data/sina_tmpdata/{date_str}/*"""):
            code_str = code_path.split("=")[-1]
            smfs_list.append(
                (
                    """/mnt/project_data/sina_tmpdata/""",
                    """/mnt/stock_data/level1_tmp/""",
                    date_str,
                    code_str,
                )
            )

    pool = multiprocessing.Pool(4)
    pool.map(smfs, smfs_list)
