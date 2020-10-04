# -*- coding: utf-8 -*-
import os
import json
import ray
from apscheduler.schedulers.blocking import BlockingScheduler
from summergreen.analysers import Ranger
from summergreen.loaders import sinastockloader

with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/config/base_config.json"""
) as f:
    redis_config = json.load(f)["redis_config"]
with open(
    f"""{os.path.dirname(os.path.dirname(__file__))}/fetchers/quotation/stock_codes.conf"""
) as f:
    stock_codes = json.load(f)["stock"]

sl = sinastockloader.SinaStockLoader(
    redis_host=redis_config["redis_host"],
    redis_port=redis_config["redis_port"],
    redis_db=redis_config["redis_db"],
)

base_json_np = sl.redis2json("2020-09-29 11:00:*")

ray.init()
chunk_size = 600
stock_chunk_codes = [
    stock_codes[x : x + chunk_size] for x in range(0, len(stock_codes), chunk_size)
]
ranger_list = [Ranger.remote(x) for x in stock_chunk_codes]
[r.merge_new_ranger_dict.remote(base_json_np) for r in ranger_list]
ranger_dict_list = [
    r.get_ranger_dict_range_ticks2bar.remote(
        "2020-09-29 09:00:00", "2020-07-27 15:00:00"
    )
    for r in ranger_list
]

ray.shutdown()

# bs = BlockingScheduler()
# print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))
# try:
#     bs.start()
# except (KeyboardInterrupt, SystemExit):
#     pass
