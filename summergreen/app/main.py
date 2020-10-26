# -*- coding: utf-8 -*-
from summergreen.schedulers.sina_scheduler import SinaScheduler
from summergreen.schedulers.joint_scheduler import JointScheduler
from fastapi import FastAPI

# 聚宽数据同步到Postgres服务
joint_scheduler = JointScheduler()
joint_scheduler.start_now()

# 新浪数据同步到Redis服务
fetcher_scheduler = SinaScheduler()
fetcher_scheduler.start_now()

# Redis数据存储


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "SummerGreen"}
