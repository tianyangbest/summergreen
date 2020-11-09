# -*- coding: utf-8 -*-
from summergreen.schedulers.k_scheduler import KScheduler
from summergreen.schedulers.sina_scheduler import SinaScheduler
from summergreen.schedulers.joint_scheduler import JointScheduler
from fastapi import FastAPI

# 聚宽数据同步到Postgres服务
joint_scheduler = JointScheduler()
joint_scheduler.start_now()

# 新浪数据同步到Redis服务
fetcher_scheduler = SinaScheduler()
fetcher_scheduler.start_now()

# Redis数据存储同步到k_scheduler服务，k数据每60秒一个bar，延迟1秒计算，每3秒更新一次
k_scheduler_1st = KScheduler(60, 1, 3)
k_scheduler_1st.start_now()

# Redis数据存储同步到k_scheduler服务，k数据每60秒一个bar，延迟75秒计算，每60秒更新一次
k_scheduler_2nd = KScheduler(60, 75, 60)
k_scheduler_2nd.start_now()

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "SummerGreen"}
