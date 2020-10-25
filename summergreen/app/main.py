# -*- coding: utf-8 -*-
from summergreen.schedulers.joint_scheduler import JointScheduler
from fastapi import FastAPI

# 聚宽数据同步
joint_scheduler = JointScheduler()
joint_scheduler.start_now()


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "SummerGreen"}
