# -*- coding: utf-8 -*-
def redis_value2list(v):

    res = [float(x) for x in v.split(",")]
    return res
