# -*- coding: utf-8 -*-
import ray


@ray.remote
class TickProcessor(object):
    def __init__(self, tick_np):
        self.tick_np = tick_np

