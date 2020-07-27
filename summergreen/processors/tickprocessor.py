# -*- coding: utf-8 -*-
import ray
import numpy as np


@ray.remote
class TickProcessor(object):
    def __init__(self, tick_np):
        self.tick_np = tick_np

    def append_np(self, new_np):
        self.tick_np = np.concatenate([self.tick_np, new_np], axis=0)
