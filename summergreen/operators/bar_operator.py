# -*- coding: utf-8 -*-
from summergreen import LoggingMixin, BaseOperator


class BarOperator(LoggingMixin, BaseOperator):
    def __init__(self):
        super().__init__()
