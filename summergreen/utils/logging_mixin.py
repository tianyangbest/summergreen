# -*- coding: utf-8 -*-
from loguru import logger


class LoggingMixin:
    """
    Convenience super-class to have a logger configured with the class name
    """

    @property
    def log(self) -> logger:
        """
        Returns a logger.
        """
        return logger
