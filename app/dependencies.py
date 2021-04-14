"""
Contains dependencies used in several places of the application.
"""
import configparser
from configparser import ConfigParser
import logging.config
import time
from logging import Logger
from functools import wraps

from prometheus_client import Histogram, Counter


class Configuration:
    """
    Contains methods to obtain configurations for this application.
    """

    def __init__(self, name: str):
        self.config = configparser.ConfigParser()
        self.config.read(['conf.ini', '/etc/osiris/conf.ini', '/etc/osiris-egress/conf.ini'])

        logging.config.fileConfig(fname=self.config['Logging']['configuration_file'], disable_existing_loggers=False)

        self.name = name

    def get_config(self) -> ConfigParser:
        """
        The configuration for the application.
        """
        return self.config

    def get_logger(self) -> Logger:
        """
        A customized logger.
        """
        return logging.getLogger(self.name)


class Metric:
    """
    Class to wrap all metrics for prometheus.
    """
    HISTOGRAM = Histogram('osiris_egress_api', 'Osiris Egress API (method, guid, time)', ['method', 'guid'])
    COUNTER = Counter('osiris_egress_api_method_counter', 'Osiris Egress API counter (method, guid)', ['method', 'guid'])

    @staticmethod
    def count_and_histogram(func):
        """
        Decorator method for metrics on count and histogram
        """
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()

            result = await func(*args, **kwargs)

            time_taken = time.time() - start_time
            Metric.HISTOGRAM.labels(func.__name__, kwargs['guid']).observe(time_taken)
            Metric.COUNTER.labels(func.__name__, kwargs['guid']).inc()
            return result

        return wrapper
