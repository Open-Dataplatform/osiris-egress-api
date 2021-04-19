"""
Contains dependencies used in several places of the application.
"""
import configparser
from configparser import ConfigParser
import logging.config
import time
from logging import Logger
from functools import wraps

from jaeger_client import Config
from jaeger_client.metrics.prometheus import PrometheusMetricsFactory
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
    COUNTER = Counter('osiris_egress_api_method_counter',
                      'Osiris Egress API counter (method, guid)',
                      ['method', 'guid'])

    @staticmethod
    def histogram(func):
        """
        Decorator method for metrics of type histogram
        """

        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()

            result = await func(*args, **kwargs)

            time_taken = time.time() - start_time
            Metric.HISTOGRAM.labels(func.__name__, kwargs['guid']).observe(time_taken)
            return result

        return wrapper

    @staticmethod
    def counter(func):
        """
        Decorator method for metrics of type counter
        """
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            Metric.COUNTER.labels(func.__name__, kwargs['guid']).inc()
            return result

        return wrapper


def singleton(cls, *args, **kw):
    """
    Function needed to create a singleton of a class
    """
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


@singleton
class TracerClass:  # pylint: disable=too-few-public-methods
    """
    Used to get a tracer for Osiris-Egress-API.
    Note: There can only be one tracers, hence we need to make a singleton class
    """
    def __init__(self, service='osiris_egress_api'):
        configuration = Configuration(__file__)
        config = configuration.get_config()

        reporting_host = config['Jaeger Agent']['reporting_host']
        reporting_port = config['Jaeger Agent']['reporting_port']

        tracer_config = Config(
            config={
                'sampler': {
                    'type': 'const',
                    'param': 1,
                },
                'local_agent': {
                    # If Jaeger backend is not on localhost - but not recommended as UDP does not guarantee delivery
                    # See: https://github.com/jaegertracing/jaeger-client-python
                    # 'reporting_host': '',
                    # 'reporting_port': '',
                },
                'logging': True,
            },
            service_name=service,
            validate=True,
            # Adds some metrics to Prometheus
            metrics_factory=PrometheusMetricsFactory(service_name_label='osiris_egress_api')
        )
        if reporting_host != 'localhost':
            tracer_config['local_agent']['reporting_host'] = reporting_host
            tracer_config['local_agent']['reporting_port'] = reporting_port

        self.tracer = tracer_config.initialize_tracer()

    def get_tracer(self):
        """
        Returns the tracer
        """
        return self.tracer
