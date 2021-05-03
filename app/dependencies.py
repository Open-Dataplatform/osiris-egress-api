"""
Contains dependencies used in several places of the application.
"""
import time
from datetime import datetime
from functools import wraps

from jaeger_client import Config
from jaeger_client.metrics.prometheus import PrometheusMetricsFactory
from osiris.core.configuration import Configuration
from osiris.core.enums import TimeResolution
import pandas as pd
from pandas import DatetimeIndex
from prometheus_client import Histogram, Counter


def __get_all_dates_to_download(from_date: datetime, to_date: datetime,
                                time_resolution: TimeResolution) -> DatetimeIndex:
    # Get all the dates we need
    if time_resolution == TimeResolution.NONE:
        # We handle this case by making a DatetimeIndex containing one element. We will ignore the
        # date anyway.
        return pd.date_range(from_date.strftime("%Y-%m-%d"), from_date.strftime("%Y-%m-%d"), freq='D')
    if time_resolution == TimeResolution.YEAR:
        return pd.date_range(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), freq='Y')
    if time_resolution == TimeResolution.MONTH:
        return pd.date_range(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), freq='M')
    if time_resolution == TimeResolution.DAY:
        return pd.date_range(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), freq='D')
    if time_resolution == TimeResolution.HOUR:
        return pd.date_range(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), freq='H')
    if time_resolution == TimeResolution.MINUTE:
        return pd.date_range(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), freq='T')

    raise ValueError('(ValueError) Unknown time resolution given.')


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

        local_agent = {}  # Default empty - if run on localhost
        if reporting_host != 'localhost':
            local_agent['reporting_host'] = reporting_host
            local_agent['reporting_port'] = reporting_port

        tracer_config = Config(
            config={
                'sampler': {
                    'type': 'const',
                    'param': 1,
                },
                'local_agent': local_agent,
                'logging': True,
            },
            service_name=service,
            validate=True,
            # Adds some metrics to Prometheus
            metrics_factory=PrometheusMetricsFactory(service_name_label='osiris_egress_api')
        )

        self.tracer = tracer_config.initialize_tracer()

    def get_tracer(self):
        """
        Returns the tracer
        """
        return self.tracer
