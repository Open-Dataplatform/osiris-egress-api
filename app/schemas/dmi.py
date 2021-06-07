"""
Schemas useed for DMI endpoint models.
"""
from enum import Enum

from pydantic import BaseModel


class EDMIWeatherType(str, Enum):
    """
    Enum for DMI weather types, in the same format as present in the data files.
    """
    RADIATION_DIFFUS = 'radiation_diffus'
    RADIATION_GLOBAL = 'radiation_global'
    TEMPERATUR_2M = 'temperatur_2m'
    WIND_DIRECTION_100M = 'wind_direction_100m'
    WIND_DIRECTION_10M = 'wind_direction_10m'
    WIND_SPEED_100M = 'wind_speed_100m'
    WIND_SPEED_10M = 'wind_speed_10m'


class CoordinatesModel(BaseModel):
    """
    Basic model for coordinates with `latitude` and `longitude` fields.
    """
    latitude: float
    longitude: float
