"""
Implements endpoints for DMI Weather data.
"""
from io import BytesIO
from typing import List, Dict, Any

from fastapi import APIRouter, Security
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration

from ..dependencies import __get_filesystem_client, __check_directory_exist, __get_filepaths, __download_file
from ..metrics import Metric
from ..schemas.dmi import EDMIWeatherType, CoordinatesModel

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['dmi'])


@router.get('/dmi/{year}/{month}/{day}/{hour}/{weather_type}', response_class=StreamingResponse)
@Metric.histogram
async def download_dmi_datetime_type(year: int,  # pylint: disable=too-many-arguments
                                     month: int,
                                     day: int,
                                     hour: int,
                                     weather_type: EDMIWeatherType,
                                     token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download the parquet file for a specific weather type on the given date and hour.
    """
    logger.debug('download DMI Datetime/Type requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        datetime_type_guid = config['DMI']['datetime_type_guid']
        path = f'{datetime_type_guid}/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/'

        stream = await __get_file_stream_for_dmi_dt_type_file(path, weather_type, filesystem_client)

        return StreamingResponse(stream, media_type='application/octet-stream')


@router.get('/dmi/{weather_type}/', response_model=List[CoordinatesModel])
@Metric.histogram
async def get_dmi_coords_for_weather_type(weather_type: EDMIWeatherType,
                                          token: str = Security(access_token_header)) -> List[Dict[str, Any]]:
    """
    Returns available coordinates for the given weather_type.
    """
    # Function is slow, can probably be optimized if needed
    logger.debug('get DMI coords for weather type requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        datetime_type_guid = config['DMI']['type_coordinate_guid']
        path = f'{datetime_type_guid}/weather_type={weather_type.value}'

        result = await __get_coordinates_for_dmi_weather_type(path, filesystem_client)

        return result


@router.get('/dmi/{weather_type}/{lat}/{lon}/', response_class=JSONResponse)
@Metric.histogram
async def get_dmi_years_for_weather_type_and_coords(weather_type: EDMIWeatherType, lat: float, lon: float,
                                                    token: str = Security(access_token_header)) -> JSONResponse:
    """
    Returns available years for the given weather_type and coordinates.
    """
    logger.debug('get DMI years for weather type and coords requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        guid = config['DMI']['type_coordinate_guid']
        path = f'{guid}/weather_type={weather_type.value}/lat={lat:.2f}/lon={lon:.2f}'

        result = await __get_years_for_dmi_weather_type_and_coords(path, filesystem_client)

        return JSONResponse(result)


@router.get('/dmi/{weather_type}/{lat}/{lon}/{year}', response_class=StreamingResponse)
@Metric.histogram
async def download_dmi_weather_type_coords(weather_type: EDMIWeatherType, lat: float, lon: float, year: int,
                                           token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download the parquet file for a specific weather type, coordinate and year.
    """
    logger.debug('download DMI Type/Coords requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        guid = config['DMI']['type_coordinate_guid']
        path = f'{guid}/weather_type={weather_type.value}/lat={lat:.2f}/lon={lon:.2f}'

        stream = await __get_file_stream_for_dmi_type_coords_file(path, year, filesystem_client)

        return StreamingResponse(stream, media_type='application/octet-stream')


async def __get_file_stream_for_dmi_type_coords_file(path: str, year: int, filesystem_client):
    guid = config['DMI']['type_coordinate_guid']
    directory_client = filesystem_client.get_directory_client(guid)
    await __check_directory_exist(directory_client)

    paths = __get_filepaths(path, filesystem_client)
    async for filepath in paths:
        _, filename = filepath.rsplit('/', maxsplit=1)
        file_year = filename[0:4]
        if str(year) == file_year:
            file_download = await __download_file(filepath, filesystem_client)
            file_content = await file_download.readall()
            return BytesIO(file_content)


async def __get_file_stream_for_dmi_dt_type_file(path: str, weather_type: EDMIWeatherType, filesystem_client):
    guid = config['DMI']['datetime_type_guid']
    directory_client = filesystem_client.get_directory_client(guid)
    await __check_directory_exist(directory_client)

    paths = __get_filepaths(path, filesystem_client)
    async for filepath in paths:
        _, filename = filepath.rsplit('/', maxsplit=1)
        if weather_type.value in filename:
            file_download = await __download_file(filepath, filesystem_client)
            file_content = await file_download.readall()
            return BytesIO(file_content)


async def __get_years_for_dmi_weather_type_and_coords(path: str, filesystem_client):
    filepaths = __get_filepaths(path, filesystem_client)
    result = []
    async for filepath in filepaths:
        year = filepath.rsplit('/', maxsplit=1)[-1][0:4]
        result.append(int(year))
    return list(set(result))    # Return unique entries only


async def __get_coordinates_for_dmi_weather_type(path: str, filesystem_client):
    result = []
    paths = filesystem_client.get_paths(path=path)
    async for filepath in paths:
        if not filepath.is_directory:
            continue
        if filepath.name not in result and 'lat=' in filepath.name and 'lon=' in filepath.name:
            lat, lon = filepath.name.split('/')[-2:]
            lat = lat.replace('lat=', '')
            lon = lon.replace('lon=', '')
            result.append({'latitude': lat, 'longitude': lon})
    return result
