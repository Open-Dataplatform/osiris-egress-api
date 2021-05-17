"""
Contains dependencies used in several places of the application.
"""
from datetime import datetime
from typing import Optional, Union
import pandas as pd

from pandas import DatetimeIndex
from fastapi import HTTPException
from jaeger_client import Span

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.storage.filedatalake.aio import DataLakeDirectoryClient, DataLakeFileClient, StorageStreamDownloader

from osiris.core.configuration import Configuration
from osiris.core.io import get_file_path_with_respect_to_time_resolution
from osiris.core.enums import TimeResolution

from app.metrics import TracerClass


configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

tracer = TracerClass().get_tracer()


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


async def __download_data(timeslot: datetime,
                          time_resolution: TimeResolution,
                          directory_client: DataLakeDirectoryClient,
                          retrieve_data_span: Span) -> Optional[Union[bytes, str]]:
    with tracer.start_span('retrieve_data_download', child_of=retrieve_data_span) as local_span:
        path = get_file_path_with_respect_to_time_resolution(timeslot, time_resolution, 'data.json')
        local_span.set_tag('path', path)

        file_client: DataLakeFileClient = await __get_file_client(directory_client, path)
        if not file_client:
            return None

        try:
            downloaded_file = await file_client.download_file()
            data = await downloaded_file.readall()
        except HttpResponseError as error:
            message = f'({type(error).__name__}) Problems downloading data file: {error}'
            logger.error(message)
            raise HTTPException(status_code=error.status_code, detail=message) from error

        return data


def __split_into_chunks(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


async def __get_file_client(directory_client: DataLakeDirectoryClient, path):
    file_client: DataLakeFileClient = directory_client.get_file_client(path)
    try:
        await file_client.get_file_properties()
    except ResourceNotFoundError:
        # We return None to indicate that the file doesnt exist.
        return None

    return file_client


async def __download_file(filename: str, directory_client: DataLakeDirectoryClient) -> StorageStreamDownloader:
    file_client = directory_client.get_file_client(filename)
    try:
        downloaded_file = await file_client.download_file()
        return downloaded_file
    except HttpResponseError as error:
        message = f'({type(error).__name__}) File could not be downloaded: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error
