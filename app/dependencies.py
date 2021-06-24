"""
Contains dependencies used in several places of the application.
"""
import asyncio
import os
from datetime import datetime
from http import HTTPStatus
from typing import Optional, Union, AsyncIterable
from io import BytesIO

import pandas as pd
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.storage.filedatalake.aio import DataLakeDirectoryClient, DataLakeFileClient, StorageStreamDownloader, \
    FileSystemClient
from fastapi import HTTPException
from jaeger_client import Span
from osiris.core.azure_client_authorization import AzureCredentialAIO
from osiris.core.configuration import Configuration
from osiris.core.enums import TimeResolution
from osiris.core.io import get_file_path_with_respect_to_time_resolution, parse_date_str
from pandas import DatetimeIndex

from app.metrics import TracerClass

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

tracer = TracerClass().get_tracer()


async def __get_filesystem_client(token: str) -> FileSystemClient:
    account_url = config['Azure Storage']['account_url']
    filesystem_name = config['Azure Storage']['filesystem_name']
    credential = AzureCredentialAIO(token)

    return FileSystemClient(account_url, filesystem_name, credential=credential)


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
    except HttpResponseError as error:
        message = f'({type(error).__name__}) Problems checking if file exist: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error

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


async def __check_directory_exist(directory_client: DataLakeDirectoryClient):
    try:
        await directory_client.get_directory_properties()
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) The given dataset doesnt exist: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error


async def __get_filepaths(path, filesystem_client) -> AsyncIterable[str]:
    """ Returns a list of paths below `path` that are not directories. """
    paths = filesystem_client.get_paths(path=path)
    async for path in paths:
        if not path.is_directory:
            yield path.name


def __get_path_for_arbitrary_file(file_date: datetime, guid: str, filesystem_client: FileSystemClient) -> str:
    path = f'{guid}/year={file_date.year:02d}/month={file_date.month:02d}/day={file_date.day:02d}'

    try:
        files = filesystem_client.get_paths(path=path)
        file = files.next()
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) Data doesnt exist for the given date: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error

    filename = os.path.relpath(file.name, guid)  # we remove GUID from the path

    return filename


def __parse_date_arguments(from_date, to_date):
    if from_date is None and to_date is None:
        return datetime(1970, 1, 1), None, TimeResolution.NONE
    if to_date is None:
        from_date_obj, time_resolution = parse_date_str(from_date)
        return from_date_obj, None, time_resolution
    if len(from_date) != len(to_date):
        message = 'Malformed request syntax: len(from_date) != len(to_date)'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    from_date_obj, time_resolution = parse_date_str(from_date)
    to_date_obj, _ = parse_date_str(to_date)
    return from_date_obj, to_date_obj, time_resolution


# async def __get_settings(directory_client: DataLakeDirectoryClient) -> Dict:
#     try:
#         file_client = directory_client.get_file_client('settings.json')
#         downloaded_file = await file_client.download_file()
#         settings_data = await downloaded_file.readall()
#         return json.loads(settings_data)
#     except ResourceNotFoundError as error:
#         message = f'({type(error).__name__}) Problems downloading setting.json: {error}'
#         logger.error(message)
#         raise HTTPException(status_code=error.status_code, detail=message) from error


async def __download_blob_to_stream(blob_name, token):
    """Returns a BytesIO of the blob

    Args:
        blob_name (str): Name of the blob including dataset_guid
        token (str): Auth token

    Raises:
        HTTPException: If file isnt found

    Returns:
        [BytesIO]: BytesIO containing the blob
    """
    try:
        dataset_id, file_path = blob_name.split("/", maxsplit=1)
    except ValueError as error:
        raise HTTPException(status_code=400,
                            detail="Wrong blob name format. Expects: <dataset_id>/<file_path>") from error

    async with await __get_filesystem_client(token) as filesystem_client:
        directory_client = filesystem_client.get_directory_client(dataset_id)

        try:
            file_client = await __get_file_client(directory_client, file_path)
            if file_client is None:
                raise ResourceNotFoundError()
            byte_stream = BytesIO()
            storage_stream = await file_client.download_file()
            await storage_stream.readinto(byte_stream)
            byte_stream.seek(0)
        except ResourceNotFoundError as error:
            raise HTTPException(status_code=404, detail="File not found") from error

    return byte_stream


async def __download_streams(blob_name_prefix, token):
    filesystem_client = await __get_filesystem_client(token)

    blob_names = await __get_filepaths(blob_name_prefix, filesystem_client)

    tasks = [__download_blob_to_stream(blob_name, token) for blob_name in blob_names]
    byte_streams = await asyncio.gather(*tasks)

    return byte_streams


async def __download_data(timeslot: datetime,
                          time_resolution: TimeResolution,
                          directory_client: DataLakeDirectoryClient,
                          filename: str,
                          retrieve_data_span: Span) -> Optional[Union[bytes, str]]:
    with tracer.start_span('retrieve_data_download', child_of=retrieve_data_span) as local_span:
        path = get_file_path_with_respect_to_time_resolution(timeslot, time_resolution, filename)
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
