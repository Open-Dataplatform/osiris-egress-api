"""
Contains dependencies used in several places of the application.
"""
import asyncio
import json
import os
from datetime import datetime
from http import HTTPStatus
from typing import Optional, Union, List

import pandas as pd
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.storage.filedatalake.aio import DataLakeDirectoryClient, DataLakeFileClient, StorageStreamDownloader, \
    FileSystemClient
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from jaeger_client import Span
from osiris.azure_client_authorization import AzureCredentialAIO
from osiris.core.configuration import Configuration
from osiris.core.enums import TimeResolution
from osiris.core.io import get_file_path_with_respect_to_time_resolution
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


async def __download_files(timeslot_chunk: List[datetime],
                           time_resolution: TimeResolution,
                           directory_client: DataLakeDirectoryClient,
                           retrieve_data_span: Span) -> List:
    async def __download(download_date: datetime):
        data = await __download_data(download_date, time_resolution, directory_client, retrieve_data_span)

        if not data:
            return None

        try:
            json_data = json.loads(data)
        except ValueError as error:
            message = f'({type(error).__name__}) File is not JSON formatted: {error}'
            logger.error(message)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=message) from error

        return json_data

    return await asyncio.gather(*[__download(timeslot) for timeslot in timeslot_chunk])


async def __download_json_file(guid: str,   # pylint: disable=too-many-locals
                               token: str,
                               from_date: Optional[str] = None,
                               to_date: Optional[str] = None) -> JSONResponse:
    from_date_obj, to_date_obj, time_resolution_enum = __parse_date_arguments(from_date, to_date)

    with tracer.start_span('download_json_file') as span:
        span.set_tag('guid', guid)
        async with await __get_filesystem_client(token) as filesystem_client:
            with tracer.start_span('get_directory_client', child_of=span):
                directory_client = filesystem_client.get_directory_client(guid)

            with tracer.start_span('check_directory_exists', child_of=span):
                await __check_directory_exist(directory_client)

            with tracer.start_span('retrieve_data', child_of=span) as retrieve_data_span:
                if to_date_obj:
                    download_dates = __get_all_dates_to_download(from_date_obj, to_date_obj, time_resolution_enum)
                    download_dates = [item.to_pydatetime() for item in download_dates.tolist()]
                else:
                    download_dates = [from_date_obj]

                concat_response = []
                for chunk in __split_into_chunks(download_dates, 200):
                    responses = await __download_files(chunk, time_resolution_enum,
                                                       directory_client, retrieve_data_span)

                    for response in responses:
                        if response:
                            concat_response += response

        status_code = 200
        if not concat_response:
            status_code = HTTPStatus.NO_CONTENT

        return JSONResponse(concat_response, status_code=status_code)


async def __check_directory_exist(directory_client: DataLakeDirectoryClient):
    try:
        await directory_client.get_directory_properties()
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) The given dataset doesnt exist: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error


async def __get_filepaths(path, filesystem_client) -> List[str]:
    """ Returns a list of paths below `path` that are not directories. """
    paths = filesystem_client.get_paths(path=path)
    return [path.name async for path in paths if not path.is_directory]


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


def __parse_date_str(date_str):
    try:
        if len(date_str) == 4:
            return pd.to_datetime(date_str, format='%Y'), TimeResolution.YEAR
        if len(date_str) == 7:
            return pd.to_datetime(date_str, format='%Y-%m'), TimeResolution.MONTH
        if len(date_str) == 10:
            return pd.to_datetime(date_str, format='%Y-%m-%d'), TimeResolution.DAY
        if len(date_str) == 13:
            return pd.to_datetime(date_str, format='%Y-%m-%dT%H'), TimeResolution.HOUR
        if len(date_str) == 16:
            return pd.to_datetime(date_str, format='%Y-%m-%dT%H:%M'), TimeResolution.MINUTE

        message = '(ValueError) Wrong string format for date(s):'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)
    except ValueError as error:
        message = f'({type(error).__name__}) Wrong string format for date(s): {error}'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message) from error


def __parse_date_arguments(from_date, to_date):
    if from_date is None and to_date is None:
        return datetime(1970, 1, 1), None, TimeResolution.NONE
    if to_date is None:
        from_date_obj, time_resolution = __parse_date_str(from_date)
        return from_date_obj, None, time_resolution
    if len(from_date) != len(to_date):
        message = 'Malformed request syntax: len(from_date) != len(to_date)'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    from_date_obj, time_resolution = __parse_date_str(from_date)
    to_date_obj, _ = __parse_date_str(to_date)
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
