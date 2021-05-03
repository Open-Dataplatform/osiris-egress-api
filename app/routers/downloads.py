"""
Contains endpoints for downloading data to the DataPlatform.
"""
import json
import os
from datetime import datetime
import asyncio
from http import HTTPStatus
from io import StringIO
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import StreamingResponse

from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient, StorageStreamDownloader
from jaeger_client import Span
from osiris.core.azure_client_authorization import AzureCredential
from osiris.core.configuration import Configuration
from osiris.core.enums import TimeResolution
from osiris.core.io import get_file_path_with_respect_to_time_resolution

from ..dependencies import Metric, TracerClass, __get_all_dates_to_download

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['downloads'])

tracer = TracerClass().get_tracer()


@router.get('/{guid}', response_class=StreamingResponse)
@Metric.histogram
async def download_file(guid: str,
                        file_date: datetime = datetime.utcnow(),
                        token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download file from data storage from the given date (UTC). This endpoint expects data to be
    stored in the folder {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/, but doesnt make
    any assumption about the filename and file extension.
    """
    logger.debug('download file requested')

    with tracer.start_span('download_file') as span:
        span.set_tag('guid', guid)
        with __get_filesystem_client(token) as filesystem_client:
            with tracer.start_active_span('get_directory_client', child_of=span):
                directory_client = filesystem_client.get_directory_client(guid)
            with tracer.start_active_span('check_directory_exists', child_of=span):
                __check_directory_exist(directory_client)
            with tracer.start_active_span('download_file', child_of=span):
                path = __get_path_for_arbitrary_file(file_date, guid, filesystem_client)
                stream = __download_file(path, directory_client)

            return StreamingResponse(stream.chunks(), media_type='application/octet-stream')


@router.get('/{guid}/json', response_class=StreamingResponse)
@Metric.histogram
async def download_json_file(guid: str,  # pylint: disable=too-many-locals
                             from_date: datetime = datetime.utcnow(),
                             to_date: Optional[datetime] = None,
                             time_resolution: str = "DAY",
                             token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download jao data requested')

    with tracer.start_span('download_json_file') as span:
        span.set_tag('guid', guid)
        with __get_filesystem_client(token) as filesystem_client:
            with tracer.start_active_span('get_directory_client', child_of=span):
                directory_client = filesystem_client.get_directory_client(guid)

            with tracer.start_active_span('check_directory_exists', child_of=span):
                __check_directory_exist(directory_client)

            with tracer.start_active_span('retrieve_data', child_of=span) as retrieve_data_span:
                time_resolution_enum = TimeResolution[time_resolution]
                if to_date:
                    download_dates = __get_all_dates_to_download(from_date, to_date, time_resolution_enum)
                    download_dates = [item.to_pydatetime() for item in download_dates.tolist()]
                else:
                    download_dates = [from_date]

                concat_response = []
                chunk_size = 200
                for i in range(0, len(download_dates), chunk_size):
                    responses = await asyncio.gather(*[__download(download_date,
                                                                  directory_client,
                                                                  time_resolution_enum)
                                                       for download_date in download_dates[i:i + chunk_size]])

                    for response in responses:
                        concat_response += response

        stream = StringIO(json.dumps(concat_response))
        return StreamingResponse(stream, media_type='application/octet-stream')


async def __download(download_date: datetime,
                     directory_client_local,
                     time_resolution_local: TimeResolution):
    path = get_file_path_with_respect_to_time_resolution(download_date, time_resolution_local, 'data.json')
    stream = __download_file(path, directory_client_local)

    try:
        json_data = json.loads(stream.readall())
    except ValueError as error:
        message = f'({type(error).__name__}) File is not JSON formatted: {error}'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=message) from error

    return json_data


def __check_directory_exist(directory_client: DataLakeDirectoryClient):
    try:
        directory_client.get_directory_properties()
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) The given dataset doesnt exist: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error


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


def __download_file(filename: str, directory_client: DataLakeDirectoryClient) -> StorageStreamDownloader:
    file_client = directory_client.get_file_client(filename)
    try:
        downloaded_file = file_client.download_file()
        return downloaded_file
    except HttpResponseError as error:
        message = f'({type(error).__name__}) File could not be downloaded: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error


def __get_filesystem_client(token: str) -> FileSystemClient:
    account_url = config['Azure Storage']['account_url']
    filesystem_name = config['Azure Storage']['filesystem_name']
    credential = AzureCredential(token)

    return FileSystemClient(account_url, filesystem_name, credential=credential)


async def __get_settings(directory_client: DataLakeDirectoryClient) -> Dict:
    try:
        file_client = directory_client.get_file_client('settings.json')
        downloaded_file = file_client.download_file()
        settings_data = downloaded_file.readall()
        return json.loads(settings_data)
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) Problems downloading setting.json: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error
