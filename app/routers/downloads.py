"""
Contains endpoints for downloading data to the DataPlatform.
"""
import json
import os
from datetime import datetime, date
import asyncio
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import StreamingResponse

from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient, StorageStreamDownloader
from osiris.core.azure_client_authorization import AzureCredential

from ..dependencies import Configuration, Metric, TracerClass

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['downloads'])

tracer = TracerClass().get_tracer()


@router.get('/{guid}/json', response_class=StreamingResponse)
@Metric.histogram
async def download_json_file(guid: str,
                             file_date: date = datetime.utcnow().date(),
                             token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download JSON file from data storage from the given date (UTC). This endpoint expects data to be
    stored in {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/data.json'.
    """
    logger.debug('download json file requested')

    with tracer.start_span('download_json_file') as span:
        with __get_filesystem_client(token) as filesystem_client:
            span.set_tag('guid', guid)
            with tracer.start_active_span('get_directory_client', child_of=span):
                directory_client = filesystem_client.get_directory_client(guid)
            with tracer.start_active_span('check_directory_exists', child_of=span):
                __check_directory_exist(directory_client)
            with tracer.start_active_span('download_file', child_of=span):
                path = f'year={file_date.year:02d}/month={file_date.month:02d}/day={file_date.day:02d}/data.json'
                stream = __download_file(path, directory_client)

            return StreamingResponse(stream.chunks(), media_type='application/octet-stream')


@router.get('/{guid}', response_class=StreamingResponse)
@Metric.histogram
async def download_file(guid: str,
                        file_date: date = datetime.utcnow().date(),
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
                path = __get_path_for_arbritary_file(file_date, guid, filesystem_client)
                stream = __download_file(path, directory_client)

            return StreamingResponse(stream.chunks(), media_type='application/octet-stream')


@router.get('/{guid}/jao', response_class=StreamingResponse)
@Metric.histogram
async def download_jao_data(guid: str,
                            from_date: date,
                            to_date: date = datetime.utcnow().date(),
                            token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download JAO data endpoint with data from from_date to to_date.
    Returns the an appended list of all JSON data.
    """
    async def download(download_date: date, filesystem_client_local, directory_client_local):
        path = __get_path_for_arbritary_file(download_date, guid, filesystem_client_local)
        stream = __download_file(path, directory_client_local)

        json_data = json.loads(stream.readall())

        return json_data

    logger.debug('download jao data requested')

    with __get_filesystem_client(token) as filesystem_client:
        directory_client = filesystem_client.get_directory_client(guid)
        __check_directory_exist(directory_client)
        # We store data the first every month - as data does not change
        fetch_date = from_date.replace(day=1)
        fetch_dates = []
        while fetch_date < to_date:
            fetch_dates.append(fetch_date)
            fetch_date += relativedelta(months=+1)

        responses = await asyncio.gather(*[download(fetch_date, filesystem_client, directory_client)
                                           for fetch_date in fetch_dates])

    concat_response = []
    for response in responses:
        concat_response.append(response)

    return StreamingResponse(iter(json.dumps(concat_response)), media_type='application/octet-stream')


@router.get('/{guid}/retrieve_state', response_class=StreamingResponse)
@Metric.histogram
async def retrieve_state(guid: str,
                         token: str = Security(access_token_header)) -> StreamingResponse:
    """
    get state file from data storage from the given guid. This endpoint expects data to be
    stored in {guid}/state.json
    """
    logger.debug('retrieve state requested')
    with __get_filesystem_client(token) as filesystem_client:
        directory_client = filesystem_client.get_directory_client(guid)
        __check_directory_exist(directory_client)
        stream = __download_file('state.json', directory_client)

    return StreamingResponse(stream.chunks(), media_type='application/octet-stream')


def __check_directory_exist(directory_client: DataLakeDirectoryClient):
    try:
        directory_client.get_directory_properties()
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) The given dataset doesnt exist: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code, detail=message) from error


def __get_path_for_arbritary_file(file_date: date, guid: str, filesystem_client: FileSystemClient) -> str:
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
