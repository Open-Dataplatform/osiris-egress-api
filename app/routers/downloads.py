"""
Contains endpoints for downloading data to the DataPlatform.
"""
import json
import os
from datetime import datetime
import asyncio
from http import HTTPStatus
from io import StringIO, BytesIO
from typing import Dict, Optional, List
import pandas as pd

from azure.storage.filedatalake.aio import DataLakeDirectoryClient, FileSystemClient
from azure.core.exceptions import ResourceNotFoundError
from fastapi import APIRouter, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import StreamingResponse
from jaeger_client import Span

from osiris.core.azure_client_authorization import AzureCredentialAIO
from osiris.core.configuration import Configuration
from osiris.core.enums import TimeResolution

from ..dependencies import __get_all_dates_to_download, __download_data, __split_into_chunks, __download_file
from ..metrics import TracerClass, Metric

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['downloads'])

tracer = TracerClass().get_tracer()


# @router.get('/{guid}', response_class=StreamingResponse)
# @Metric.histogram
# async def download_file(guid: str,
#                         file_date: datetime = datetime.utcnow(),
#                         token: str = Security(access_token_header)) -> StreamingResponse:
#     """
#     Download file from data storage from the given date (UTC). This endpoint expects data to be
#     stored in the folder {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/, but doesnt make
#     any assumption about the filename and file extension.
#     """
#     logger.debug('download file requested')
#
#     with tracer.start_span('download_file') as span:
#         span.set_tag('guid', guid)
#         async with await __get_filesystem_client(token) as filesystem_client:
#             with tracer.start_active_span('get_directory_client', child_of=span):
#                 directory_client = filesystem_client.get_directory_client(guid)
#             with tracer.start_active_span('check_directory_exists', child_of=span):
#                 __check_directory_exist(directory_client)
#             with tracer.start_active_span('download_file', child_of=span):
#                 path = __get_path_for_arbitrary_file(file_date, guid, filesystem_client)
#                 stream = __download_file(path, directory_client)
#
#             return StreamingResponse(stream.chunks(), media_type='application/octet-stream')


@router.get('/{guid}/json', response_class=StreamingResponse)
@Metric.histogram
async def download_json_file(guid: str,   # pylint: disable=too-many-locals
                             from_date: Optional[str] = None,
                             to_date: Optional[str] = None,
                             token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download jao data requested')
    return await __download_json_file(guid, token, from_date, to_date)


@router.get('/jao', response_class=StreamingResponse)
async def download_jao_data(horizon: str,  # pylint: disable=too-many-locals
                            from_date: Optional[str] = None,
                            to_date: Optional[str] = None,
                            token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download jao data requested')
    if horizon == "Yearly":
        guid = config['JAO']['yearly_guid']
    elif horizon == "Monthly":
        guid = config['JAO']['monthly_guid']
    else:
        message = '(ValueError) horizon value can only be Yearly or Monthly'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    return await __download_json_file(guid, token, from_date, to_date)


@router.get('/ikontrol/getallprojects', response_class=StreamingResponse)
async def download_ikontrol_project_ids(token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download a list of all projects with project details.
    """
    logger.debug('download iKontrol project ids requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        file_path = 'ProjectDetails.json'

        stream = await __get_file_stream_for_ikontrol_file(file_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/json')


@router.get('/ikontrol', response_class=StreamingResponse)
async def download_ikontrol_data(project_id: int, token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download the data for a project using the project ID.
    """
    logger.debug('download iKontrol data requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        file_path = f'{project_id}/{project_id}.json'

        stream = await __get_file_stream_for_ikontrol_file(file_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/json')


@router.get('/ikontrol/getzip', response_class=StreamingResponse)
async def download_ikontrol_zip(project_id: int, token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download a project ZIP file using the project ID.
    """
    logger.debug('download iKontrol project zip requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        zip_path = f'{project_id}/{project_id}.zip'

        stream = await __get_file_stream_for_ikontrol_file(zip_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/zip')


async def __download_json_file(guid: str,   # pylint: disable=too-many-locals
                               token: str,
                               from_date: Optional[str] = None,
                               to_date: Optional[str] = None) -> StreamingResponse:

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

        stream = StringIO(json.dumps(concat_response))
        return StreamingResponse(stream, media_type='application/octet-stream', status_code=status_code)


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


async def __check_directory_exist(directory_client: DataLakeDirectoryClient):
    try:
        await directory_client.get_directory_properties()
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


# def __download_file(filename: str, directory_client: DataLakeDirectoryClient) -> StorageStreamDownloader:
#     file_client = directory_client.get_file_client(filename)
#     try:
#         downloaded_file = file_client.download_file()
#         return downloaded_file  # pylint: disable=duplicate-code
#     except HttpResponseError as error:
#         message = f'({type(error).__name__}) File could not be downloaded: {error}'
#         logger.error(message)
#         raise HTTPException(status_code=error.status_code, detail=message) from error


async def __get_filesystem_client(token: str) -> FileSystemClient:
    account_url = config['Azure Storage']['account_url']
    filesystem_name = config['Azure Storage']['filesystem_name']
    credential = AzureCredentialAIO(token)

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


async def __get_file_stream_for_ikontrol_file(file_path: str, filesystem_client: FileSystemClient) -> BytesIO:
    guid = config['iKontrol']['guid']
    directory_client = filesystem_client.get_directory_client(guid)
    await __check_directory_exist(directory_client)

    file_download = await __download_file(file_path, directory_client)
    file_content = await file_download.readall()

    stream = BytesIO(file_content)

    return stream
