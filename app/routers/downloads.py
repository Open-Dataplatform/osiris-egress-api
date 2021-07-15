"""
Contains endpoints for downloading data to the DataPlatform.
"""
import asyncio
import json
from datetime import datetime
from http import HTTPStatus
from io import BytesIO
from typing import Optional, Tuple, List
import pandas as pd

from azure.storage.filedatalake.aio import FileSystemClient, DataLakeDirectoryClient
from fastapi import APIRouter, HTTPException, Security
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration
from osiris.core.enums import TimeResolution
from jaeger_client import Span

from ..dependencies import (__download_file, __check_directory_exist, __get_filesystem_client,
                            __download_blob_to_stream, __split_into_chunks,
                            __get_all_dates_to_download, __parse_date_arguments, __download_data)
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


@router.get('/{guid}/json', response_class=JSONResponse)
@Metric.histogram
async def download_json_file(guid: str,   # pylint: disable=too-many-locals
                             from_date: Optional[str] = None,
                             to_date: Optional[str] = None,
                             token: str = Security(access_token_header)) -> JSONResponse:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download json data requested')

    result, status_code = await __download_json_data(guid, token, from_date, to_date)

    return JSONResponse(result, status_code=status_code)


@router.get('/jao', tags=["jao"], response_class=JSONResponse)
@Metric.histogram
async def download_jao_data(horizon: str,  # pylint: disable=too-many-locals
                            from_date: Optional[str] = None,
                            to_date: Optional[str] = None,
                            token: str = Security(access_token_header)) -> JSONResponse:
    """
    Download JAO data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.

    The horizon parameter must be set to Yearly or Monthly depending on what dataset you want data from.
    """
    logger.debug('download jao data requested')
    if horizon.lower() == "yearly":
        guid = config['JAO']['yearly_guid']
    elif horizon.lower() == "monthly":
        guid = config['JAO']['monthly_guid']
    else:
        message = '(ValueError) horizon value can only be Yearly or Monthly.'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    result, status_code = await __download_json_data(guid, token, from_date, to_date)

    return JSONResponse(result, status_code=status_code)


@router.get('/ikontrol/getallprojects', response_class=StreamingResponse)
@Metric.histogram
async def download_ikontrol_project_ids(token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download a list of all projects with project details.
    """
    logger.debug('download iKontrol project ids requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        file_path = 'ProjectDetails.json'

        stream = await __get_file_stream_for_ikontrol_file(file_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/json')


@router.get('/ikontrol/{project_id}', tags=["ikontrol"], response_class=StreamingResponse)
@Metric.histogram
async def download_ikontrol_data(project_id: int, token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download the data for a project using the project ID.
    """
    logger.debug('download iKontrol data requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        file_path = f'{project_id}/{project_id}.json'

        stream = await __get_file_stream_for_ikontrol_file(file_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/json')


@router.get('/ikontrol/getzip/{project_id}', tags=["ikontrol"], response_class=StreamingResponse)
@Metric.histogram
async def download_ikontrol_zip(project_id: int, token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download a project ZIP file using the project ID.
    """
    logger.debug('download iKontrol project zip requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        zip_path = f'{project_id}/{project_id}.zip'

        stream = await __get_file_stream_for_ikontrol_file(zip_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/zip')


async def __get_file_stream_for_ikontrol_file(file_path: str, filesystem_client: FileSystemClient) -> BytesIO:
    guid = config['iKontrol']['guid']
    directory_client = filesystem_client.get_directory_client(guid)
    await __check_directory_exist(directory_client)

    file_download = await __download_file(file_path, directory_client)
    file_content = await file_download.readall()

    stream = BytesIO(file_content)

    return stream


@router.get('/neptun', tags=["neptun"], response_class=JSONResponse)
@Metric.histogram
async def download_neptun_data(horizon: str,  # pylint: disable=too-many-locals
                               from_date: Optional[str] = None,
                               to_date: Optional[str] = None,
                               tags: str = '',
                               token: str = Security(access_token_header)) -> JSONResponse:
    """
    Download Neptun data from from_date to to_date (time period).
    If from_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.

    The horizon parameter must be set to Daily, Hourly or Minutely depending on what dataset you want data from:

    If Horizon is set to Daily the date(s) must be of the form {year}-{month}
    If Horizon is set to Hourly the date(s) must be of the form {year}-{month}-{day}
    If Horizon is set to Minutely the date(s) must be of the form {year}-{month}-{day}T{hour}

    The tags parameter is a list of tags (comma-separated string) which can be used to filter the data based on
    the Tag column.
    """
    logger.debug('download Neptun data requested')

    if horizon.lower() == 'daily':
        guid = config['Neptun']['daily_guid']
    elif horizon.lower() == 'hourly':
        guid = config['Neptun']['hourly_guid']
    elif horizon.lower() == 'minutely':
        guid = config['Neptun']['minutely_guid']
    else:
        message = '(ValueError) The horizon parameter must be Daily, Hourly or Minutely'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    # create filter. The outer list is OR and the inner list is AND.
    tag_filters = [[('Tag', '=', tag)] for tag in tags.split(',')] if tags else None

    events, status_code = await __download_parquet_data(guid, token, from_date, to_date, tag_filters)

    return JSONResponse(events, status_code=status_code)


@router.get('/delfin', tags=["delfin"], response_class=JSONResponse)
@Metric.histogram
async def download_delfin_data(horizon: str,  # pylint: disable=too-many-locals
                               from_date: Optional[str] = None,
                               to_date: Optional[str] = None,
                               table_indices: str = '',
                               token: str = Security(access_token_header)) -> JSONResponse:
    """
    Download Delfin data from from_date to to_date (time period).
    If from_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.

    The horizon parameter must be set to Daily, Hourly or Minutely depending on what dataset you want data from:

    If Horizon is set to Daily the date(s) must be of the form {year}-{month}-{day}
    If Horizon is set to Hourly the date(s) must be of the form {year}-{month}-{day}T{hour}
    If Horizon is set to Minutely the date(s) must be of the form {year}-{month}-{day}T{hour}:{minutes}

    The tags parameter is a list of tags (comma-separated string) which can be used to filter the data based on
    the Tag column.
    """
    logger.debug('download delfin data requested')

    if horizon.lower() == 'daily':
        guid = config['Delfin']['daily_guid']
    elif horizon.lower() == 'hourly':
        guid = config['Delfin']['hourly_guid']
    elif horizon.lower() == 'minutely':
        guid = config['Delfin']['minutely_guid']
    else:
        message = '(ValueError) The horizon parameter must be Daily, Hourly or Minutely.'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    # create filter. The outer list is OR and the inner list is AND.
    table_indices_filters = [[('TABLE_INDEX', '=', index)] for index in table_indices.split(',')] if table_indices \
        else None

    events, status_code = await __download_parquet_data(guid, token, from_date, to_date, table_indices_filters)

    return JSONResponse(events, status_code=status_code)


@router.get("/download", response_class=StreamingResponse)
@Metric.histogram
async def download_file(
    blob_name: str, token: str = Security(access_token_header)
) -> StreamingResponse:
    """
    Download any blob of any format.
    """
    byte_stream = await __download_blob_to_stream(blob_name, token)

    return StreamingResponse(byte_stream, media_type="application/octet-stream")


# pylint: disable=too-many-arguments
async def __download_json_data(guid: str,  # pylint: disable=too-many-locals
                               token: str,
                               from_date: Optional[str] = None,
                               to_date: Optional[str] = None,
                               filter_key: Optional[str] = None,
                               filters: Optional[List] = None) -> Tuple[Optional[List], int]:

    try:
        from_date_obj, to_date_obj, time_resolution_enum = __parse_date_arguments(from_date, to_date)
    except ValueError as error:
        message = f'({type(error).__name__}) Wrong string format for date(s): {error}'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message) from error

    with tracer.start_span('download_json_data') as span:
        span.set_tag('guid', guid)
        async with await __get_filesystem_client(token) as filesystem_client:
            with tracer.start_span('get_directory_client', child_of=span):
                directory_client = filesystem_client.get_directory_client(guid)

            with tracer.start_span('check_directory_exists', child_of=span):
                await __check_directory_exist(directory_client)

            with tracer.start_span('retrieve_json_data', child_of=span) as retrieve_data_span:
                if to_date_obj:
                    download_dates = __get_all_dates_to_download(from_date_obj, to_date_obj, time_resolution_enum)
                    download_dates = [item.to_pydatetime() for item in download_dates.tolist()]
                else:
                    download_dates = [from_date_obj]

                concat_response = []
                for chunk in __split_into_chunks(download_dates, 200):
                    responses = await __download_json_files(chunk, time_resolution_enum,
                                                            directory_client, retrieve_data_span,
                                                            filter_key, filters)

                    for response in responses:
                        if response:
                            concat_response += response

        status_code = 200
        if not concat_response:
            status_code = HTTPStatus.NO_CONTENT

        return concat_response, status_code


# pylint: disable=too-many-arguments
async def __download_parquet_data(guid: str,  # pylint: disable=too-many-locals
                                  token: str,
                                  from_date: Optional[str] = None,
                                  to_date: Optional[str] = None,
                                  filters: Optional[List] = None) -> Tuple[Optional[List], int]:

    try:
        from_date_obj, to_date_obj, time_resolution_enum = __parse_date_arguments(from_date, to_date)
    except ValueError as error:
        message = f'({type(error).__name__}) Wrong string format for date(s): {error}'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message) from error

    with tracer.start_span('download_delfin_files') as span:
        span.set_tag('guid', guid)
        async with await __get_filesystem_client(token) as filesystem_client:
            with tracer.start_span('get_directory_client', child_of=span):
                directory_client = filesystem_client.get_directory_client(guid)

            with tracer.start_span('check_directory_exists', child_of=span):
                await __check_directory_exist(directory_client)

            with tracer.start_span('retrieve_parquet_data', child_of=span) as retrieve_data_span:
                if to_date_obj:
                    download_dates = __get_all_dates_to_download(from_date_obj, to_date_obj, time_resolution_enum)
                    download_dates = [item.to_pydatetime() for item in download_dates.tolist()]
                else:
                    download_dates = [from_date_obj]

                concat_response = []
                for chunk in __split_into_chunks(download_dates, 200):
                    responses = await __download_delfin_files(chunk, time_resolution_enum,
                                                              directory_client, retrieve_data_span, filters)

                    for response in responses:
                        if response:
                            concat_response += response

        status_code = 200
        if not concat_response:
            status_code = HTTPStatus.NO_CONTENT

        return concat_response, status_code


# pylint: disable=too-many-arguments
async def __download_json_files(timeslot_chunk: List[datetime],
                                time_resolution: TimeResolution,
                                directory_client: DataLakeDirectoryClient,
                                retrieve_data_span: Span,
                                filter_key: Optional[str] = None,
                                filters: Optional[List] = None) -> List:
    async def __download(download_date: datetime):
        data = await __download_data(download_date, time_resolution, directory_client, 'data.json', retrieve_data_span)

        if not data:
            return None

        try:
            records = json.loads(data)
        except ValueError as error:
            message = f'({type(error).__name__}) File is not JSON formatted: {error}'
            logger.error(message)
            raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=message) from error

        if filters and filter_key:
            records = [record for record in records if record[filter_key] in filters]

        return records

    return await asyncio.gather(*[__download(timeslot) for timeslot in timeslot_chunk]) # noqa


# pylint: disable=too-many-arguments
async def __download_delfin_files(timeslot_chunk: List[datetime],
                                  time_resolution: TimeResolution,
                                  directory_client: DataLakeDirectoryClient,
                                  retrieve_data_span: Span,
                                  filters: Optional[List] = None) -> List:
    async def __download(download_date: datetime):
        data = await __download_data(download_date, time_resolution, directory_client,
                                     'data.parquet', retrieve_data_span)
        if not data:
            return None

        records = pd.read_parquet(BytesIO(data), engine='pyarrow', filters=filters)  # type: ignore
        # JSONResponse cannot handle NaN values
        records = records.fillna('null')

        return records.to_dict(orient='records')

    return await asyncio.gather(*[__download(timeslot) for timeslot in timeslot_chunk])  # noqa
