"""
Contains endpoints for downloading data to the DataPlatform.
"""
from http import HTTPStatus
from io import BytesIO
from typing import Optional, Tuple, List

from azure.storage.filedatalake.aio import FileSystemClient
from fastapi import APIRouter, HTTPException, Security
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration

from ..dependencies import (__download_file, __check_directory_exist, __get_filesystem_client,
                            __download_blob_to_stream, __split_into_chunks, __download_json_files,
                            __get_all_dates_to_download, __parse_date_arguments)
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
    logger.debug('download jao data requested')

    result, status_code = await __download_json_data(guid, token, from_date, to_date)

    return JSONResponse(result, status_code=status_code)


@router.get('/jao', response_class=JSONResponse)
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
    if horizon == "Yearly":
        guid = config['JAO']['yearly_guid']
    elif horizon == "Monthly":
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


@router.get('/ikontrol/{project_id}', response_class=StreamingResponse)
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


@router.get('/ikontrol/getzip/{project_id}', response_class=StreamingResponse)
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


@router.get('/neptun', response_class=JSONResponse)
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

    The horizon parameter must be set to Yearly, Monthly or Daily depending on what dataset you want data from.

    The tags parameter is a list of tags which can be used to filter the data based on the Tag column.
    """
    logger.debug('download jao data requested')
    if horizon == 'Monthly':
        guid = config['Neptun']['monthly_guid']
    elif horizon == 'Daily':
        guid = config['Neptun']['daily_guid']
    elif horizon == 'Hourly':
        guid = config['Neptun']['hourly_guid']
    else:
        message = '(ValueError) The horizon parameter must be Monthly, Daily or Hourly.'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    tags_filters = tags.split(',') if tags else None

    events, status_code = await __download_json_data(guid, token, from_date, to_date, 'Tag', tags_filters)

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
