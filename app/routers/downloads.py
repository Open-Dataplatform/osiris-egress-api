"""
Contains endpoints for downloading data to the DataPlatform.
"""
import asyncio
import json
import typing
from datetime import datetime
from http import HTTPStatus
from io import BytesIO
from typing import Optional, Tuple, List
import pandas as pd
from azure.core.exceptions import ResourceNotFoundError

from azure.storage.filedatalake.aio import FileSystemClient, DataLakeDirectoryClient
from fastapi import APIRouter, HTTPException, Security
from fastapi.responses import Response, StreamingResponse, JSONResponse
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


@router.get('/{guid}/json', response_class=JSONResponse)
@Metric.histogram
async def download_json_file(guid: str,   # pylint: disable=too-many-locals
                             from_date: Optional[str] = None,
                             to_date: Optional[str] = None,
                             token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download json data requested')

    result, status_code = await __download_parquet_data(guid, token, from_date, to_date)

    if result:
        return JSONResponse(result, status_code=status_code)
    return Response(status_code=status_code)    # No data


@router.get('/dmi/', response_class=StreamingResponse)
@Metric.histogram
async def download_dmi_coord(lon: float,
                             lat: float,
                             from_date: str,
                             to_date: Optional[str] = None,
                             token: str = Security(access_token_header)) -> typing.Union[StreamingResponse,
                                                                                         Response]:
    """
    Download DMI endpoint with data from from_date to to_date (time period) for given coordinates (lon and lat)

    from_date: YYYY-MM
    to_date: YYYY-MM
    lon: <two digit float> (e.g. 15.19)
    lat: <two digit float> (e.g. 55.00)

    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download dmi data requested')

    guid = config['DMI']['guid']

    filters = [('lon', '=', lon), ('lat', '=', lat)]
    result, status_code = await __download_parquet_data_raw(guid, token, from_date, to_date, filters)

    if result:
        return StreamingResponse(result, status_code=status_code)
    return Response(status_code=status_code)    # No data


@router.get('/dmi_list/', response_class=JSONResponse)
@Metric.histogram
async def download_dmi_list(from_date: str,
                            token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
    """
    List all available coordinates (lon and lat) for a given date and return as json.

    from_date: YYYY-MM
    """
    logger.debug('download dmi list of coord data requested')

    guid = config['DMI']['guid']

    from_date_obj, _, _ = __parse_date_arguments(from_date, None)
    path = f'year={from_date_obj.year}/month={from_date_obj.month:02d}/data.parquet'

    result, status_code = await __download_parquet_file_path_raw(guid, path, token)

    result_df = pd.read_parquet(BytesIO(result), engine='pyarrow')
    result_df = result_df[['lon', 'lat']].drop_duplicates()

    json_response = json.loads(result_df.to_json(orient='records'))

    if result:
        return JSONResponse(json_response, status_code=status_code)
    return Response(status_code=status_code)    # No data


@router.get('/{guid}/parquet', response_class=JSONResponse)
@Metric.histogram
async def download_parquet_files(guid: str,   # pylint: disable=too-many-locals
                                 from_date: Optional[str] = None,
                                 to_date: Optional[str] = None,
                                 token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
    """
    Download Parquet endpoint with data from from_date to to_date (time period).

    from_date: YYYY-MM
    to_date: YYYY-MM

    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download parquet data requested')

    result, status_code = await __download_parquet_data_raw(guid, token, from_date, to_date)

    if result:
        return StreamingResponse(result, status_code=status_code)
    return Response(status_code=status_code)    # No data


@router.get('/jao', tags=["jao"], response_class=JSONResponse)
@Metric.histogram
async def download_jao_data(horizon: str,  # pylint: disable=too-many-locals
                            from_date: Optional[str] = None,
                            to_date: Optional[str] = None,
                            token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
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
        message = '(ValueError) The horizon value can only be Yearly or Monthly.'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    result, status_code = await __download_parquet_data(guid, token, from_date, to_date)

    if result:
        return JSONResponse(result, status_code=status_code)

    return Response(status_code=status_code)   # No data


@router.get('/jao_eds/{year}/{month}/{border}', tags=["jao_eds"], response_class=StreamingResponse)
@Metric.histogram
async def download_jao_eds_data(year: int,
                                month: int,
                                border: str,
                                token: str = Security(access_token_header)) -> typing.Union[StreamingResponse,
                                                                                            Response]:
    """
    Download JAO EDS calculations for a specific year, month, and border.

    Example: jao_eds/2021/04/D2-DE
    Returns the content in parquet format.
    """
    guid = config['JAO EDS']['guid']
    path = f'year={year}/month={month:02d}/{border}.parquet'

    json_data, status_code = await __download_parquet_file_path(guid, path, token)

    if json_data:
        return JSONResponse(json_data, status_code=status_code)

    return Response(status_code=status_code)   # No data


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
                               token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
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
        message = '(ValueError) The horizon parameter must be Daily, Hourly or Minutely.'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    # create filter. The outer list is OR and the inner list is AND.
    tag_filters = [[('Tag', '=', tag)] for tag in tags.split(',')] if tags else None

    events, status_code = await __download_parquet_data(guid, token, from_date, to_date, tag_filters)

    if events:
        return JSONResponse(events, status_code=status_code)
    return Response(status_code=status_code)    # No data


@router.get('/delfin', tags=["delfin"], response_class=JSONResponse)
@Metric.histogram
async def download_delfin_data(horizon: str,  # pylint: disable=too-many-locals
                               from_date: Optional[str] = None,
                               to_date: Optional[str] = None,
                               table_indices: str = '',
                               token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
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

    if events:
        return JSONResponse(events, status_code=status_code)
    return Response(status_code=status_code)    # No data


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
                    responses = await __download_parquet_files(chunk, time_resolution_enum,
                                                               directory_client, retrieve_data_span, filters)

                    for response in responses:
                        if response:
                            concat_response += response

        status_code = 200
        if not concat_response:
            status_code = HTTPStatus.NO_CONTENT

        return concat_response, status_code


# pylint: disable=too-many-arguments
async def __download_parquet_files(timeslot_chunk: List[datetime],
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

        # It would be better to use records.to_dict, but pandas uses narray type which JSONResponse can't handle.
        return json.loads(records.to_json(orient='records'))

    return await asyncio.gather(*[__download(timeslot) for timeslot in timeslot_chunk])  # noqa


# pylint: disable=too-many-arguments
async def __download_parquet_data_raw(guid: str,  # pylint: disable=too-many-locals
                                      token: str,
                                      from_date: Optional[str] = None,
                                      to_date: Optional[str] = None,
                                      filters: Optional[List] = None) -> Tuple[Optional[BytesIO], int]:

    try:
        from_date_obj, to_date_obj, time_resolution_enum = __parse_date_arguments(from_date, to_date)
    except ValueError as error:
        message = f'({type(error).__name__}) Wrong string format for date(s): {error}'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message) from error

    with tracer.start_span('download_parquet_files') as span:
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
                    responses = await __download_parquet_files_raw(chunk, time_resolution_enum,
                                                                   directory_client, retrieve_data_span,
                                                                   filters)

                    for response in responses:
                        if response is not None:
                            concat_response += [response]

        status_code = 200
        if not concat_response:
            status_code = HTTPStatus.NO_CONTENT
            return None, status_code

        with tracer.start_span('concat_dataframes', child_of=span):
            df_concat = pd.concat(concat_response)
            df_concat.reset_index(drop=True, inplace=True)

        with tracer.start_span('to_parquet', child_of=span):
            bytes_io_file = BytesIO()
            df_concat.to_parquet(bytes_io_file, engine='pyarrow', compression='snappy')
            bytes_io_file.seek(0)

        return bytes_io_file, status_code


# pylint: disable=too-many-arguments
async def __download_parquet_files_raw(timeslot_chunk: List[datetime],
                                       time_resolution: TimeResolution,
                                       directory_client: DataLakeDirectoryClient,
                                       retrieve_data_span: Span,
                                       filters: Optional[List] = None) -> List:
    async def __download(download_date: datetime):
        data = await __download_data(download_date, time_resolution, directory_client,
                                     'data.parquet', retrieve_data_span)

        if data is None:
            return None
        if isinstance(data, str):
            return None
        return pd.read_parquet(BytesIO(data), engine='pyarrow', filters=filters)

    return await asyncio.gather(*[__download(timeslot) for timeslot in timeslot_chunk])  # noqa


async def __download_parquet_file_path_raw(guid, path, token):
    full_path = f'{guid}/{path}'
    async with await __get_filesystem_client(token) as filesystem_client:
        try:
            directory_client = filesystem_client.get_directory_client(guid)
            await __check_directory_exist(directory_client)
            file_download = await __download_file(full_path, filesystem_client)
            file_content = await file_download.readall()

            return file_content, 200
        except ResourceNotFoundError:
            return None, HTTPStatus.NOT_FOUND


async def __download_parquet_file_path(guid, path, token):
    full_path = f'{guid}/{path}'
    async with await __get_filesystem_client(token) as filesystem_client:
        try:
            directory_client = filesystem_client.get_directory_client(guid)
            await __check_directory_exist(directory_client)
            file_download = await __download_file(full_path, filesystem_client)
            file_content = await file_download.readall()

            records = pd.read_parquet(BytesIO(file_content), engine='pyarrow')  # type: ignore

            # It would be better to use records.to_dict, but pandas uses narray type which JSONResponse can't handle.
            json_data = json.loads(records.to_json(orient='records'))

            return json_data, 200
        except ResourceNotFoundError:
            return None, HTTPStatus.NOT_FOUND
