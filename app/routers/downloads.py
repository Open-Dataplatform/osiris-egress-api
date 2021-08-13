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

from azure.storage.filedatalake.aio import DataLakeDirectoryClient
from fastapi import APIRouter, HTTPException, Security
from fastapi.responses import Response, StreamingResponse, JSONResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration
from osiris.core.enums import TimeResolution
from jaeger_client import Span

from ..dependencies import (__check_directory_exist, __get_filesystem_client,
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


@router.get('/v1/{guid}/json', response_class=JSONResponse)
@router.get('/{guid}/test_json', response_class=JSONResponse)
@Metric.histogram
async def download_json_file_range(guid: str,   # pylint: disable=too-many-locals
                                   from_date: Optional[str] = None,
                                   to_date: Optional[str] = None,
                                   token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    """
    logger.debug('download json data requested')

    index, time_resolution = __get_guid_config(guid, from_date, to_date)

    result, status_code = await __download_parquet_data_v1(guid, token, index, time_resolution, from_date, to_date)

    if result is not None:
        return JSONResponse(__dataframe_to_json(result), status_code=status_code)
    return Response(status_code=status_code)    # No data


@router.get('/dmi', response_class=StreamingResponse)
@router.get('/v1/dmi', response_class=StreamingResponse)
@Metric.histogram
async def download_dmi_coord(lon: float,
                             lat: float,
                             from_date: str,
                             to_date: Optional[str],
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
    index, time_resolution = __get_guid_config(guid, from_date, to_date)

    filters = [('lon', '=', lon), ('lat', '=', lat)]
    result, status_code = await __download_parquet_data_v1(guid, token, index,
                                                           time_resolution, from_date, to_date, filters)

    if result is not None:
        return StreamingResponse(__dataframe_to_parquet(result), status_code=status_code)
    return Response(status_code=status_code)    # No data


@router.get('/dmi_list', response_class=JSONResponse)
@router.get('/v1/dmi_list', response_class=JSONResponse)
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
    blob_name = f'{guid}/year={from_date_obj.year}/month={from_date_obj.month:02d}/data.parquet'
    try:
        byte_stream = await __download_blob_to_stream(blob_name, token)

        result_df = pd.read_parquet(byte_stream, engine='pyarrow', columns=['lon', 'lat'])
        result_df = result_df.drop_duplicates()

        json_data = json.loads(result_df.to_json(orient='records'))
        status_code = HTTPStatus.OK
    except HTTPException as exception:
        json_data = [{'Message': exception.detail}]
        status_code = exception.status_code

    return JSONResponse(json_data, status_code=status_code)


@router.get('/v1/{guid}/parquet', response_class=JSONResponse)
@Metric.histogram
async def download_parquet_files(guid: str,   # pylint: disable=too-many-locals
                                 from_date: Optional[str] = None,
                                 to_date: Optional[str] = None,
                                 token: str = Security(access_token_header)) -> typing.Union[JSONResponse, Response]:
    """
    Download Parquet endpoint with data from from_date to to_date (time period).

    from_date: YYYY-MM
    to_date: YYYY-MM

    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download parquet data requested')

    index, time_resolution = __get_guid_config(guid, from_date, to_date)
    result, status_code = await __download_parquet_data_v1(guid, token, index, time_resolution, from_date, to_date)

    if result is not None:
        return StreamingResponse(__dataframe_to_parquet(result), status_code=status_code)
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


@router.get('/jao_eds/{year}/{month}/{border}', tags=["jao_eds"], response_class=JSONResponse)
@Metric.histogram
async def download_jao_eds_data(year: int,
                                month: int,
                                border: str,
                                token: str = Security(access_token_header)) -> JSONResponse:
    """
    Download JAO EDS calculations for a specific year, month, and border.

    Example: jao_eds/2021/04/D2-DE
    Returns the content in parquet format.
    """
    guid = config['JAO EDS']['guid']
    blob_name = f'{guid}/year={year}/month={month:02d}/{border}.parquet'

    try:
        byte_stream = await __download_blob_to_stream(blob_name, token)

        records = pd.read_parquet(byte_stream, engine='pyarrow')  # type: ignore

        # It would be better to use records.to_dict, but pandas uses narray type which JSONResponse can't handle.
        json_data = json.loads(records.to_json(orient='records'))
        status_code = HTTPStatus.OK
    except HTTPException as exception:
        json_data = [{'Message': exception.detail}]
        status_code = exception.status_code

    return JSONResponse(json_data, status_code=status_code)


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


@router.get("/v1/download", response_class=StreamingResponse)
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
async def __download_parquet_data_v1(guid: str,  # pylint: disable=too-many-locals
                                     token: str,
                                     index: str,
                                     time_resolution: TimeResolution,
                                     from_date: Optional[str] = None,
                                     to_date: Optional[str] = None,
                                     filters: Optional[List] = None) -> Tuple[Optional[pd.DataFrame], int]:

    try:
        from_date_obj, to_date_obj, _ = __parse_date_arguments(from_date, to_date)
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
                    download_dates = __get_all_dates_to_download(from_date_obj, to_date_obj, time_resolution)
                    download_dates = [item.to_pydatetime() for item in download_dates.tolist()]
                else:
                    download_dates = [from_date_obj]

                concat_response = []
                for chunk in __split_into_chunks(download_dates, 200):
                    responses = await __download_parquet_files_v1(chunk, time_resolution,
                                                                  directory_client, retrieve_data_span, filters)

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

        # Filter only the dates needed (we cannot do it directly in Parquet, as dates can be stored as strings)
        if from_date_obj is not None and to_date_obj is not None and len(df_concat) > 0:
            # Some are stored as datetime64[ns, UTC] and cannot compare directly with Timestemp objects
            # - Hence we make all to utc=True and use tz_localize('utc') to get around that
            df_concat['_dp_datetime_utc'] = pd.to_datetime(df_concat[index], utc=True)
            df_concat = df_concat[(df_concat['_dp_datetime_utc'] >= from_date_obj.tz_localize('utc'))
                                  & (df_concat['_dp_datetime_utc'] < to_date_obj.tz_localize('utc'))]

        # The time stamps are kept as strings - hence, they need to be parsed to datetime to filter on them
        return df_concat, status_code


def __dataframe_to_json(records: pd.DataFrame) -> List:
    # JSONResponse cannot handle NaN values
    records = records.fillna('null')

    # It would be better to use records.to_dict, but pandas uses narray type which JSONResponse can't handle.
    return json.loads(records.to_json(orient='records'))


def __dataframe_to_parquet(records: pd.DataFrame) -> BytesIO:
    bytes_io_file = BytesIO()
    records.to_parquet(bytes_io_file, engine='pyarrow', compression='snappy')
    bytes_io_file.seek(0)

    return bytes_io_file


def __get_guid_config(guid: str,
                      from_date: Optional[str] = None,
                      to_date: Optional[str] = None) -> Tuple[str, TimeResolution]:
    try:
        guid_config = json.loads(config['Dataset Config'][guid])
        index = guid_config['index']
        horizon = guid_config['horizon']
        time_resolution = TimeResolution[horizon]

        if (from_date is None or to_date is None) and time_resolution != TimeResolution.NONE:
            message = f'Both from_date and to_date need to be set for guid {guid}'
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    except KeyError as error:
        message = f'The server is not configured for guid: {guid}'
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message) from error

    return index, time_resolution


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
async def __download_parquet_files_v1(timeslot_chunk: List[datetime],
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
        records = pd.read_parquet(BytesIO(data), engine='pyarrow', filters=filters)
        return records

    return await asyncio.gather(*[__download(timeslot) for timeslot in timeslot_chunk])  # noqa
