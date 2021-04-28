"""
Implements endpoints which are required by the Json plugin for Grafana.
"""
from datetime import datetime
from http import HTTPStatus
from typing import Dict, List, Optional
import json
import asyncio

import pandas as pd
import numpy as np

from fastapi import APIRouter, HTTPException, Header
from fastapi.security.api_key import APIKeyHeader
from jaeger_client import Span
from osiris.core.azure_client_authorization import ClientAuthorization
from pandas import DataFrame

from azure.storage.filedatalake.aio import DataLakeDirectoryClient, DataLakeFileClient
from azure.core.exceptions import ResourceNotFoundError

from ..dependencies import Configuration, Metric, TracerClass
from ..schemas.json_request import QueryRequest, TagValuesRequest

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

api_key_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['grafana'])

tracer = TracerClass().get_tracer()


@router.get('/grafana/{guid}', status_code=HTTPStatus.OK)
@Metric.histogram
async def test_connection(guid: str, client_id: str = Header(None), client_secret: str = Header(None)):
    """
    Endpoint for Grafana connectivity test. This checks if the GUID folder exist and the client_id
    and client_secret are valid.
    """
    if not guid or not client_id or not client_secret:
        message = 'One or more of the values (GUID, client-id or client-secret) is missing.'
        logger.debug(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    logger.debug('Grafana root requested for GUID %s', guid)

    directory_client = await __get_directory_client(guid, client_id, client_secret)

    await directory_client.close()

    return {'message': 'Grafana datasource used for timeseries data.'}


@router.post('/grafana/{guid}/search', status_code=HTTPStatus.OK)
@Metric.histogram
async def search(guid: str, client_id: str = Header(None), client_secret: str = Header(None)):
    """
    Returns the valid metrics.
    """
    logger.debug('Grafana search requested for GUID %s', guid)
    with tracer.start_span('grafana_search') as span:
        span.set_tag('guid', guid)

        with tracer.start_active_span('get_directory_client', child_of=span):
            directory_client = await __get_directory_client(guid, client_id, client_secret)
        with tracer.start_active_span('get_grafana_settings', child_of=span):
            grafana_settings = await __get_grafana_settings(directory_client)
        with tracer.start_active_span('sort metrics', child_of=span):
            metrics = grafana_settings['metrics']
            metrics.sort()

        await directory_client.close()
        return metrics


@router.post('/grafana/{guid}/query', status_code=HTTPStatus.OK)
@Metric.histogram
async def query(guid: str, request: QueryRequest,
                client_id: str = Header(None), client_secret: str = Header(None)) -> List[Dict]:
    """
    Returns the data based on time range and target metric.
    """
    logger.debug('Grafana query requested for GUID %s', guid)

    with tracer.start_span('grafana_query') as span:
        span.set_tag('guid', guid)
        if not __is_targets_set_for_all(request.targets):
            return []

        with tracer.start_span('get_directory_client', child_of=span):
            directory_client = await __get_directory_client(guid, client_id, client_secret)
        from_date = pd.Timestamp(request.range['from']).to_pydatetime()
        to_date = pd.Timestamp(request.range['to']).to_pydatetime()
        span.set_tag('request_from_date', str(from_date))
        span.set_tag('request_to_date', str(to_date))

        with tracer.start_span('retrieve_data', child_of=span) as retrieve_data_span:
            data_df = await __retrieve_data(from_date, to_date, directory_client, retrieve_data_span)
        with tracer.start_span('filter_with_adhoc_filters', child_of=span):
            data_df = await __filter_with_adhoc_filters(directory_client, data_df, request.adhocFilters)

        freq = f'{request.intervalMs}ms'

        results = []
        with tracer.start_span('data_frame_to_response', child_of=span):
            for target in request.targets:
                results.extend(__dataframe_to_response(data_df, target.type, target.target, target.data, freq))

        with tracer.start_span('get_size_of_result', child_of=span):
            # We use repr(.) here, which is the printable representation of the object
            # - it is not fully accurate - as, i.e., a float or integer printed can take up less space
            # - Not sure how objects like floats and ints are transmitted
            # - We did not use len(pickle(results)) as it is a security issue according to bandit
            span.set_tag('result_size', len(repr(results)))

        await directory_client.close()
        return results


@router.post('/grafana/{guid}/annotations', status_code=HTTPStatus.OK)
@Metric.histogram
async def annotation(guid: str) -> List:
    """
    Returns empty list of annotations.
    """
    logger.debug('Grafana annotations requested for GUID %s', guid)

    return []


@router.post('/grafana/{guid}/tag-keys', status_code=HTTPStatus.OK)
@Metric.histogram
async def tag_keys(guid: str, client_id: str = Header(None), client_secret: str = Header(None)) -> List:
    """
    Returns list of tag-keys.
    """
    logger.debug('Grafana tag-keys requested for GUID %s', guid)

    directory_client = await __get_directory_client(guid, client_id, client_secret)
    grafana_settings = await __get_grafana_settings(directory_client)

    await directory_client.close()
    return grafana_settings['tag_keys']


@router.post('/grafana/{guid}/tag-values', status_code=HTTPStatus.OK)
@Metric.histogram
async def tag_values(guid: str, request: TagValuesRequest,
                     client_id: str = Header(None), client_secret: str = Header(None)) -> List:
    """
    Returns list of tag values corresponding to request key.
    """
    logger.debug('Grafana tag-values requested for GUID %s', guid)

    directory_client = await __get_directory_client(guid, client_id, client_secret)
    grafana_settings = await __get_grafana_settings(directory_client)

    if request.key in grafana_settings['tag_values']:
        return grafana_settings['tag_values'][request.key]

    await directory_client.close()
    return []


def __is_targets_set_for_all(targets):
    for target in targets:
        if not target.target:
            return False

    return True


def __dataframe_to_response(data_df: DataFrame, target_type: str, target: str,
                            additional_filters: Dict, freq: str) -> List[Dict]:
    response: List[Dict] = []

    if data_df is None or data_df.empty:
        return response

    target_return_name = ''
    for metric, value in additional_filters.items():
        try:
            data_df = data_df[data_df[metric] == value]
            target_return_name += value + '_'
        except KeyError:
            continue
    target_return_name += target

    # The target value Raw is not part of the valid metrics. It's purpose is to return data as it is stored
    # on the filesystem. The Raw value only makes sense for the "table" panel in Grafana.
    if target == 'Raw' and target_type == 'timeseries':
        return response

    if target != 'Raw':
        if freq is not None:
            orig_tz = data_df.index.tz
            data_df = data_df.tz_convert('UTC').resample(rule=freq, label='right', closed='right') \
                             .mean().tz_convert(orig_tz)

        if target:
            data_df = data_df[target]

    data_df = data_df.replace({np.nan: None}).sort_index()

    if target_type == 'timeseries':
        return __dataframe_to_timeserie_response(data_df, target_return_name)

    return __dataframe_to_table_response(data_df)


def __dataframe_to_timeserie_response(data_df: DataFrame, target_return_name: str) -> List[Dict]:
    if data_df.empty:
        return [{'target': target_return_name, 'datapoints': []}]

    timestamps = (data_df.index.astype(np.int64) // 10 ** 6).values.tolist()
    values = data_df.values.tolist()

    return [{'target': target_return_name, 'datapoints': list(zip(values, timestamps))}]


def __dataframe_to_table_response(data_df: DataFrame) -> List[Dict]:
    response: List[Dict] = []

    no_index_data_df = data_df.reset_index(level=0)
    response.append({'type': 'table',
                     'columns': no_index_data_df.columns.map(lambda col: {'text': col}).tolist(),
                     'rows': no_index_data_df.where(pd.notnull(no_index_data_df), None).values.tolist()})

    return response


async def __get_directory_client(guid: str, client_id: str, client_secret: str) -> DataLakeDirectoryClient:
    tenant_id = config['Azure Authentication']['tenant_id']
    account_url = config['Azure Storage']['account_url']
    filesystem_name = config['Azure Storage']['filesystem_name']

    client_auth = ClientAuthorization(tenant_id, client_id, client_secret)
    credential = client_auth.get_credential_async()

    directory_client = DataLakeDirectoryClient(account_url, filesystem_name, guid, credential=credential)
    try:
        await directory_client.get_directory_properties()  # Test if the directory exist otherwise return error.
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) The given dataset doesnt exist: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code,
                            detail=message) from error

    return directory_client


def __arrange_time_range_in_dict(from_date: datetime, to_date: datetime) -> Dict:
    time_range = pd.date_range(from_date, to_date, freq='H')

    time_range_dict: Dict[str, Dict[str, Dict[str, List]]] = {}
    for timeslot in time_range:
        if timeslot.year not in time_range_dict:
            time_range_dict[timeslot.year] = {}

        if timeslot.month not in time_range_dict[timeslot.year]:
            time_range_dict[timeslot.year][timeslot.month] = {}

        if timeslot.day not in time_range_dict[timeslot.year][timeslot.month]:
            time_range_dict[timeslot.year][timeslot.month][timeslot.day] = []

        time_range_dict[timeslot.year][timeslot.month][timeslot.day].append(timeslot.hour)

    return time_range_dict


async def __download_files(timeslot_chunk: List[datetime],
                           directory_client: DataLakeDirectoryClient,
                           retrieve_data_span: Span) -> List:

    async def download(timeslot: datetime, retrieve_data_span_local: Span) -> Optional[str]:
        with tracer.start_span('retrieve_data_download', child_of=retrieve_data_span_local) as local_span:
            path = f'year={timeslot.year}/month={timeslot.month:02d}/day={timeslot.day:02d}/data.json'
            local_span.set_tag('path', path)

            file_client: DataLakeFileClient = await __get_file_client(directory_client, path)
            if not file_client:
                return None

            downloaded_file = await file_client.download_file()
            file_json = pd.read_json(await downloaded_file.readall())

            return file_json

    return await asyncio.gather(*[download(timeslot, retrieve_data_span) for timeslot in timeslot_chunk])


def __split_into_chunks(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


async def __retrieve_data(from_date: datetime, to_date: datetime,
                          directory_client: DataLakeDirectoryClient,
                          retrieve_data_span: Span) -> Optional[DataFrame]:

    time_range = pd.date_range(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), freq='D')

    data = None
    # We need to divide the timeslots into chunks so we don't hit the limit of asyncio.gather.
    for chunk in __split_into_chunks(time_range, 200):
        df_list = await __download_files(chunk, directory_client, retrieve_data_span)

        if all(elem is None for elem in df_list):
            continue

        df_list.append(data)
        data = pd.concat(df_list)

    if data is None:
        return None

    data.set_index('datetime', inplace=True)

    return data


async def __get_file_client(directory_client: DataLakeDirectoryClient, path):
    file_client: DataLakeFileClient = directory_client.get_file_client(path)
    try:
        await file_client.get_file_properties()
    except ResourceNotFoundError:
        return None

    return file_client


async def __get_grafana_settings(directory_client: DataLakeDirectoryClient) -> Dict:
    try:
        file_client = directory_client.get_file_client('grafana_settings1.json')
        downloaded_file = await file_client.download_file()
        settings_data = await downloaded_file.readall()
        return json.loads(settings_data)
    except ResourceNotFoundError as error:
        message = f'({type(error).__name__}) Problems downloading grafana_setting.json: {error}'
        logger.error(message)
        raise HTTPException(status_code=error.status_code,
                            detail=message) from error


async def __filter_with_adhoc_filters(directory_client: DataLakeDirectoryClient,
                                      data_df: DataFrame, adhoc_filters: List):
    grafana_settings = await __get_grafana_settings(directory_client)

    for adhoc_filter in adhoc_filters:
        key_type = __find_key_type(grafana_settings['tag_keys'], adhoc_filter.key)

        if key_type == "string":
            data_df = await __filter_with_adhoc_filter_string(adhoc_filter, data_df)
        elif key_type == "number":
            data_df = await __filter_with_adhoc_filter_number(adhoc_filter, data_df)
        else:
            logger.debug('Unknown key type. Supported types are "text" and "number.')
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST,
                                detail='Unknown key type. Supported types are "text" and "number."')

    return data_df


async def __filter_with_adhoc_filter_number(adhoc_filter, data_df):
    try:
        value = float(adhoc_filter.value)
    except Exception as error:
        logger.debug('Value been used in filter is not a number. The user has not entered a number value.')
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST,
                            detail='Value not a number') from error
    if adhoc_filter.operator == "=":
        data_df = data_df.loc[data_df[adhoc_filter.key] == value]
    elif adhoc_filter.operator == "!=":
        data_df = data_df.loc[data_df[adhoc_filter.key] != value]
    elif adhoc_filter.operator == ">":
        data_df = data_df.loc[data_df[adhoc_filter.key] > value]
    elif adhoc_filter.operator == "<":
        data_df = data_df.loc[data_df[adhoc_filter.key] < value]
    else:
        logger.debug('Operator not supported for number values. The user has chosen an illegal operator in Grafana.')
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail='Operator not supported for number values.')
    return data_df


async def __filter_with_adhoc_filter_string(adhoc_filter, data_df):
    if adhoc_filter.operator == "=":
        data_df = data_df.loc[data_df[adhoc_filter.key] == adhoc_filter.value]
    elif adhoc_filter.operator == "!=":
        data_df = data_df.loc[data_df[adhoc_filter.key] != adhoc_filter.value]
    elif adhoc_filter.operator == "=~" or adhoc_filter.operator == "!~":
        try:
            matches = data_df[adhoc_filter.key].str.match(adhoc_filter.value)
            if adhoc_filter.operator == "!~":
                matches = ~matches

            data_df = data_df.loc[matches]
        except Exception as error:
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST,
                                detail='Malformed regular expression') from error
    else:
        logger.debug('Operator not supported for string values. The user has chosen an illegal operator in Grafana.')
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail='Operator not supported for string values.')
    return data_df


def __find_key_type(tags: List[Dict], tag_key):
    for tag in tags:
        if tag['text'] == tag_key:
            return tag['type']

    logger.debug('Could not find type because key is unknown.')
    raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail='Could not find type because key is unknown.')
