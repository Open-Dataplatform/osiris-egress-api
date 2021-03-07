"""
Implements endpoints which are required by the SimpleJson plugin for Grafana.
"""
from datetime import datetime
from http import HTTPStatus
from typing import Dict, List
import json

import pandas as pd
import numpy as np

import msal
from fastapi import APIRouter, HTTPException, Header
from fastapi.security.api_key import APIKeyHeader
from pandas import DataFrame

from azure.storage.filedatalake import DataLakeDirectoryClient, DataLakeFileClient
from azure.core.exceptions import ResourceNotFoundError

from ..dependencies import Configuration, AzureCredential
from ..schemas.simplejson_request import QueryRequest, TagValuesRequest

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

api_key_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['Grafana'])


@router.get('/{guid}', status_code=HTTPStatus.OK)
async def test_connection(guid: str, client_id: str = Header(None), client_secret: str = Header(None)):
    """
    Endpoint for Grafana connectivity test. This is checks if the GUID folder exist and the client_id
    and client_secret are valid.
    """
    logger.debug('Grafana root requested for GUID %s', guid)

    __get_directory_client(guid, client_id, client_secret)

    return {'message': 'Grafana datasource used for timeseries data.'}


@router.post('/{guid}/search', status_code=HTTPStatus.OK)
async def search(guid: str, client_id: str = Header(None), client_secret: str = Header(None)):
    """
    Returns the valid metrics.
    """
    logger.debug('Grafana search requested for GUID %s', guid)

    directory_client = __get_directory_client(guid, client_id, client_secret)
    grafana_settings = __get_grafana_settings(directory_client)

    return grafana_settings['metrics']


@router.post('/{guid}/query', status_code=HTTPStatus.OK)
async def query(guid: str, request: QueryRequest,
                client_id: str = Header(None), client_secret: str = Header(None)) -> List[Dict]:
    """
    Returns the data based on time range and target metric.
    """
    logger.debug('Grafana query requested for GUID %s', guid)

    if not __target_set_for_timeseries(request.targets):
        return []

    directory_client = __get_directory_client(guid, client_id, client_secret)
    from_date = pd.Timestamp(request.range['from']).to_pydatetime()
    to_date = pd.Timestamp(request.range['to']).to_pydatetime()

    data_df = __retrieve_data(from_date, to_date, directory_client)
    data_df = __filter_with_adhoc_filters(data_df, request.adhocFilters)

    results = []
    for target in request.targets:
        if target.type == 'timeseries':
            results.extend(__dataframe_to_timeserie_response(data_df, target.target))
        else:
            results.extend(__dataframe_to_table_response(data_df))

    return results


@router.post('/{guid}/annotations', status_code=HTTPStatus.OK)
async def annotation(guid: str) -> List:
    """
    Returns empty list of annotations.
    """
    logger.debug('Grafana annotations requested for GUID %s', guid)

    return []


@router.post('/{guid}/tag-keys', status_code=HTTPStatus.OK)
async def tag_keys(guid: str, client_id: str = Header(None), client_secret: str = Header(None)) -> List:
    """
    Returns list of tag-keys.
    """
    logger.debug('Grafana tag-keys requested for GUID %s', guid)

    directory_client = __get_directory_client(guid, client_id, client_secret)
    grafana_settings = __get_grafana_settings(directory_client)

    return grafana_settings['tag_keys']


@router.post('/{guid}/tag-values', status_code=HTTPStatus.OK)
async def tag_values(guid: str, request: TagValuesRequest,
                     client_id: str = Header(None), client_secret: str = Header(None)) -> List:
    """
    Returns list of tag values corresponding to request key.
    """
    logger.debug('Grafana tag-values requested for GUID %s', guid)

    directory_client = __get_directory_client(guid, client_id, client_secret)
    grafana_settings = __get_grafana_settings(directory_client)

    return grafana_settings['tag_values'][request.key]


def __target_set_for_timeseries(targets):
    for target in targets:
        if target.type == 'timeseries':
            if not target.target:
                return False

    return True


def __dataframe_to_timeserie_response(data_df: DataFrame, target: str) -> List[Dict]:
    response: List[Dict] = []

    if data_df is None or data_df.empty:
        return response

    data_df = data_df[target]

    response.append(__series_to_response(data_df, target))

    return response


def __dataframe_to_table_response(data_df: DataFrame) -> List[Dict]:
    response: List[Dict] = []

    if data_df is None or data_df.empty:
        return response

    no_index_data_df = data_df.reset_index(level=0)
    response.append({'type': 'table',
                     'columns': no_index_data_df.columns.map(lambda col: {'text': col}).tolist(),
                     'rows': no_index_data_df.where(pd.notnull(no_index_data_df), None).values.tolist()})

    return response


def __series_to_response(data_df: DataFrame, target: str) -> Dict:
    if data_df.empty:
        return {'target': target, 'datapoints': []}

    sorted_data_df = data_df.dropna().sort_index()
    timestamps = (sorted_data_df.index.astype(np.int64) // 10 ** 6).values.tolist()
    values = sorted_data_df.values.tolist()

    return {'target': target, 'datapoints': list(zip(values, timestamps))}


def __get_access_token(client_id: str, client_secret: str) -> str:
    authority = config['Azure Authentication']['authority']
    scopes = config['Azure Authentication']['scopes'].split()

    confidential_client = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret
    )

    result = confidential_client.acquire_token_silent(scopes, account=None)

    if not result:
        result = confidential_client.acquire_token_for_client(scopes=scopes)

    return result['access_token']


def __get_directory_client(guid: str, client_id: str, client_secret: str) -> DataLakeDirectoryClient:
    account_url = config['Azure Storage']['account_url']
    file_system_name = config['Azure Storage']['file_system_name']
    credential = AzureCredential(__get_access_token(client_id, client_secret))

    directory_client = DataLakeDirectoryClient(account_url, file_system_name, guid, credential=credential)
    try:
        directory_client.get_directory_properties()  # Test if the directory exist otherwise return error.
    except ResourceNotFoundError as error:
        logger.error(type(error).__name__, error)
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='The given dataset doesnt exist') from error

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


def __retrieve_data(from_date: datetime, to_date: datetime,
                    directory_client: DataLakeDirectoryClient) -> DataFrame:

    data = __retrieve_data_from_storage(from_date, to_date, directory_client)

    if data is not None:
        data.set_index('timestamp', inplace=True)
        data = data[from_date: to_date]  # type: ignore

    return data


def __retrieve_data_from_storage(from_date: datetime, to_date: datetime,
                                 directory_client: DataLakeDirectoryClient) -> DataFrame:

    time_range_dict = __arrange_time_range_in_dict(from_date, to_date)

    data = None
    for year in time_range_dict:
        year_dc = __get_sub_directory_client(directory_client, f'year={year}')
        if not year_dc:
            continue

        for month in time_range_dict[year]:
            month_dc = __get_sub_directory_client(year_dc, f'month={month:02d}')
            if not month_dc:
                continue

            for day in time_range_dict[year][month]:
                day_dc = __get_sub_directory_client(month_dc, f'day={day:02d}')
                if not day_dc:
                    continue

                for hour in time_range_dict[year][month][day]:
                    file_client: DataLakeFileClient = __get_file_client(day_dc, f'hour={hour}/data.json')
                    if not file_client:
                        continue

                    file_df = pd.read_json(file_client.download_file().readall())
                    data = pd.concat([data, file_df])

                    if data is not None and data.size > 10000:
                        return data

    return data


def __get_sub_directory_client(directory_client: DataLakeDirectoryClient, name: str):
    sub_directory_client: DataLakeDirectoryClient = directory_client.get_sub_directory_client(name)
    try:
        sub_directory_client.get_directory_properties()
    except ResourceNotFoundError:
        return None

    return sub_directory_client


def __get_file_client(directory_client: DataLakeDirectoryClient, path):
    file_client: DataLakeFileClient = directory_client.get_file_client(path)
    try:
        file_client.get_file_properties()
    except ResourceNotFoundError:
        return None

    return file_client


def __get_grafana_settings(directory_client: DataLakeDirectoryClient) -> Dict:
    settings_data = directory_client.get_file_client('grafana_settings.json').download_file().readall()
    return json.loads(settings_data)


def __filter_with_adhoc_filters(data_df: DataFrame, adhoc_filters: List):
    for adhoc_filter in adhoc_filters:
        data_df = data_df.loc[data_df[adhoc_filter.key] == adhoc_filter.value]

    return data_df
