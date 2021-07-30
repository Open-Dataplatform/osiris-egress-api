from datetime import datetime
from http import HTTPStatus
from types import SimpleNamespace
from unittest.mock import patch
import pytest

import pandas as pd
from fastapi import HTTPException

from fastapi.testclient import TestClient
from osiris.core.enums import TimeResolution


def get_app():
    with patch('osiris.core.configuration.Configuration') as _:
        from app.main import app

        return TestClient(app)


client = get_app()


def test_connection(mocker):
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    check_directory_exist = mocker.patch('app.routers.grafana_json.__check_directory_exist')

    response = client.get(
        'grafana/12345',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'}
    )

    assert response.status_code == HTTPStatus.OK
    assert directory_client.called
    assert directory_client.call_args.args == ('12345', 'mr_test', 'secret')
    assert check_directory_exist.called


def test_connection_missing_headers(mocker):
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    check_directory_exist = mocker.patch('app.routers.grafana_json.__check_directory_exist')

    response = client.get(
        'grafana/12345'
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert not directory_client.called
    assert not check_directory_exist.called


def test_search(mocker):
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    get_grafana_settings = mocker.patch('app.routers.grafana_json.__get_grafana_settings')

    get_grafana_settings.return_value = {'metrics': ['c', 'a', 'b']}

    response = client.post(
        'grafana/12345/search',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'}
    )

    assert response.status_code == HTTPStatus.OK
    assert directory_client.called
    assert directory_client.call_args.args == ('12345', 'mr_test', 'secret')
    assert get_grafana_settings.called
    assert response.json() == ['a', 'b', 'c']


def test_query_without_targets(mocker):
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')

    response = client.post(
        'grafana/12345/query',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'},
        json={'maxDataPoints': 31,
              'adhocFilters': [],
              'intervalMs': 47,
              'targets': [],
              'range': {'from': '2021-01-01',
                        'to': '2021-02-01'
                        }
              }
    )

    assert response.status_code == HTTPStatus.OK
    assert not directory_client.called

    response = client.post(
        'grafana/12345/query',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'},
        json={'maxDataPoints': 31,
              'adhocFilters': [],
              'intervalMs': 47,
              'targets': [{'refId': 'id', 'target': '', 'type': 'atype', 'data': {}}],
              'range': {'from': '2021-01-01', 'to': '2021-02-01'}
              }
    )

    assert response.status_code == HTTPStatus.OK
    assert not directory_client.called


def test_query_with_targets(mocker):
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    get_grafana_settings = mocker.patch('app.routers.grafana_json.__get_grafana_settings')
    retrieve_data = mocker.patch('app.routers.grafana_json.__retrieve_data')
    filter_with_adhoc_filters = mocker.patch('app.routers.grafana_json.__filter_with_adhoc_filters')
    dataframe_to_response = mocker.patch('app.routers.grafana_json.__dataframe_to_response')

    response = client.post(
        'grafana/12345/query',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'},
        json={'maxDataPoints': 31,
              'adhocFilters': [],
              'intervalMs': 47,
              'targets': [{'refId': 'id', 'target': 'atarget', 'type': 'atype', 'data': {}}],
              'range': {'from': '2021-01-01T10:01:02', 'to': '2021-02-01T08:23:20'}
              }
    )

    assert response.status_code == HTTPStatus.OK
    assert directory_client.called
    assert get_grafana_settings.called
    assert retrieve_data.called
    assert retrieve_data.await_args.args[0] == datetime(2021, 1, 1, 10, 1, 2)
    assert retrieve_data.await_args.args[1] == datetime(2021, 2, 1, 8, 23, 20)
    assert filter_with_adhoc_filters.called
    assert dataframe_to_response.called
    assert response.json() == []


def test_annotations():

    response = client.post(
        'grafana/12345/annotations',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'}
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == []


def test_tag_keys(mocker):
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    get_grafana_settings = mocker.patch('app.routers.grafana_json.__get_grafana_settings')

    get_grafana_settings.return_value = {'tag_keys': ['a', 'b', 'c']}

    response = client.post(
        'grafana/12345/tag-keys',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'}
    )

    assert response.status_code == HTTPStatus.OK
    assert directory_client.called
    assert directory_client.call_args.args == ('12345', 'mr_test', 'secret')
    assert get_grafana_settings.called
    assert response.json() == ['a', 'b', 'c']


def test_tag_values(mocker):
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    get_grafana_settings = mocker.patch('app.routers.grafana_json.__get_grafana_settings')

    get_grafana_settings.return_value = {'tag_values': {'test_key': ['a', 'b', 'c']}}

    response = client.post(
        'grafana/12345/tag-values',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'},
        json={'key': 'test_key'}
    )

    assert response.status_code == HTTPStatus.OK
    assert directory_client.called
    assert directory_client.call_args.args == ('12345', 'mr_test', 'secret')
    assert get_grafana_settings.called
    assert response.json() == ['a', 'b', 'c']

    response = client.post(
        'grafana/12345/tag-values',
        headers={'client-id': 'mr_test', 'client-secret': 'secret'},
        json={'key': 'unknown_key'}
    )

    assert response.status_code == HTTPStatus.OK
    assert directory_client.called
    assert get_grafana_settings.called
    assert response.json() == []


def test_is_targets_set_for_all():
    from app.routers.grafana_json import __is_targets_set_for_all

    targets = []

    result = __is_targets_set_for_all(targets)

    assert not result

    targets = [SimpleNamespace(**{'refId': 'id', 'target': '', 'type': 'atype', 'data': 'adata'}),
               SimpleNamespace(**{'refId': 'id', 'target': '323', 'type': 'atype', 'data': 'adata'})]

    result = __is_targets_set_for_all(targets)

    assert not result

    targets = [SimpleNamespace(**{'refId': 'id', 'target': '23', 'type': 'atype', 'data': 'adata'}),
               SimpleNamespace(**{'refId': 'id', 'target': '323', 'type': 'atype', 'data': 'adata'})]

    result = __is_targets_set_for_all(targets)

    assert result


def test_filter_with_additional_filters():
    from app.routers.grafana_json import __filter_with_additional_filters

    data = [{'metric_1': 'value_1', 'metric_2': 'value_2'},
            {'metric_1': 'value_2', 'metric_2': 'value_1'},
            {'metric_1': 'value_6', 'metric_2': 'value_4'},
            {'metric_1': 'value_2', 'metric_2': 'value_2'}]

    data_df = pd.DataFrame(data)

    additional_filters = {'metric_1': 'value_2', 'metric_2': 'value_1'}
    target = 'test_query'

    target_return_name, res_data_df = __filter_with_additional_filters(data_df, target, additional_filters)

    assert target_return_name == 'value_2_value_1_test_query'
    assert res_data_df.shape[0] == 1
    assert res_data_df.to_dict(orient='records') == [{'metric_1': 'value_2', 'metric_2': 'value_1'}]


def test_dataframe_to_response(mocker):
    from app.routers.grafana_json import __dataframe_to_response
    resample_timeframe = mocker.patch('app.routers.grafana_json.__resample_timeframe')
    dataframe_to_timeserie_response = mocker.patch('app.routers.grafana_json.__dataframe_to_timeserie_response')
    dataframe_to_table_response = mocker.patch('app.routers.grafana_json.__dataframe_to_table_response')

    data_df = pd.DataFrame([])
    additional_filters = {}
    target = 'Raw'
    target_type = 'timeseries'
    freq = '1000ms'
    grafana_settings = {}

    result = __dataframe_to_response(data_df, target_type, target, additional_filters,  freq, grafana_settings)

    assert result == []

    data = [{'metric_1': 'value_1', 'metric_2': 'value_2'},
            {'metric_1': 'value_2', 'metric_2': 'value_1'},
            {'metric_1': 'value_6', 'metric_2': 'value_4'},
            {'metric_1': 'value_2', 'metric_2': 'value_2'}]

    data_df = pd.DataFrame(data)

    result = __dataframe_to_response(data_df, target_type, target, additional_filters,  freq, grafana_settings)

    assert result == []

    target = 'metric_1'
    resample_timeframe.return_value = data_df

    _ = __dataframe_to_response(data_df, target_type, target, additional_filters, freq, grafana_settings)

    assert resample_timeframe.called
    assert dataframe_to_timeserie_response.called
    assert not dataframe_to_table_response.called

    target_type = 'table'

    _ = __dataframe_to_response(data_df, target_type, target, additional_filters, freq, grafana_settings)

    assert resample_timeframe.called
    assert dataframe_to_timeserie_response.call_count == 1
    assert dataframe_to_table_response.called


def test_dataframe_to_timeserie_response():
    from app.routers.grafana_json import __dataframe_to_timeserie_response

    data_df = pd.DataFrame([])
    target_return_name = 'query1'

    result = __dataframe_to_timeserie_response(data_df, target_return_name)

    assert result == [{'target': 'query1', 'datapoints': []}]

    data = [{'datetime': '2021-01-01T01:01:01', 'metric_1': 'value_1', 'metric_2': 'value_2'},
            {'datetime': '2021-01-02T01:01:01', 'metric_1': 'value_2', 'metric_2': 'value_1'},
            {'datetime': '2021-01-03T01:01:01', 'metric_1': 'value_6', 'metric_2': 'value_4'},
            {'datetime': '2021-01-04T01:01:01', 'metric_1': 'value_2', 'metric_2': 'value_2'}]

    data_df = pd.DataFrame(data)
    data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df.set_index('datetime', inplace=True)

    result = __dataframe_to_timeserie_response(data_df, target_return_name)

    assert result == [{'target': 'query1',
                       'datapoints': [(['value_1', 'value_2'], 1609462861000),
                                      (['value_2', 'value_1'], 1609549261000),
                                      (['value_6', 'value_4'], 1609635661000),
                                      (['value_2', 'value_2'], 1609722061000)]}]


def test_dataframe_to_table_response():
    from app.routers.grafana_json import __dataframe_to_table_response

    grafana_settings = {'date_key_field': 'datetime'}
    data = [{'datetime': '2021-01-01T01:01:01', 'metric_1': 'value_1', 'metric_2': 'value_2'},
            {'datetime': '2021-01-02T01:01:01', 'metric_1': 'value_2', 'metric_2': 'value_1'},
            {'datetime': '2021-01-03T01:01:01', 'metric_1': 'value_6', 'metric_2': 'value_4'},
            {'datetime': '2021-01-04T01:01:01', 'metric_1': 'value_2', 'metric_2': 'value_2'}]

    data_df = pd.DataFrame(data)
    data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df.set_index('datetime', inplace=True)

    result = __dataframe_to_table_response(data_df, grafana_settings)

    assert result == [{'type': 'table',
                       'columns': [{'text': 'datetime'}, {'text': 'metric_1'}, {'text': 'metric_2'}],
                       'rows': [['2021-01-01T01:01:01', 'value_1', 'value_2'],
                                ['2021-01-02T01:01:01', 'value_2', 'value_1'],
                                ['2021-01-03T01:01:01', 'value_6', 'value_4'],
                                ['2021-01-04T01:01:01', 'value_2', 'value_2']]}]


async def test_download_files(mocker):
    from app.routers.grafana_json import __download_files
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    read_parquet = mocker.patch('app.routers.grafana_json.pd.read_parquet')
    read_parquet.return_value = 'some data'
    span = mocker.patch('jaeger_client.Span')
    download_data = mocker.patch('app.routers.grafana_json.__download_data')
    download_data.return_value = b'some data'

    timeslot_chunks = [datetime(2021, 1, 1), datetime(2021, 2, 1)]
    time_resolution = TimeResolution.DAY

    result = await __download_files(timeslot_chunks, time_resolution, directory_client, span)

    assert download_data.call_count == 2
    assert read_parquet.call_count == 2
    assert result == ['some data', 'some data']


async def test_retrieve_data(mocker):
    from app.routers.grafana_json import __retrieve_data
    directory_client = mocker.patch('app.routers.grafana_json.__get_directory_client')
    download_files = mocker.patch('app.routers.grafana_json.__download_files')

    data = [{'datetime': '2021-01-01T01:01:01', 'metric_1': 'value_1', 'metric_2': 'value_2'}]
    data_df = pd.DataFrame(data)

    download_files.return_value = [data_df]
    grafana_settings = {'time_resolution': 'DAY', 'date_key_field': 'datetime'}
    span = mocker.patch('jaeger_client.Span')

    from_date = datetime(2021, 1, 1)
    to_date = datetime(2021, 1, 3)

    result = await __retrieve_data(from_date, to_date, grafana_settings, directory_client, span)

    assert download_files.call_count == 1
    assert result.index.values.astype(str)[0][:19] == '2021-01-01T01:01:01'


def test_filter_with_adhoc_filters(mocker):
    from app.routers.grafana_json import __filter_with_adhoc_filters

    filter_with_adhoc_filter_string = mocker.patch('app.routers.grafana_json.__filter_with_adhoc_filter_string')
    filter_with_adhoc_filter_number = mocker.patch('app.routers.grafana_json.__filter_with_adhoc_filter_number')

    grafana_settings = {'tag_keys': [{'text': 'metric_1', 'type': 'number'},
                                     {'text': 'metric_2', 'type': 'string'},
                                     {'text': 'metric_3', 'type': 'error_type'}]}

    data = [{'metric_1': 1, 'metric_2': 'value_2'},
            {'metric_1': 3, 'metric_2': 'value_1'},
            {'metric_1': 4, 'metric_2': 'value_4'},
            {'metric_1': 5, 'metric_2': 'value_2'}]
    data_df = pd.DataFrame(data)

    adhoc_filters = [SimpleNamespace(**{'key': 'metric_1'})]

    _ = __filter_with_adhoc_filters(data_df, adhoc_filters, grafana_settings)

    assert filter_with_adhoc_filter_number.called
    assert not filter_with_adhoc_filter_string.called

    adhoc_filters = [SimpleNamespace(**{'key': 'metric_2'})]

    _ = __filter_with_adhoc_filters(data_df, adhoc_filters, grafana_settings)

    assert filter_with_adhoc_filter_number.call_count == 1
    assert filter_with_adhoc_filter_string.called

    adhoc_filters = [SimpleNamespace(**{'key': 'metric_3'})]

    with pytest.raises(HTTPException) as excinfo:
        _ = __filter_with_adhoc_filters(data_df, adhoc_filters, grafana_settings)

    assert excinfo.value.detail == 'Unknown key type. Supported types are "text" and "number".'
    assert filter_with_adhoc_filter_number.call_count == 1
    assert filter_with_adhoc_filter_string.call_count == 1


def test_filter_with_adhoc_filter_number():
    from app.routers.grafana_json import  __filter_with_adhoc_filter_number

    data = [{'metric_1': 1, 'metric_2': 'value_1'},
            {'metric_1': 2, 'metric_2': 'value_2'},
            {'metric_1': 3, 'metric_2': 'value_3'},
            {'metric_1': 4, 'metric_2': 'value_3'}]
    data_df = pd.DataFrame(data)

    adhoc_filters = SimpleNamespace(**{'key': 'metric_1', 'operator': '=', 'value': 3})
    result = __filter_with_adhoc_filter_number(adhoc_filters, data_df)

    assert result.shape[0] == 1
    assert result.iloc[0]['metric_1'].item() == 3

    adhoc_filters = SimpleNamespace(**{'key': 'metric_1', 'operator': '!=', 'value': 2})
    result = __filter_with_adhoc_filter_number(adhoc_filters, data_df)

    assert result.shape[0] == 3
    assert [value for _, value in result['metric_1'].items()] == [1, 3, 4]

    adhoc_filters = SimpleNamespace(**{'key': 'metric_1', 'operator': '<', 'value': 3})
    result = __filter_with_adhoc_filter_number(adhoc_filters, data_df)

    assert result.shape[0] == 2
    assert [value for _, value in result['metric_1'].items()] == [1, 2]

    adhoc_filters = SimpleNamespace(**{'key': 'metric_1', 'operator': '>', 'value': 2})
    result = __filter_with_adhoc_filter_number(adhoc_filters, data_df)

    assert result.shape[0] == 2
    assert [value for _, value in result['metric_1'].items()] == [3, 4]

    adhoc_filters = SimpleNamespace(**{'key': 'metric_1', 'operator': '!!', 'value': 2})
    with pytest.raises(HTTPException) as excinfo:
        _ = __filter_with_adhoc_filter_number(adhoc_filters, data_df)

    assert excinfo.value.detail == 'Operator not supported for number values.'
    assert  excinfo.value.status_code == HTTPStatus.BAD_REQUEST


def test_filter_with_adhoc_filter_string():
    from app.routers.grafana_json import __filter_with_adhoc_filter_string

    data = [{'metric_1': 1, 'metric_2': 'value_1'},
            {'metric_1': 2, 'metric_2': 'value_2'},
            {'metric_1': 3, 'metric_2': 'value_3'},
            {'metric_1': 4, 'metric_2': 'value_3'}]
    data_df = pd.DataFrame(data)

    adhoc_filters = SimpleNamespace(**{'key': 'metric_2', 'operator': '=', 'value': 'value_1'})
    result = __filter_with_adhoc_filter_string(adhoc_filters, data_df)

    assert result.shape[0] == 1
    assert result.iloc[0]['metric_2'] == 'value_1'

    adhoc_filters = SimpleNamespace(**{'key': 'metric_2', 'operator': '!=', 'value': 'value_2'})
    result = __filter_with_adhoc_filter_string(adhoc_filters, data_df)

    assert result.shape[0] == 3
    assert [value for _, value in result['metric_2'].items()] == ['value_1', 'value_3', 'value_3']

    adhoc_filters = SimpleNamespace(**{'key': 'metric_2', 'operator': '=~', 'value': 'v.*3'})
    result = __filter_with_adhoc_filter_string(adhoc_filters, data_df)

    assert result.shape[0] == 2
    assert [value for _, value in result['metric_2'].items()] == ['value_3', 'value_3']

    adhoc_filters = SimpleNamespace(**{'key': 'metric_2', 'operator': '!~', 'value': 'value_3'})
    result = __filter_with_adhoc_filter_string(adhoc_filters, data_df)

    assert result.shape[0] == 2
    assert [value for _, value in result['metric_2'].items()] == ['value_1', 'value_2']

    adhoc_filters = SimpleNamespace(**{'key': 'metric_2', 'operator': '!!', 'value': 'value_3'})
    with pytest.raises(HTTPException) as excinfo:
        _ = __filter_with_adhoc_filter_string(adhoc_filters, data_df)

    assert excinfo.value.detail == 'Operator not supported for string values.'
    assert  excinfo.value.status_code == HTTPStatus.BAD_REQUEST


def test_find_key_type():
    from app.routers.grafana_json import __find_key_type

    tags = [{'text': 'key_1', 'type': 'number'}, {'text': 'key_2', 'type': 'number'}]
    tag_key = 'key_1'

    result = __find_key_type(tags, tag_key)

    assert result == 'number'

    tag_key = 'key_3'
    with pytest.raises(HTTPException) as excinfo:
        _ = __find_key_type(tags, tag_key)

    assert excinfo.value.detail == 'Could not find type because key is unknown.'
    assert  excinfo.value.status_code == HTTPStatus.BAD_REQUEST

