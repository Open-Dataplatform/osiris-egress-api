from datetime import datetime
from http import HTTPStatus
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi import HTTPException

from fastapi.testclient import TestClient
from osiris.core.enums import TimeResolution

import app.routers.downloads


def get_app():
    with patch('osiris.core.configuration.Configuration') as _:
        from app.main import app

        return TestClient(app)


client = get_app()


@pytest.mark.parametrize('test_endpoint', ['/12345/json', '/jao', '/neptun', '/delfin'])
def test_upload_file_no_authorization_token(test_endpoint):
    response = client.get(
        test_endpoint,
    )

    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json() == {'detail': 'Not authenticated'}


def test_download_json(mocker):
    download_parquet_data = mocker.patch('app.routers.downloads.__download_parquet_data')
    download_parquet_data.return_value = ({'data': 'data'}, HTTPStatus.OK)

    response = client.get(
        '/12345/json',
        headers={'Authorization': 'secret'},
        params={'from_date': '2021-01-01', 'to_date': '2021-01-02'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('12345', 'secret', '2021-01-01', '2021-01-02')
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/12345/json',
        headers={'Authorization': 'secret'},
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('12345', 'secret', None, None)
    assert response.json() == {'data': 'data'}


def test_download_jao(mocker):
    download_parquet_data = mocker.patch('app.routers.downloads.__download_parquet_data')
    download_parquet_data.return_value = ({'data': 'data'}, HTTPStatus.OK)

    app.routers.downloads.config = {'JAO': {'yearly_guid': 'yearly_guid_1234',
                                            'monthly_guid': 'monthly_guid_1234'}}

    response = client.get(
        '/jao',
        headers={'Authorization': 'secret'},
        params={'horizon': 'YEARLY', 'from_date': '2021-01-01', 'to_date': '2021-01-02'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('yearly_guid_1234', 'secret', '2021-01-01', '2021-01-02')
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/jao',
        headers={'Authorization': 'secret'},
        params={'horizon': 'MONTHLY', 'from_date': '2021-01-01', 'to_date': '2021-01-02'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('monthly_guid_1234', 'secret', '2021-01-01', '2021-01-02')
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/jao',
        headers={'Authorization': 'secret'},
        params={'horizon': 'MONTHLY'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('monthly_guid_1234', 'secret', None, None)
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/jao',
        headers={'Authorization': 'secret'},
        params={'horizon': 'DAILY'}
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json()['detail'] == '(ValueError) The horizon value can only be Yearly or Monthly.'


def test_download_neptun(mocker):
    download_parquet_data = mocker.patch('app.routers.downloads.__download_parquet_data')
    download_parquet_data.return_value = ({'data': 'data'}, HTTPStatus.OK)

    app.routers.downloads.config = {'Neptun': {'daily_guid': 'daily_guid_1234',
                                               'hourly_guid': 'hourly_guid_1234',
                                               'minutely_guid': 'minutely_guid_1234'}}

    response = client.get(
        '/neptun',
        headers={'Authorization': 'secret'},
        params={'horizon': 'DAILY', 'from_date': '2021-01-01', 'to_date': '2021-01-02', 'tags': 'TAG1, TAG2'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('daily_guid_1234', 'secret', '2021-01-01', '2021-01-02',
                                                     [[('Tag', '=', 'TAG1')], [('Tag', '=', ' TAG2')]])
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/neptun',
        headers={'Authorization': 'secret'},
        params={'horizon': 'HOURLY', 'from_date': '2021-01-01', 'to_date': '2021-01-02'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('hourly_guid_1234', 'secret', '2021-01-01', '2021-01-02', None)
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/neptun',
        headers={'Authorization': 'secret'},
        params={'horizon': 'MINUTELY', 'from_date': '2021-01-01', 'to_date': '2021-01-02'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('minutely_guid_1234', 'secret', '2021-01-01', '2021-01-02', None)
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/neptun',
        headers={'Authorization': 'secret'},
        params={'horizon': 'MINUTELY'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('minutely_guid_1234', 'secret', None, None, None)
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/neptun',
        headers={'Authorization': 'secret'},
        params={'horizon': 'YEARLY'}
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json()['detail'] == '(ValueError) The horizon parameter must be Daily, Hourly or Minutely.'


def test_download_delfin(mocker):
    download_parquet_data = mocker.patch('app.routers.downloads.__download_parquet_data')
    download_parquet_data.return_value = ({'data': 'data'}, HTTPStatus.OK)

    app.routers.downloads.config = {'Delfin': {'daily_guid': 'daily_guid_1234',
                                               'hourly_guid': 'hourly_guid_1234',
                                               'minutely_guid': 'minutely_guid_1234'}}

    response = client.get(
        '/delfin',
        headers={'Authorization': 'secret'},
        params={'horizon': 'DAILY', 'from_date': '2021-01-01', 'to_date': '2021-01-02', 'table_indices': 'TAG1, TAG2'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('daily_guid_1234', 'secret', '2021-01-01', '2021-01-02',
                                                     [[('TABLE_INDEX', '=', 'TAG1')], [('TABLE_INDEX', '=', ' TAG2')]])
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/delfin',
        headers={'Authorization': 'secret'},
        params={'horizon': 'HOURLY', 'from_date': '2021-01-01', 'to_date': '2021-01-02'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('hourly_guid_1234', 'secret', '2021-01-01', '2021-01-02', None)
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/delfin',
        headers={'Authorization': 'secret'},
        params={'horizon': 'MINUTELY', 'from_date': '2021-01-01', 'to_date': '2021-01-02'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('minutely_guid_1234', 'secret', '2021-01-01', '2021-01-02', None)
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/delfin',
        headers={'Authorization': 'secret'},
        params={'horizon': 'MINUTELY'}
    )

    assert response.status_code == HTTPStatus.OK
    assert download_parquet_data.called
    assert download_parquet_data.await_args.args == ('minutely_guid_1234', 'secret', None, None, None)
    assert response.json() == {'data': 'data'}

    response = client.get(
        '/delfin',
        headers={'Authorization': 'secret'},
        params={'horizon': 'YEARLY'}
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json()['detail'] == '(ValueError) The horizon parameter must be Daily, Hourly or Minutely.'


async def test_download_parquet_data_parse_exeception(mocker):
    from app.routers.downloads import __download_parquet_data
    parse_date_arguments = mocker.patch('app.routers.downloads.__parse_date_arguments')

    parse_date_arguments.side_effect = ValueError('Exception')
    guid = '12345'
    token = 'secret'
    from_date = '2021-01-01'
    to_date = '2021-02-01'
    filters = ['some filter']

    with pytest.raises(HTTPException) as execinfo:
        _ = await __download_parquet_data(guid, token, from_date, to_date, filters)

    assert execinfo.value.status_code == HTTPStatus.BAD_REQUEST
    assert execinfo.value.detail == '(ValueError) Wrong string format for date(s): Exception'


async def test_download_parquet_data(mocker):
    from app.routers.downloads import __download_parquet_data

    filesystem_client = mocker.patch('app.routers.downloads.__get_filesystem_client')
    check_directory_exist = mocker.patch('app.routers.downloads.__check_directory_exist')
    download_parquet_files = mocker.patch('app.routers.downloads.__download_parquet_files')
    download_parquet_files.return_value = [[{'data': 'data'}]]

    guid = '12345'
    token = 'secret'
    from_date = '2021-01-01'
    to_date = '2021-01-02'
    filters = ['some filter']

    result = await __download_parquet_data(guid, token, from_date, to_date, filters)

    assert filesystem_client.called
    assert filesystem_client.call_args.args[0] == 'secret'
    assert check_directory_exist.called
    assert download_parquet_files.called
    assert download_parquet_files.await_args.args[0:2] == ([datetime(2021, 1, 1), datetime(2021, 1, 2)],
                                                           TimeResolution.DAY)

    assert result[0] == [{'data': 'data'}]
    assert result[1] == HTTPStatus.OK

    download_parquet_files.return_value = []

    result = await __download_parquet_data(guid, token, from_date, to_date, filters)
    assert result[0] == []
    assert result[1] == HTTPStatus.NO_CONTENT


@pytest.mark.filterwarnings("ignore:coroutine 'AsyncMockMixin._execute_mock_call' was never awaited")
async def test_download_parquet_files(mocker):
    from app.routers.downloads import __download_parquet_files
    directory_client = mocker.patch('azure.storage.filedatalake.aio.DataLakeDirectoryClient')
    read_parquet = mocker.patch('app.routers.downloads.pd.read_parquet')
    read_parquet.return_value = pd.DataFrame([{'metric_1': 'value_1', 'metric_2': 'value_2'}])
    span = mocker.patch('jaeger_client.Span')
    download_data = mocker.patch('app.routers.downloads.__download_data')
    download_data.return_value = b'some data'

    timeslot_chunks = [datetime(2021, 1, 1), datetime(2021, 2, 1)]
    time_resolution = TimeResolution.DAY
    filters = ['some filter']

    result = await __download_parquet_files(timeslot_chunks, time_resolution, directory_client, span, filters)

    assert download_data.call_count == 2
    assert read_parquet.call_count == 2
    assert result == [[{'metric_1': 'value_1', 'metric_2': 'value_2'}],
                      [{'metric_1': 'value_1', 'metric_2': 'value_2'}]]
