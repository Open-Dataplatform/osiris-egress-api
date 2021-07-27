import json
from unittest.mock import patch, AsyncMock

import pytest
from fastapi.testclient import TestClient


def get_app():
    with patch('osiris.core.configuration.Configuration') as _:
        from app.main import app

        return TestClient(app)


client = get_app()


@pytest.fixture()
def mock_download_parquet_data(mocker):
    mock = mocker.patch('app.routers.downloads.__download_parquet_data')
    return mock


@pytest.fixture()
def mock_download_parquet_file_path(mocker):
    mock = mocker.patch('app.routers.downloads.__download_parquet_file_path')
    return mock


@pytest.mark.parametrize('from_date', [
    '2019',
    '2020',
])
def test_download_jao_data_yearly(from_date,
                                  mock_download_parquet_data: AsyncMock):
    """
    Tests calling the JAO Yearly download endpoint with valid dates.
    """
    mock_download_parquet_data.return_value = json.loads('{}'), 200
    response = client.get(
        '/jao',
        params={'from_date': from_date, 'horizon': 'Yearly'},
        headers={'Authorization': 'secret'}
    )
    assert response.status_code == 200
    assert mock_download_parquet_data.call_count == 1


@pytest.mark.parametrize('from_date', [
    '2019-04',
    '2020-04',
])
def test_download_jao_data_monthly(from_date,
                                   mock_download_parquet_data: AsyncMock):
    """
    Tests calling the JAO Monthly download endpoint with valid dates.
    """
    mock_download_parquet_data.return_value = json.loads('{}'), 200
    response = client.get(
        '/jao',
        params={'from_date': from_date, 'horizon': 'Monthly'},
        headers={'Authorization': 'secret'}
    )
    assert response.status_code == 200
    assert mock_download_parquet_data.call_count == 1


@pytest.mark.parametrize('year', [
    '2019',
    '2020',
])
def test_download_jao_eds_data(year,
                               mock_download_parquet_file_path: AsyncMock):
    """
    Tests calling the JAO Monthly download endpoint with valid dates.
    """
    mock_download_parquet_file_path.return_value = json.loads('{}'), 200
    response = client.get(
        f'/jao_eds/{year}/04/D2-DE',
        headers={'Authorization': 'secret'}
    )
    assert response.status_code == 200
    assert mock_download_parquet_file_path.call_count == 1
