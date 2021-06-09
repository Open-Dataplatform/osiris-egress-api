from io import BytesIO
from unittest.mock import patch, AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.schemas.dmi import CoordinatesModel


def get_app():
    with patch('app.dependencies.Configuration') as _:
        from app.main import app

        return TestClient(app)


client = get_app()


@pytest.fixture()
def mock_get_filesystem_client(mocker):
    mock = mocker.patch('app.routers.dmi.__get_filesystem_client')
    return mock


@pytest.fixture()
def mock_get_file_stream_for_dmi_dt_type_file(mocker):
    mock = mocker.patch('app.routers.dmi.__get_file_stream_for_dmi_dt_type_file')
    return mock


@pytest.fixture()
def mock_get_file_stream_for_dmi_type_coords_file(mocker):
    mock = mocker.patch('app.routers.dmi.__get_file_stream_for_dmi_type_coords_file')
    return mock


@pytest.fixture()
def mock_get_coordinates_for_dmi_weather_type(mocker):
    mock = mocker.patch('app.routers.dmi.__get_coordinates_for_dmi_weather_type')
    return mock


@pytest.mark.parametrize('url', [
    '2018/06/05/10/radiation_diffus',
    '2018/4/1/00019/temperatur_2m',
])
def test_download_dmi_datetime_type(url,
                                    mock_get_filesystem_client: AsyncMock,
                                    mock_get_file_stream_for_dmi_dt_type_file: AsyncMock):
    """
    TODO: Docstring
    """
    mock_get_file_stream_for_dmi_dt_type_file.return_value = BytesIO(b'data')
    response = client.get(
        f'/dmi/{url}',
        headers={'Authorization': 'secret'}
    )
    assert response.status_code == 200
    assert mock_get_filesystem_client.call_count == 1


@pytest.mark.parametrize('url', [
    'abc/06/05/10/radiation_diffus',
    '2018/4/1/19/non_existant',
])
def test_download_dmi_datetime_type_fails_type_validation(url):
    """
    TODO: Docstring
    """
    response = client.get(
        f'/dmi/{url}',
        headers={'Authorization': 'secret'}
    )
    assert 'type_error' in response.json()['detail'][0]['type']


@pytest.mark.parametrize('url', [
    'radiation_diffus/12.3/45.6/2018',
    'temperatur_2m/12/45/2018',
])
def test_download_dmi_weather_type_coords(url,
                                          mock_get_filesystem_client: AsyncMock,
                                          mock_get_file_stream_for_dmi_type_coords_file: AsyncMock):
    """
    TODO: Docstring
    """
    mock_get_file_stream_for_dmi_type_coords_file.return_value = BytesIO(b'data')
    response = client.get(
        f'/dmi/{url}',
        headers={'Authorization': 'secret'}
    )
    assert response.status_code == 200
    assert mock_get_filesystem_client.call_count == 1


@pytest.mark.parametrize('url', [
    'radiation_diffus/abc/45.6/2018',
    'non_existant/12/45/2018',
])
def test_download_dmi_weather_type_coords_fails_type_validation(url):
    """
    TODO: Docstring
    """
    response = client.get(
        f'/dmi/{url}',
        headers={'Authorization': 'secret'}
    )
    assert 'type_error' in response.json()['detail'][0]['type']


@pytest.mark.parametrize('url', [
    'radiation_diffus',
    'temperatur_2m',
])
def test_get_dmi_coords_for_weather_type(url,
                                         mock_get_coordinates_for_dmi_weather_type: AsyncMock,
                                         mock_get_filesystem_client: AsyncMock):
    """
    TODO: Docstring
    """
    # Returns a `schemas.dmi.CoordinatesModel`
    model_data = CoordinatesModel(latitude=12.3, longitude=45.6)
    mock_get_coordinates_for_dmi_weather_type.return_value = [model_data]
    response = client.get(
    f'/dmi/{url}',
        headers={'Authorization': 'secret'}
    )
    assert response.status_code == 200
    assert mock_get_filesystem_client.call_count == 1


@pytest.mark.parametrize('url', [
    '123',
    'non_existant',
])
def test_get_dmi_coords_for_weather_type_fails_type_validation(url):
    """
    TODO: Docstring
    """
    # Returns a `schemas.dmi.CoordinatesModel`
    response = client.get(
        f'/dmi/{url}',
        headers={'Authorization': 'secret'}
    )
    assert 'type_error' in response.json()['detail'][0]['type']
