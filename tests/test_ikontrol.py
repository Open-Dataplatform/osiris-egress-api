from io import BytesIO
from unittest.mock import AsyncMock
import pytest
from tests.test_main import client


@pytest.fixture()
def mock_get_filesystem_client(mocker):
    mock = mocker.patch('app.routers.downloads.__get_filesystem_client')
    return mock


@pytest.fixture()
def mock_get_file_stream_for_ikontrol_file(mocker):
    mock = mocker.patch('app.routers.downloads.__get_file_stream_for_ikontrol_file')
    return mock


@pytest.mark.parametrize('data', [b'{"test": "value"}', b'[{"abc": 123}]'])
def test_ikontrol_get_all_projects(data, mock_get_filesystem_client: AsyncMock,
                                   mock_get_file_stream_for_ikontrol_file: AsyncMock):
    """
    Mock get_file_system_client and get_file_stream_for_ikontrol_file.
    Mock the return value for get_file_stream_for_ikontrol_file
    Call the end point
    Assert if status code OK
    Assert if mock_get_filesystem_client was called exactly 1 time
    Assert if data equals the response of the call
    """
    mock_get_file_stream_for_ikontrol_file.return_value = BytesIO(data)
    response = client.get(
        '/ikontrol/getallprojects',
        headers={'Authorization': 'secret'}
    )

    assert response.status_code == 200
    assert mock_get_filesystem_client.call_count == 1
    assert data == response.content


@pytest.mark.parametrize('data', [b'{"a": {}, "b": [], "c": []}'])
def test_ikontrol_download_data(data, mock_get_filesystem_client: AsyncMock,
                                mock_get_file_stream_for_ikontrol_file: AsyncMock):
    """
    Mock get_file_system_client and get_file_stream_for_ikontrol_file.
    Mock the return value for get_file_stream_for_ikontrol_file
    Call the end point
    Assert if status code OK
    Assert if mock_get_filesystem_client was called exactly 1 time
    Assert if data equals the response of the call
    """
    mock_get_file_stream_for_ikontrol_file.return_value = BytesIO(data)
    response = client.get(
        '/ikontrol/123',
        headers={'Authorization': 'secret'}
    )

    assert response.status_code == 200
    assert mock_get_filesystem_client.call_count == 1
    assert data == response.content


@pytest.mark.parametrize('data', [b'abc123'])
def test_ikontrol_download_zip(data, mock_get_filesystem_client: AsyncMock,
                               mock_get_file_stream_for_ikontrol_file: AsyncMock):
    """
    Mock get_file_system_client and get_file_stream_for_ikontrol_file.
    Mock the return value for get_file_stream_for_ikontrol_file
    Call the end point
    Assert if status code OK
    Assert if mock_get_filesystem_client was called exactly 1 time
    Assert if data equals the response of the call
    """
    mock_get_file_stream_for_ikontrol_file.return_value = BytesIO(data)
    response = client.get(
        '/ikontrol/getzip/123',
        headers={'Authorization': 'secret'}
    )

    assert response.status_code == 200
    assert mock_get_filesystem_client.call_count == 1
    assert data == response.content
