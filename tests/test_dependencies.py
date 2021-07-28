from datetime import datetime
from http import HTTPStatus
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from fastapi.testclient import TestClient
from osiris.core.enums import TimeResolution


def get_app():
    with patch('osiris.core.configuration.Configuration') as _:
        from app.main import app

        return TestClient(app)


client = get_app()


def test_get_all_dates_to_download():
    from app.dependencies import __get_all_dates_to_download

    from_date = datetime(2021, 1, 1)
    to_date = datetime(2021, 2, 1)
    time_resolution = TimeResolution.NONE

    result = __get_all_dates_to_download(from_date, to_date, time_resolution)

    assert result.size == 1
    assert result.values.astype(str)[0][0:10] == '2021-01-01'

    from_date = datetime(2021, 1, 1)
    to_date = datetime(2022, 1, 1)
    time_resolution = TimeResolution.YEAR

    result = __get_all_dates_to_download(from_date, to_date, time_resolution)

    assert result.size == 2
    assert result.values.astype(str)[0][0:10] == '2021-01-01'
    assert result.values.astype(str)[1][0:10] == '2022-01-01'

    from_date = datetime(2021, 1, 3)
    to_date = datetime(2021, 2, 4)
    time_resolution = TimeResolution.MONTH

    result = __get_all_dates_to_download(from_date, to_date, time_resolution)

    assert result.size == 2
    assert result.values.astype(str)[0][0:10] == '2021-01-01'
    assert result.values.astype(str)[1][0:10] == '2021-02-01'

    from_date = datetime(2021, 1, 3)
    to_date = datetime(2021, 1, 4)
    time_resolution = TimeResolution.DAY

    result = __get_all_dates_to_download(from_date, to_date, time_resolution)

    assert result.size == 2
    assert result.values.astype(str)[0][0:10] == '2021-01-03'
    assert result.values.astype(str)[1][0:10] == '2021-01-04'

    from_date = datetime(2021, 1, 3, 1)
    to_date = datetime(2021, 1, 3, 2)
    time_resolution = TimeResolution.HOUR

    result = __get_all_dates_to_download(from_date, to_date, time_resolution)

    assert result.size == 2
    assert result.values.astype(str)[0][0:13] == '2021-01-03T01'
    assert result.values.astype(str)[1][0:13] == '2021-01-03T02'

    from_date = datetime(2021, 1, 3, 1, 2)
    to_date = datetime(2021, 1, 3, 1, 3)
    time_resolution = TimeResolution.MINUTE

    result = __get_all_dates_to_download(from_date, to_date, time_resolution)

    assert result.size == 2
    assert result.values.astype(str)[0][0:16] == '2021-01-03T01:02'
    assert result.values.astype(str)[1][0:16] == '2021-01-03T01:03'


def test_split_into_chunks():
    from app.dependencies import __split_into_chunks
    lst = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

    result = __split_into_chunks(lst, 1)

    assert len(list(result)) == 8

    result = __split_into_chunks(lst, 2)

    assert len(list(result)) == 4

    result = __split_into_chunks(lst, 3)

    assert len(list(result)) == 3

    result = __split_into_chunks(lst, 8)

    assert len(list(result)) == 1


def test_parse_date_arguments():
    from app.dependencies import __parse_date_arguments

    from_date = None
    to_date = None

    res_from_date, res_to_date, time_resolution = __parse_date_arguments(from_date, to_date)

    assert res_from_date == datetime(1970, 1, 1)
    assert res_to_date is None
    assert time_resolution == TimeResolution.NONE

    from_date = '2021-02'
    to_date = None

    res_from_date, res_to_date, time_resolution = __parse_date_arguments(from_date, to_date)

    assert res_from_date == datetime(2021, 2, 1)
    assert res_to_date is None
    assert time_resolution == TimeResolution.MONTH

    from_date = '2021-02-01'
    to_date = '2021-03-03'

    res_from_date, res_to_date, time_resolution = __parse_date_arguments(from_date, to_date)

    assert res_from_date == datetime(2021, 2, 1)
    assert res_to_date == datetime(2021, 3, 3)
    assert time_resolution == TimeResolution.DAY

    from_date = '2021-02-01'
    to_date = '2021-03'

    with pytest.raises(HTTPException) as execinfo:
        _ = __parse_date_arguments(from_date, to_date)

    assert execinfo.value.status_code == HTTPStatus.BAD_REQUEST
    assert execinfo.value.detail == 'Malformed request syntax: len(from_date) != len(to_date)'

