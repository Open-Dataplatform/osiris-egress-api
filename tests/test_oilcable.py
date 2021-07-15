import io
import json
import pytest
import datetime
import pandas as pd
from unittest.mock import patch
from fastapi.testclient import TestClient

from app.routers.oilcable import _filter_datetime


@pytest.fixture()
def client():
    with patch("osiris.core.configuration.Configuration"):
        from app.main import app

        yield TestClient(app)


# -- get_leak_cable_daily() --------------------------------------------------


@patch("app.routers.oilcable.__download_blob_to_stream")
def test__get_leak_cable_daily__should_return_correct_dataset(
    download_blob_to_stream_mock, client
):
    """
    :param unittest.mock.AsyncMock download_blob_to_stream_mock:
    :param TestClient client:
    """

    # -- Arrange -------------------------------------------------------------

    config = {
        "Oilcable": {
            "pt24h_guid": "TEST-GUID",
        }
    }

    now = datetime.datetime.now()

    df = pd.DataFrame(
        {
            "timestamp": [now, now, now, now, now, now, now, now, now],
            "reading_type": ["i", "p", "t", "i", "p", "t", "i", "p", "t"],
            "cable_id": ["c1", "c1", "c1", "c2", "c2", "c2", "c3", "c3", "c3"],
            "mean": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "std": [10, 11, 12, 13, 14, 15, 16, 17, 18],
            "max": [19, 20, 21, 22, 23, 24, 25, 26, 27],
            "min": [28, 29, 30, 31, 32, 33, 34, 35, 36],
            "count": [37, 38, 39, 40, 41, 42, 43, 44, 45],
        }
    )

    byte_stream = io.BytesIO()
    df.to_parquet(byte_stream)
    byte_stream.seek(0)

    download_blob_to_stream_mock.return_value = byte_stream

    # -- Act -----------------------------------------------------------------

    with patch("app.routers.oilcable.config", new=config):
        response = client.get("/oilcable/daily", headers={"Authorization": "secret"})

    # -- Assert --------------------------------------------------------------

    response_data = response.content.decode("utf8")
    response_json = json.loads(response_data)

    assert response.status_code == 200
    assert response_json == [
        {
            "date": now.strftime("%Y-%m-%d"),
            "cable_id": "c1",
            "i_mean": 1,
            "i_std": 10,
            "i_max": 19,
            "i_min": 28,
            "i_count": 37,
            "p_mean": 2,
            "p_std": 11,
            "p_max": 20,
            "p_min": 29,
            "p_count": 38,
            "t_mean": 3,
            "t_std": 12,
            "t_max": 21,
            "t_min": 30,
            "t_count": 39,
        },
        {
            "date": now.strftime("%Y-%m-%d"),
            "cable_id": "c2",
            "i_mean": 4,
            "i_std": 13,
            "i_max": 22,
            "i_min": 31,
            "i_count": 40,
            "p_mean": 5,
            "p_std": 14,
            "p_max": 23,
            "p_min": 32,
            "p_count": 41,
            "t_mean": 6,
            "t_std": 15,
            "t_max": 24,
            "t_min": 33,
            "t_count": 42,
        },
        {
            "date": now.strftime("%Y-%m-%d"),
            "cable_id": "c3",
            "i_mean": 7,
            "i_std": 16,
            "i_max": 25,
            "i_min": 34,
            "i_count": 43,
            "p_mean": 8,
            "p_std": 17,
            "p_max": 26,
            "p_min": 35,
            "p_count": 44,
            "t_mean": 9,
            "t_std": 18,
            "t_max": 27,
            "t_min": 36,
            "t_count": 45,
        },
    ]


# -- _filter_datetime() ------------------------------------------------------


def test__filter_datetime__datetime_type():
    """
    Tests that a _filter_datetime() filters a DataFrame
    by from- and to-date correctly.
    """

    # -- Arrange -------------------------------------------------------------

    today = pd.Timestamp("2021-01-01")

    date1 = today - datetime.timedelta(days=2)
    date2 = today - datetime.timedelta(days=1)
    date3 = today
    date4 = today + datetime.timedelta(days=1)
    date5 = today + datetime.timedelta(days=2)

    df = pd.DataFrame(
        {
            "date": [date1, date2, date3, date4, date5],
            "name": ["John", "Will", "Bill", "Joe", "Tedd"],
        }
    )

    from_date = date2.strftime("%Y-%m-%d")
    to_date = date4.strftime("%Y-%m-%d")

    # -- Act -----------------------------------------------------------------

    df_returned = _filter_datetime(
        dataframe=df,
        from_date=from_date,
        to_date=to_date,
        date_column="date",
    )

    # -- Assert --------------------------------------------------------------

    df_expected = pd.DataFrame(
        {
            "date": [date2, date3, date4],
            "name": ["Will", "Bill", "Joe"],
        }
    )

    # Indexes has to be the same in both dataframes, so overwrite
    # it here since its irrelevant for this test
    df_returned.index = [0, 1, 2]

    assert df_returned.equals(df_expected)
