"""
Implements endpoints for delfin oilcable data.
"""
import functools
from typing import Optional
from http import HTTPStatus

import pandas as pd
from fastapi import APIRouter, Security, HTTPException
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration

from ..dependencies import (
    __download_blob_to_stream,
    __parse_date_arguments,
    __download_streams,
)
from ..metrics import Metric

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name="Authorization", auto_error=True)

router = APIRouter(tags=["oilcable"])


@router.get("/oilcable/leak/{cable_id}", response_class=JSONResponse)
@Metric.histogram
async def get_leak_cable_id(
    cable_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    token: str = Security(access_token_header),
):
    """
    Download probalilities of leak on a single oilcables
    """
    guid = config["Oilcable"]["leakprop_guid"]
    blob_name = f"{guid}/{cable_id}.parquet"

    byte_stream = await __download_blob_to_stream(blob_name, token)
    dataframe = pd.read_parquet(byte_stream)

    dataframe = _filter_datetime(dataframe, from_date, to_date)

    json_data = dataframe.to_json(orient="records")

    return JSONResponse(json_data)


@router.get("/oilcable/leak", response_class=JSONResponse)
@Metric.histogram
async def get_leak_cable(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    token: str = Security(access_token_header),
):
    """
    Download probalilities of leak on all oilcables
    """
    guid = config["Oilcable"]["leakprop_guid"]
    blob_name_prefix = f"{guid}/"

    byte_streams = await __download_streams(blob_name_prefix, token)

    dfs = [pd.read_parquet(byte_stream) for byte_stream in byte_streams]
    dataframe = pd.concat(dfs)

    dataframe = _filter_datetime(dataframe, from_date, to_date)
    dataframe["date"] = dataframe["date"].dt.strftime("%Y-%m-%d")

    json_data = dataframe.to_json(orient="records")

    return JSONResponse(json_data)


@router.get("/oilcable/daily", response_class=JSONResponse)
@Metric.histogram
async def get_leak_cable_daily(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    cable_id: Optional[str] = None,
    token: str = Security(access_token_header),
):
    """
    Download daily mean value readings for oilcables
    """
    guid = config["Oilcable"]["pt24h_guid"]

    blob_name = f"{guid}/data.parquet"
    byte_stream = await __download_blob_to_stream(blob_name, token)

    dataframe = pd.read_parquet(byte_stream)

    dataframe = _filter_datetime(
        dataframe, from_date, to_date, date_column="timestamp"
    )

    if cable_id:
        dataframe = dataframe.query("cable_id == @cable_id")

    dataframe["timestamp"] = dataframe["timestamp"].dt.strftime("%Y-%m-%d")

    groups = []

    for reading_type, group in dataframe.groupby(["reading_type"]):
        cnames = {
            "mean": f"{reading_type}_mean",
            "std": f"{reading_type}_std",
            "max": f"{reading_type}_max",
            "min": f"{reading_type}_min",
            "count": f"{reading_type}_count",
            "timestamp": "date",
        }

        del group["reading_type"]

        group.rename(columns=cnames, inplace=True)
        groups.append(group)

    if len(groups) == 0:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,  # TODO Return status 204 instead?
            detail="No data available"
        )

    dataframe = functools.reduce(
        lambda a, b: pd.merge(a, b, on=["cable_id", "date"]), groups
    )

    json_data = dataframe.to_dict(orient="records")

    return JSONResponse(json_data)


def _filter_datetime(dataframe, from_date, to_date, date_column="date"):
    from_date_obj, to_date_obj, _ = __parse_date_arguments(from_date, to_date)

    if to_date_obj is None:
        to_date_obj = pd.Timestamp.max

    dataframe = dataframe[
        (from_date_obj <= dataframe[date_column])
        & (dataframe[date_column] <= to_date_obj)
    ]

    return dataframe
