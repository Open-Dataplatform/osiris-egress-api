"""
Implements endpoints for delfin oilcable data.
"""
import asyncio
import functools
from typing import Optional

from fastapi import APIRouter, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration
import pandas as pd

from ..dependencies import (
    __get_filesystem_client,
    __get_filepaths,
    __download_blob_to_stream,
    __parse_date_arguments,
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
    guid = config["Oilcable"]["leakprop_guid"]
    blob_name = f"{guid}/egress/{cable_id}.parquet"

    byte_stream = await __download_blob_to_stream(blob_name, token)
    df = pd.read_parquet(byte_stream)

    json_data = filter_datetime(df, from_date, to_date)

    return JSONResponse(json_data)


@router.get("/oilcable/leak", response_class=JSONResponse)
@Metric.histogram
async def get_leak_cable_id(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    token: str = Security(access_token_header),
):
    guid = config["Oilcable"]["leakprop_guid"]
    blob_name_prefix = f"{guid}/egress/"

    byte_streams = await download_streams(blob_name_prefix, token)

    dfs = [pd.read_parquet(byte_stream) for byte_stream in byte_streams]
    df = pd.concat(dfs)

    df = filter_datetime(df, from_date, to_date)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    json_data = df.to_json(orient="records")

    return JSONResponse(json_data)


@router.get("/oilcable/daily", response_class=JSONResponse)
@Metric.histogram
async def get_leak_cable_id(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    token: str = Security(access_token_header),
):
    guid = config["Oilcable"]["pt24h_guid"]
    blob_name_prefix = f"{guid}"

    byte_streams = await download_streams(blob_name_prefix, token)

    dfs = [pd.read_json(byte_stream) for byte_stream in byte_streams]
    df = pd.concat(dfs)

    df = filter_datetime(df, from_date, to_date, date_column="timestamp")
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    groups = []

    for reading_type, group in df.groupby(["reading_type"]):
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

    df = functools.reduce(lambda a, b: pd.merge(a, b, on=["cable_id", "date"]), groups)

    json_data = df.to_json(orient="records")

    return JSONResponse(json_data)


def filter_datetime(df, from_date, to_date, date_column="date"):
    from_date_obj, to_date_obj, _ = __parse_date_arguments(from_date, to_date)

    if to_date_obj is None:
        to_date_obj = pd.Timestamp.max

    df = df[(from_date_obj <= df[date_column]) & (df[date_column] <= to_date_obj)]

    return df


async def download_streams(blob_name_prefix, token):
    filesystem_client = await __get_filesystem_client(token)

    blob_names = await __get_filepaths(blob_name_prefix, filesystem_client)

    tasks = [__download_blob_to_stream(blob_name, token) for blob_name in blob_names]
    byte_streams = await asyncio.gather(*tasks)

    return byte_streams
