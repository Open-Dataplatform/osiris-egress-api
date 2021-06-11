"""
Contains endpoints for downloading data to the DataPlatform.
"""
from io import BytesIO
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

from fastapi import APIRouter, Security, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration

from osiris.core.azure_client_authorization import AzureCredentialAIO
from azure.storage.filedatalake.aio import DataLakeDirectoryClient

from ..dependencies import __get_file_client
from ..metrics import TracerClass

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name="Authorization", auto_error=True)

router = APIRouter(tags=["download_file"])

tracer = TracerClass().get_tracer()


@router.get("/download", response_class=StreamingResponse)
async def download_file(
    file_path: str, token: str = Security(access_token_header)
) -> StreamingResponse:
    """
    Download a file.
    """
    try:
        dataset_id, file_path = file_path.split("/", maxsplit=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Wrong file format. Expects: <dataset_id>/<blob_name>")

    account_url = config["Azure Storage"]["account_url"]
    filesystem_name = config["Azure Storage"]["filesystem_name"]
    credential = AzureCredentialAIO(token)

    file_system = DataLakeDirectoryClient(
        account_url, filesystem_name, dataset_id, credential=credential
    )

    try:
        file_client = await __get_file_client(file_system, file_path)
        if file_client is None:
            raise ResourceNotFoundError()
        byte_stream = BytesIO()
        storage_stream = await file_client.download_file()
        await storage_stream.readinto(byte_stream)
        byte_stream.seek(0)
    except ResourceNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")

    return StreamingResponse(byte_stream, media_type="application/octet-stream")
