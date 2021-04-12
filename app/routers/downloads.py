"""
Contains endpoints for downloading data to the DataPlatform.
"""
import os
from http import HTTPStatus
from datetime import datetime, date

from fastapi import APIRouter, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import StreamingResponse

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient, StorageStreamDownloader
from osiris.azure_client_authorization import AzureCredential

from prometheus_client import Info

from ..dependencies import Configuration


configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['downloads'])

DOWNLOAD_JSON_FILE_GUID_INFO = Info('download_json_file_guid', 'download json file guid')
DOWNLOAD_FILE_GUID_INFO = Info('download_file_guid', 'download file guid')


@router.get('/{guid}/json', response_class=StreamingResponse)
async def download_json_file(guid: str,
                             file_date: date = datetime.utcnow().date(),
                             token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download JSON file from data storage from the given date (UTC). This endpoint expects data to be
    stored in {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/data.json'.
    """
    logger.debug('download json file requested')
    DOWNLOAD_JSON_FILE_GUID_INFO.info({'guid': guid})

    with __get_filesystem_client(token) as filesystem_client:
        directory_client = filesystem_client.get_directory_client(guid)
        __check_directory_exist(directory_client)
        path = f'year={file_date.year:02d}/month={file_date.month:02d}/day={file_date.day:02d}/data.json'
        print(type(file_date), file_date, path)
        stream = __download_file(path, directory_client)

        return StreamingResponse(stream.chunks(), media_type='application/octet-stream')
    


@router.get('/{guid}', response_class=StreamingResponse)
async def download_file(guid: str,
                        file_date: date = datetime.utcnow().date(),
                        token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download file from data storage from the given date (UTC). This endpoint expects data to be
    stored in the folder {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/, but doesnt make
    any assumption about the filename and file extension.
    """
    logger.debug('download file requested')
    DOWNLOAD_FILE_GUID_INFO.info({'guid': guid})

    with __get_filesystem_client(token) as filesystem_client:
        directory_client = filesystem_client.get_directory_client(guid)
        __check_directory_exist(directory_client)
        path = __get_path_for_generic_file(file_date, guid, filesystem_client)
        stream = __download_file(path, directory_client)
        print(path)

        return StreamingResponse(stream.chunks(), media_type='application/octet-stream')


def __check_directory_exist(directory_client: DataLakeDirectoryClient):
    try:
        directory_client.get_directory_properties()
    except ResourceNotFoundError as error:
        logger.error(type(error).__name__, error)
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='The given dataset doesnt exist') from error


def __get_path_for_generic_file(file_date: date, guid: str, filesystem_client: FileSystemClient) -> str:
    path = f'{guid}/year={file_date.year:02d}/month={file_date.month:02d}/day={file_date.day:02d}'

    try:
        files = filesystem_client.get_paths(path=path)
        file = files.next()
    except ResourceNotFoundError as error:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='Data doesnt exist for the given date') from error

    filename = os.path.relpath(file.name, guid)  # we remove GUID from the path

    return filename


def __download_file(filename: str, directory_client: DataLakeDirectoryClient) -> StorageStreamDownloader:
    file_client = directory_client.get_file_client(filename)
    try:
        downloaded_file = file_client.download_file()
        return downloaded_file
    except Exception as error:
        logger.error(type(error).__name__, error)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                            detail='File could not be downloaded') from error


def __get_filesystem_client(token: str) -> FileSystemClient:
    account_url = config['Azure Storage']['account_url']
    filesystem_name = config['Azure Storage']['filesystem_name']
    credential = AzureCredential(token)

    return FileSystemClient(account_url, filesystem_name, credential=credential)
