"""
Contains endpoints for downloading data to the DataPlatform.
"""
from io import BytesIO

from azure.storage.filedatalake.aio import FileSystemClient
from fastapi import APIRouter, Security
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration

from ..dependencies import (__download_file, __check_directory_exist, __get_filesystem_client)
from ..metrics import Metric

configuration = Configuration(__file__)
logger = configuration.get_logger()
config = configuration.get_config()

access_token_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['downloads'])


@router.get('/ikontrol/getallprojects', response_class=StreamingResponse, deprecated=True)
@router.get('/v1/ikontrol/getallprojects', response_class=StreamingResponse)
@Metric.histogram
async def download_ikontrol_project_ids(token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download a list of all projects with project details.
    """
    logger.debug('download iKontrol project ids requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        file_path = 'ProjectDetails.json'

        stream = await __get_file_stream_for_ikontrol_file(file_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/json')


@router.get('/ikontrol/{project_id}', tags=["ikontrol"], response_class=StreamingResponse, deprecated=True)
@router.get('/v1/ikontrol/{project_id}', tags=["ikontrol"], response_class=StreamingResponse)
@Metric.histogram
async def download_ikontrol_data(project_id: int, token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download the data for a project using the project ID.
    """
    logger.debug('download iKontrol data requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        file_path = f'{project_id}/{project_id}.json'

        stream = await __get_file_stream_for_ikontrol_file(file_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/json')


@router.get('/ikontrol/getzip/{project_id}', tags=["ikontrol"], response_class=StreamingResponse, deprecated=True)
@router.get('/v1/ikontrol/getzip/{project_id}', tags=["ikontrol"], response_class=StreamingResponse)
@Metric.histogram
async def download_ikontrol_zip(project_id: int, token: str = Security(access_token_header)) -> StreamingResponse:
    """
    Download a project ZIP file using the project ID.
    """
    logger.debug('download iKontrol project zip requested')
    async with await __get_filesystem_client(token) as filesystem_client:
        zip_path = f'{project_id}/{project_id}.zip'

        stream = await __get_file_stream_for_ikontrol_file(zip_path, filesystem_client)

        return StreamingResponse(stream, media_type='application/zip')


async def __get_file_stream_for_ikontrol_file(file_path: str, filesystem_client: FileSystemClient) -> BytesIO:
    guid = config['iKontrol']['guid']
    directory_client = filesystem_client.get_directory_client(guid)
    await __check_directory_exist(directory_client)

    file_download = await __download_file(file_path, directory_client)
    file_content = await file_download.readall()

    stream = BytesIO(file_content)

    return stream
