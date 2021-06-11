"""
Contains endpoints for downloading data to the DataPlatform.
"""
from http import HTTPStatus
from io import BytesIO
from typing import Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake.aio import FileSystemClient
from fastapi import APIRouter, HTTPException, Security
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security.api_key import APIKeyHeader
from osiris.core.configuration import Configuration

from ..dependencies import (__download_file, __check_directory_exist, __get_filesystem_client, __download_json_file, __get_file_client)
from ..metrics import TracerClass, Metric

configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

access_token_header = APIKeyHeader(name='Authorization', auto_error=True)

router = APIRouter(tags=['downloads'])

tracer = TracerClass().get_tracer()


# @router.get('/{guid}', response_class=StreamingResponse)
# @Metric.histogram
# async def download_file(guid: str,
#                         file_date: datetime = datetime.utcnow(),
#                         token: str = Security(access_token_header)) -> StreamingResponse:
#     """
#     Download file from data storage from the given date (UTC). This endpoint expects data to be
#     stored in the folder {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/, but doesnt make
#     any assumption about the filename and file extension.
#     """
#     logger.debug('download file requested')
#
#     with tracer.start_span('download_file') as span:
#         span.set_tag('guid', guid)
#         async with await __get_filesystem_client(token) as filesystem_client:
#             with tracer.start_active_span('get_directory_client', child_of=span):
#                 directory_client = filesystem_client.get_directory_client(guid)
#             with tracer.start_active_span('check_directory_exists', child_of=span):
#                 __check_directory_exist(directory_client)
#             with tracer.start_active_span('download_file', child_of=span):
#                 path = __get_path_for_arbitrary_file(file_date, guid, filesystem_client)
#                 stream = __download_file(path, directory_client)
#
#             return StreamingResponse(stream.chunks(), media_type='application/octet-stream')


@router.get('/{guid}/json', response_class=JSONResponse)
@Metric.histogram
async def download_json_file(guid: str,   # pylint: disable=too-many-locals
                             from_date: Optional[str] = None,
                             to_date: Optional[str] = None,
                             token: str = Security(access_token_header)) -> JSONResponse:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download jao data requested')
    return await __download_json_file(guid, token, from_date, to_date)


@router.get('/jao', response_class=JSONResponse)
@Metric.histogram
async def download_jao_data(horizon: str,  # pylint: disable=too-many-locals
                            from_date: Optional[str] = None,
                            to_date: Optional[str] = None,
                            token: str = Security(access_token_header)) -> JSONResponse:
    """
    Download JSON endpoint with data from from_date to to_date (time period).
    If form_date is left out, current UTC time is used.
    If to_date is left out, only one data point is retrieved.
    """
    logger.debug('download jao data requested')
    if horizon == "Yearly":
        guid = config['JAO']['yearly_guid']
    elif horizon == "Monthly":
        guid = config['JAO']['monthly_guid']
    else:
        message = '(ValueError) horizon value can only be Yearly or Monthly'
        logger.error(message)
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=message)

    return await __download_json_file(guid, token, from_date, to_date)


@router.get('/ikontrol/getallprojects', response_class=StreamingResponse)
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


@router.get('/ikontrol/{project_id}', response_class=StreamingResponse)
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


@router.get('/ikontrol/getzip/{project_id}', response_class=StreamingResponse)
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


@router.get("/download", response_class=StreamingResponse)
@Metric.histogram
async def download_file(
    blob_name: str, token: str = Security(access_token_header)
) -> StreamingResponse:
    """
    Download any blob of any format.
    """
    try:
        dataset_id, file_path = blob_name.split("/", maxsplit=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Wrong blob name format. Expects: <dataset_id>/<file_path>")

    file_system = await __get_filesystem_client(token)

    directory_client = file_system.get_directory_client(dataset_id)

    try:
        file_client = await __get_file_client(directory_client, file_path)
        if file_client is None:
            raise ResourceNotFoundError()
        byte_stream = BytesIO()
        storage_stream = await file_client.download_file()
        await storage_stream.readinto(byte_stream)
        byte_stream.seek(0)
    except ResourceNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")

    return StreamingResponse(byte_stream, media_type="application/octet-stream")
