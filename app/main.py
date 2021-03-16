"""
The egress API to download data from the DataPlatform
"""

from typing import Dict

from http import HTTPStatus
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics

from .dependencies import Configuration
from .routers import grafana_simplejson


configuration = Configuration(__file__)
config = configuration.get_config()
logger = configuration.get_logger()

app = FastAPI(
    title='Osiris Egress API',
    version='0.1.0',
    root_path=config['FastAPI']['root_path']
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['POST'],
    allow_headers=['accept', 'content-type'],
)

app.add_middleware(
    PrometheusMiddleware,
    app_name=__name__,
    group_paths=True
)

app.add_route('/metrics', handle_metrics)
app.include_router(grafana_simplejson.router)


@app.get('/', status_code=HTTPStatus.OK)
async def root() -> Dict[str, str]:
    """
    Endpoint for basic connectivity test.
    """
    logger.debug('root requested')
    return {'message': 'OK'}
