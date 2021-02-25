"""
The egress API to download data from the DataPlatform
"""

import logging.config
from typing import Dict
import configparser

from fastapi import FastAPI


config = configparser.ConfigParser()

all_config_files = ['conf.ini', '/etc/config/conf.ini']
config.read(all_config_files)

logging.config.fileConfig(fname=config['Misc']["log_configuration_file"], disable_existing_loggers=False)
logger = logging.getLogger(__file__)

app = FastAPI(root_path=config['Misc']["root_path"])


@app.get("/")
async def root() -> Dict[str, str]:
    """
    A simple endpoint example.
    """
    logger.debug('root requested')
    return {"message": "Hello World"}
