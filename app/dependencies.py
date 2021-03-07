"""
Contains dependencies used in several places of the application.
"""

import time
import configparser
from configparser import ConfigParser
import logging.config
from logging import Logger

from azure.core.credentials import AccessToken


class Configuration:
    """
    Contains methods to obtain configurations for this application.
    """

    def __init__(self, name: str):
        self.config = configparser.ConfigParser()
        self.config.read(['conf.ini', '/etc/osiris/conf.ini'])

        logging.config.fileConfig(fname=self.config['Logging']['configuration_file'], disable_existing_loggers=False)

        self.name = name

    def get_config(self) -> ConfigParser:
        """
        The configuration for the application.
        """
        return self.config

    def get_logger(self) -> Logger:
        """
        A customized logger.
        """
        return logging.getLogger(self.name)


class AzureCredential:  # pylint: disable=too-few-public-methods
    """
    Represents a Credential object. This is a hack to use a access token
    received from a client.
    """

    # NOTE: This doesn't necessarily correspond to the token lifetime,
    # however it doesn't matter as it gets recreated per request
    EXPIRES_IN = 1000

    def __init__(self, token: str):
        self.token = token
        self.expires_on = self.EXPIRES_IN + time.time()

    def get_token(self, *scopes, **kwargs):  # pylint: disable=unused-argument
        """
        Returns an AcccesToken object.
        """
        return AccessToken(self.token, self.expires_on)
