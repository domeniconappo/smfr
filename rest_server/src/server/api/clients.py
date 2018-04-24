"""
Module containing clients for Annotator and Geocoder microservices
"""

import logging

import requests

from smfrcore.errors import SMFRRestException
from server.config import RestServerConfiguration


class MicroserviceClient:

    base_uri = None

    @classmethod
    def _check_response(cls, res, code):
        if code >= 400:
            raise SMFRRestException(res.json(), code)

    @classmethod
    def running(cls):
        """

        :return:
        :rtype:
        """
        url = '{}/running'.format(cls.base_uri)
        res = requests.get(url)
        cls._check_response(res, res.status_code)
        return res.json(), res.status_code


class AnnotatorClient(MicroserviceClient):
    """

    """
    configuration = RestServerConfiguration()
    host = configuration.annotator_host
    port = configuration.annotator_port
    base_uri = 'http://{}:{}'.format(host, port)
    logger = logging.getLogger(__name__)

    @classmethod
    def start(cls, collection_id, lang):
        """

        :param lang:
        :type lang:
        :param collection_id: ID of Collection as it's stored in virtual_twitter_collection.id field in MySQL
        :type collection_id: int
        :return: JSON result from Geocoder API
        :rtype: dict
        """
        url = '{}/{}/{}/start'.format(cls.base_uri, collection_id, lang)
        res = requests.put(url)
        cls._check_response(res, res.status_code)
        return res.json(), res.status_code

    @classmethod
    def stop(cls, collection_id, lang):
        """

        :param lang:
        :type lang:
        :param collection_id: ID of Collection as it's stored in virtual_twitter_collection.id field in MySQL
        :type collection_id: int
        :return: JSON result from Geocoder API
        :rtype: dict
        """
        url = '{}/{}/{}/stop'.format(cls.base_uri, collection_id, lang)
        res = requests.put(url)
        cls._check_response(res, res.status_code)
        return res.json(), res.status_code


class GeocoderClient(MicroserviceClient):
    """

    """
    configuration = RestServerConfiguration()
    host = configuration.geocoder_host
    port = configuration.geocoder_port
    base_uri = 'http://{}:{}'.format(host, port)
    logger = logging.getLogger(__name__)

    @classmethod
    def start(cls, collection_id):
        """

        :param collection_id: ID of Collection as it's stored in virtual_twitter_collection.id field in MySQL
        :type collection_id: int
        :return: JSON result from Geocoder API
        :rtype: dict
        """
        url = '{}/{}/start'.format(cls.base_uri, collection_id)
        res = requests.put(url)
        cls._check_response(res, res.status_code)
        return res.json(), res.status_code

    @classmethod
    def stop(cls, collection_id):
        """

        :param collection_id: ID of Collection as it's stored in virtual_twitter_collection.id field in MySQL
        :type collection_id: int
        :return: JSON result from Geocoder API
        :rtype: dict
        """
        url = '{}/{}/stop'.format(cls.base_uri, collection_id)
        res = requests.put(url)
        cls._check_response(res, res.status_code)
        return res.json(), res.status_code
