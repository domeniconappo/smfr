"""
Module containing clients for Annotator and Geocoder microservices
"""

import logging

import requests

from smfrcore.errors import SMFRRestException
from server.config import RestServerConfiguration


class MicroserviceClient:
    """Base classe for microservices with common methods"""

    configuration = RestServerConfiguration()
    base_uri = None
    host = None
    port = None
    logger = logging.getLogger(__name__)

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
    host = MicroserviceClient.configuration.annotator_host
    port = MicroserviceClient.configuration.annotator_port
    base_uri = 'http://{}:{}'.format(host, port)

    @classmethod
    def models(cls):
        url = '{}/models'.format(cls.base_uri)
        res = requests.get(url)
        cls._check_response(res, res.status_code)
        return res.json(), res.status_code

    @classmethod
    def start(cls, collection_id, lang, start_date=None, end_date=None):
        """

        :param end_date:
        :param start_date:
        :param lang:
        :type lang:
        :param collection_id: ID of Collection as it's stored in virtual_twitter_collection.id field in MySQL
        :type collection_id: int
        :return: JSON result from Geocoder API
        :rtype: dict
        """
        url = '{}/{}/{}/start'.format(cls.base_uri, collection_id, lang)
        params = {'start_date': start_date, 'end_date': end_date}
        res = requests.put(url, params=params)
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
    host = MicroserviceClient.configuration.geocoder_host
    port = MicroserviceClient.configuration.geocoder_port
    base_uri = 'http://{}:{}'.format(host, port)

    @classmethod
    def start(cls, collection_id, start_date, end_date):
        """

        :param end_date:
        :param start_date:
        :param collection_id: ID of Collection as it's stored in virtual_twitter_collection.id field in MySQL
        :type collection_id: int
        :return: JSON result from Geocoder API
        :rtype: dict
        """
        url = '{}/{}/start'.format(cls.base_uri, collection_id)
        params = {'start_date': start_date, 'end_date': end_date}
        res = requests.put(url, params=params)
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
