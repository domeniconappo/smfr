"""
Module containing clients for Annotator and Geocoder microservices
"""

import requests

from smfrcore.errors import SMFRRestException
from server.config import RestServerConfiguration


class MicroserviceClient:
    """Base classe for microservices with common methods"""

    base_uri = None
    host = None
    port = None

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


class PersisterClient(MicroserviceClient):
    """

    """
    host = RestServerConfiguration.persister_host
    port = RestServerConfiguration.persister_port
    base_uri = 'http://{}:{}'.format(host, port)

    @classmethod
    def counters(cls):
        url = '{}/counters'.format(cls.base_uri)
        res = requests.get(url)
        cls._check_response(res, res.status_code)
        result = res.json()
        return result, res.status_code


class AnnotatorClient(MicroserviceClient):
    """

    """
    host = RestServerConfiguration.annotator_host
    port = RestServerConfiguration.annotator_port
    base_uri = 'http://{}:{}'.format(host, port)
    _models = None

    @classmethod
    def models(cls):
        if cls._models:
            return cls._models, 200

        url = '{}/models'.format(cls.base_uri)
        res = requests.get(url)
        cls._check_response(res, res.status_code)
        cls._models = res.json()
        return cls._models, res.status_code

    @classmethod
    def counters(cls):

        url = '{}/counters'.format(cls.base_uri)
        res = requests.get(url)
        cls._check_response(res, res.status_code)
        result = res.json()
        return result, res.status_code

    @classmethod
    def available_languages(cls):
        models = cls.models()[0]['models']
        return tuple(models.keys())

    @classmethod
    def start(cls, collection_id, start_date=None, end_date=None):
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


class GeocoderClient(MicroserviceClient):
    """

    """
    host = RestServerConfiguration.geocoder_host
    port = RestServerConfiguration.geocoder_port
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
