"""
Module containing clients for Annotator and Geocoder microservices
"""

import logging

import requests

from server.config import RestServerConfiguration


class AnnotatorClient:
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
        cls.logger.info(url)
        res = requests.put(url)
        cls.logger.info(res)
        cls.logger.info(res.text)
        return res

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
        return res.json()

    @classmethod
    def running(cls):
        """

        :return:
        :rtype: dict
        """
        url = '{}/_running'.format(cls.base_uri)
        res = requests.get(url)
        return res.json()


class GeocoderClient:
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
        return res.json()

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
        return res.json()

    @classmethod
    def running(cls):
        """

        :return:
        :rtype:
        """
        url = '{}/_running'.format(cls.base_uri)
        res = requests.get(url)
        return res.json()
