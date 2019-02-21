"""
Module for API client to the SMFR Rest Server
"""
import os
import json
import logging
from json.decoder import JSONDecodeError

import requests

from smfrcore.client.conf import ServerConfiguration
from smfrcore.utils.errors import SMFRError
from smfrcore.utils import DEFAULT_HANDLER, smfr_json_encoder, IN_DOCKER

from .marshmallow import OnDemandPayload, CollectionPayload


logger = logging.getLogger('API Clients')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)


class ApiLocalClient:
    """
    Simple requests client to SMFR Rest Server
    """
    endpoints = {
        'list_admin': '/admin',
        'restart_collector': '/admin/{trigger_type}/restart',
        'list_collections': '/collections',
        'new_collection': '/collections',
        'stop_collection': '/collections/{id}/stop',
        'start_collection': '/collections/{id}/start',
        'list_running_collections': '/collections/running',
        'list_inactive_collections': '/collections/inactive',
        'list_active_collections': '/collections/active',
        'remove_collection': '/collections/{id}/remove',
        'collection_details': '/collections/{id}',
        'annotate_collection_start': '/collections/{id}/startannotate',
        'geotag_collection_start': '/collections/{id}/startgeo',
        'annotate_collection_stop': '/collections/{id}/stopannotate',
        'geotag_collection_stop': '/collections/{id}/stopgeo',
        'fetch_efas': '/collections/fetch_efas',
        'add_ondemand': '/collections/add_ondemand',
    }

    def __init__(self):
        self.config = ServerConfiguration
        self.base_uri = self.config.restserver_baseurl

    def _build_url(self, endpoint, path_kwargs=None):
        endpoint = self.endpoints[endpoint]
        if path_kwargs:
            endpoint = endpoint.format(**path_kwargs)
        return '{}{}'.format(self.base_uri, endpoint)

    @classmethod
    def _check_response(cls, res):
        code = res.status_code
        if code >= 400:
            logger.error('ERROR from Rest Server API: %s', code)
            try:
                content = res.json()
            except JSONDecodeError:
                content = {'error': 'Error {}'.format(code), 'description': res.text}
            raise SMFRRestException(content, code)

    def _get(self, endpoint, path_kwargs=None, query_params=None):
        try:
            url = self._build_url(endpoint, path_kwargs)
            requests_kwargs = {}
            if query_params:
                requests_kwargs['params'] = query_params
            res = requests.get(url, **requests_kwargs)
            self._check_response(res)
        except SMFRRestException as e:
            logger.error('REST API Error %s', str(e))
            raise e
        except ConnectionError:
            logger.error('SMFR REST API server is not listening...')
        else:
            return res.json()

    def _post(self, endpoint, payload=None, path_kwargs=None, query_params=None):
        """
        Main method that executes POST calls
        :param query_params: dict for querystring part to put into `params` kwarg of request.post
        :param endpoint: endpoint url name (see ApiLocalClient.endpoints)
        :param payload: dict to put into `data` kwarg of request.post
        :param path_kwargs: dict used for variable replacement in endpoint paths
        :return: dict representing JSON response
        """
        headers = {'Content-Type': 'application/json'}
        requests_kwargs = {'headers': headers,
                           'data': json.dumps(payload, default=smfr_json_encoder) if payload else '{}',
                           }

        if query_params:
            requests_kwargs['params'] = query_params

        url = self._build_url(endpoint, path_kwargs)

        logger.debug('POST %s %s', url, requests_kwargs)
        try:
            res = requests.post(url, **requests_kwargs)
            self._check_response(res)
        except SMFRRestException as e:
            logger.error('REST API Error %s', str(e))
            raise e
        except ConnectionError as e:
            logger.error('SMFR REST API server is not listening...%s', str(e))
            raise SMFRRestException({'error': {'description': 'SMFR Rest Server is not listening'}},
                                    status_code=500)
        else:
            try:
                return res.json()
            except ValueError:
                return {}

    def list_collectors(self):
        return self._get('list_admin'), 200

    def restart_collector(self, trigger_type):
        return self._post('restart_collector', path_kwargs={'trigger_type': trigger_type}), 204

    def list_collections(self):
        """
        Get all collections defined in SMFR
        :return: collections defined in SMFR
        """
        return self._get('list_collections'), 200

    def list_running_collections(self):
        """
        Get collections that are currently fetching from Twitter Stream
        :return: running collections
        """
        return self._get('list_running_collections'), 200

    def list_active_collections(self):
        """
        Get collections that are currently fetching from Twitter Stream or were stopped recently
        :return: active collections
        """
        return self._get('list_active_collections'), 200

    def list_inactive_collections(self):
        """
        Get inactive collections
        :return: Collections for whose collector was paused
        """
        return self._get('list_inactive_collections'), 200

    def new_collection(self, input_payload):
        schema = CollectionPayload()
        payload = schema.load(input_payload).data
        logger.debug('Payload %s', input_payload)
        return self._post('new_collection', payload=payload)

    def logout_user(self, user_id):
        return self._post('logout_user', path_kwargs={'id': user_id})

    def remove_collection(self, collection_id):
        return self._post('remove_collection', path_kwargs={'id': collection_id}), 204

    def stop_collection(self, collector_id):
        return self._post('stop_collection', path_kwargs={'id': collector_id})

    def start_collection(self, collector_id):
        return self._post('start_collection', path_kwargs={'id': collector_id})

    def get_collection(self, collection_id):
        return self._get('collection_details', path_kwargs={'id': collection_id}), 200

    def start_annotation(self, collection_id):
        return self._post('annotate_collection_start', path_kwargs={'id': collection_id})

    def start_geotagging(self, collection_id):
        return self._post('geotag_collection_start', path_kwargs={'id': collection_id})

    def stop_annotation(self, collection_id):
        return self._post('annotate_collection_stop', path_kwargs={'id': collection_id})

    def stop_geotagging(self, collection_id):
        return self._post('geotag_collection_stop', path_kwargs={'id': collection_id})

    def fetch_efas(self, since='latest'):
        return self._get('fetch_efas', query_params={'since': since}), 200

    def add_ondemand_collections(self, events):
        payload = OnDemandPayload().load(events, many=True).data
        return self._post('add_ondemand', payload=payload), 201


class SMFRRestException(SMFRError):
    def __init__(self, response, status_code):
        err = response.get('error', {})
        message = err.get('description', 'No details.') if err else str(response)
        self.status_code = status_code
        self.message = message
        super().__init__(message)

    def __str__(self):
        return '<{o.status_code}: {o.message}>'.format(o=self)


class MicroserviceClient:
    """Base classe for microservices with common methods"""

    host = None
    port = None
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

    @classmethod
    def counters(cls):
        url = '{}/counters'.format(cls.base_uri)
        res = requests.get(url)
        cls._check_response(res, res.status_code)
        result = res.json()
        return result, res.status_code

    @classmethod
    def start(cls, collection_id, start_date=None, end_date=None):
        """

        :param end_date:
        :param start_date:
        :param collection_id: ID of Collection as it's stored in virtual_twitter_collection.id field in MySQL
        :type collection_id: int
        :return: JSON result from microservice API
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
        :return: JSON result from microservice API
        :rtype: dict
        """
        url = '{}/{}/stop'.format(cls.base_uri, collection_id)
        res = requests.put(url)
        cls._check_response(res, res.status_code)
        return res.json(), res.status_code


class PersisterClient(MicroserviceClient):
    """

    """
    host = '127.0.0.1' if not IN_DOCKER else os.getenv('PERSISTER_HOST', 'persister')
    port = os.getenv('PERSISTER_PORT_PORT', 5558)
    base_uri = 'http://{}:{}'.format(host, port)


class GeocoderClient(MicroserviceClient):
    """

    """
    host = '127.0.0.1' if not IN_DOCKER else os.getenv('GEOCODER_HOST', 'geocoder')
    port = os.getenv('GEOCODER_PORT', 5557)
    base_uri = 'http://{}:{}'.format(host, port)


class AnnotatorClient(MicroserviceClient):
    """

    """
    host = '127.0.0.1' if not IN_DOCKER else os.getenv('ANNOTATOR_HOST', 'annotator')
    port = os.getenv('ANNOTATOR_PORT', 5556)
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
    def available_languages(cls):
        models = cls.models()[0]['models']
        return tuple(models.keys())
