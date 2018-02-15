"""

"""

import json
import os
import logging

import requests

from client.conf import ServerConfiguration
from client.errors import SMFRError

os.environ['NO_PROXY'] = ServerConfiguration.rest_server_host
logging.basicConfig(level=logging.INFO if not ServerConfiguration.debug else logging.DEBUG,
                    format='%(asctime)s:[%(levelname)s] (%(threadName)-10s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class ApiLocalClient:
    """

    """
    endpoints = {
        'list_collections': '/collections',
        'start_new_collector': '/collections',
        'stop_collector': '/collections/stop/{id}',
        'stop_all': '/collections/stop',
        'start_all': '/collections/start',
        'start_collector': '/collections/start/{id}',
        'list_running_collectors': '/collections/active',
    }

    def __init__(self):
        self.config = ServerConfiguration
        self.base_uri = 'http://{}:{}{}'.format(self.config.rest_server_host, self.config.rest_server_port, self.config.base_path)

    def _build_url(self, endpoint, path_kwargs=None):
        endpoint = self.endpoints[endpoint]
        if path_kwargs:
            endpoint = endpoint.format(**path_kwargs)
        return '{}{}'.format(self.base_uri, endpoint)

    @classmethod
    def _check_response(cls, res):
        code = res.status_code
        if code >= 400:
            raise SMFRRestException(res.json())

    def _get(self, endpoint):
        try:
            url = self._build_url(endpoint)
            res = requests.get(url)
            self._check_response(res)
        except SMFRRestException as e:
            logger.error('REST API Error %s', str(e))
        except ConnectionError:
            logger.error('SMFR REST API server is not listening...')
        else:
            return res.json()

    def _post(self, endpoint, payload=None, path_kwargs=None):
        requests_kwargs = {'json': payload} if payload else {}
        try:
            url = self._build_url(endpoint, path_kwargs)
            res = requests.post(url, **requests_kwargs)
            self._check_response(res)
        except SMFRRestException as e:
            logger.error('REST API Error %s', str(e))
        except ConnectionError:
            logger.error('SMFR REST API server is not listening...')
        else:
            try:
                return res.json()
            except json.decoder.JSONDecodeError:
                return {}

    def list_collections(self):
        return self._get('list_collections')

    def list_running_collectors(self):
        return self._get('list_running_collectors')

    def start_new_collector(self, payload):
        from .marshmallow import CollectorPayload
        schema = CollectorPayload()
        payload = schema.load(payload).data
        return self._post('start_new_collector', payload=payload)

    def stop_collector(self, collector_id):
        return self._post('stop_collector', path_kwargs={'id': collector_id})

    def start_collector(self, collector_id):
        return self._post('start_collector', path_kwargs={'id': collector_id})

    def stop_all(self):
        return self._post('stop_all')

    def start_all(self):
        return self._post('start_all')


class SMFRRestException(SMFRError):
    def __init__(self, response):
        message = '{}: {} {}'.format(response.get('status'), response.get('title'), response.get('description'))
        super().__init__(message)
