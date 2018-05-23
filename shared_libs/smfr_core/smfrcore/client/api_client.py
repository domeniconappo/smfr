"""
Module for API client to the SMFR Rest Server
"""

import logging
from json import JSONDecodeError

import requests
from werkzeug.datastructures import FileStorage

from smfrcore.client.conf import ServerConfiguration, DATE_FORMAT, LOGGER_FORMAT
from smfrcore.errors import SMFRError

from .marshmallow import CollectorPayload

logging.basicConfig(level=logging.INFO if not ServerConfiguration.debug else logging.DEBUG,
                    format=LOGGER_FORMAT, datefmt=DATE_FORMAT)


class ApiLocalClient:
    """
    Simple requests client to SMFR Rest Server
    """
    logger = logging.getLogger(__name__)
    endpoints = {
        'list_collections': '/collections',
        'new_collection': '/collections',
        'stop_collector': '/collections/stop/{id}',
        'stopall': '/collections/stopall',
        'startall': '/collections/startall',
        'start_collector': '/collections/start/{id}',
        'list_running_collectors': '/collections/active',
        'list_inactive_collectors': '/collections/inactive',
        'remove_collection': '/collections/{id}/remove',
        'collection_details': '/collections/{id}/details',
        'annotate_collection': '/collections/{id}/annotate',
        'geotag_collection': '/collections/{id}/geo',
        'signup_user': '/users',
        'signin_user': '/users/signin',
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
            raise SMFRRestException(res.json(), code)

    def _get(self, endpoint, path_kwargs=None):
        try:
            url = self._build_url(endpoint, path_kwargs)
            res = requests.get(url)
            self._check_response(res)
        except SMFRRestException as e:
            self.logger.error('REST API Error %s', str(e))
        except ConnectionError:
            self.logger.error('SMFR REST API server is not listening...')
        else:
            return res.json()

    def _post(self, endpoint, payload=None, path_kwargs=None, formdata=None, query_params=None):
        """
        Main method that executes POST calls
        :param query_params: dict for querystring part to put into `params` kwarg of request.post
        :param endpoint: endpoint url name (see ApiLocalClient.endpoints)
        :param payload: dict to put into `json` kwarg of request.post
        :param path_kwargs: dict used for variable replacement in endpoint paths
        :param formdata: dict used for multipart/form-data calls (e.g. with files)
        :return: JSON text
        """
        requests_kwargs = {'json': payload} if payload else {}
        if query_params:
            requests_kwargs['params'] = query_params

        if formdata:
            files = {}
            data = {}

            for k, v in formdata.items():
                if k.endswith('_file') and v:
                    k = k.split('_')[0]
                    files[k] = v if isinstance(v, FileStorage) else FileStorage(open(v, 'rb'))
                elif not k.endswith('_file') and v:
                    data[k] = v

            if files:
                requests_kwargs['files'] = files
                requests_kwargs['headers'] = {
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate, br'
                }
            if data:
                requests_kwargs['data'] = data

        url = self._build_url(endpoint, path_kwargs)

        try:
            res = requests.post(url, **requests_kwargs)
            self._check_response(res)
        except SMFRRestException as e:
            self.logger.error('REST API Error %s', str(e))
            raise e
        except ConnectionError as e:
            self.logger.error('SMFR REST API server is not listening...')
            raise SMFRRestException({'status': 500, 'title': 'SMFR Rest Server is not listening', 'description': url})
        else:
            try:
                return res.json()
            except JSONDecodeError:
                return {}

    def list_collections(self):
        """
        Get all collections defined in SMFR
        :return: collections defined in SMFR
        """
        return self._get('list_collections')

    def list_running_collectors(self):
        """
        Get collections that are currently fetching from Twitter Stream
        :return: running collections
        """
        return self._get('list_running_collectors')

    def list_inactive_collectors(self):
        """
        Get inactive collections
        :return: Collections for whose collector was paused
        """
        return self._get('list_inactive_collectors')

    def new_collection(self, input_payload):
        schema = CollectorPayload()
        formdata = schema.load(input_payload).data
        formdata['kwfile_file'] = input_payload.get('kwfile')
        formdata['locfile_file'] = input_payload.get('locfile')
        formdata['config_file'] = input_payload.get('config')
        formdata['tzclient'] = input_payload.get('tzclient')
        return self._post('new_collection', formdata=formdata)

    def signup_user(self, input_payload):
        data = {'username': input_payload['username'], 'password': input_payload['password']}
        return self._post('signup_user', data)

    def login_user(self, input_payload):
        data = {'username': input_payload['username'], 'password': input_payload['password']}
        return self._post('login_user', data)

    def logout_user(self, user_id):
        return self._post('logout_user', path_kwargs={'id': user_id})

    def remove_collection(self, collection_id):
        return self._post('remove_collection', path_kwargs={'id': collection_id})

    def stop_collector(self, collector_id):
        return self._post('stop_collector', path_kwargs={'id': collector_id})

    def start_collector(self, collector_id):
        return self._post('start_collector', path_kwargs={'id': collector_id})

    def stop_all(self):
        return self._post('stopall')

    def start_all(self):
        return self._post('startall')

    def get_collection(self, collection_id):
        return self._get('collection_details', path_kwargs={'id': collection_id})

    def start_annotation(self, collection_id, lang='en'):
        return self._post('annotate_collection', path_kwargs={'id': collection_id}, query_params={'lang': lang})

    def start_geotagging(self, collection_id):
        return self._post('geotag_collection', path_kwargs={'id': collection_id})


class SMFRRestException(SMFRError):
    def __init__(self, response, status_code):
        err = response.get('error', {})
        message = '{}: ({})'.format(status_code, err.get('description', 'No details.'))
        super().__init__(message)
