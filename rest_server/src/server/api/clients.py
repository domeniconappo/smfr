import requests

from server.config import RestServerConfiguration


class AnnotatorClient:
    configuration = RestServerConfiguration()
    host = configuration.annotator_host
    port = configuration.annotator_port
    base_uri = 'http://{}:{}'.format(host, port)

    @classmethod
    def start(cls, collection_id, lang):
        url = '{}/{}/{}/{}'.format(cls.base_uri, collection_id, lang, 'start')
        res = requests.put(url)
        return res

    @classmethod
    def stop(cls, collection_id, lang):
        url = '{}/{}/{}/{}'.format(cls.base_uri, collection_id, lang, 'stop')
        res = requests.put(url)
        return res

    @classmethod
    def running(cls):
        url = '{}/{}'.format(cls.base_uri, 'running')
        res = requests.get(url)
        return res
