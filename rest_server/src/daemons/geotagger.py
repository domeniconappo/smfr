import logging
import threading

import ujson as json
from mordecai import Geoparser

from server.config import server_configuration
from server.models import Tweet


class Geotagger:

    running = []

    def __init__(self, collection_id, ttype='annotated', lang='en'):
        self.tagger = Geoparser('geonames')

        self.rest_server_conf = server_configuration()

        self.logger = logging.getLogger(__name__)
        self.kafka_topic = self.rest_server_conf.server_config['kafka_topic']
        self.producer = self.rest_server_conf.kafka_producer
        self.collection_id = collection_id
        self.ttype = ttype
        self.lang = lang

    def start(self):
        self.logger.info('Starting Geotagging collection: {}'.format(self.collection_id))

        self.running.append(self.collection_id)

        tweets = Tweet.get_iterator(self.collection_id, self.ttype, lang=self.lang)

        for t in tweets:
            original_json = json.loads(t.tweet)
            t.ttype = 'geotagged'
            message = t.serialize()
            self.logger.info('Sending to queue: {}'.format(message[:120]))
            self.producer.send(self.kafka_topic, message)

        # remove from `running` list
        self.running.remove(self.collection_id)

        self.logger.info('Geotagging process terminated! Collection: {}'.format(self.collection_id))

    def launch(self):
        """
        Launch an Annotator process in a separate thread
        """
        t = threading.Thread(target=self.start, name='Annotator ({}) - collection id: {}'.format(self.lang, self.collection_id))
        t.start()

    @classmethod
    def is_running_for(cls, collection_id):
        return collection_id in cls.running
