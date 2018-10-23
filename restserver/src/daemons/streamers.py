import datetime
import http
import logging
from collections import deque
from http.client import IncompleteRead
import os
import socket
import threading
import time
import urllib3

import requests
from twython import TwythonStreamer

from smfrcore.models import Tweet, create_app
from smfrcore.utils import DEFAULT_HANDLER

from daemons.utils import safe_langdetect, tweet_normalization_aggressive
from server.config import RestServerConfiguration, DEVELOPMENT
from server.api.clients import AnnotatorClient


logger = logging.getLogger('RestServer Streamer')
logger.setLevel(RestServerConfiguration.logger_level)
logger.addHandler(DEFAULT_HANDLER)
logger.propagate = False


class BaseStreamer(TwythonStreamer):
    """

    """
    def __init__(self, producer, **api_keys):
        self._lock = threading.RLock()
        self.query = {}
        self.consumer_key = api_keys['consumer_key']
        self.consumer_secret = api_keys['consumer_secret']
        self.access_token = api_keys['access_token']
        self.access_token_secret = api_keys['access_token_secret']
        self.persister_kafka_topic = RestServerConfiguration.persister_kafka_topic
        self.annotator_kafka_topic = RestServerConfiguration.annotator_kafka_topic
        self._errors = deque(maxlen=50)
        # A Kafka Producer
        self.producer = producer

        self.collections = []
        self.collection = None

        self.client_args = {}
        if os.environ.get('http_proxy'):
            self.client_args = dict(proxies=dict(http=os.environ['http_proxy'],
                                                 https=os.environ.get('https_proxy') or os.environ['http_proxy']))
        logger.debug('Instantiate a streamer with args %s', str(self.client_args))
        super().__init__(self.consumer_key, self.consumer_secret,
                         self.access_token, self.access_token_secret, retry_count=3, retry_in=30, chunk_size=10,
                         client_args=self.client_args)

    def __str__(self):
        return '{o.__class__.__name__}'.format(o=self)

    def _build_query_for(self):
        query = {}
        locations = []
        track = set()
        languages = []
        for collection in self.collections:
            if collection.locations:
                bbox = collection.locations
                locations += ['{},{},{},{}'.format(bbox['min_lon'], bbox['min_lat'], bbox['max_lon'], bbox['max_lat'])]
            if collection.tracking_keywords:
                track.update(collection.tracking_keywords)
            if collection.languages:
                languages += collection.languages
        if track:
            # max 400 keywords for Twitter API Stream
            query['track'] = list(track)[:400]
        if locations:
            # max 25 bounding boxes for Twitter API Stream
            query['locations'] = locations[:25]
        if languages:
            query['languages'] = languages
        return query

    def on_error(self, status_code, data):
        err = str(data) or 'No data'
        logger.error(status_code)
        logger.error(err)
        self._errors.append('{}: {} - {}'.format(status_code, datetime.datetime.now(), err))
        self.disconnect(deactivate_collections=False)
        self.collections = []
        self.collection = None
        sleep_time = 60 if str(status_code) == '420' and 'Exceeded connection limit' in 'data' else 5
        time.sleep(sleep_time)

    def on_timeout(self):
        logger.error('Timeout...')
        self._errors.append('{}: {} - {}'.format('500', datetime.datetime.now(), 'Timeout'))
        time.sleep(30)

    def use_pipeline(self, collection):
        return collection.is_using_pipeline

    def run_collections(self, collections):
        t = threading.Thread(target=self.connect, name=str(self), args=(collections,), daemon=True)
        t.start()

    @property
    def is_connected(self):
        with self._lock:
            return self.connected

    def connect(self, collections):
        with self._lock:
            if self.connected:
                self.disconnect(deactivate_collections=False)

        self.collections = collections
        self.query = self._build_query_for()
        filter_args = {k: ','.join(v) for k, v in self.query.items() if k != 'languages' and v}
        logger.info('Starting twython filtering with %s', str(filter_args))
        logger.info('Streaming for collections: \n%s', '\n'.join(str(c) for c in collections))
        stay_active = True
        while stay_active:
            try:
                self.statuses.filter(**filter_args)
            except (urllib3.exceptions.ReadTimeoutError, socket.timeout, requests.exceptions.ConnectionError,) as e:
                logger.warning('A timeout occurred. Streamer is sleeping for 10 seconds: %s', e)
                time.sleep(10)
            except (requests.exceptions.ChunkedEncodingError, http.client.IncompleteRead) as e:
                logger.warning('Incomplete Read: %s.', e)
                self._errors.append('{}: {} - {}'.format('400', datetime.datetime.now(), 'Incomplete Read'))
            except Exception as e:
                if DEVELOPMENT:
                    import traceback
                    traceback.print_exc()
                logger.warning('An error occurred during filtering in Streamer %s: %s. '
                               'Disconnecting collector due an unexpected error', self.__class__.__name__, e)
                stay_active = False
                self.disconnect(deactivate_collections=False)
                self._errors.append('{}: {} - {}'.format('500', datetime.datetime.now(), str(e)))

    def disconnect(self, deactivate_collections=True):
        with self._lock:
            logger.info('Disconnecting twitter streamer (thread %s)', threading.current_thread().name)
            super().disconnect()
            self.connected = False
        if deactivate_collections:
            logger.warning('Deactivating all collections!')
            app = create_app()
            with app.app_context():
                for c in self.collections:
                    c.deactivate()

    @property
    def errors(self):
        return list(self._errors)

    def track_error(self, http_error_code, message):
        self._errors.append('{}: {} - {}'.format(http_error_code, datetime.datetime.now(), message))


class BackgroundStreamer(BaseStreamer):
    """

    """

    def connect(self, collections):
        self.collection = collections[0]
        super().connect(collections)

    def on_success(self, data):

        if 'text' in data:
            lang = safe_langdetect(tweet_normalization_aggressive(data['text']))

            if lang == 'en' or lang in self.collection.languages:
                data['lang'] = lang
                tweet = Tweet.from_tweet(self.collection.id, data, ttype='collected')
                # the tweet is sent immediately to kafka queue
                message = tweet.serialize()
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('\n\nSending to PERSISTER: %s\n', tweet)
                self.producer.send(self.persister_kafka_topic, message)

                if self.use_pipeline(self.collection) and lang in AnnotatorClient.available_languages():
                    topic = '{}_{}'.format(self.annotator_kafka_topic, lang)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('\n\nSending to annotator queue: %s %s\n', topic, tweet)
                    self.producer.send(topic, message)
        else:
            logger.error('No Data: %s', str(data))


class OnDemandStreamer(BaseStreamer):
    """

    """

    def on_success(self, data):

        if 'text' in data:
            lang = safe_langdetect(tweet_normalization_aggressive(data['text']))
            data['lang'] = lang
            tweet = Tweet.from_tweet(Tweet.NO_COLLECTION_ID, data, ttype='collected')
            message = tweet.serialize()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('\n\nSending to PERSISTER: %s\n', tweet)
            self.producer.send(self.persister_kafka_topic, message)

    def use_pipeline(self, collection):
        return True


class ManualStreamer(OnDemandStreamer):
    def use_pipeline(self, collection):
        return collection.is_using_pipeline
