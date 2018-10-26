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
        self._errors = deque(maxlen=50)

        # A Kafka Producer to send tweets to store to PERSISTER queue
        self.producer = producer

        self.collections = []
        self.collection = None

        self.client_args = {}
        if os.getenv('http_proxy'):
            self.client_args = dict(proxies=dict(http=os.environ['http_proxy'],
                                                 https=os.getenv('https_proxy') or os.environ['http_proxy']))
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
        self.track_error(status_code, err)
        self.disconnect(deactivate_collections=False)
        self.collections = []
        self.collection = None
        sleep_time = 60 if str(status_code) == '420' and 'Exceeded connection limit' in 'data' else 5
        time.sleep(sleep_time)

    def on_timeout(self):
        logger.error('Timeout...')
        self.track_error(500, 'Timeout')
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
        if self.is_connected:
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
                self.track_error(400, 'Incomplete Read')
            except Exception as e:
                if DEVELOPMENT:
                    import traceback
                    traceback.print_exc()
                logger.warning('An error occurred during filtering in Streamer %s: %s. '
                               'Disconnecting collector due an unexpected error', self.__class__.__name__, e)
                stay_active = False
                self.disconnect(deactivate_collections=False)
                self.track_error(500, str(e))

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
        with self._lock:
            return list(self._errors)

    def track_error(self, http_error_code, message):
        message = message.decode('utf-8') if isinstance(message, bytes) else str(message)
        with self._lock:
            self._errors.append('{code}: {date} - {error}'.format(
                code=http_error_code,
                date=datetime.datetime.now().strftime('%Y%m%d %H:%M'),
                error=message.strip('\r\n')))


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
                tweet = Tweet.from_tweet(self.collection.id, data, ttype=Tweet.COLLECTED_TYPE)
                # the tweet is sent immediately to kafka queue
                message = tweet.serialize()
                self.producer.send(self.persister_kafka_topic, message)

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Collected tweet sent to PERSISTER: %s', tweet.tweetid)

        else:
            logger.error('No Data: %s', str(data))


class OnDemandStreamer(BaseStreamer):
    """

    """

    def on_success(self, data):

        if 'text' in data:
            lang = safe_langdetect(tweet_normalization_aggressive(data['text']))
            data['lang'] = lang
            tweet = Tweet.from_tweet(Tweet.NO_COLLECTION_ID, data, ttype=Tweet.COLLECTED_TYPE)
            message = tweet.serialize()
            self.producer.send(self.persister_kafka_topic, message)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Sent to PERSISTER: %s', tweet.tweetid)

    def use_pipeline(self, collection):
        return True


class ManualStreamer(OnDemandStreamer):
    def use_pipeline(self, collection):
        return collection.is_using_pipeline
