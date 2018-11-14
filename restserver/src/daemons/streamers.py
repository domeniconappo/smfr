import datetime
import http
import logging
from http.client import IncompleteRead
import os
import socket
import multiprocessing
import time
from queue import Empty

import urllib3

import requests
from smfrcore.utils.kafka import make_kafka_producer
from twython import TwythonStreamer

from smfrcore.models.cassandra import Tweet
from smfrcore.models.sql import create_app
from smfrcore.utils import DEFAULT_HANDLER

from daemons.utils import safe_langdetect, tweet_normalization_aggressive
from server.config import RestServerConfiguration, DEVELOPMENT


logger = logging.getLogger('Streamer')
logger.setLevel(RestServerConfiguration.logger_level)
logger.addHandler(DEFAULT_HANDLER)
logger.propagate = False


class BaseStreamer(TwythonStreamer):
    """

    """
    mp_manager = multiprocessing.Manager()
    max_errors_len = 50

    def __init__(self, **api_keys):
        self.lock = multiprocessing.RLock()
        self.query = {}
        self.consumer_key = api_keys['consumer_key']
        self.consumer_secret = api_keys['consumer_secret']
        self.access_token = api_keys['access_token']
        self.access_token_secret = api_keys['access_token_secret']
        self.persister_kafka_topic = RestServerConfiguration.persister_kafka_topic
        self._shared_errors = multiprocessing.Queue(maxsize=self.max_errors_len)

        self.producer = None

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

    def log(self, tweet):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Sent to %s topic: %s', self.persister_kafka_topic, tweet.tweetid)

    def on_error(self, status_code, data):
        status_code = int(status_code)
        data = data.decode('utf-8') if isinstance(data, bytes) else str(data)
        err = str(data) or 'No error message'
        logger.error('ON ERROR EVENT: %s', status_code)
        logger.error('ON ERROR EVENT: %s', err)
        self.track_error(status_code, err)
        self.disconnect(deactivate_collections=False)
        self.collections = []
        self.collection = None
        sleep_time = 60 if status_code == 420 and 'Exceeded connection limit' in err else 5
        logger.debug('Sleeping %d secs', sleep_time)
        time.sleep(sleep_time)

    def on_timeout(self):
        logger.error('Timeout...')
        self.track_error(500, 'Timeout')
        time.sleep(30)

    def use_pipeline(self, collection):
        return collection.is_using_pipeline

    def run_collections(self, collections):
        t = multiprocessing.Process(target=self.connect, name=str(self), args=(collections,))
        t.daemon = True
        t.start()

    def connect(self, collections):

        # A Kafka Producer to send tweets to store to PERSISTER queue. Must be instantiated in same process
        self.producer = make_kafka_producer()
        with self.lock:
            if self.connected:
                self.disconnect(deactivate_collections=False)

        self.collections = collections
        self.query = self._build_query_for()
        filter_args = {k: ','.join(v).strip(',') for k, v in self.query.items() if k != 'languages' and v}
        logger.info('Streaming for collections: \n%s', '\n'.join(str(c) for c in collections))
        stay_active = True
        self.connected = True
        while stay_active:
            try:
                logger.info('Connecting to streamer %s', str(filter_args))
                self.statuses.filter(**filter_args)
            except (urllib3.exceptions.ReadTimeoutError, socket.timeout, requests.exceptions.ConnectionError,) as e:
                logger.warning('A timeout occurred. Streamer is sleeping for 10 seconds: %s', e)
                self.track_error(500, 'Timeout')
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
        if self.producer:
            self.producer.flush()
        logger.info('Disconnecting twitter streamer')
        with self.lock:
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
        with self.lock:
            try:
                res = [e for e in iter(self._shared_errors.get_nowait, None)]
            except Empty:
                res = []
            return res

    def track_error(self, http_error_code, message):
        message = message.decode('utf-8') if isinstance(message, bytes) else str(message)
        with self.lock:
            if self._shared_errors.full():
                self._shared_errors = multiprocessing.Queue()
            self._shared_errors.put('{code}: {date} - {error}'.format(
                code=http_error_code,
                date=datetime.datetime.now().strftime('%Y%m%d %H:%M'),
                error=message.strip('\r\n')), block=False
            )


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
                message = tweet.serialize()
                self.producer.send(self.persister_kafka_topic, message)
                self.log(tweet)
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
            self.log(tweet)

    def use_pipeline(self, collection):
        return True


class ManualStreamer(OnDemandStreamer):
    def use_pipeline(self, collection):
        return collection.is_using_pipeline
