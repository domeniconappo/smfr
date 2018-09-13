import logging
import os
import threading

from twython import TwythonStreamer

from smfrcore.models import Tweet, create_app

from daemons.utils import safe_langdetect, tweet_normalization_aggressive
from server.config import RestServerConfiguration
from server.api.clients import AnnotatorClient


logger = logging.getLogger('RestServer Streamer')
logger.setLevel(RestServerConfiguration.logger_level)
hdlr = logging.FileHandler(RestServerConfiguration.not_reconciled_log_path)
hdlr.setLevel(logging.ERROR)
logger.addHandler(hdlr)


class BaseStreamer(TwythonStreamer):
    """

    """
    def __init__(self, producer, consumer_key, consumer_secret, access_token, access_token_secret):
        self.query = {}
        self.persister_kafka_topic = RestServerConfiguration.persister_kafka_topic
        self.annotator_kafka_topic = RestServerConfiguration.annotator_kafka_topic

        # A Kafka Producer
        self.producer = producer

        # starting streamer...
        self.collections = []
        self.collection = None

        self.client_args = {}
        if os.environ.get('http_proxy'):
            self.client_args = {
                'proxies': {
                    'http': os.environ['http_proxy'],
                    'https': os.environ.get('https_proxy') or os.environ['http_proxy']
                }
            }
        logger.info('Instantiate a streamer with args %s', str(self.client_args))
        super().__init__(consumer_key, consumer_secret,
                         access_token, access_token_secret,
                         client_args=self.client_args)

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
        logger.error(status_code)
        logger.error(str(data) or 'No data')
        self.disconnect()
        self.collections = []
        self.collection = None

    def on_timeout(self):
        logger.error('Timeout')

    def use_pipeline(self, collection):
        return collection.is_using_pipeline

    def run_collections(self, collections):
        t = threading.Thread(target=self.run, name='Streamer {}'.format(collections[0].trigger),
                             args=(collections,),
                             daemon=True)
        t.start()

    def run(self, collections):
        self.collections = collections
        self.query = self._build_query_for()
        filter_args = {k: ','.join(v) for k, v in self.query.items() if k != 'languages' and v}
        logger.info('Starting twython filtering with %s', str(filter_args))
        logger.info('Streaming for collections: \n%s', '\n'.join(str(c) for c in collections))
        self.statuses.filter(**filter_args)

    def disconnect(self):
        app = create_app()
        with app.app_context():
            for c in self.collections:
                c.deactivate()
        super().disconnect()


class BackgroundStreamer(BaseStreamer):
    """

    """

    def run(self, collections):
        self.collection = collections[0]
        super().run(collections)

    def on_success(self, data):

        if 'text' in data:
            lang = safe_langdetect(tweet_normalization_aggressive(data['text']))

            if lang == 'en' or lang in self.collection.languages:
                data['lang'] = lang
                tweet = Tweet.build_from_tweet(self.collection.id, data, ttype='collected')
                # the tweet is sent immediately to kafka queue
                message = tweet.serialize()
                logger.debug('\n\nSending to PERSISTER: %s\n', str(tweet))
                self.producer.send(self.persister_kafka_topic, message)

                # On Demand collections always use pipelines
                if self.use_pipeline(self.collection) and lang in AnnotatorClient.available_languages():
                    topic = '{}_{}'.format(self.annotator_kafka_topic, lang)
                    self.producer.send(topic, message)


class OnDemandStreamer(BaseStreamer):
    """

    """
    def reconcile_tweet_with_collection(self, data):
        for c in self.collections:
            if c.is_tweet_in_bounding_box(data) or c.tweet_matched_keyword(data):
                return c
        # no collection found for ingested tweet...
        return None

    def on_success(self, data):

        if 'text' in data:
            lang = safe_langdetect(tweet_normalization_aggressive(data['text']))
            data['lang'] = lang
            collection = self.reconcile_tweet_with_collection(data)
            if not collection:
                logger.error('Tweet was not reconciled with any collection! %s\n ', data)
                return

            tweet = Tweet.build_from_tweet(collection.id, data, ttype='collected')
            message = tweet.serialize()
            logger.debug('\n\nSending to persister queue: %s\n', str(tweet))
            self.producer.send(self.persister_kafka_topic, message)
            # send to next topic in the pipeline in case collection.is_using_pipeline == True
            # On Demand collections always use pipelines
            if self.use_pipeline(collection) and lang in AnnotatorClient.available_languages():
                topic = '{}_{}'.format(self.annotator_kafka_topic, lang)
                self.producer.send(topic, message)

    def use_pipeline(self, collection):
        return True


class ManualStreamer(OnDemandStreamer):
    def use_pipeline(self, collection):
        return collection.is_using_pipeline
