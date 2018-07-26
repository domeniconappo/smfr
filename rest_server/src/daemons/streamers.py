import logging
from datetime import datetime

from smfrcore.models.cassandramodels import Tweet
from twython import TwythonStreamer

from smfrcore.models.sqlmodels import TwitterCollection

from daemons.utils import safe_langdetect, tweet_normalization_aggressive
from server.config import RestServerConfiguration


class CollectorStreamer(TwythonStreamer):
    """

    """

    logger = logging.getLogger('RestServer Streamer')
    logger.setLevel(RestServerConfiguration.logger_level)

    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret,
                 client_args, collection, producer, quiet=False):

        self.kafka_topic = RestServerConfiguration.kafka_topic
        self.quiet = quiet
        # A TwitterCollection object (it's a row in MySQL DB)
        self.collection = collection

        # A Kafka Producer
        self.producer = producer

        # starting streamer...
        self.logger.info('Instantiate a streamer with args %s', str(client_args))
        super().__init__(app_key, app_secret,
                         oauth_token, oauth_token_secret,
                         client_args=client_args)

    def on_success(self, data):

        if 'text' in data:
            lang = safe_langdetect(tweet_normalization_aggressive(data['text'])) if not self.collection.is_ondemand else None
            languages = self.collection.languages
            if self.collection.is_ondemand or lang == 'en' or lang in languages:
                data['lang'] = lang
                tweet = Tweet.build_from_tweet(self.collection, data)
                # the tweet is sent immediately to kafka queue
                message = tweet.serialize()
                self.logger.debug('Sending to queue: %s', str(tweet))
                self.producer.send(self.kafka_topic, message)

    def on_error(self, status_code, data):
        self.logger.error(status_code)
        self.logger.error(str(data) or 'No data')
        self.disconnect()

        self.collection.status = TwitterCollection.INACTIVE_STATUS
        self.collection.stopped_at = datetime.utcnow()
        self.collection.save()

    def on_timeout(self):
        self.logger.error('Timeout')
