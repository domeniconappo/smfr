from collections import namedtuple, Counter
import os
import logging
from logging.handlers import RotatingFileHandler
import sys
import threading
import time

from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError, CQLEngineException
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import CommitFailedError, NoBrokersAvailable

from smfrcore.models import Tweet, TwitterCollection, create_app
from smfrcore.utils import RUNNING_IN_DOCKER, NULL_HANDLER, DEFAULT_HANDLER
from smfrcore.client.api_client import AnnotatorClient

PersisterConfiguration = namedtuple('PersisterConfiguration', ['persister_kafka_topic', 'kafka_bootstrap_server',
                                                               'annotator_kafka_topic', 'geocoder_kafka_topic']
                                    )

logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

os.environ['NO_PROXY'] = ','.join((AnnotatorClient.host,))

logger = logging.getLogger('PERSISTER')
logger.addHandler(DEFAULT_HANDLER)
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))

file_logger = logging.getLogger('Not Reconciled Tweets')
file_logger.addHandler(NULL_HANDLER)
file_logger.setLevel(logging.ERROR)
file_logger.propagate = False


if RUNNING_IN_DOCKER:
    filelog_path = os.path.join(os.path.dirname(__file__), '../../logs/not_reconciled_tweets.log') if not RUNNING_IN_DOCKER else '/logs/not_reconciled_tweets.log'
    hdlr = RotatingFileHandler(filelog_path, maxBytes=10485760, backupCount=2)
    hdlr.setLevel(logging.ERROR)
    file_logger.addHandler(hdlr)


class Persister:
    """
        Persister component to save Tweet messages in Cassandra.
        It listens to the Kafka queue, build a Tweet object from messages and save it in Cassandra.
        """
    config = PersisterConfiguration(
        persister_kafka_topic=os.getenv('PERSISTER_KAFKA_TOPIC', 'persister'),
        kafka_bootstrap_server=os.getenv('KAFKA_BOOTSTRAP_SERVER', 'kafka:9094'),
        annotator_kafka_topic=os.getenv('ANNOTATOR_KAFKA_TOPIC', 'annotator'),
        geocoder_kafka_topic=os.getenv('GEOCODER_KAFKA_TOPIC', 'geocoder'),
    )
    _running_instance = None
    _lock = threading.RLock()
    app = create_app()

    @classmethod
    def running_instance(cls):
        """
        The running Persister object
        :return: Persister instance
        """
        with cls._lock:
            return cls._running_instance

    @classmethod
    def set_running(cls, inst=None):
        """
        Set the running instance
        :param inst: Persister object
        """
        with cls._lock:
            cls._running_instance = inst

    def __init__(self, group_id='PERSISTER', auto_offset_reset='earliest'):
        self.topic = self.config.persister_kafka_topic
        self.bootstrap_server = self.config.kafka_bootstrap_server
        self.auto_offset_reset = auto_offset_reset
        self.group_id = group_id
        self.language_models = AnnotatorClient.available_languages()
        with self.app.app_context():
            self.collections = TwitterCollection.get_running()

        retries = 5

        while retries >= 0:
            try:
                self.consumer = KafkaConsumer(self.topic, group_id=self.group_id,
                                              auto_offset_reset=self.auto_offset_reset,
                                              bootstrap_servers=self.bootstrap_server,
                                              max_poll_records=100, max_poll_interval_ms=1000000,
                                              session_timeout_ms=90000, heartbeat_interval_ms=90000)
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_server, compression_type='gzip',
                                              buffer_memory=134217728, batch_size=1048576)
            except NoBrokersAvailable:
                logger.warning('Waiting for Kafka to boot...')
                time.sleep(5)
                retries -= 1
                if retries < 0:
                    sys.exit(1)
            else:
                break
        self.counter = Counter()

    def set_collections(self, collections):
        with self._lock:
            self.collections = collections

    def reconcile_tweet_with_collection(self, tweet):
        for c in self.collections:
            if c.is_tweet_in_bounding_box(tweet) or c.tweet_matched_keyword(tweet):
                return c
        # no collection found for ingested tweet...
        return None

    def start(self):
        """
        Main method that iterate over messages coming from Kafka queue, build a Tweet object and save it in Cassandra
        """

        logger.info('Persister started %s...Resetting counters', str(self))
        self.set_running(inst=self)
        self.counter = Counter({Tweet.ANNOTATED_TYPE: 0, Tweet.COLLECTED_TYPE: 0, Tweet.GEOTAGGED_TYPE: 0})
        try:
            for i, msg in enumerate(self.consumer, start=1):
                tweet = None
                try:
                    msg = msg.value.decode('utf-8')
                    tweet = Tweet.from_json(msg)
                    if tweet.collectionid == Tweet.NO_COLLECTION_ID:
                        # reconcile with running collections
                        collection = self.reconcile_tweet_with_collection(tweet)
                        if not collection:
                            # we log it to use to improve reconciliation in the future
                            file_logger.error('%s', msg)
                            continue
                        tweet.collectionid = collection.id

                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('Saving tweet: %s - collection %d', tweet.tweetid, tweet.collectionid)

                    tweet.save()
                    self.counter[tweet.ttype] += 1
                    self.counter['{}-{}'.format(tweet.lang, tweet.ttype)] += 1

                    self.send_to_pipeline(tweet)

                    if logger.isEnabledFor(logging.INFO) and not (i % 5000):
                        logger.info('Scanned/Saved since last restart \nTOTAL: %d \n%s', i, str(self.counter))

                except (ValidationError, ValueError, TypeError, InvalidRequest) as e:
                    logger.error(e)
                    logger.error('Poison message for Cassandra: %s', tweet or msg)
                except CQLEngineException as e:
                    logger.error(e)
                except Exception as e:
                    logger.error(type(e))
                    logger.error(e)
                    logger.error(msg)

        except CommitFailedError:
            logger.error('Persister was disconnected during I/O operations. Exited.')
        except ValueError:
            # tipically an I/O operation on closed epoll object
            # as the consumer can be disconnected in another thread (see signal handling in start.py)
            if self.consumer._closed:
                logger.info('Persister was disconnected during I/O operations. Exited.')
            elif self.running_instance() and not self.consumer._closed:
                self.running_instance().stop()
        except KeyboardInterrupt:
            self.stop()

    def send_to_pipeline(self, tweet):
        if not tweet.use_pipeline:
            return
        topic = None
        if tweet.ttype == Tweet.COLLECTED_TYPE and tweet.lang in self.language_models:
            # tweet will go to the next in pipeline: annotator queue
            topic = '{}_{}'.format(self.config.annotator_kafka_topic, tweet.lang)
        elif tweet.ttype == Tweet.ANNOTATED_TYPE:
            # tweet will go to the next in pipeline: geocoder queue
            topic = self.config.geocoder_kafka_topic
        if topic:
            message = tweet.serialize()
            self.producer.send(topic, message)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Sent to pipeline: %s %s', topic, tweet.tweetid)

    def stop(self):
        """
        Stop processing messages from queue, close KafkaConsumer and unset running instance.
        """
        self.consumer.close()
        self.set_running(inst=None)
        logger.info('Persister connection closed!')

    def __str__(self):
        return 'Persister ({}): {}@{}:{}'.format(id(self), self.topic, self.bootstrap_server, self.group_id)

    def counters(self):
        return self.counter
