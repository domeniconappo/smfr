from collections import namedtuple, Counter
import os
import logging
from logging.handlers import RotatingFileHandler
import sys
import threading
import time
import multiprocessing

from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError, CQLEngineException
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import CommitFailedError, NoBrokersAvailable

from smfrcore.models import Tweet, TwitterCollection, create_app
from smfrcore.utils import IN_DOCKER, NULL_HANDLER, DEFAULT_HANDLER
from smfrcore.client.api_client import AnnotatorClient


logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('cassandra').setLevel(logging.WARNING)

os.environ['NO_PROXY'] = ','.join((AnnotatorClient.host,))

logger = logging.getLogger('PERSISTER')
logger.addHandler(DEFAULT_HANDLER)
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.propagate = False

file_logger = logging.getLogger('UNRECONCILED')
file_logger.addHandler(NULL_HANDLER)
file_logger.setLevel(logging.ERROR)
file_logger.propagate = False


if IN_DOCKER:
    filelog_path = os.path.join(os.path.dirname(__file__), '../../logs/not_reconciled_tweets.log') if not IN_DOCKER else '/logs/not_reconciled_tweets.log'
    hdlr = RotatingFileHandler(filelog_path, maxBytes=10485760, backupCount=2)
    hdlr.setLevel(logging.ERROR)
    file_logger.addHandler(hdlr)


class Persister:
    """
        Persister component to save Tweet messages in Cassandra.
        It listens to the Kafka queue, build a Tweet object from messages and save it in Cassandra.
        """
    PersisterConfiguration = namedtuple('PersisterConfiguration', ['persister_kafka_topic', 'kafka_bootstrap_server',
                                                                   'annotator_kafka_topic', 'geocoder_kafka_topic'])
    config = PersisterConfiguration(
        persister_kafka_topic=os.getenv('PERSISTER_KAFKA_TOPIC', 'persister'),
        kafka_bootstrap_server=os.getenv('KAFKA_BOOTSTRAP_SERVER', 'kafka:9092') if IN_DOCKER else '127.0.0.1:9092',
        annotator_kafka_topic=os.getenv('ANNOTATOR_KAFKA_TOPIC', 'annotator'),
        geocoder_kafka_topic=os.getenv('GEOCODER_KAFKA_TOPIC', 'geocoder'),
    )
    _lock = multiprocessing.RLock()
    app = create_app()
    _manager = multiprocessing.Manager()
    shared_counter = _manager.dict({Tweet.ANNOTATED_TYPE: 0, Tweet.COLLECTED_TYPE: 0, Tweet.GEOTAGGED_TYPE: 0})

    def __init__(self, group_id='PERSISTER', auto_offset_reset='earliest'):
        self.topic = self.config.persister_kafka_topic
        self.bootstrap_server = self.config.kafka_bootstrap_server
        self.auto_offset_reset = auto_offset_reset
        self.group_id = group_id
        self.language_models = AnnotatorClient.available_languages()
        self.background_process = None
        with self.app.app_context():
            self.collections = TwitterCollection.get_running()

        retries = 5

        while retries >= 0:
            try:
                self.consumer = KafkaConsumer(self.topic,
                                              group_id=self.group_id,
                                              auto_offset_reset=self.auto_offset_reset,
                                              check_crcs=False,
                                              bootstrap_servers=self.bootstrap_server,
                                              max_poll_records=100, max_poll_interval_ms=600000,
                                              request_timeout_ms=300000,
                                              session_timeout_ms=10000, heartbeat_interval_ms=3000
                                              )

                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_server, compression_type='gzip',
                                              request_timeout_ms=50000, buffer_memory=134217728,
                                              linger_ms=500, batch_size=1048576)
            except NoBrokersAvailable:
                logger.warning('Waiting for Kafka to boot...')
                time.sleep(5)
                retries -= 1
                if retries < 0:
                    sys.exit(1)
            else:
                break

    def set_collections(self, collections):
        with self._lock:
            self.collections = collections

    def reconcile_tweet_with_collection(self, tweet):
        for c in self.collections:
            if c.is_tweet_in_bounding_box(tweet) or c.tweet_matched_keyword(tweet):
                return c
        # no collection found for ingested tweet...
        return None

    def start_in_background(self):
        p = multiprocessing.Process(target=self.start, name='Persister Consumer')
        self.background_process = p
        p.start()

    def start(self):
        """
        Main method that iterate over messages coming from Kafka queue, build a Tweet object and save it in Cassandra
        """

        logger.info('Starting %s...Reset counters', str(self))
        try:
            logger.info('===> Entering in consumer loop...')
            for i, msg in enumerate(self.consumer, start=1):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Fetched %d', i)
                tweet = None
                try:
                    msg = msg.value.decode('utf-8')
                    tweet = Tweet.from_json(msg)
                    if tweet.collectionid == Tweet.NO_COLLECTION_ID:
                        # reconcile with running collections
                        start = time.time()
                        logger.debug('Start Reconcile')
                        collection = self.reconcile_tweet_with_collection(tweet)
                        logger.debug('End Reconcile in %s', time.time() - start)
                        if not collection:
                            # we log it to use to improve reconciliation in the future
                            file_logger.error('%s', msg)
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug('No collection for tweet %s', tweet.tweetid)
                            continue
                        tweet.collectionid = collection.id

                    tweet.save()

                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('Saved tweet: %s - collection %d', tweet.tweetid, tweet.collectionid)
                    if logger.isEnabledFor(logging.INFO) and not (i % 5000):
                        logger.info('Scanned/Saved since last restart \nTOTAL: %d \n%s', i, str(self.shared_counter))

                    with self._lock:
                        self.shared_counter[tweet.ttype] += 1
                        self.shared_counter['{}-{}'.format(tweet.lang, tweet.ttype)] += 1

                    self.send_to_pipeline(tweet)

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
        except KeyboardInterrupt:
            self.stop()

    def send_to_pipeline(self, tweet):
        if not tweet.use_pipeline:
            return
        topic = None
        if tweet.ttype == Tweet.COLLECTED_TYPE and tweet.lang in self.language_models:
            # tweet will go to the next in pipeline: annotator queue
            topic = '{}-{}'.format(self.config.annotator_kafka_topic, tweet.lang)
        elif tweet.ttype == Tweet.ANNOTATED_TYPE:
            # tweet will go to the next in pipeline: geocoder queue
            topic = self.config.geocoder_kafka_topic
        if not topic:
            return
        message = tweet.serialize()
        self.producer.send(topic, message)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Sent to pipeline: %s %s', topic, tweet.tweetid)

    def stop(self):
        """
        Stop processing messages from queue, close KafkaConsumer and unset running instance.
        """
        if self.background_process:
            self.background_process.terminate()
        self.consumer.close()
        logger.info('Persister connection closed!')

    def __str__(self):
        return 'Persister ({}): {}@{}:{}'.format(id(self), self.topic, self.bootstrap_server, self.group_id)

    def counters(self):
        with self._lock:
            return dict(self.shared_counter)
