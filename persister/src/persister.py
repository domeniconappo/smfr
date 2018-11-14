from collections import namedtuple
import os
import logging
from logging.handlers import RotatingFileHandler
import time
import multiprocessing
from multiprocessing.managers import BaseManager, DictProxy
from collections import defaultdict

from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError, CQLEngineException
from kafka.errors import CommitFailedError, ConsumerTimeout

from smfrcore.models.sql import TwitterCollection, create_app
from smfrcore.utils import IN_DOCKER, NULL_HANDLER, DEFAULT_HANDLER
from smfrcore.utils.kafka import make_kafka_consumer, make_kafka_producer
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
    filelog_path = os.path.join(os.path.dirname(__file__),
                                '../../logs/not_reconciled_tweets.log') if not IN_DOCKER else '/logs/not_reconciled_tweets.log'
    hdlr = RotatingFileHandler(filelog_path, maxBytes=10485760, backupCount=2)
    hdlr.setLevel(logging.ERROR)
    file_logger.addHandler(hdlr)


class DefaultDictSyncManager(BaseManager):
    pass


# multiprocessing.Manager does not include defaultdict: we need to use a customized Manager
DefaultDictSyncManager.register('defaultdict', defaultdict, DictProxy)


class Persister:
    """
        Persister component to save Tweet messages in Cassandra.
        It listens to the Kafka queue, build a Tweet object from messages and save it in Cassandra.
        """
    auto_offset_reset = 'earliest'
    group_id = 'PERSISTER'
    PersisterConfiguration = namedtuple('PersisterConfiguration', ['persister_kafka_topic', 'kafka_bootstrap_servers',
                                                                   'annotator_kafka_topic', 'geocoder_kafka_topic'])
    config = PersisterConfiguration(
        persister_kafka_topic=os.getenv('PERSISTER_KAFKA_TOPIC', 'persister'),
        kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9090,kafka:9092') if IN_DOCKER else '127.0.0.1:9090,127.0.0.1:9092',
        annotator_kafka_topic=os.getenv('ANNOTATOR_KAFKA_TOPIC', 'annotator'),
        geocoder_kafka_topic=os.getenv('GEOCODER_KAFKA_TOPIC', 'geocoder'),
    )
    _lock = multiprocessing.RLock()
    app = create_app()
    _manager = DefaultDictSyncManager()
    _manager.start()
    shared_counter = _manager.defaultdict(int)

    def __init__(self):
        self.topic = self.config.persister_kafka_topic
        self.bootstrap_servers = self.config.kafka_bootstrap_servers.split(',')
        self.language_models = AnnotatorClient.available_languages()
        self.background_process = None
        self.active = True
        with self.app.app_context():
            self.collections = TwitterCollection.get_running()

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
        p = multiprocessing.Process(target=self.start, name='PersisterProcess')
        self.background_process = p
        p.daemon = True
        p.start()
        return p

    def start(self):
        """
        Main method that iterate over messages coming from Kafka queue, build a Tweet object and save it in Cassandra
        """
        from smfrcore.models.cassandra import Tweet
        logger.info('Starting %s...Reset counters and making kafka connections', str(self))
        producer = make_kafka_producer()
        consumer = make_kafka_consumer(topic=self.topic)
        while self.active:
            try:
                logger.info('===> Entering in consumer loop...')
                for i, msg in enumerate(consumer, start=1):
                    tweet = None
                    try:
                        msg = msg.value.decode('utf-8')
                        tweet = Tweet.from_json(msg)
                        if tweet.collectionid == Tweet.NO_COLLECTION_ID:
                            # reconcile with running collections
                            start = time.time()
                            collection = self.reconcile_tweet_with_collection(tweet)
                            if not collection:
                                if logger.isEnabledFor(logging.DEBUG):
                                    logger.debug('No collection for tweet %s', tweet.tweetid)
                                # we log it to file as data for improving reconciliation in the future
                                logger.debug('Saving to file unreconciled tweet %s', tweet.tweetid)
                                file_logger.error('%s', msg)
                                continue  # continue the consumer for loop
                            tweet.collectionid = collection.id
                        tweet.save()
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug('Saved tweet: %s - collection %d', tweet.tweetid, tweet.collectionid)
                        if logger.isEnabledFor(logging.INFO) and not (i % 5000):
                            logger.info('Scanned/Saved since last restart \nTOTAL: %d \n%s', i, str(self.shared_counter))

                        with self._lock:
                            self.shared_counter[tweet.ttype] += 1
                            self.shared_counter['{}-{}'.format(tweet.lang, tweet.ttype)] += 1

                        self.send_to_pipeline(producer, tweet)

                    except (ValidationError, ValueError, TypeError, InvalidRequest) as e:
                        logger.error(e)
                        logger.error('Poison message for Cassandra: %s', tweet or msg)
                        continue
                    except CQLEngineException as e:
                        logger.error(e)
                        continue
                    except Exception as e:
                        logger.error(type(e))
                        logger.error(e)
                        continue
            except ConsumerTimeout:
                logger.warning('Consumer Timeout...sleep 5 seconds')
                time.sleep(5)
            except CommitFailedError:
                self.active = False
                logger.error('Persister was disconnected during I/O operations. Exited.')
            except ValueError:
                # tipically an I/O operation on closed epoll object
                # as the consumer can be disconnected in another thread (see signal handling in start.py)
                if consumer._closed:
                    logger.info('Persister was disconnected during I/O operations. Exited.')
                    self.active = False
            except KeyboardInterrupt:
                self.stop()
                self.active = False

    def send_to_pipeline(self, producer, tweet):
        if not tweet.use_pipeline:
            return
        topic = None
        from smfrcore.models.cassandra import Tweet
        if tweet.ttype == Tweet.COLLECTED_TYPE and tweet.lang in self.language_models:
            # tweet will go to the next in pipeline: annotator queue
            topic = '{}-{}'.format(self.config.annotator_kafka_topic, tweet.lang)
        elif tweet.ttype == Tweet.ANNOTATED_TYPE:
            # tweet will go to the next in pipeline: geocoder queue
            topic = self.config.geocoder_kafka_topic
        if not topic:
            return
        message = tweet.serialize()
        producer.send(topic, message)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Sent to pipeline: %s %s', topic, tweet.tweetid)

    def stop(self):
        """
        Stop processing messages from queue, close KafkaConsumer and unset running instance.
        """
        if self.background_process:
            self.background_process.terminate()
        with self._lock:
            self.active = False
        logger.info('Persister connection closed!')

    def __str__(self):
        return 'Persister ({}): {}@{}:{}'.format(id(self), self.topic, self.bootstrap_servers, self.group_id)

    def counters(self):
        with self._lock:
            return dict(self.shared_counter)
