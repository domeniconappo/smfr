import os
import logging
import sys
import threading
import time

from datetime import datetime, timedelta
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from smfrcore.models.cassandramodels import Tweet
from smfrcore.models.sqlmodels import TwitterCollection
from smfrcore.utils import RUNNING_IN_DOCKER, LOGGER_FORMAT, LOGGER_DATE_FORMAT
from sqlalchemy import or_

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)

logger = logging.getLogger(__name__)
logging.getLogger('cassandra').setLevel(logging.ERROR)


class Products:
    """
    Products component implementation
    """
    _running = []
    _stop_signals = []
    _lock = threading.RLock()
    kafka_bootstrap_server = '{}:9092'.format('kafka' if RUNNING_IN_DOCKER else '127.0.0.1')

    kafkaup = False
    retries = 5
    while (not kafkaup) and retries >= 0:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server, compression_type='gzip')
        except NoBrokersAvailable:
            logger.warning('Waiting for Kafka to boot...')
            time.sleep(5)
            retries -= 1
            if retries < 0:
                sys.exit(1)
        else:
            kafkaup = True
            break

    persister_kafka_topic = os.environ.get('PERSISTER_KAFKA_TOPIC', 'persister')

    @classmethod
    def log_config(cls):
        logger.info('Products configuration...')

    @classmethod
    def start(cls, collection_id):
        """
        Heatmaps and most relevant tweets for an ondemand collection
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        """
        logger.info('Production of heatmap and most relevant tweets started for collection: %d.', collection_id)
        ttype = 'geotagged'

        # tweets = Tweet.get_iterator(collection_id, ttype)
        # for t in tweets:
        #     pass

        logger.info('Production of heatmap and most relevant tweets terminated for collection: %d.', collection_id)

    @classmethod
    def produce(cls):
        # create products for on-demand active colletions or recently stopped collections
        collections = TwitterCollection.query.filter(TwitterCollection.trigger == TwitterCollection.TRIGGER_ONDEMAND).filter(
            or_(
                TwitterCollection.status == 'active',
                TwitterCollection.stopped_at >= datetime.now() - timedelta(days=2)
            )
        )
        args = [(c.id,) for c in collections]
        with ThreadPool(cpu_count() - 1) as p:
            p.starmap(cls.start, args)
