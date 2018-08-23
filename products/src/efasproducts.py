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

        tweets = Tweet.get_iterator(collection_id, ttype)
        for t in tweets:
            pass

        logger.info('Production of shapefile "heatmap" and most relevant tweets terminated for collection: %d.', collection_id)

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
