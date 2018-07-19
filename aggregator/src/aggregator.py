from collections import Counter
import functools
from datetime import timedelta, datetime
import logging
import multiprocessing

from sqlalchemy import or_

from smfrcore.models.sqlmodels import TwitterCollection, Aggregation, create_app
from smfrcore.models.cassandramodels import Tweet

LOGGER_FORMAT = '%(asctime)s: Aggregator - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'

logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger(__name__)

flask_app = create_app()


def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info('Running job "%s"(args="%s", kwargs="%s")' % (func.__name__, str(args), str(kwargs)))
        result = func(*args, **kwargs)
        logger.info('Job "%s" completed' % func.__name__)
        return result
    return wrapper


@with_logging
def aggregate(everything=False, background=False):
    with flask_app.app_context():
        if everything:
            logger.debug('Aggregate everything')
            collections_to_aggregate = TwitterCollection.query.all()
        elif background:
            logger.debug('Aggregate background collector')
            collections_to_aggregate = TwitterCollection.query.filter_by(trigger='background')
        else:

            collections_to_aggregate = TwitterCollection.query.filter(
                or_(
                    TwitterCollection.status == 'active',
                    TwitterCollection.stopped_at >= datetime.now() - timedelta(hours=6)
                )
            )
        aggregations_args = []
        for coll in collections_to_aggregate:
            logger.debug('Aggregating %s' % coll)
            aggregation = Aggregation.query.filter_by(collection_id=coll.id).first()
            if not aggregation:
                aggregation = Aggregation(collection_id=coll.id, values={})
                aggregation.save()
            aggregations_args.append((coll.id, aggregation.last_tweetid or None))

        # max 6 aggregation processes running at same time
        with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
            p.starmap(run_single_aggregation, aggregations_args)


def run_single_aggregation(collection_id, last_tweetid):
    last_tweet = None
    counter = Counter()
    logger.info('START AGGREGATION FOR collection id %d' % collection_id)
    collected_tweets = Tweet.get_iterator(collection_id, 'collected', last_tweetid=last_tweetid)

    for i, t in enumerate(collected_tweets, start=1):
        logger.debug('%d' % i)
        last_tweet = t
        counter['collected'] += 1
        if not (i % 100):
            logger.debug('WENT TROUGH %d tweets already' % i)

    annotated_tweets = Tweet.get_iterator(collection_id, 'annotated', last_tweetid=last_tweetid)
    for t in annotated_tweets:
        last_tweet = t
        counter['annotated'] += 1

    geotagged_tweets = Tweet.get_iterator(collection_id, 'geotagged', last_tweetid=last_tweetid)
    for t in geotagged_tweets:
        last_tweet = t
        counter['geotagged'] += 1
    aggregation = Aggregation.query.filter_by(collection_id=collection_id.id).first()
    aggregation.last_tweetid = last_tweet.tweetid if last_tweet else None
    aggregation.values = dict(counter)
    logger.info('Aggregated %d %s', collection_id, str(counter))
    aggregation.save()
    return 0
