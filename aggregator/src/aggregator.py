from collections import Counter
import functools
from datetime import timedelta, datetime
import logging
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import os

from sqlalchemy import or_

from smfrcore.models.sqlmodels import TwitterCollection, Aggregation, create_app

LOGGER_FORMAT = '%(asctime)s: Aggregator - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=DATE_FORMAT)

logger = logging.getLogger(__name__)
logging.getLogger('cassandra').setLevel(logging.ERROR)

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
def aggregate(running=True, everything=False, background=False):
    with flask_app.app_context():
        if running:
            collections_to_aggregate = TwitterCollection.query.filter(
                or_(
                    TwitterCollection.status == 'active',
                    TwitterCollection.stopped_at >= datetime.now() - timedelta(hours=6)
                )
            )
        elif everything:
            collections_to_aggregate = TwitterCollection.query.all()
        elif background:
            collections_to_aggregate = TwitterCollection.query.filter_by(trigger='background')
        else:
            raise ValueError('Aggregator must be started with a parameter: [-r (running)| -a (all) | -b (background)]')

        aggregations_args = []

        for coll in collections_to_aggregate:
            logger.debug('Aggregating %s' % coll)
            aggregation = Aggregation.query.filter_by(collection_id=coll.id).first()
            if not aggregation:
                aggregation = Aggregation(collection_id=coll.id, values={})
                aggregation.save()
            aggregations_args.append(
                (coll.id,
                 aggregation.last_tweetid_collected,
                 aggregation.last_tweetid_annotated,
                 aggregation.last_tweetid_geotagged,
                 aggregation.values)
            )

        # # max cpu_count() - 1 aggregation threads running at same time
        with ThreadPool(cpu_count() - 1) as p:
            p.starmap(run_single_aggregation, aggregations_args)


def run_single_aggregation(collection_id,
                           last_tweetid_collected, last_tweetid_annotated, last_tweetid_geotagged,
                           initial_values):
    from smfrcore.models.cassandramodels import Tweet

    max_collected_tweetid = 0
    max_annotated_tweetid = 0
    max_geotagged_tweetid = 0
    last_tweetid_collected = int(last_tweetid_collected) if last_tweetid_collected else 0
    last_tweetid_annotated = int(last_tweetid_annotated) if last_tweetid_annotated else 0
    last_tweetid_geotagged = int(last_tweetid_geotagged) if last_tweetid_geotagged else 0
    counter = Counter(initial_values)
    logger.info(' >>>>>>>>>>>> Starting aggregation for collection id %d' % collection_id)

    collected_tweets = Tweet.get_iterator(collection_id, 'collected', last_tweetid=last_tweetid_collected)
    for t in collected_tweets:
        max_collected_tweetid = max(max_collected_tweetid, t.tweet_id)
        counter['collected'] += 1

    annotated_tweets = Tweet.get_iterator(collection_id, 'annotated', last_tweetid=last_tweetid_annotated)
    for t in annotated_tweets:
        max_annotated_tweetid = max(max_annotated_tweetid, t.tweet_id)
        counter['annotated'] += 1
        # TODO put here most representative tweets logic....?

    geotagged_tweets = Tweet.get_iterator(collection_id, 'geotagged', last_tweetid=last_tweetid_geotagged)
    for t in geotagged_tweets:
        max_geotagged_tweetid = max(max_geotagged_tweetid, t.tweet_id)
        counter['geotagged'] += 1

    with flask_app.app_context():
        aggregation = Aggregation.query.filter_by(collection_id=collection_id).first()
        aggregation.last_tweetid_collected = max_collected_tweetid if max_collected_tweetid else last_tweetid_collected
        aggregation.last_tweetid_annotated = max_annotated_tweetid if max_annotated_tweetid else last_tweetid_annotated
        aggregation.last_tweetid_geotagged = max_geotagged_tweetid if max_geotagged_tweetid else last_tweetid_geotagged
        aggregation.values = dict(counter)
        aggregation.save()
        logger.info(' <<<<<<<<<<< Aggregation terminated for collection %d: %s', collection_id, str(aggregation.values))
    return 0
