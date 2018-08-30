from collections import Counter
import functools
from datetime import timedelta, datetime
import logging
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import os
import time

import cassandra
from sqlalchemy import or_

from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT

from smfrcore.models.sqlmodels import TwitterCollection, Aggregation, create_app


logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)

logger = logging.getLogger('AGGREGATOR')
logging.getLogger('cassandra').setLevel(logging.ERROR)

flask_app = create_app()

running_aggregators = set()
flood_propability_ranges = ((0, 10), (10, 90), (90, 100))


class MostRelevantTweets:

    @classmethod
    def _sortkey(cls, t):
        return t['annotations']['flood_probability']['yes']

    def __init__(self, maxsize, initial=None):
        self.maxsize = maxsize
        self._tweets = sorted(initial, key=self._sortkey, reverse=True) if initial else []
        self._tweets = self._tweets[:self.maxsize]
        self.min_prob = self._tweets[-1]['annotations']['flood_probability']['yes'] if initial else 0

    @property
    def values(self):
        return self._tweets

    def push_if_relevant(self, item):
        if item['annotations']['flood_probability']['yes'] >= self.min_prob or len(self._tweets) < self.maxsize:
            self._tweets.append(item)
            self._tweets = sorted(self._tweets, key=self._sortkey, reverse=True)
            self._tweets = self._tweets[:self.maxsize]
            self.min_prob = self._tweets[-1]['annotations']['flood_probability']['yes']


def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        time_start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - time_start
        logger.info('Job "%s" completed. Elapsed time: %s' % (func.__name__, str(timedelta(seconds=elapsed))))
        return result
    return wrapper


@with_logging
def aggregate(running_conf=None):
    """

    :param running_conf: ArgumentParser object with running, all and background attributes
    :return:
    """
    if not running_conf:
        raise ValueError('No parsed arguments were passed')

    with flask_app.app_context():

        if running_conf.running:
            collections_to_aggregate = TwitterCollection.query.filter(
                or_(
                    TwitterCollection.status == 'active',
                    TwitterCollection.stopped_at >= datetime.now() - timedelta(days=2)
                )
            )
        elif running_conf.all:
            collections_to_aggregate = TwitterCollection.query.all()
        elif running_conf.background:
            collections_to_aggregate = TwitterCollection.query.filter_by(trigger='background')
        elif running_conf.collections:
            collections_to_aggregate = TwitterCollection.query.filter(TwitterCollection.id.in_(running_conf.collections)).all()
        else:
            raise ValueError('Aggregator must be started with a parameter: '
                             '[-c id1,...,idN | -r (running)| -a (all) | -b (background)]')

        if not list(collections_to_aggregate):
            logger.info('No collections to aggregate with configuration: %s', pretty_running_conf(running_conf))

        aggregations_args = []
        for coll in collections_to_aggregate:
            aggregation = Aggregation.query.filter_by(collection_id=coll.id).first()

            if not aggregation:
                aggregation = Aggregation(collection_id=coll.id, values={}, relevant_tweets=[])
                aggregation.save()

            aggregations_args.append(
                (coll.id,
                 aggregation.last_tweetid_collected,
                 aggregation.last_tweetid_annotated,
                 aggregation.last_tweetid_geotagged,
                 aggregation.timestamp_start, aggregation.timestamp_end,
                 aggregation.values, aggregation.relevant_tweets)
            )

        # cpu_count() - 1 aggregation threads running at same time
        with ThreadPool(cpu_count() - 1) as p:
            p.starmap(run_single_aggregation, aggregations_args)


def inc_annotated_counter(counter, flood_probability, nuts2=None):
    flood_probability *= 100
    for range_a, range_b in flood_propability_ranges:
        if range_a < flood_probability <= range_b:
            counter_key = '{}_num_tweets_{}-{}'.format(nuts2, range_a, range_b)
            counter[counter_key] += 1
            break


def run_single_aggregation(collection_id,
                           last_tweetid_collected, last_tweetid_annotated, last_tweetid_geotagged,
                           timestamp_start, timestamp_end,
                           initial_values, initial_relevant_tweets):
    """
    Calculating stats with attributes:
    - NUTS2
    - timestamp_start
    - timestamp_end
    - num_tweets_00-20
    - num_tweets_20-40
    - num_tweets_40-60
    - num_tweets_60-80
    - num_tweets_80-100
    - NUTS2ID_num_tweets_60-80 etc.
    :param initial_relevant_tweets:
    :param collection_id:
    :param last_tweetid_collected:
    :param last_tweetid_annotated:
    :param last_tweetid_geotagged:
    :param timestamp_end:
    :param timestamp_start:
    :param initial_values:
    :return:
    """
    from smfrcore.models.cassandramodels import Tweet

    if collection_id in running_aggregators:
        logger.warning('!!!!!! Previous aggregation for collection id %d is not finished yet !!!!!!' % collection_id)
        return 0
    relevant_tweets_number = int(os.environ.get('NUM_RELEVANT_TWEETS', 5))
    relevant_tweets = MostRelevantTweets(relevant_tweets_number, initial=initial_relevant_tweets)
    max_collected_tweetid = 0
    max_annotated_tweetid = 0
    max_geotagged_tweetid = 0
    last_timestamp_start = timestamp_start or datetime(2100, 12, 30)
    last_timestamp_end = timestamp_end or datetime(1970, 1, 1)
    last_tweetid_collected = int(last_tweetid_collected) if last_tweetid_collected else 0
    last_tweetid_annotated = int(last_tweetid_annotated) if last_tweetid_annotated else 0
    last_tweetid_geotagged = int(last_tweetid_geotagged) if last_tweetid_geotagged else 0

    # init counter
    counter = Counter(initial_values)

    logger.info(' >>>>>>>>>>>> Starting aggregation for collection id %d' % collection_id)

    running_aggregators.add(collection_id)

    try:
        collected_tweets = Tweet.get_iterator(collection_id, 'collected', last_tweetid=last_tweetid_collected)
        for t in collected_tweets:
            max_collected_tweetid = max(max_collected_tweetid, t.tweet_id)
            last_timestamp_start = min(last_timestamp_start, t.created_at)
            last_timestamp_end = max(last_timestamp_end, t.created_at)
            counter['collected'] += 1

        annotated_tweets = Tweet.get_iterator(collection_id, 'annotated', last_tweetid=last_tweetid_annotated)
        for t in annotated_tweets:
            max_annotated_tweetid = max(max_annotated_tweetid, t.tweet_id)
            counter['annotated'] += 1
            inc_annotated_counter(counter, t.annotations['flood_probability'][1])
            relevant_tweets.push_if_relevant(Tweet.to_json(t))

        geotagged_tweets = Tweet.get_iterator(collection_id, 'geotagged', last_tweetid=last_tweetid_geotagged)
        for t in geotagged_tweets:
            max_geotagged_tweetid = max(max_geotagged_tweetid, t.tweet_id)
            counter['geotagged'] += 1
            efas_id = t.geo.get('efas_id')
            nuts_id = t.geo.get('nuts_id', 'N/A')
            inc_annotated_counter(counter, t.annotations['flood_probability'][1], nuts2='%s_%s' % (efas_id, nuts_id))
    except cassandra.ReadFailure as e:
        logger.error('Cassandra Read failure: %s', e)
        running_aggregators.remove(collection_id)
        return 1
    except Exception as e:
        logger.error('An error occurred: %s %s', type(e), e)
        running_aggregators.remove(collection_id)
        return 1

    with flask_app.app_context():
        aggregation = Aggregation.query.filter_by(collection_id=collection_id).first()
        aggregation.last_tweetid_collected = max_collected_tweetid if max_collected_tweetid else last_tweetid_collected
        aggregation.last_tweetid_annotated = max_annotated_tweetid if max_annotated_tweetid else last_tweetid_annotated
        aggregation.last_tweetid_geotagged = max_geotagged_tweetid if max_geotagged_tweetid else last_tweetid_geotagged
        # if timestamp_start/end were none, last_timestamp_start contains an
        # invalid timestamp for MySQL (timestamp in the future) so we store a NULL value
        aggregation.timestamp_start = last_timestamp_start if last_timestamp_start != datetime(2100, 12, 30) else None
        aggregation.timestamp_end = last_timestamp_end if last_timestamp_end != datetime(1970, 1, 1) else None
        aggregation.values = dict(counter)
        aggregation.relevant_tweets = relevant_tweets.values
        aggregation.save()
        running_aggregators.remove(collection_id)

    logger.info(' <<<<<<<<<<< Aggregation terminated for collection %d: %s', collection_id, str(aggregation.values))
    return 0


def pretty_running_conf(conf):
    for k, v in vars(conf).items():
        if k != 'collections' and v:
            return 'Aggregation on {} collections'.format(k)
        elif k == 'collections' and v:
            return 'Aggregation on collections: {}'.format(v)
