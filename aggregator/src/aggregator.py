import os
import logging
import functools
from collections import Counter, defaultdict, Iterable
from datetime import datetime
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

import cassandra

from smfrcore.utils import logged_job, job_exceptions_catcher, FALSE_VALUES, DEFAULT_HANDLER
from smfrcore.models.sql import TwitterCollection, Aggregation, create_app

logger = logging.getLogger('AGGREGATOR')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)

logging.getLogger('cassandra').setLevel(logging.ERROR)

flask_app = create_app()

flood_propability_ranges_env = os.getenv('FLOOD_PROBABILITY_RANGES', '0-10,10-90,90-100')
flood_propability_ranges = [[int(g) for g in t.split('-')] for t in flood_propability_ranges_env.split(',')]
orange_th = flood_propability_ranges[-2][0]
red_th = flood_propability_ranges[-1][0]

running_aggregators = set()


class MostRelevantTweets:
    min_relevant_probability = int(os.getenv('MIN_RELEVANT_FLOOD_PROBABILITY', 90)) / 100
    max_size = int(os.getenv('NUM_RELEVANT_TWEETS_AGGREGATED', 300))

    @classmethod
    def _sortkey(cls, t):
        return t['annotations']['flood_probability']['yes']

    def __init__(self, initial=None):
        self._tweets = defaultdict(list)
        if not initial:
            initial = {}
        for geocoded_id, relevant_tweets in initial.items():
            self._tweets[geocoded_id] = relevant_tweets

    @property
    def values(self):
        for key in self._tweets.keys():
            self._tweets[key] = sorted(self._tweets[key], key=self._sortkey, reverse=True)
            self._tweets[key] = self._tweets[key][:self.max_size]
        return self._tweets

    def is_relevant(self, item):
        flood_prob = item['annotations']['flood_probability']['yes']
        return flood_prob >= self.min_relevant_probability and (item['geo']['nuts_efas_id'] or (item['geo']['is_european'] not in FALSE_VALUES))

    def push_if_relevant(self, item):
        """

        :param item: a dict representing a smfrcore.models.cassandra.Tweet object
        :return:
        """
        if self.is_relevant(item):
            key = item['geo']['nuts_efas_id'] or 'G%s' % (item['geo']['geonameid'] or '-')
            self._tweets[key].append(item)


@logged_job
@job_exceptions_catcher
def aggregate(running_conf=None):
    """

    :param running_conf: ArgumentParser object with running, all and background attributes
    :return:
    """
    if not running_conf:
        raise ValueError('No parsed arguments were passed')

    with flask_app.app_context():

        collections = find_collections_to_aggregate(running_conf)

        if not collections:
            logger.info('No collections to aggregate with configuration: %s', pretty_running_conf(running_conf))
            return
        if not isinstance(collections, Iterable):
            collections = [collections]

        aggregations_args = []
        for c in collections:
            aggregation = c.aggregation or Aggregation.get_by_collection(c.id)

            if not aggregation:
                # init aggregation object
                aggregation = Aggregation(collection_id=c.id, values={}, relevant_tweets={}, trends={})
                aggregation.save()

            aggregations_args.append(
                (c.id,
                 aggregation.last_tweetid_collected,
                 aggregation.last_tweetid_annotated,
                 aggregation.last_tweetid_geotagged,
                 aggregation.timestamp_start,
                 aggregation.timestamp_end,
                 aggregation.values,
                 aggregation.relevant_tweets,
                 aggregation.trends,
                 aggregation)
            )

        # cpu_count() - 1 aggregation threads running at same time
        with ThreadPool(cpu_count() - 1) as p:
            p.starmap(run_single_aggregation, aggregations_args)


def find_collections_to_aggregate(running_conf):
    if running_conf.running:
        collections_to_aggregate = TwitterCollection.get_active()
    elif running_conf.all:
        collections_to_aggregate = TwitterCollection.query.all()
    elif running_conf.background:
        collections_to_aggregate = TwitterCollection.get_active_background()
    elif running_conf.collections:
        collections_to_aggregate = TwitterCollection.get_collections(running_conf.collections)
    else:
        raise ValueError('Aggregator must be started with a parameter: '
                         '[-c id1,...,idN | -r (running)| -a (all) | -b (background)]')
    return collections_to_aggregate


def flood_probability(t):
    return t.annotations['flood_probability'][1] * 100


def inc_annotated_counter(counter, probability, place_id=None):
    """

    :param counter: Counter object
    :param probability: flood  probability * 100
    :param place_id: string composed like <efas_id>_<nuts_id>. It's present only for geotagged tweets
    """
    key = functools.partial('num_tweets_{}-{}'.format) if place_id is None else functools.partial('{}_num_tweets_{}-{}'.format)
    for range_a, range_b in flood_propability_ranges:
        counter_key = key(range_a, range_b) if place_id is None else key(place_id, range_a, range_b)
        if counter_key not in counter:
            counter[counter_key] = 0
        if range_a < probability <= range_b:
            counter[counter_key] += 1
            if place_id:
                geotagged_counter_key = 'geotagged_{}-{}'.format(range_a, range_b)
                counter[geotagged_counter_key] += 1


def run_single_aggregation(collection_id,
                           last_tweetid_collected, last_tweetid_annotated, last_tweetid_geotagged,
                           timestamp_start, timestamp_end,
                           initial_values, initial_relevant_tweets, initial_trends,
                           aggregation=None):
    """
    Calculate current stats for a collection

    :param collection_id:
    :param last_tweetid_collected:
    :param last_tweetid_annotated:
    :param last_tweetid_geotagged:
    :param timestamp_end:
    :param timestamp_start:
    :param initial_values: counters dictionary from last execution
    :param initial_relevant_tweets: relevant tweets from last execution
    :param initial_trends:
    :param aggregation: Aggregation object
    :return:
    """
    from smfrcore.models.cassandra import Tweet
    res = 0
    if collection_id in running_aggregators:
        logger.warning('!!!!!! Previous aggregation for %d is not finished yet !!!!!!' % collection_id)
        return res

    logger.info(' >>>>>>>>>>>> Starting aggregation - %d' % collection_id)
    with flask_app.app_context():
        collection = TwitterCollection.get_collection(collection_id)
        aggregation = collection.aggregation
        if not aggregation:
            aggregation = Aggregation.query.filter_by(collection_id=collection_id).first()

        max_collected_tweetid = 0
        max_annotated_tweetid = 0
        max_geotagged_tweetid = 0

        last_timestamp_start = timestamp_start or datetime(2100, 12, 30)
        last_timestamp_end = timestamp_end or datetime(1970, 1, 1)

        last_tweetid_collected = int(last_tweetid_collected) if last_tweetid_collected else 0
        last_tweetid_annotated = int(last_tweetid_annotated) if last_tweetid_annotated else 0
        last_tweetid_geotagged = int(last_tweetid_geotagged) if last_tweetid_geotagged else 0

        # init counters
        relevant_tweets = MostRelevantTweets(initial=initial_relevant_tweets)
        counter = Counter(initial_values)
        trends = Counter(initial_trends)
        running_aggregators.add(collection_id)
        try:
            logger.info('[+] Counting collected for %d', collection_id)
            collected_tweets = Tweet.get_iterator(collection_id, Tweet.COLLECTED_TYPE, last_tweetid=last_tweetid_collected)
            for t in collected_tweets:
                max_collected_tweetid = max(max_collected_tweetid, t.tweet_id)
                last_timestamp_start = min(last_timestamp_start, t.created_at)
                last_timestamp_end = max(last_timestamp_end, t.created_at)
                counter['collected'] += 1
                counter['{}_collected'.format(t.lang)] += 1
            aggregation.last_tweetid_collected = max_collected_tweetid if max_collected_tweetid else last_tweetid_collected
            aggregation.timestamp_start = last_timestamp_start if last_timestamp_start != datetime(2100, 12, 30) else None
            aggregation.timestamp_end = last_timestamp_end if last_timestamp_end != datetime(1970, 1, 1) else None
            aggregation.values = dict(counter)
            aggregation.save()

            annotated_tweets = Tweet.get_iterator(collection_id, Tweet.ANNOTATED_TYPE, last_tweetid=last_tweetid_annotated)
            logger.info('[+] Counting annotated for %d', collection_id)
            for t in annotated_tweets:
                max_annotated_tweetid = max(max_annotated_tweetid, t.tweet_id)
                counter['annotated'] += 1
                counter['{}_annotated'.format(t.lang)] += 1
                inc_annotated_counter(counter, flood_probability(t))
            aggregation.last_tweetid_annotated = max_annotated_tweetid if max_annotated_tweetid else last_tweetid_annotated
            aggregation.values = dict(counter)
            aggregation.save()

            geotagged_tweets = Tweet.get_iterator(collection_id, Tweet.GEOTAGGED_TYPE, last_tweetid=last_tweetid_geotagged)
            logger.info('[+] Counting geotagged for %d', collection_id)
            for t in geotagged_tweets:
                max_geotagged_tweetid = max(max_geotagged_tweetid, t.tweet_id)
                if not t.geo:
                    continue
                prob = flood_probability(t)
                counter['geotagged'] += 1
                counter['{}_geotagged'.format(t.lang)] += 1
                geoloc_id = t.geo['nuts_efas_id'] or 'G%s' % (t.geo['geonameid'] or '-')
                nuts_id = t.geo['nuts_id']
                geo_identifier = '%s_%s' % (geoloc_id, nuts_id) if nuts_id else geoloc_id
                inc_annotated_counter(counter, prob, place_id=geo_identifier)
                if collection.trigger != TwitterCollection.TRIGGER_BACKGROUND:
                    relevant_tweets.push_if_relevant(Tweet.to_json(t))
                    if t.geo['nuts_efas_id'] and prob >= orange_th:
                        # trends....counters by efas cycle (i.e. date)
                        t = Tweet.to_obj(t)
                        relevance = 'medium'
                        if prob >= red_th:
                            relevance = 'high'
                        day_key = '{}_{}_{}'.format(t.geo['nuts_efas_id'], relevance, t.efas_cycle)
                        trends[day_key] += 1
            aggregation.last_tweetid_geotagged = max_geotagged_tweetid if max_geotagged_tweetid else last_tweetid_geotagged
            aggregation.values = dict(counter)
            if collection.trigger != TwitterCollection.TRIGGER_BACKGROUND:
                aggregation.relevant_tweets = relevant_tweets.values
                aggregation.trends = dict(trends)
            aggregation.save()

        except cassandra.ReadFailure as e:
            logger.error('Cassandra Read failure: %s', e)
            res = 1
        except Exception as e:
            logger.error('ERROR during aggregation collection %d: error: %s %s', collection_id, type(e), e)
            res = 1
        else:
            logger.info(' <<<<<<<<<<< Aggregation terminated - %d', collection_id)
            res = 0
        finally:
            running_aggregators.remove(collection_id)
        return res


def pretty_running_conf(conf):
    for k, v in vars(conf).items():
        if k != 'collections' and v:
            return 'Aggregation on {} collections'.format(k)
        elif k == 'collections' and v:
            return 'Aggregation on collections: {}'.format(v)
