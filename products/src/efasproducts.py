import os
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import ujson
import geojson
from geojson import Feature, FeatureCollection
from geojson.geometry import Geometry
import fiona
from Levenshtein import ratio

from smfrcore.models.sql import TwitterCollection, Aggregation, create_app
from smfrcore.utils import DEFAULT_HANDLER, RUNNING_IN_DOCKER
from smfrcore.text_utils import tweet_normalization_aggressive
from sqlalchemy import or_

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)

logging.getLogger('cassandra').setLevel(logging.ERROR)


class Products:
    """
    Products component implementation
    """
    config_folder = '/config' if RUNNING_IN_DOCKER else os.path.join(os.path.dirname(__file__), '../config')
    output_folder = '/output' if RUNNING_IN_DOCKER else os.path.join(os.path.dirname(__file__), '../output')

    template = os.path.join(config_folder, 'maptemplate.shp')
    output_filename_tpl = os.path.join(output_folder, 'SMFR_products_{}.geojson')
    out_crs = dict(type='EPSG', properties=dict(code=4326, coordinate_order=[1, 0]))

    RGB = {'red': '255 0 0', 'orange': '255 128 0', 'gray': '225 225 225'}
    high_prob_range = os.getenv('HIGH_PROB_RANGE', '90-100')
    low_prob_range = os.getenv('LOW_PROB_RANGE', '0-10')

    # this variable reflects the following heuristic:
    # GREEN - less than 10 high relevant tweets
    # ORANGE - num of high rel > 5 * num low rel
    # RED - num of high rel > 9 * num low rel
    alert_heuristic = os.getenv('THRESHOLDS', '10:5:9')
    max_relevant_tweets = int(os.getenv('NUM_RELEVANT_TWEETS_PRODUCTS', 5))

    @classmethod
    def log_config(cls):
        heuristics = cls.alert_heuristic.split(':')
        logger.info('=================================')
        logger.info('Products configuration:')
        logger.info('High Probability range %s', cls.high_prob_range)
        logger.info('Low Probability range %s', cls.low_prob_range)
        logger.info('Alert thresholds: ')
        logger.info('Gray: less than %s relevant tweets or '
                    'Num of high prob. relevant tweets <= %s Num of low prob. relevant tweets',
                    heuristics[0], heuristics[1])
        logger.info('Orange: Num of high prob. relevant tweets > %s Num of low prob. relevant tweets', heuristics[1])
        logger.info('Red: Num of high prob. relevant tweets > %s Num of low prob. relevant tweets', heuristics[2])
        logger.info('=================================')

    @classmethod
    def produce(cls):
        # create products for on-demand active colletions or recently stopped collections
        app = create_app()

        with app.app_context():
            collections = TwitterCollection.query.filter(
                TwitterCollection.trigger == TwitterCollection.TRIGGER_ONDEMAND).filter(
                or_(
                    TwitterCollection.status == 'active',
                    TwitterCollection.stopped_at >= datetime.now() - timedelta(days=2)
                )
            )
            aggregations = Aggregation.query.filter(Aggregation.collection_id.in_([c.id for c in collections])).all()
            counters = defaultdict(int)
            relevant_tweets_aggregated = defaultdict(list)

            # TODO it's a mapreduce procedure...it can be parallelized somehow
            for aggregation in aggregations:

                values = aggregation.values
                for key, value in values.items():
                    if not cls.is_efas_id_counter(key):
                        continue
                    counters[key] += value

                relevant_tweets = aggregation.relevant_tweets
                for key in relevant_tweets.keys():
                    if not cls.is_efas_id(key):
                        continue
                    relevant_tweets_aggregated[key] += relevant_tweets[key]

        counters_by_efas_id = defaultdict(defaultdict)
        for key, value in counters.items():
            # key format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
            # key is like "1301_UKF2_num_tweets_0-10" or "1301_num_tweets_0-10" (in case nuts_id is not present)
            tokens = key.split('_')
            efas_id = tokens[0]
            probs_interval = tokens[-1]
            counters_by_efas_id[efas_id][probs_interval] = value

        for efas_id, tweets in relevant_tweets_aggregated.items():
            relevant_tweets_aggregated[efas_id] = TweetsDeduplicator.deduplicate(relevant_tweets_aggregated[efas_id])[:cls.max_relevant_tweets]

        geojson_output_filename = cls.output_filename_tpl.format(datetime.now().strftime('%Y%m%d%H%M'))
        logger.info('<<<<<< Producing %s', geojson_output_filename)
        with fiona.open(cls.template) as source:
            with open(geojson_output_filename, 'w') as sink:
                out_data = []
                for feat in source:
                    efas_id = feat['id']
                    risk_color = cls.determine_color(counters_by_efas_id[efas_id])
                    if risk_color == cls.RGB['gray'] and not relevant_tweets_aggregated[efas_id]:
                        continue
                    geom = Geometry(
                        coordinates=feat['geometry']['coordinates'],
                        type=feat['geometry']['type'],
                        # crs=cls.out_crs,
                    )
                    out_data.append(Feature(geometry=geom, properties={
                        'efas_id': efas_id,
                        'risk_color': cls.determine_color(counters_by_efas_id[efas_id]),
                        'counters': counters_by_efas_id[efas_id],
                        'relevant_tweets': relevant_tweets_aggregated[efas_id]
                    }))

                geojson.dump(FeatureCollection(out_data), sink, sort_keys=True, indent=2)
        logger.info('>>>>>> Wrote %s', geojson_output_filename)

    @classmethod
    def is_efas_id_counter(cls, key):
        # good key is like "1301_UKF2_num_tweets_0-10"
        # format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
        tokens = key.split('_')
        return cls.is_efas_id(tokens[0])

    @classmethod
    def determine_color(cls, counters):
        heuristics = list(map(int, cls.alert_heuristic.split(':')))
        gray_th = heuristics[0]
        orange_th = heuristics[1]
        red_th = heuristics[2]
        if counters.get(cls.high_prob_range, 0) < gray_th or counters.get(cls.high_prob_range, 0) <= orange_th * counters.get(cls.low_prob_range, 0):
            color = 'gray'
        elif orange_th * counters.get(cls.low_prob_range, 0) < counters.get(cls.high_prob_range, 0) <= red_th * counters.get(cls.low_prob_range, 0):
            color = 'orange'
        else:
            color = 'red'
        return cls.RGB[color]

    @classmethod
    def is_efas_id(cls, key):
        try:
            int(key)
            return True
        except ValueError:
            return False


class TweetsDeduplicator:
    # A threshold of predicted probabily under which
    # edit distance is checked (to discard duplicates)
    SIMILAR_PREDICTION_TRIGGER_EDIT_DISTANCE_CHECK = 0.0001

    # A threshold for edit distance; this is applied twice:
    # 1. For pairs of tweets with predictions within SIMILAR_PREDICTION_TRIGGER_EDIT_DISTANCE_CHECK
    # 2. For all pairs of the top MAX_TWEETS_CENTRALITY tweets
    SIMILAR_PREDICTION_EDIT_DISTANCE_MAX = 0.8

    # Tweets for centrality computation (cost is quadratic on this number, so stay small)
    MAX_TWEETS_CENTRALITY = 100

    @classmethod
    def deduplicate(cls, tweets):
        ids = []
        deduplicated = []
        for t in tweets:
            if t['tweetid'] not in ids:
                t['label_predicted'] = t['annotations']['flood_probability']['yes']
                t['tweet'] = ujson.loads(t['tweet'])
                t['_normalized_text'] = tweet_normalization_aggressive(t['tweet']['text'])
                deduplicated.append(t)
                ids.append(t['tweetid'])

        is_duplicate = {}
        multiplicity = defaultdict(int)
        for tu in deduplicated:
            for tv in deduplicated:
                if abs(tu['label_predicted'] - tv['label_predicted']) < cls.SIMILAR_PREDICTION_TRIGGER_EDIT_DISTANCE_CHECK:
                    normalized_edit_similarity = ratio(tu['_normalized_text'], tv['_normalized_text'])
                    if normalized_edit_similarity > cls.SIMILAR_PREDICTION_EDIT_DISTANCE_MAX:

                        # The newer tweet (larger id) is marked as a duplicate of the older (smaller id) tweet
                        # Count the == in case there are duplicate ids in the
                        # REMEMBER: tweet_id is an integer (same as t['tweet']['id']) while tweetid is a string (same as t['tweet']['id_str'])
                        if tu['tweet_id'] < tv['tweet_id']:
                            is_duplicate[tv['tweetid']] = tu['tweetid']
                            multiplicity[tu['tweetid']] = multiplicity[tu['tweetid']] + 1
        # Remove duplicates
        tweets_unique = [tweet for tweet in deduplicated if tweet['tweetid'] not in is_duplicate]
        # Add multiplicity
        for tweet in tweets_unique:
            tweet['_multiplicity'] = multiplicity[tweet['tweetid']] or 1

        # Create set for second pass (centrality)
        centrality = defaultdict(float)
        for tu in tweets_unique:
            for tv in tweets_unique:
                normalized_edit_similarity = ratio(tu['_normalized_text'], tv['_normalized_text'])

                # Compute centrality as sum of similarities
                centrality[tu['tweetid']] = centrality[tu['tweetid']] + normalized_edit_similarity
                centrality[tv['tweetid']] = centrality[tv['tweetid']] + normalized_edit_similarity

                # Discard duplicates
                if normalized_edit_similarity > cls.SIMILAR_PREDICTION_EDIT_DISTANCE_MAX:
                    if tu['tweet_id'] < tv['tweet_id']:
                        is_duplicate[tv['tweetid']] = tu['tweetid']

        # Add centrality and mark centrality=0.0 for duplicates
        for tweet in tweets_unique:
            if not tweet['tweetid'] in is_duplicate:
                tweet['_centrality'] = centrality[tweet['tweetid']]
            else:
                tweet['_centrality'] = 0.0

        # Sort by multiplicity and probability of being relevant
        tweets_sorted = sorted(tweets_unique, key=lambda x: x['label_predicted'] * x['_multiplicity'] * x['_centrality'], reverse=True)

        return tweets_sorted
