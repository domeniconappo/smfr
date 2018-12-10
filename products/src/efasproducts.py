import os
import logging
from collections import defaultdict
from datetime import datetime

import ujson
import geojson
from geojson import Feature, FeatureCollection
from geojson.geometry import Geometry
import fiona
from Levenshtein import ratio

from smfrcore.models.sql import TwitterCollection, Aggregation, Nuts2, Product, create_app
from smfrcore.utils import DEFAULT_HANDLER, IN_DOCKER, RGB
from smfrcore.client.api_here import HereClient
from smfrcore.client.ftp import SFTPClient
from smfrcore.utils.text import tweet_normalization_aggressive

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)

logging.getLogger('cassandra').setLevel(logging.ERROR)

# FTP client for KAJO server
server = os.getenv('KAJO_FTP_SERVER', '207.180.226.197')
user = os.getenv('KAJO_FTP_USER', 'jrc')
password = os.getenv('KAJO_FTP_PASSWORD')
folder = os.getenv('KAJO_FTP_FOLDER')
DEVELOPMENT = bool(int(os.getenv('DEVELOPMENT', 0)))


class Products:
    """
    Products component implementation
    """
    config_folder = '/config' if IN_DOCKER else os.path.join(os.path.dirname(__file__), '../config')
    output_folder = '/output' if IN_DOCKER else os.path.join(os.path.dirname(__file__), '../output')

    template = os.path.join(config_folder, 'maptemplate.shp')
    output_filename_tpl = os.path.join(output_folder, 'SMFR_products_{}.geojson')
    output_heatmap_filename_tpl = os.path.join(output_folder, 'heatmaps/SMFR_heatmaps_{}.geojson')
    output_incidents_filename_tpl = os.path.join(output_folder, 'incidents/SMFR_incidents_{}.geojson')
    output_relevant_tweets_filename_tpl = os.path.join(output_folder, 'tweets/SMFR_tweets_{}.geojson')
    out_crs = dict(type='EPSG', properties=dict(code=4326, coordinate_order=[1, 0]))
    high_prob_range = os.getenv('HIGH_PROB_RANGE', '90-100')
    low_prob_range = os.getenv('LOW_PROB_RANGE', '0-10')

    # this variable reflects the following heuristic (e.g. considering default 10:5:9):
    # GREEN - less than 10 high relevant tweets
    # ORANGE - num of high rel > 5 * num low rel
    # RED - num of high rel > 9 * num low rel
    alert_heuristic = os.getenv('THRESHOLDS', '10:5:9')
    max_relevant_tweets = int(os.getenv('NUM_RELEVANT_TWEETS_PRODUCTS', 5))

    # here api
    here_client = HereClient()
    app = create_app()

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
    def produce(cls, efas_cycle='12'):
        # create products for on-demand active collections or recently stopped collections
        with cls.app.app_context():
            collections = TwitterCollection.get_active_ondemand()
            collection_ids = [c.id for c in collections]
            aggregations = Aggregation.query.filter(Aggregation.collection_id.in_(collection_ids)).all()
            counters = defaultdict(int)
            relevant_tweets_aggregated = defaultdict(list)

            for aggregation in aggregations:
                collection_id = aggregation.collection_id
                for key, value in aggregation.values.items():
                    if not cls.is_efas_id_counter(key) or not value:
                        continue
                    counters['{}@{}'.format(collection_id, key)] += value

                for key, tweets in aggregation.relevant_tweets.items():
                    if not cls.is_efas_id(key) or not tweets:
                        continue
                    relevant_tweets_aggregated[key] += tweets

        relevant_tweets_output = {}
        for efas_id, tweets in relevant_tweets_aggregated.items():
            deduplicated_tweets = TweetsDeduplicator.deduplicate(tweets)[:cls.max_relevant_tweets]
            if not deduplicated_tweets:
                continue
            relevant_tweets_output[efas_id] = deduplicated_tweets

        counters_by_efas_id = defaultdict(defaultdict)
        for key, value in counters.items():
            collection_id, key = key.split('@')
            # key format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
            # key is like "1301_UKF2_num_tweets_0-10" or "1301_num_tweets_0-10" (in case nuts_id is not present)
            tokens = key.split('_')
            efas_id = tokens[0]
            probs_interval = tokens[-1]
            counters_by_efas_id[efas_id][probs_interval] = value
            counters_by_efas_id[efas_id]['collection_id'] = collection_id
        counters_by_efas_id_output = {k: v for k, v in counters_by_efas_id.items() if v}

        heatmap_file = cls.write_heatmap_geojson(counters_by_efas_id_output, efas_cycle)
        relevant_tweets_file = cls.write_relevant_tweets_geojson(relevant_tweets_output, efas_cycle)
        if not DEVELOPMENT:
            ftp_client = SFTPClient(server, user, password, folder)
            ftp_client.send(heatmap_file)
            ftp_client.send(relevant_tweets_file)
            ftp_client.close()
            logger.info('[OK] Pushed files %s to SFTP %s', [heatmap_file, relevant_tweets_file], server)
        cls.write_incidents_geojson(counters_by_efas_id_output)
        cls.write_to_sql(counters_by_efas_id_output, relevant_tweets_output, collection_ids)

    @classmethod
    def write_to_sql(cls, counters_by_efas_id, relevant_tweets_aggregated, collection_ids):
        heuristics = list(map(int, cls.alert_heuristic.split(':')))
        gray_th = heuristics[0]
        orange_th = heuristics[1]
        red_th = heuristics[2]
        with cls.app.app_context():
            highlights = {}
            for efas_id, counters in counters_by_efas_id.items():
                if not (counters.get(cls.high_prob_range, 0) < gray_th or counters.get(cls.high_prob_range, 0) <= orange_th * counters.get(cls.low_prob_range, 0)) or orange_th * counters.get(cls.low_prob_range, 0) < counters.get(cls.high_prob_range, 0) <= red_th * counters.get(cls.low_prob_range, 0):
                    highlights[efas_id] = counters
            product = Product(aggregated=counters_by_efas_id, relevant_tweets=relevant_tweets_aggregated,
                              highlights=highlights, collection_ids=collection_ids)
            product.save()

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
        return RGB[color]

    @classmethod
    def is_efas_id(cls, key):
        try:
            int(key)
            return True
        except ValueError:
            return False

    @classmethod
    def get_incidents(cls, efas_id):
        """

        :param efas_id:
        :return: list of items
        An item is a dictionary:
        {
            'traffic_item_id': item['TRAFFIC_ITEM_ID'],
            'start_date': item['START_TIME'],
            'end_date': item['END_TIME'],
            'lat': item['GEOLOC']['ORIGIN']['LATITUDE'],
            'lon': item['GEOLOC']['ORIGIN']['LONGITUDE'],
            'text': 'Flooding incident: from {} to {}. Severity: {}'.format(
                item['START_TIME'], item['END_TIME'], item['CRITICALITY'].get('DESCRIPTION', 'minor'))
            ,
            'risk_color': self._risk_color(item['CRITICALITY'])
        }
        """
        bbox = Nuts2.efas_id_bbox(efas_id)
        if not bbox:
            return []
        bbox_for_here = '{max_lat},{min_lon};{min_lat},{max_lon}'.format(
            max_lat=bbox['max_lat'], min_lon=bbox['min_lon'], min_lat=bbox['min_lat'], max_lon=bbox['max_lon']
        )
        return cls.here_client.get_by_bbox(bbox_for_here)

    @classmethod
    def write_heatmap_geojson(cls, counters_by_efas_id, efas_cycle):
        geojson_output_filename = cls.output_heatmap_filename_tpl.format('{}{}'.format(datetime.now().strftime('%Y%m%d'), efas_cycle))
        logger.info('<<<<<< Writing %s', geojson_output_filename)
        with cls.app.app_context():
            with fiona.open(cls.template) as source:
                with open(geojson_output_filename, 'w') as sink:
                    out_data = []
                    for feat in source:
                        efas_id = feat['id']
                        if efas_id not in counters_by_efas_id:
                            continue
                        collection_id = counters_by_efas_id[efas_id]['collection_id']
                        collection = TwitterCollection.get_collection(collection_id)
                        properties = feat['properties']
                        efas_name = properties['EFAS_name']
                        risk_color = cls.determine_color(counters_by_efas_id[efas_id])
                        geom = Geometry(
                            coordinates=feat['geometry']['coordinates'],
                            type=feat['geometry']['type'],
                        )
                        out_data.append(Feature(geometry=geom, properties={
                            'collection_id': collection_id,
                            'efas_trigger': collection.forecast_id,
                            'efas_id': efas_id,
                            'efas_name': efas_name,
                            'risk_color': risk_color,
                            'counters': counters_by_efas_id[efas_id],
                            'type': 'heatmap',
                        }))
                    geojson.dump(FeatureCollection(out_data), sink, sort_keys=True, indent=2)
        logger.info('>>>>>> Wrote %s', geojson_output_filename)
        return geojson_output_filename

    @classmethod
    def write_incidents_geojson(cls, counters_by_efas_id, efas_cycle):
        geojson_output_filename = cls.output_incidents_filename_tpl.format('{}{}'.format(datetime.now().strftime('%Y%m%d'), efas_cycle))
        logger.info('<<<<<< Writing %s', geojson_output_filename)
        with cls.app.app_context():
            with fiona.open(cls.template) as source:
                with open(geojson_output_filename, 'w') as sink:
                    out_data = []
                    for feat in source:
                        efas_id = feat['id']
                        if efas_id not in counters_by_efas_id:
                            continue
                        incidents = cls.get_incidents(efas_id)
                        if not incidents:
                            continue
                        for inc in incidents:
                            geom = Geometry(
                                coordinates=[inc['lon'], inc['lat']],
                                type='Point',
                            )
                            out_data.append(Feature(geometry=geom, properties={
                                'efas_id': efas_id,
                                'incident': inc,
                                'type': 'incident',
                            }))
                    geojson.dump(FeatureCollection(out_data), sink, sort_keys=True, indent=2)
        logger.info('>>>>>> Wrote %s', geojson_output_filename)
        return geojson_output_filename

    @classmethod
    def write_relevant_tweets_geojson(cls, relevant_tweets, efas_cycle):
        geojson_output_filename = cls.output_relevant_tweets_filename_tpl.format('{}{}'.format(datetime.now().strftime('%Y%m%d'), efas_cycle))
        logger.info('<<<<<< Writing %s', geojson_output_filename)
        with fiona.open(cls.template) as source:
            with open(geojson_output_filename, 'w') as sink:
                out_data = []
                for feat in source:
                    efas_id = feat['id']
                    if efas_id not in relevant_tweets:
                        continue
                    efas_nuts2 = Nuts2.get_by_efas_id(efas_id)
                    efas_name = efas_nuts2.efas_name
                    for tweet in relevant_tweets[efas_id]:
                        geom = Geometry(
                            coordinates=[tweet['latlong'][1], tweet['latlong'][0]],
                            type='Point',
                        )
                        out_data.append(Feature(geometry=geom, properties={
                            'tweet_id': tweet['tweetid'],
                            'creation_time': tweet['created_at'],
                            'collection_id': tweet.collectionid,
                            'efas_trigger': tweet.collection.efas_id,
                            'efas_id': efas_id,
                            'efas_name': efas_name,
                            'prediction': tweet['label_predicted'],
                            'centrality': tweet['_centrality'],
                            'multiplicity': tweet['_multiplicity'],
                            'reprindex': tweet['representativeness'],
                            'text': tweet.get('_normalized_text') or tweet.get('full_text') or tweet['tweet'].get('text', ''),
                            'tweet': tweet['tweet'],
                            'type': 'tweet',
                        }))
                geojson.dump(FeatureCollection(out_data), sink, sort_keys=True, indent=2)
        logger.info('>>>>>> Wrote %s', geojson_output_filename)
        return geojson_output_filename

    @classmethod
    def makedirs(cls):
        for d in ('heatmaps', 'tweets', 'incidents'):
            maked = os.path.join(cls.output_folder, d)
            os.makedirs(maked, exist_ok=True)


class TweetsDeduplicator:
    # A threshold of predicted probabily under which
    # edit distance is checked (to discard duplicates)
    SIMILAR_PREDICTION_TRIGGER_DISTANCE_CHECK = 0.0001

    # A threshold for edit distance; this is applied twice:
    # 1. For pairs of tweets with predictions within SIMILAR_PREDICTION_TRIGGER_EDIT_DISTANCE_CHECK
    # 2. For all pairs of the top MAX_TWEETS_CENTRALITY tweets
    SIMILAR_PREDICTION_DISTANCE_MAX = 0.8

    # Tweets for centrality computation (cost is quadratic on this number, so stay small)
    MAX_TWEETS_CENTRALITY = 100

    @classmethod
    def deduplicate(cls, tweets):
        if not tweets:
            return []
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
                if abs(tu['label_predicted'] - tv['label_predicted']) < cls.SIMILAR_PREDICTION_TRIGGER_DISTANCE_CHECK:
                    normalized_edit_similarity = ratio(tu['_normalized_text'], tv['_normalized_text'])
                    if normalized_edit_similarity > cls.SIMILAR_PREDICTION_DISTANCE_MAX:

                        # The newer tweet (larger id) is marked as a duplicate of the older (smaller id) tweet
                        # Count the == in case there are duplicate ids in the
                        # REMEMBER: tweet_id is an integer (same as t['tweet']['id'])
                        # while tweetid is a string (same as t['tweet']['id_str'])
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
                if normalized_edit_similarity > cls.SIMILAR_PREDICTION_DISTANCE_MAX:
                    if tu['tweet_id'] < tv['tweet_id']:
                        is_duplicate[tv['tweetid']] = tu['tweetid']

        # Add centrality and mark centrality=0.0 for duplicates
        for tweet in tweets_unique:
            if not tweet['tweetid'] in is_duplicate:
                tweet['_centrality'] = centrality[tweet['tweetid']]
            else:
                tweet['_centrality'] = 0.0
            tweet['representativeness'] = tweet['label_predicted'] * tweet['_multiplicity'] * tweet['_centrality']

        # Sort by multiplicity and probability of being relevant
        tweets_sorted = sorted(tweets_unique, key=lambda x: x['representativeness'], reverse=True)
        return tweets_sorted
