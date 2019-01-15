import os
import logging
from collections import defaultdict, OrderedDict
import datetime

import ujson
from copy import deepcopy

import geojson
from geojson import Feature, FeatureCollection
from geojson.geometry import Geometry
import fiona
from Levenshtein import ratio

from smfrcore.models.sql import TwitterCollection, Aggregation, Nuts2, Product, create_app
from smfrcore.utils import DEFAULT_HANDLER, IN_DOCKER, RGB, IS_DEVELOPMENT
from smfrcore.client.api_here import HereClient
from smfrcore.client.ftp import SFTPClient
from smfrcore.utils.text import tweet_normalization_aggressive

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)
logger.propagate = False

logging.getLogger('cassandra').setLevel(logging.ERROR)
logging.getLogger('fiona').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('requests_oauthlib').setLevel(logging.ERROR)
logging.getLogger('paramiko').setLevel(logging.ERROR)
logging.getLogger('oauthlib').setLevel(logging.ERROR)

# FTP client for KAJO server
server = os.getenv('KAJO_FTP_SERVER', '207.180.226.197')
user = os.getenv('KAJO_FTP_USER', 'jrc')
password = os.getenv('KAJO_FTP_PASSWORD')
folder = os.getenv('KAJO_FTP_FOLDER', '/home/jrc')


class Products:
    """
    Products component implementation
    """
    config_folder = '/config' if IN_DOCKER else os.path.join(os.path.dirname(__file__), '../config')
    output_folder = '/output' if IN_DOCKER else os.path.join(os.path.dirname(__file__), '../output')

    template = os.path.join(config_folder, 'maptemplate.shp')
    output_heatmap_filename_tpl = os.path.join(output_folder, 'heatmaps/SMFR_heatmaps_{}.json')
    output_incidents_filename_tpl = os.path.join(output_folder, 'incidents/SMFR_incidents_{}.json')
    output_relevant_tweets_filename_tpl = os.path.join(output_folder, 'tweets/SMFR_tweets_{}.json')

    out_crs = dict(type='EPSG', properties=dict(code=4326, coordinate_order=[1, 0]))
    probability_ranges = os.getenv('FLOOD_PROBABILITY_RANGES', '0-10,10-80,80-100')
    low_prob_range, mid_prob_range, high_prob_range = probability_ranges.split(',')

    # This variable reflects the following heuristic (e.g. considering default 10:5:9)
    # GRAY   - Less than 10 high relevant tweets or #highrel < 1/9 * #midrel
    # ORANGE - 1/5 * #midrel > #highrel >= 1/9 * #midrel
    # RED    - 1/9 * #midrel > #highrel >= 1/5 * #midrel
    alert_heuristic = os.getenv('THRESHOLDS', '10:5:9')

    max_relevant_tweets = int(os.getenv('NUM_RELEVANT_TWEETS_PRODUCTS', 5))
    flood_indexes = {
        'gray': 'low',
        'orange': 'medium',
        'red': 'high',
    }

    # here api
    here_client = HereClient()
    app = create_app()

    with app.app_context():
        nuts2 = Nuts2.load_nuts()

    @classmethod
    def log_config(cls):
        heuristics = cls.alert_heuristic.split(':')
        logger.info('============================================================================')
        logger.info('Products configuration')
        logger.info('-----------------------')
        logger.info('High relevance probability range %s', cls.high_prob_range)
        logger.info('Medium relevance probability range %s', cls.mid_prob_range)
        logger.info('Low relevance probability range %s', cls.low_prob_range)
        logger.info('Alert thresholds: ')
        logger.info('Gray: less than %s relevant tweets or '
                    'Num of high prob. relevant tweets <= 1/%s Num of mid prob. relevant tweets',
                    heuristics[0], heuristics[2])
        logger.info('Orange: Num of high prob. relevant tweets > 1/%s Num of mid prob. relevant tweets', heuristics[2])
        logger.info('Red: Num of high prob. relevant tweets > 1/%s Num of mid prob. relevant tweets', heuristics[1])
        logger.info('============================================================================')

    @classmethod
    def produce(cls, forecast):
        # create products for on-demand active collections or recently stopped collections
        with cls.app.app_context():
            collections = TwitterCollection.get_active_ondemand()
            collections = {c.id: c for c in collections}

            collection_ids = collections.keys()
            aggregations = Aggregation.query.filter(Aggregation.collection_id.in_(collection_ids)).all()
            counters = defaultdict(int)
            relevant_tweets_aggregated = defaultdict(list)

            trends = defaultdict(defaultdict)

            for aggregation in aggregations:
                # FIXME now collections (and so aggregations) do not have "out of bbox" tweets
                # this means that "if not (cls.is_efas_id(key) and int(key) == efas_id)" tests are superfluos
                collection_id = aggregation.collection_id
                efas_id = collections[collection_id].efas_id

                for key, value in aggregation.values.items():
                    if not cls.is_efas_id_counter(key, efas_id):
                        continue
                    counters[key] = value

                for key, tweets in aggregation.relevant_tweets.items():

                    if not (cls.is_efas_id(key) and int(key) == efas_id and tweets):
                        continue
                    relevant_tweets_aggregated[int(key)] = tweets
                    break

                # Trends (see issue #19)
                for key, value in aggregation.trends.items():
                    # trends key is in the format <EFAS_ID>_[medium|high]_YYYYMMDD[00|12]
                    # eg. 502_high_20190111400
                    efas_id_tkn, relevance_tkn, efas_cycle_tkn = key.split('_')
                    if not (cls.is_efas_id(efas_id_tkn) and int(efas_id_tkn) == efas_id):
                        continue
                    efas_cycle = datetime.datetime.strptime(efas_cycle_tkn, '%Y%m%d%H')
                    if relevance_tkn not in trends[efas_id]:
                        trends[efas_id][relevance_tkn] = OrderedDict({efas_cycle: {'value': value, 'trend': '-'}})
                    else:
                        trends[efas_id][relevance_tkn].update({efas_cycle: {'value': value, 'trend': '-'}})

        relevant_tweets_output = {}
        for efas_id, tweets in relevant_tweets_aggregated.items():
            deduplicated_tweets = TweetsDeduplicator.deduplicate(tweets)[:cls.max_relevant_tweets]
            if not deduplicated_tweets:
                continue
            relevant_tweets_output[efas_id] = deduplicated_tweets

        counters_by_efas_id = defaultdict(defaultdict)
        for key, value in counters.items():
            # key format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
            # key is like "1301_UKF2_num_tweets_0-10" or "1301_num_tweets_0-10" (in case nuts_id is not present)
            tokens = key.split('_')
            efas_id = int(tokens[0])
            probs_interval = tokens[-1]
            counters_by_efas_id[efas_id][probs_interval] = value

        # calculate trends
        trends_copy = deepcopy(trends)
        for efas_id, collection_trends in trends_copy.items():
            for relevance, values_by_efas_cycles in collection_trends.items():
                # values_by_day is an OrderedDict
                for efas_cycle, value in values_by_efas_cycles.items():
                    previous_cycle = efas_cycle - datetime.timedelta(days=1)
                    if previous_cycle not in values_by_efas_cycles:
                        continue
                    previous_value = values_by_efas_cycles[previous_cycle]
                    trend_perc = 100 * (value - previous_value) / previous_value
                    trends[efas_id][relevance][efas_cycle]['trend'] = '{:+0.2}%'.format(trend_perc)

        heatmap_file = cls.write_heatmap_geojson(counters_by_efas_id, forecast, collections, relevant_tweets_output, trends)
        relevant_tweets_file = cls.write_relevant_tweets_geojson(relevant_tweets_output, forecast, collections)
        cls.push_products_to_sftp(heatmap_file, relevant_tweets_file)
        cls.write_incidents_geojson(counters_by_efas_id, forecast)
        cls.write_to_sql(counters_by_efas_id, relevant_tweets_output, collection_ids, forecast, trends)

    @classmethod
    def push_products_to_sftp(cls, heatmap_file, relevant_tweets_file):
        if not IS_DEVELOPMENT:
            ftp_client = SFTPClient(server, user, password, folder)
            ftp_client.send(heatmap_file)
            ftp_client.send(relevant_tweets_file)
            ftp_client.close()
            logger.info('[OK] Pushed files %s to SFTP %s', [heatmap_file, relevant_tweets_file], server)

    @classmethod
    def get_incidents(cls, efas_id):
        # TODO see issue #20
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
        nut = cls.nuts2.get(efas_id) or Nuts2.get_by_efas_id(efas_id)
        bbox = nut.bbox if nut else None
        if not bbox:
            return []
        bbox_for_here = '{max_lat},{min_lon};{min_lat},{max_lon}'.format(
            max_lat=bbox['max_lat'], min_lon=bbox['min_lon'], min_lat=bbox['min_lat'], max_lon=bbox['max_lon']
        )
        return cls.here_client.get_by_bbox(bbox_for_here)

    @classmethod
    def write_heatmap_geojson(cls, counters_by_efas_id, forecast_date, collections, relevant_tweets, trends):
        geojson_output_filename = cls.output_heatmap_filename_tpl.format(forecast_date)
        logger.info('<<<<<< Writing %s', geojson_output_filename)
        with cls.app.app_context():
            collection_ids_by_efas_id = {c.efas_id: c.id for c in collections.values()}
            with fiona.open(cls.template) as source:
                with open(geojson_output_filename, 'w') as sink:
                    out_data = []
                    for feat in source:
                        efas_id = int(feat['id'])
                        if efas_id not in counters_by_efas_id:
                            continue
                        collection_id = collection_ids_by_efas_id[efas_id]
                        collection = collections[collection_id]
                        efas_nuts2 = cls.nuts2.get(efas_id) or Nuts2.get_by_efas_id(efas_id)
                        efas_name = efas_nuts2.efas_name
                        color_str, risk_color = cls.apply_heuristic(counters_by_efas_id[efas_id])
                        flood_index = cls.flood_indexes[color_str]
                        stopdate = collection.runtime if collection.runtime and collection.is_active else collection.stopped_at
                        properties = {
                            'collection_id': collection_id,
                            'efas_trigger': collection.forecast_id,
                            'stopdate': stopdate.strftime('%Y%m%d') if stopdate else '',
                            'efas_id': efas_id,
                            'efas_name': efas_name,
                            'risk_color': risk_color,
                            'smfr_flood_index': flood_index,
                            'counters': counters_by_efas_id[efas_id],
                            'repr_tweets': cls.tweets_for_riskmap(relevant_tweets.get(efas_id, []), collection, efas_id) if flood_index != cls.flood_indexes['gray'] else [],
                            'type': 'heatmap',
                        }
                        geom = Geometry(
                            coordinates=feat['geometry']['coordinates'],
                            type=feat['geometry']['type'],
                        )
                        trends = trends[efas_id].get('high')
                        if trends:
                            properties.update({'trends': trends})
                        out_data.append(Feature(geometry=geom, properties=properties))
                    geojson.dump(FeatureCollection(out_data), sink, sort_keys=True, indent=2)
        logger.info('>>>>>> Wrote %s', geojson_output_filename)
        return geojson_output_filename

    @classmethod
    def write_relevant_tweets_geojson(cls, relevant_tweets, forecast_date, collections):
        geojson_output_filename = cls.output_relevant_tweets_filename_tpl.format(forecast_date)
        logger.info('<<<<<< Writing %s', geojson_output_filename)
        with cls.app.app_context():
            with fiona.open(cls.template) as source:
                with open(geojson_output_filename, 'w') as sink:
                    out_data = []
                    for feat in source:
                        efas_id = int(feat['id'])
                        if efas_id not in relevant_tweets:
                            continue
                        efas_nuts2 = cls.nuts2.get(efas_id) or Nuts2.get_by_efas_id(efas_id)
                        efas_name = efas_nuts2.efas_name
                        for tweet in relevant_tweets[efas_id]:
                            collection = collections.get(tweet['collectionid'])
                            if not collection:
                                continue
                            geom = Geometry(
                                coordinates=[tweet['latlong'][1], tweet['latlong'][0]],
                                type='Point',
                            )
                            out_data.append(Feature(geometry=geom, properties={
                                'tweet_id': tweet['tweetid'],
                                'creation_time': tweet['created_at'],
                                'collection_id': tweet['collectionid'],
                                'efas_trigger': collection.forecast_id,
                                'efas_id': efas_id,
                                'efas_name': efas_name,
                                'prediction': tweet['label_predicted'],
                                'centrality': tweet['_centrality'],
                                'multiplicity': tweet['_multiplicity'],
                                'reprindex': tweet['representativeness'],
                                'text': tweet.get('_normalized_text') or tweet.get('full_text') or tweet['tweet'].get('text', ''),
                                'type': 'tweet',
                            }))
                    geojson.dump(FeatureCollection(out_data), sink, sort_keys=True, indent=2)
        logger.info('>>>>>> Wrote %s', geojson_output_filename)
        return geojson_output_filename

    @classmethod
    def write_incidents_geojson(cls, counters_by_efas_id, forecast_date):
        geojson_output_filename = cls.output_incidents_filename_tpl.format(forecast_date)
        logger.info('<<<<<< Writing %s', geojson_output_filename)
        with cls.app.app_context():
            with fiona.open(cls.template) as source:
                with open(geojson_output_filename, 'w') as sink:
                    out_data = []
                    for feat in source:
                        efas_id = int(feat['id'])
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
    def makedirs(cls):
        for d in ('heatmaps', 'tweets', 'incidents'):
            maked = os.path.join(cls.output_folder, d)
            os.makedirs(maked, exist_ok=True)

    @classmethod
    def write_to_sql(cls, counters_by_efas_id, relevant_tweets_aggregated, collection_ids, forecast_date, trends):
        heuristics = list(map(int, cls.alert_heuristic.split(':')))
        gray_th = heuristics[0]
        orange_th = heuristics[1]
        with cls.app.app_context():
            highlights = {}
            for efas_id, counters in counters_by_efas_id.items():
                if not (counters.get(cls.high_prob_range, 0) < gray_th or
                        counters.get(cls.high_prob_range, 0) < 1/orange_th * counters.get(cls.mid_prob_range, 0)):
                    highlights[efas_id] = counters

            product = Product(aggregated=counters_by_efas_id, relevant_tweets=relevant_tweets_aggregated,
                              highlights=highlights, collection_ids=collection_ids, trends=trends, efas_cycle=forecast_date)
            product.save()

    @classmethod
    def is_efas_id_counter(cls, key, efas_id):
        # good key is like "1301_UKF2_num_tweets_0-10"
        # format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
        tokens = key.split('_')
        return cls.is_efas_id(tokens[0]) and int(tokens[0]) == efas_id

    @classmethod
    def apply_heuristic(cls, counters):
        """
        # GRAY   - Less than 10 high relevant tweets
        # ORANGE - 1/5 * #midrel > #highrel > 1/9 * #midrel
        # RED    - #highrel > 1/5 * #midrel
        """
        heuristics = list(map(int, cls.alert_heuristic.split(':')))
        gray_th = heuristics[0]  # 10
        red_th = heuristics[1]  # 5
        orange_th = heuristics[2]  # 9
        color = 'gray'
        if counters.get(cls.high_prob_range, 0) >= gray_th:
            if 1/orange_th * counters.get(cls.mid_prob_range, 0) <= counters.get(cls.high_prob_range, 0) < 1/red_th * counters.get(cls.mid_prob_range, 0):
                color = 'orange'
            elif counters.get(cls.high_prob_range, 0) >= 1/red_th * counters.get(cls.mid_prob_range, 0):
                color = 'red'
        return color, RGB[color]

    @classmethod
    def is_efas_id(cls, key):
        try:
            int(key)
            return True
        except ValueError:
            return False

    @classmethod
    def tweets_for_riskmap(cls, tweets, collection, efas_id):
        out = []
        efas_nuts2 = cls.nuts2.get(efas_id) or Nuts2.get_by_efas_id(efas_id)
        efas_name = efas_nuts2.efas_name
        for tweet in tweets:
            out.append({
                'tweet_id': tweet['tweetid'],
                'lat': tweet['latlong'][0],
                'lon': tweet['latlong'][1],
                'creation_time': tweet['created_at'],
                'collection_id': tweet['collectionid'],
                'efas_trigger': collection.forecast_id,
                'efas_id': efas_id,
                'efas_name': efas_name,
                'prediction': tweet['label_predicted'],
                'centrality': tweet['_centrality'],
                'multiplicity': tweet['_multiplicity'],
                'reprindex': tweet['representativeness'],
                'text': tweet.get('_normalized_text') or tweet.get('full_text') or tweet['tweet'].get('text', ''),
            })
        return out


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
