import os
import logging
from collections import defaultdict, OrderedDict

from datetime import datetime, timedelta

import fiona
from smfrcore.models import TwitterCollection, Aggregation, create_app
from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT, RUNNING_IN_DOCKER
from sqlalchemy import or_

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)

logger = logging.getLogger(__name__)
logging.getLogger('cassandra').setLevel(logging.ERROR)


class Products:
    """
    Products component implementation
    """
    config_folder = '/config' if RUNNING_IN_DOCKER else os.path.join(os.path.dirname(__file__), '../config')
    output_folder = '/output' if RUNNING_IN_DOCKER else os.path.join(os.path.dirname(__file__), '../output')
    template = os.path.join(config_folder, 'maptemplate.shp')
    output_filename_tpl = os.path.join(output_folder, 'SMFR_map_{}.shp')

    RGB = {'red': '255 0 0', 'orange': '255 128 0', 'gray': '225 225 225'}
    high_prob_range = os.environ.get('HIGH_PROB_RANGE', '90-100')
    low_prob_range = os.environ.get('LOW_PROB_RANGE', '0-10')

    # this variable reflects the following heuristic:
    # GREEN - less than 10 high relevant tweets
    # ORANGE - num of high rel > 5 * num low rel
    # RED - num of high rel > 9 * num low rel
    alert_heuristic = os.environ.get('THRESHOLDS', '10:5:9')

    schema = {
        'geometry': 'Polygon',
        'properties': OrderedDict(
                  [
                      ('risk_color', 'str:80'),
                      ('efas_id', 'int:10'),
                      ('counters', 'str:255'),
                  ]
              )
    }

    @classmethod
    def log_config(cls):
        heuristics = cls.alert_heuristic.split(':')
        logger.info('=================================')
        logger.info('Products configuration:')
        logger.info('High Probability range %s', cls.high_prob_range)
        logger.info('Low Probability range %s', cls.low_prob_range)
        logger.info('Alert thresholds: ')
        logger.info('Gray: less than %d relevant tweets or '
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

        counters_by_efas_id = defaultdict(defaultdict(int))
        for key, value in counters:
            # key format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
            # key is like "1301_UKF2_num_tweets_0-10"
            tokens = key.split('_')
            efas_id = tokens[0]
            probs_interval = tokens[4]
            counters_by_efas_id[efas_id][probs_interval] = value

        for efas_id, tweets in relevant_tweets_aggregated.items():
            # TODO
            # need to apply heuristic from
            # https://bitbucket.org/lorinivalerio/smfr/src/master/lab/classify/select_representative_tweets.py
            # mostly to deduplicate
            pass

        shp_output_filename = cls.output_filename_tpl.format(datetime.now().strftime('%Y%m%d%H%M'))
        with fiona.open(cls.template) as source:
            with fiona.open(shp_output_filename, 'w',
                            driver=source.driver,
                            schema=cls.schema,
                            crs=source.crs) as sink:
                for feat in source:
                    out_data = {}
                    efas_id = feat['id']
                    out_data['id'] = feat['id']
                    out_data['geometry'] = feat['geometry']
                    out_data['properties'] = {'efas_id': efas_id,
                                              'risk_color': cls.determine_color(counters_by_efas_id[efas_id]),
                                              'counters': counters_by_efas_id[efas_id]}
                    sink.write(out_data)

        source.close()

    @classmethod
    def is_efas_id_counter(cls, key):
        # good key is like "1301_UKF2_num_tweets_0-10"
        # format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
        tokens = key.split('_')
        try:
            int(tokens[0])
            return True
        except ValueError:
            return False

    @classmethod
    def determine_color(cls, counters):
        heuristics = list(map(int, cls.alert_heuristic.split(':')))
        gray_th = heuristics[0]
        orange_th = heuristics[1]
        red_th = heuristics[2]
        if counters[cls.high_prob_range] < gray_th or counters[cls.high_prob_range] <= orange_th * counters[cls.low_prob_range]:
            color = 'gray'
        elif orange_th * counters[cls.low_prob_range] < counters[cls.high_prob_range] <= red_th * counters[cls.low_prob_range]:
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
