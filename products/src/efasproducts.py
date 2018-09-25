import os
import logging
from collections import defaultdict, OrderedDict

from datetime import datetime, timedelta

import fiona
from smfrcore.models import Tweet, TwitterCollection, Aggregation, create_app
from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT, RUNNING_IN_DOCKER
from sqlalchemy import or_

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)

logger = logging.getLogger(__name__)
logging.getLogger('cassandra').setLevel(logging.ERROR)


config_folder = '/config' if RUNNING_IN_DOCKER else os.path.join(os.path.dirname(__file__), '../config')


class Products:
    """
    Products component implementation
    """
    template = os.path.join(config_folder, 'maptemplate.shp')
    output_filename_tpl = os.path.join(config_folder, 'SMFR_map_{}.shp')
    schema = {
        'geometry': 'Polygon',
        'properties': OrderedDict(
                  [
                      ('RiskColor', 'str:80'),
                      ('efas_id', 'int:10'),

                  ]
              )
    }

    @classmethod
    def log_config(cls):
        logger.info('Products configuration...')

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
            res = defaultdict(int)
            # TODO it's a mapreduce procedure...it can be parallelized somehow
            for aggregation in aggregations:
                values = aggregation.values
                for key, value in values:
                    if not cls.is_efas_id_counter(key):
                        continue
                    res[key] += value

        counters_by_efas_id = defaultdict(defaultdict(int))
        for key, value in res:
            # key format: <efasid>_<nutsid>_num_tweets_<minprob>-<maxprob>
            # key is like "1301_UKF2_num_tweets_0-10"
            tokens = key.split('_')
            efas_id = tokens[0]
            probs_interval = tokens[4]
            counters_by_efas_id[efas_id][probs_interval] = value

        source = fiona.open(cls.template)
        with fiona.open(cls.output_filename_tpl.format('tmp'), 'w',
                        driver=source.driver, schema=cls.schema, crs=source.crs) as sink:
            for feat in source:
                out_data = {}
                efas_id = feat['id']
                geom = feat['geometry']
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
