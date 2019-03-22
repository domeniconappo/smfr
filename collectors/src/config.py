import codecs
import logging
import os
from decimal import Decimal

import numpy as np
import yaml
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import default_lbp_factory
from cassandra.util import OrderedMapSerializedKey
from flask.json import JSONEncoder
from sqlalchemy_utils import Choice
from smfrcore.utils import IN_DOCKER, DEFAULT_HANDLER, UNDER_TESTS


CONFIG_STORE_PATH = os.getenv('SERVER_PATH_UPLOADS', os.path.join(os.path.dirname(__file__), '../../../uploads/'))
CONFIG_FOLDER = '/configuration/' if IN_DOCKER else os.path.join(os.path.dirname(__file__), '../config/')
NUM_SAMPLES = os.getenv('NUM_SAMPLES', 100)

logging.getLogger('cassandra').setLevel(logging.WARNING)
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('connexion').setLevel(logging.ERROR)
logging.getLogger('swagger_spec_validator').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('requests_oauthlib').setLevel(logging.ERROR)
logging.getLogger('paramiko').setLevel(logging.ERROR)
logging.getLogger('oauthlib').setLevel(logging.ERROR)

os.makedirs(CONFIG_STORE_PATH, exist_ok=True)

codecs.register(lambda name: codecs.lookup('utf8') if name.lower() == 'utf8mb4' else None)


# class CustomJSONEncoder(JSONEncoder):
#     """
#
#     """
#
#     def default(self, obj):
#         if isinstance(obj, (np.float32, np.float64, Decimal)):
#             return float(obj)
#         elif isinstance(obj, Choice):
#             return float(obj.code)
#         elif isinstance(obj, (np.int32, np.int64)):
#             return int(obj)
#         elif isinstance(obj, OrderedMapSerializedKey):
#             res = {}
#             for k, v in obj.items():
#                 res[k] = (v[0], float(v[1]))
#             return res
#         return super().default(obj)


class Singleton(type):
    """

    """
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


class Configuration(metaclass=Singleton):
    """
    A class whose objects hold SMFR Rest Server Configuration as singletons.
    Constructor accepts a connexion app object.
    """
    kafka_bootstrap_servers = ['127.0.0.1:9090', '127.0.0.1:9092'] if not IN_DOCKER else os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092,kafka:9094').split(',')
    mysql_host = '127.0.0.1' if not IN_DOCKER else os.getenv('MYSQL_HOST', 'mysql')
    __mysql_user = os.getenv('MYSQL_USER', 'root')
    __mysql_pass = os.getenv('MYSQL_PASSWORD', 'example')
    cassandra_host = '127.0.0.1' if not IN_DOCKER else os.getenv('CASSANDRA_HOST', 'cassandrasmfr')
    cassandra_keyspace = '{}{}'.format(os.getenv('CASSANDRA_KEYSPACE', 'smfr_persistent'), '_test' if UNDER_TESTS else '')
    mysql_db_name = '{}{}'.format(os.getenv('MYSQL_DBNAME', 'smfr'), '_test' if UNDER_TESTS else '')
    persister_kafka_topic = os.getenv('PERSISTER_KAFKA_TOPIC', 'persister')

    debug = not UNDER_TESTS and os.getenv('DEVELOPMENT', True)

    logger_level = logging.ERROR if UNDER_TESTS else logging.getLevelName(os.getenv('LOGGING_LEVEL', 'DEBUG').upper())
    logger = logging.getLogger('Collectors config')
    logger.setLevel(logger_level)
    logger.addHandler(DEFAULT_HANDLER)

    def __init__(self):

        if Configuration not in self.__class__.instances:
            from start import app
        # noinspection PyMethodFirstArgAssignment
        self = self.__class__.instances[Configuration]
        self.producer = None
        self.collectors = {}

    @classmethod
    def default_keywords(cls):
        with open(os.path.join(CONFIG_FOLDER, 'flood_keywords.yaml')) as f:
            floods_keywords = yaml.load(f)
            languages = sorted(list(floods_keywords.keys()))
            track = sorted(list(set(w for s in floods_keywords.values() for w in s)))
        return languages, track

    @classmethod
    def admin_twitter_keys(cls, iden):
        keys = {
            k: os.getenv('{}_{}'.format(iden, k).upper())
            for k in ('consumer_key', 'consumer_secret', 'access_token', 'access_token_secret')
        }
        return keys

    # def set_flaskapp(self, connexion_app):
    #     app = connexion_app.app
    #     app.json_encoder = CustomJSONEncoder
    #     app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://{}:{}@{}/{}?charset=utf8mb4'.format(
    #         self.__mysql_user, self.__mysql_pass, self.mysql_host, self.mysql_db_name
    #     )
    #     app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    #     app.config['SQLALCHEMY_POOL_TIMEOUT'] = 360
    #     app.config['SQLALCHEMY_POOL_RECYCLE'] = 120
    #     app.config['SQLALCHEMY_POOL_SIZE'] = 10
    #
    #     app.config['CASSANDRA_HOSTS'] = [self.cassandra_host]
    #     app.config['CASSANDRA_KEYSPACE'] = self.cassandra_keyspace
    #     app.config['CASSANDRA_SETUP_KWARGS'] = {
    #         'auth_provider': PlainTextAuthProvider(username=os.getenv('CASSANDRA_USER'),
    #                                                password=os.getenv('CASSANDRA_PASSWORD')),
    #         'load_balancing_policy': default_lbp_factory(),
    #         'compression': True,
    #     }
    #     return app

    @property
    def kafka_producer(self):
        return self.producer

    def set_collectors(self, collectors):
        """

        :param collectors: dict of collectors with keys ('manual', 'ondemand', background')
        """
        self.collectors = collectors

    @property
    def running_collections(self):
        return (c for collector in self.collectors for c in collector.collections)
