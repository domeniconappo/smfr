import codecs
import logging
import os
import threading

import yaml

from smfrcore.utils import IN_DOCKER, DEFAULT_HANDLER, UNDER_TESTS, Singleton


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
    lock = threading.RLock()

    def __init__(self):
        self.producer = None
        self._collectors = {}

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

    @property
    def kafka_producer(self):
        return self.producer

    @property
    def collectors(self):
        with self.lock:
            return self._collectors

    def set_collectors(self, collectors):
        """

        :param collectors: dict of collectors with keys ('manual', 'ondemand', background')
        """
        with self.lock:
            self._collectors = collectors

    @property
    def running_collections(self):
        return (c for collector in self._collectors for c in collector.collections)


configuration_object = Configuration()
