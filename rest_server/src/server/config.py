import logging
import os
import re
import sys
import threading
from time import sleep

import yaml
from cassandra.cluster import NoHostAvailable
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import database_exists, create_database
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_cqlalchemy import CQLAlchemy

from utils import CustomJSONEncoder

UNDER_TESTS = any('nose2' in x for x in sys.argv)
SERVER_BOOTSTRAP = 'gunicorn' in sys.argv[0]
LOGGER_FORMAT = '%(asctime)s: Server - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'
CONFIG_STORE_PATH = os.environ.get('SERVER_PATH_UPLOADS', os.path.join(os.path.dirname(__file__), '../../../uploads/'))


logging.getLogger('cassandra').setLevel(logging.ERROR)
logging.getLogger('kafka').setLevel(logging.ERROR)
logging.getLogger('connexion').setLevel(logging.ERROR)
logging.getLogger('swagger_spec_validator').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('fiona').setLevel(logging.ERROR)
logging.getLogger('Fiona').setLevel(logging.ERROR)
logging.getLogger('shapely').setLevel(logging.ERROR)

os.makedirs(CONFIG_STORE_PATH, exist_ok=True)


def _read_server_configuration():
    from utils import running_in_docker
    in_docker = running_in_docker()
    config_path = os.path.join(os.path.dirname(__file__), '../config/') if not in_docker else '/configuration/'
    config_file = 'config.yaml.tpl' if not os.path.exists(os.path.join(config_path, 'config.yaml')) else 'config.yaml'

    with open(os.path.join(config_path, config_file)) as f:
        server_config = yaml.load(f)
    server_config['mysql_db_host'] = os.environ.get('MYSQL_HOST', '127.0.0.1') if not in_docker else 'mysql'
    server_config['cassandra_db_host'] = os.environ.get('CASSANDRA_HOST', '127.0.0.1') if not in_docker else 'cassandra'
    server_config['geonames_host'] = os.environ.get('GEONAMES_HOST', '127.0.0.1') if not in_docker else 'geonames'
    server_config['kafka_host'] = os.environ.get('KAFKA_ADVERTISED_HOST', '127.0.0.1') if not in_docker else 'kafka'
    return config_path, server_config


class Singleton(type):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


class RestServerConfiguration(metaclass=Singleton):
    """
    A class whose objects hold SMFR Rest Server Configuration as singletons.
    Constructor accepts a connexion app object.
    """
    find_mysqluri_regex = re.compile(r'(?<=:)(.*)(?=@)')
    config_dir, server_config = _read_server_configuration()
    debug = not UNDER_TESTS and not server_config.get('production', True)
    logger_level = logging.getLevelName(server_config['logger_level'].upper())
    logger = logging.getLogger(__name__)
    logger.setLevel(logger_level)

    def __init__(self, connexion_app=None):
        if not connexion_app:
            if RestServerConfiguration not in self.__class__.instances:
                from start import app
            self = self.__class__.instances[RestServerConfiguration]
        else:
            mysql_db_name = '{}{}'.format(self.server_config['mysql_db_name'], '_test' if UNDER_TESTS else '')
            mysql_db_host = self.server_config['mysql_db_host']
            cassandra_keyspace = '{}{}'.format(self.server_config['cassandra_keyspace'], '_test' if UNDER_TESTS else '')
            self.flask_app = connexion_app.app
            self.flask_app.json_encoder = CustomJSONEncoder
            self.flask_app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:example@{}/{}'.format(mysql_db_host, mysql_db_name)
            self.flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
            self.flask_app.config['CASSANDRA_HOSTS'] = [self.server_config['cassandra_db_host']]
            self.flask_app.config['CASSANDRA_KEYSPACE'] = cassandra_keyspace
            self.geonames_host = self.server_config['geonames_host']
            self.kafka_topic = self.server_config['kafka_topic']
            self.kafka_bootstrap_server = '{}:9092'.format(self.server_config['kafka_host'])
            self.rest_server_port = self.server_config['rest_server_port']
            self.min_flood_probability = self.server_config.get('min_flood_probability', 0.59)
            self.producer = None

            up = False
            retries = 1
            while not up and retries <= 5:
                try:
                    self.db_mysql = SQLAlchemy(self.flask_app, session_options={'expire_on_commit': False})
                    self.db_cassandra = CQLAlchemy(self.flask_app)
                    self.migrate = Migrate(self.flask_app, self.db_mysql)
                    if SERVER_BOOTSTRAP and not self.producer:
                        # Flask apps are setup when issuing CLI commands as well.
                        # This code is executed in case of launching REST Server
                        self.producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server, compression_type='gzip')
                except (NoHostAvailable, OperationalError, NoBrokersAvailable):
                    self.logger.warning('Cassandra/Mysql/Kafka were not up...waiting')
                    sleep(5)
                    retries += 1
                else:
                    up = True
                finally:
                    if not up and retries >= 5:
                        self.logger.error('Cannot boot because DB servers are not reachable')
                        sys.exit(1)
            self.log_configuration()

    @property
    def base_path(self):
        return self.server_config['base_path']

    def init_cassandra(self):
        from cassandra.cqlengine.connection import _connections, DEFAULT_CONNECTION
        session = _connections[DEFAULT_CONNECTION].session
        session.execute(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};" %
            self.server_config['cassandra_keyspace']
        )

        # do not remove the import below
        from server.models import Tweet
        self.db_cassandra.sync_db()

    def init_mysql(self):
        engine = create_engine(self.flask_app.config['SQLALCHEMY_DATABASE_URI'])
        if not database_exists(engine.url):
            create_database(engine.url)

    @property
    def kafka_producer(self):
        return self.producer

    def log_configuration(self):

        self.logger.info('SMFR Rest Server and Collector manager')
        self.logger.info('======= START LOGGING Configuration =======')
        self.logger.info('+++ Kafka')
        self.logger.info(' - Topic: {}'.format(self.kafka_topic))
        self.logger.info(' - Bootstrap server: {}'.format(self.kafka_bootstrap_server))
        self.logger.info('+++ Cassandra')
        self.logger.info(' - Host: {}'.format(self.flask_app.config['CASSANDRA_HOSTS']))
        self.logger.info(' - Keyspace: {}'.format(self.flask_app.config['CASSANDRA_KEYSPACE']))
        self.logger.info('+++ MySQL')
        masked = self.find_mysqluri_regex.sub('******', self.flask_app.config['SQLALCHEMY_DATABASE_URI'])
        self.logger.info(' - URI: {}'.format(masked))
        self.logger.info('======= END LOGGING Configuration =======')
