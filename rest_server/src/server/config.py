import logging
import os
import re
import sys
import threading

import yaml
from cassandra.cluster import NoHostAvailable
from kafka import KafkaProducer

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import database_exists, create_database
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_cqlalchemy import CQLAlchemy


UNDER_TESTS = any('nose2' in x for x in sys.argv)
LOGGER_FORMAT = '%(asctime)s: Server - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'
CONFIG_STORE_PATH = os.environ.get('SERVER_PATH_UPLOADS', os.path.join(os.path.dirname(__file__), '../../../uploads/'))

logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT, datefmt=DATE_FORMAT)

logger = logging.getLogger(__name__)
logging.getLogger('cassandra').setLevel(logging.ERROR)
logging.getLogger('kafka').setLevel(logging.ERROR)
logging.getLogger('connexion').setLevel(logging.ERROR)
logging.getLogger('swagger_spec_validator').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

os.makedirs(CONFIG_STORE_PATH, exist_ok=True)


def _read_server_configuration():
    from utils import running_in_docker
    in_docker = running_in_docker()
    config_path = os.path.join(os.path.dirname(__file__), '../config/') if not in_docker else '/configuration/'
    config_file = 'config.yaml.tpl' if not os.path.exists(os.path.join(config_path, 'config.yaml')) else 'config.yaml'
    with open(os.path.join(config_path, config_file)) as f:
        server_config = yaml.load(f)
    server_config['mysql_db_host'] = '127.0.0.1' if not in_docker else 'mysql'
    server_config['cassandra_db_host'] = '127.0.0.1' if not in_docker else 'cassandra'
    kafka_host = os.environ.get('KAFKA_ADVERTISED_HOST', '127.0.0.1')
    server_config['kafka_host'] = kafka_host if not in_docker else 'kafka'
    return server_config


class Singleton(type):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


def start_consumer():
    from daemons.consumer import Consumer
    consumer = Consumer()
    t_cons = threading.Thread(target=consumer.start, name='Consumer', daemon=True)
    t_cons.start()


class RestServerConfiguration(metaclass=Singleton):
    """
    A class whose objects hold SMFR Rest Server Configuration as singletons.
    Constructor accepts a connexion app object.
    """

    server_config = _read_server_configuration()
    debug = not UNDER_TESTS and not server_config.get('production', True)

    def __init__(self, connexion_app=None, bootstrap_server=False):
        mysql_db_name = '{}{}'.format(self.server_config['mysql_db_name'], '_test' if UNDER_TESTS else '')
        mysql_db_host = self.server_config['mysql_db_host']
        cassandra_keyspace = '{}{}'.format(self.server_config['cassandra_keyspace'], '_test' if UNDER_TESTS else '')
        self.flask_app = connexion_app.app
        try:
            self.flask_app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:example@{}/{}'.format(mysql_db_host, mysql_db_name)
            self.flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
            self.flask_app.config['CASSANDRA_HOSTS'] = [self.server_config['cassandra_db_host']]
            self.flask_app.config['CASSANDRA_KEYSPACE'] = cassandra_keyspace
            os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'
            self.db_mysql = SQLAlchemy(self.flask_app)
            self.db_cassandra = CQLAlchemy(self.flask_app)
            self.migrate = Migrate(self.flask_app, self.db_mysql)
        except (NoHostAvailable, OperationalError):
            logger.error('Cannot boot because DB servers are not reachable')
            sys.exit(1)
        self.kafka_topic = self.server_config['kafka_topic']
        self.kafka_bootstrap_server = '{}:9092'.format(self.server_config['kafka_host'])
        self.rest_server_port = self.server_config['rest_server_port']

        if bootstrap_server:
            self.producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server, compression_type='gzip')
            self.log_configuration()

    @property
    def base_path(self):
        return self.server_config['base_path']

    def init_cassandra(self):
        from cassandra.cqlengine.connection import _connections, DEFAULT_CONNECTION
        session = _connections[DEFAULT_CONNECTION].session
        session.execute(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};" %
            self.server_config['cassandra_keyspace'])

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
        logger.info('SMFR Rest Server and Collector manager')
        logger.info('======= START LOGGING Configuration =======')
        logger.info('+++ Kafka')
        logger.info(' - Topic: {}'.format(self.kafka_topic))
        logger.info(' - Bootstrap server: {}'.format(self.kafka_bootstrap_server))
        logger.info('+++ Cassandra')
        logger.info(' - Host: {}'.format(self.flask_app.config['CASSANDRA_HOSTS']))
        logger.info(' - Keyspace: {}'.format(self.flask_app.config['CASSANDRA_KEYSPACE']))
        logger.info('+++ MySQL')
        masked = re.sub(r"(?<=:)(.*)(?=@)", '******', self.flask_app.config['SQLALCHEMY_DATABASE_URI'])
        logger.info(' - URI: {}'.format(masked))
        logger.info('======= END LOGGING Configuration =======')


def server_configuration():
    conf = Singleton.instances.get(RestServerConfiguration)
    if not conf:
        from start import app
        conf = Singleton.instances[RestServerConfiguration]
    return conf
