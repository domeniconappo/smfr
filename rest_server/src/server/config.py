import logging
import os
import re
import socket
import sys
from decimal import Decimal
from time import sleep

import numpy as np
import yaml
from cassandra.cluster import NoHostAvailable
from cassandra.cqlengine import connection
from cassandra.util import OrderedMapSerializedKey
from flask.json import JSONEncoder
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import database_exists, create_database
from flask_migrate import Migrate
from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token,
    get_jwt_identity
)

from smfrcore.auth import authenticate, identity
from smfrcore.utils import RUNNING_IN_DOCKER

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
logging.getLogger('requests_oauthlib').setLevel(logging.ERROR)

os.makedirs(CONFIG_STORE_PATH, exist_ok=True)


def _read_config_file():

    config_path = os.path.join(os.path.dirname(__file__), '../config/') if not RUNNING_IN_DOCKER else '/configuration/'
    config_file = 'config.yaml.tpl' if not os.path.exists(os.path.join(config_path, 'config.yaml')) else 'config.yaml'

    with open(os.path.join(config_path, config_file)) as f:
        server_config = yaml.load(f)
    return config_path, server_config


class CustomJSONEncoder(JSONEncoder):
    """

    """

    def default(self, obj):
        if isinstance(obj, (np.float32, np.float64, Decimal)):
            return float(obj)
        elif isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, OrderedMapSerializedKey):
            res = {}
            for k, v in obj.items():
                res[k] = (v[0], float(v[1]))
            return res
        return super().default(obj)


class Singleton(type):
    """

    """
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
    config_dir, server_config = _read_config_file()
    geonames_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'geonames'
    kafka_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'kafka'
    mysql_db_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'mysql'
    cassandra_host = '127.0.0.1' if not RUNNING_IN_DOCKER else os.environ.get('CASSANDRA_HOST', 'cassandrasmfr')
    annotator_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'annotator'
    geocoder_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'geocoder'
    restserver_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'restserver'
    kafka_bootstrap_server = '{}:9092'.format(kafka_host)

    mysql_db_name = '{}{}'.format(server_config['mysql_db_name'], '_test' if UNDER_TESTS else '')
    mysql_user = 'root'
    mysql_pass = os.environ.get('MYSQL_PASSWORD', 'password')
    restserver_port = server_config['restserver_port']
    annotator_port = server_config['annotator_port']
    geocoder_port = server_config['geocoder_port']

    kafka_topic = server_config['kafka_topic']

    debug = not UNDER_TESTS and not server_config.get('production', True)

    logger_level = logging.ERROR if UNDER_TESTS else logging.getLevelName(server_config.get('logging_level', os.environ.get('LOGGING_LEVEL', 'DEBUG')).upper())
    logger = logging.getLogger(__name__)
    logger.setLevel(logger_level)

    def __init__(self, connexion_app=None):
        if not connexion_app:
            if RestServerConfiguration not in self.__class__.instances:
                from start import app
            # noinspection PyMethodFirstArgAssignment
            self = self.__class__.instances[RestServerConfiguration]
        else:
            self.flask_app = self.set_flaskapp(connexion_app)
            self.flask_app.config['JWT_SECRET_KEY'] = os.environ.get('SECRET_KEY', 'super-secret')
            self.jwt = JWTManager(self.flask_app)
            self.logger.debug('Pushing application context')
            self.flask_app.app_context().push()
            self.producer = None

            up = False
            retries = 1
            while not up and retries <= 5:
                try:
                    from smfrcore.models.sqlmodels import sqldb
                    from smfrcore.models.cassandramodels import cqldb
                    sqldb.init_app(self.flask_app)
                    cqldb.init_app(self.flask_app)
                    self.db_mysql = sqldb
                    self.db_cassandra = cqldb
                    self.migrate = Migrate(self.flask_app, self.db_mysql)
                    if SERVER_BOOTSTRAP and not self.producer:
                        # Flask apps are setup when issuing CLI commands as well.
                        # This code is executed in case of launching REST Server
                        self.producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server, compression_type='gzip')
                except (NoHostAvailable, OperationalError, NoBrokersAvailable, socket.gaierror):
                    self.logger.error('Missing link with a db server.')
                    self.logger.warning('Cassandra/Mysql/Kafka were not up...wait 5 seconds before retrying')
                    sleep(5)
                    retries += 1
                else:
                    up = True
                finally:
                    if not up and retries >= 5:
                        self.logger.error('Cannot boot because DB servers are not reachable! Exiting...')
                        sys.exit(1)
            self.log_configuration()

    def set_flaskapp(self, connexion_app):
        cassandra_keyspace = '{}{}'.format(self.server_config['cassandra_keyspace'], '_test' if UNDER_TESTS else '')

        app = connexion_app.app
        app.json_encoder = CustomJSONEncoder
        app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:example@{}/{}'.format(self.mysql_db_host, self.mysql_db_name)
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        app.config['CASSANDRA_HOSTS'] = [self.cassandra_host]
        app.config['CASSANDRA_KEYSPACE'] = cassandra_keyspace
        return app

    @property
    def base_path(self):
        return self.server_config['base_path']

    def init_cassandra(self):
        """

        :return:
        """
        session = connection._connections[connection.DEFAULT_CONNECTION].session
        session.execute(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};" %
            self.server_config['cassandra_keyspace']
        )

        # do not remove the import below
        from smfrcore.models.cassandramodels import Tweet
        self.db_cassandra.sync_db()

    def init_mysql(self):
        """

        :return:
        """
        with self.flask_app.app_context():
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
        masked = re.sub(r'(?<=:)(.*)(?=@)', '******', self.flask_app.config['SQLALCHEMY_DATABASE_URI'])
        self.logger.info(' - URI: {}'.format(masked))
        self.logger.info('+++ Annotator microservice')
        self.logger.info(' - {}:{}'.format(self.annotator_host, self.annotator_port))
        self.logger.info('+++ Geocoder microservice')
        self.logger.info(' - {}:{}'.format(self.geocoder_host, self.geocoder_port))
        self.logger.info('+++ Geonames service (used by Geocoder/mordecai)')
        self.logger.info(' - {}'.format(self.geonames_host))
        self.logger.info('======= END LOGGING Configuration =======')
