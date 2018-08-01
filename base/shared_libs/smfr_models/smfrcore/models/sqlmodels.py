import os
import datetime
import logging
import uuid

import yaml
from passlib.apps import custom_app_context as pwd_context

from sqlalchemy import Column, BigInteger, Integer, String, TIMESTAMP, Float, ForeignKey, inspect, Index
from sqlalchemy_utils import ChoiceType, ScalarListType, JSONType
from flask import Flask

from .base import SMFRModel, metadata
from ..ext.database import SQLAlchemy
from .utils import jwt_token, jwt_decode

from smfrcore.utils import RUNNING_IN_DOCKER

sqldb = SQLAlchemy(metadata=metadata, session_options={'expire_on_commit': False})


logger = logging.getLogger('SQL')
logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))


def create_app(app_name='SMFR'):
    _mysql_host = '127.0.0.1' if not RUNNING_IN_DOCKER else os.environ.get('MYSQL_HOST', 'mysql')
    _mysql_db_name = os.environ.get('MYSQL_DBNAME', 'smfr')
    _mysql_user = os.environ.get('MYSQL_USER')
    _mysql_pass = os.environ.get('MYSQL_PASSWORD')
    app = Flask(app_name)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://{}:{}@{}/{}?charset=utf8mb4'.format(
        _mysql_user, _mysql_pass, _mysql_host, _mysql_db_name
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    sqldb.init_app(app)
    return app


class User(SMFRModel):
    __tablename__ = 'users'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    ROLES = [
        ('admin', 'Admin'),
        ('user', 'Normal User'),
    ]

    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    email = Column(String(100), index=True, unique=True)
    password_hash = Column(String(128))
    role = Column(ChoiceType(ROLES), nullable=False, default='user')

    @classmethod
    def create(cls, name='', email=None, password=None, role=None):
        if not email or not password:
            raise ValueError('Email and Password are required')
        user = cls(name=name, email=email,
                   password_hash=cls.hash_password(password),
                   role=role)
        user.save()
        return user

    @classmethod
    def hash_password(cls, password):
        return pwd_context.encrypt(password)

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def generate_auth_token(self, app, expires=10):
        """
        Generate a JWT Token for user. Default expire is 10 minutes
        :return: bytes representing the JWT token
        :raise: JWT encoding exceptions
        """
        payload = {
            'type': 'access',
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=expires),
            'iat': datetime.datetime.utcnow(),
            'sub': self.id,
            'identity': self.email,
            'jti': '{}==={}'.format(self.email, self.id),
            'fresh': '',
        }
        return jwt_token(app, payload)

    @classmethod
    def decode_auth_token(cls, app, auth_token):
        """
        Decode the auth token
        :param app:
        :param auth_token:
        :return: user id
        :raise jwt.ExpiredSignatureError, jwt.InvalidTokenError
        """
        payload = jwt_decode(app, auth_token)
        return payload['sub']


class CollectorConfiguration(SMFRModel):
    __tablename__ = 'collector_configuration'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}
    id = Column(Integer, primary_key=True)
    name = Column(String(500), nullable=True)
    consumer_key = Column(String(200), nullable=False)
    consumer_secret = Column(String(200), nullable=False)
    access_token = Column(String(200), nullable=False)
    access_token_secret = Column(String(200), nullable=False)

    @classmethod
    def create(cls, config):
        if not all(config.get(k) for k in ('access_token', 'access_token_secret', 'consumer_key', 'consumer_secret')):
            return CollectorConfiguration.query.filter_by(name='admin').first()
        obj = CollectorConfiguration(name=uuid.uuid4(), consumer_key=config['consumer_key'],
                                     consumer_secret=config['consumer_secret'],
                                     access_token=config['access_token_secret'])
        obj.save()
        return obj

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def delete(self):
        sqldb.session.delete(self)
        sqldb.session.commit()


class TwitterCollection(SMFRModel):
    """

    """
    __tablename__ = 'virtual_twitter_collection'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    ACTIVE_STATUS = 'active'
    INACTIVE_STATUS = 'inactive'

    TRIGGER_ONDEMAND = 'on-demand'
    TRIGGER_BACKGROUND = 'background'
    TRIGGER_MANUAL = 'manual'

    TRIGGERS = [
        (TRIGGER_BACKGROUND, 'Background'),
        (TRIGGER_ONDEMAND, 'On Demand'),
        (TRIGGER_MANUAL, 'Manual'),
    ]

    STATUSES = [
        (ACTIVE_STATUS, 'Active'),
        (INACTIVE_STATUS, 'Inactive'),
    ]

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    forecast_id = Column(Integer)
    trigger = Column(ChoiceType(TRIGGERS), nullable=False)
    tracking_keywords = Column(ScalarListType(str), nullable=True)
    locations = Column(JSONType, nullable=True)
    languages = Column(ScalarListType(str), nullable=True)
    status = Column(ChoiceType(STATUSES), nullable=False, default='inactive')
    nuts2 = Column(String(50), nullable=True)
    started_at = Column(TIMESTAMP, nullable=True)
    stopped_at = Column(TIMESTAMP, nullable=True)
    runtime = Column(TIMESTAMP, nullable=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    user = sqldb.relationship('User', backref=sqldb.backref('users', uselist=False))
    configuration_id = Column(Integer, ForeignKey('collector_configuration.id'))
    configuration = sqldb.relationship('CollectorConfiguration', lazy='subquery',
                                       backref=sqldb.backref('collector_configuration', uselist=False))

    def __str__(self):
        return 'Collection<{o.id}: {o.forecast_id} - {o.trigger.value}>'.format(o=self)

    @classmethod
    def create(cls, **kwargs):
        user = kwargs.get('user')
        obj = cls(nuts2=kwargs.get('nuts2'), trigger=kwargs['trigger'],
                  runtime=kwargs.get('runtime'), user_id=user.id if user else 1)
        obj.forecast_id = kwargs.get('forecast_id')
        keywords = kwargs.get('keywords')
        languages = kwargs.get('languages')
        if languages and isinstance(keywords, list):
            obj.languages = languages
            obj.tracking_keywords = keywords
        elif ':' in keywords:
            kwdict = {}
            groups = keywords.split(' ')
            for g in groups:
                lang, kws = list(map(str.strip, g.split(':')))
                kwdict[lang] = list(map(str.strip, kws.split(',')))
            obj.languages = sorted(list(keywords.keys()))
            obj.tracking_keywords = sorted(list(set(w for s in keywords.values() for w in s)))
        else:
            obj.tracking_keywords = list(map(str.strip, sorted(list(set(w for w in keywords.split(','))))))

        locations = kwargs.get('bounding_boxes') or kwargs.get('locations')
        if locations:
            if isinstance(locations, str):
                coords = list(map(str.strip, locations.split(',')))
                bbox = {'min_lon': coords[0], 'min_lat': coords[1], 'max_lon': coords[2], 'max_lat': coords[3]}
                obj.locations = bbox
            elif isinstance(locations, dict):
                obj.locations = locations

        collector_config = CollectorConfiguration.create(kwargs.get('configuration'))
        obj.configuration_id = collector_config.id
        obj.save()
        return obj

    @property
    def bboxfinder(self):
        bbox = ''
        if self.locations:
            bbox = '{},{},{},{}'.format(self.locations['min_lat'], self.locations['min_lon'],
                                        self.locations['max_lat'], self.locations['max_lon'])
        return '' if not bbox else 'http://bboxfinder.com/#{}'.format(bbox)

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def delete(self):
        sqldb.session.delete(self)
        sqldb.session.commit()

    def deactivate(self):
        self.status = self.INACTIVE_STATUS
        self.stopped_at = datetime.datetime.utcnow()
        self.save()

    def activate(self):
        self.status = self.ACTIVE_STATUS
        self.started_at = datetime.datetime.utcnow()
        self.save()

    @property
    def is_ondemand(self):
        return self.trigger == self.TRIGGER_ONDEMAND


class Nuts2(SMFRModel):
    """

    """

    __tablename__ = 'nuts2'
    __table_args__ = (
        Index('bbox_index', 'min_lon', 'min_lat', 'max_lon', 'max_lat'),
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4',
         'mysql_collate': 'utf8mb4_general_ci'}
    )
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    efas_id = Column(Integer, nullable=False, index=True)
    efas_name = Column(String(1000))
    nuts_id = Column(String(10))
    country = Column(String(500))
    geometry = Column(JSONType, nullable=False)
    country_code = Column(String(5))
    min_lon = Column(Float)
    max_lon = Column(Float)
    min_lat = Column(Float)
    max_lat = Column(Float)

    @classmethod
    def nuts2_bbox(cls, efas_id):
        """

        :param efas_id:
        :return:
        """
        row = cls.query.filter_by(efas_id=efas_id).first()
        plain_bbox = '({}, {}, {}, {})'.format(row.min_lon, row.min_lat, row.max_lon, row.max_lat)
        bbox = {'min_lat': row.min_lat, 'max_lat': row.max_lat,
                'min_lon': row.min_lon, 'max_lon': row.max_lon,
                'plain': plain_bbox,
                'bboxfinder': 'http://bboxfinder.com/#{},{},{},{}'.format(row.min_lat, row.min_lon, row.max_lat,
                                                                          row.max_lon)}
        return bbox

    @classmethod
    def get_nuts2(cls, lat, lon):
        """

        :param lat:
        :param lon:
        :return:
        """
        rows = cls.query.filter(Nuts2.min_lon <= lon, Nuts2.max_lon >= lon, Nuts2.min_lat <= lat, Nuts2.max_lat >= lat)
        return list(rows)

    @classmethod
    def from_feature(cls, feature):
        """

        :param feature:
        :return:
        """
        properties = feature['properties']
        efas_id = feature['id']
        geometry = feature['geometry']['coordinates']
        return cls(
            id=properties['ID'],
            efas_id=efas_id,
            efas_name=properties['EFAS_name'],
            nuts_id=properties['NUTS_ID'],
            country=properties['COUNTRY'],
            geometry=geometry,
            country_code=properties['CNTR_CODE'],
        )


class Nuts3(SMFRModel):
    """
    
    """
    __tablename__ = 'nuts3'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    efas_id = Column(Integer, nullable=False, index=True)
    name = Column(String(500), nullable=False)
    name_ascii = Column(String(500), nullable=False, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    names = Column(JSONType, nullable=False)
    properties = Column(JSONType, nullable=False)
    country_name = Column(String(500), nullable=False)
    nuts_id = Column(String(10), nullable=True)
    country_code = Column(String(5), nullable=False)

    @classmethod
    def from_feature(cls, feature):
        """

        :param feature:
        :return:
        """
        properties = feature['properties']
        names_by_lang = {lang.split('_')[1]: cityname
                         for lang, cityname in properties.items() if lang.startswith('name_')
                         }
        additional_props = {
            'is_megacity': bool(properties['MEGACITY']),
            'is_worldcity': bool(properties['WORLDCITY']),
            'is_admcap': bool(properties['ADM0CAP']),
        }

        return cls(join_id=properties['ID'],
                   name=properties['NAME'] or properties['NUTS_NAME'],
                   name_ascii=properties['NAMEASCII'] or properties['NAME_ASCI'],
                   nuts_id=properties['NUTS_ID'],
                   country_name=properties['SOV0NAME'],
                   country_code=properties['ISO_A2'] or properties['CNTR_CODE'],
                   latitude=properties['LAT'],
                   longitude=properties['LON'],
                   names=names_by_lang,
                   properties=additional_props
                   )


class Aggregation(SMFRModel):
    """

    """
    __tablename__ = 'aggregation'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_id = Column(Integer, ForeignKey('virtual_twitter_collection.id'))
    collection = sqldb.relationship('TwitterCollection', backref=sqldb.backref('twitter_collection', uselist=False))
    values = Column(JSONType, nullable=False)
    last_tweetid_collected = Column(BigInteger, nullable=True)
    last_tweetid_annotated = Column(BigInteger, nullable=True)
    last_tweetid_geotagged = Column(BigInteger, nullable=True)
    timestamp_start = Column(TIMESTAMP, nullable=True)
    timestamp_end = Column(TIMESTAMP, nullable=True)

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def delete(self):
        sqldb.session.delete(self)
        sqldb.session.commit()

    @property
    def data(self):
        # TODO rearrange values dictionary for cleaner output...
        return self.values

    def __str__(self):
        return 'Aggregation ID: {} (collection: {})'.format(self.id, self.collection_id)
