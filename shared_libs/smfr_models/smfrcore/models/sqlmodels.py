import os
import datetime

from passlib.apps import custom_app_context as pwd_context

from sqlalchemy import Column, Integer, String, TIMESTAMP, Float, ForeignKey, inspect, Index
from sqlalchemy_utils import ChoiceType, ScalarListType, JSONType
from flask import Flask

from .base import SMFRModel, metadata
from ..ext.database import SQLAlchemy
from .utils import jwt_token, jwt_decode

from smfrcore.utils import RUNNING_IN_DOCKER

sqldb = SQLAlchemy(metadata=metadata, session_options={'expire_on_commit': False})


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


class TwitterCollection(SMFRModel):
    """

    """
    __tablename__ = 'virtual_twitter_collection'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    ACTIVE_STATUS = 'active'
    INACTIVE_STATUS = 'inactive'

    TYPES = [
        ('keywords', 'Keywords'),
        ('geo', 'Geotagged'),
    ]
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
    ctype = Column(ChoiceType(TYPES), nullable=False)
    tracking_keywords = Column(ScalarListType(str), nullable=True)
    locations = Column(JSONType, nullable=True)
    languages = Column(ScalarListType(str), nullable=True)
    status = Column(ChoiceType(STATUSES), nullable=False, default='inactive')
    nuts2 = Column(String(50), nullable=True)
    nuts2source = Column(String(255), nullable=True)
    started_at = Column(TIMESTAMP, nullable=True)
    stopped_at = Column(TIMESTAMP, nullable=True)
    runtime = Column(TIMESTAMP, nullable=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    collection = sqldb.relationship('User', backref=sqldb.backref('users', uselist=False))

    def __str__(self):
        return 'TwitterCollection<{o.id}: {o.forecast_id} - {o.trigger.value}/{o.ctype.value}>'.format(o=self)

    @property
    def bboxfinder(self):
        bbox = ''
        if self.locations:
            coords = self.locations[0].split(', ')
            bbox = '{},{},{},{}'.format(coords[1], coords[0], coords[3], coords[2])
        return '' if not bbox else 'http://bboxfinder.com/#{}'.format(bbox)

    @classmethod
    def build_from_collector(cls, collector, user=None):
        """
        Construct a TwitterCollection object mapped to a row in MySQL
        :param user: User object
        :param collector: A :class:`daemons.collector.Collector` object
        :return: A :class:`TwitterCollection` object
        """
        query = collector.query
        collection = cls(
            trigger=collector.trigger,
            ctype=collector.ctype,
            forecast_id=collector.forecast_id or 123456789,
            nuts2=collector.nuts2,
            nuts2source=collector.nuts2source,
            tracking_keywords=query['track'],
            languages=query['languages'],
            locations=query['locations'],
            runtime=collector.runtime,
            user_id=user.id if user else 1
        )
        kwargs = {}
        for k, v in vars(collection).items():
            if k in ('_sa_instance_state', 'id') or not v or (isinstance(v, ChoiceType) and not v.value):
                continue
            kwargs[k] = v if not isinstance(v, ChoiceType) else v.value

        existing = TwitterCollection.query.filter_by(**kwargs).first()
        if existing:
            return existing
        collection.save()
        return collection

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


class StoredCollector(SMFRModel):
    """

    """
    __tablename__ = 'stored_collector'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_id = Column(Integer, ForeignKey('virtual_twitter_collection.id'))
    collection = sqldb.relationship('TwitterCollection',
                                    backref=sqldb.backref('virtual_twitter_collection', uselist=False))
    parameters = Column(JSONType, nullable=False)
    """
    parameters = {"trigger": "manual", "tzclient": "+02:00", "forecast": 123456789, 
    "kwfile": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_kwfile.yaml", 
    "locfile": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_locfile.yaml", 
    "config": "/path/c78e0f08-98c9-4553-b8ee-a41f15a34110_config.yaml"}
    """

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def delete(self):
        sqldb.session.delete(self)
        sqldb.session.commit()

    def __str__(self):
        return 'Collector stored ID: {} (collection: {})'.format(self.id, self.collection_id)


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
    join_id = Column(Integer, nullable=False, index=True)
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
        names_by_lang = {lang.split('_')[1]: cityname for lang, cityname in properties.items() if
                         lang.startswith('name_')}
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
    collection = sqldb.relationship(
        'TwitterCollection',
        backref=sqldb.backref('twitter_collection', uselist=False)
    )
    values = Column(JSONType, nullable=False)
    last_tweetid = Column(String(100), nullable=True)

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def delete(self):
        sqldb.session.delete(self)
        sqldb.session.commit()

    def __str__(self):
        return 'Aggregation ID: {} (collection: {})'.format(self.id, self.collection_id)
