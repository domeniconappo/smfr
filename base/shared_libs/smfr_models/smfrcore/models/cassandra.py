"""
Module for CQLAlchemy models to map to Cassandra keyspaces
"""
import logging
import os
import time
import datetime
from decimal import Decimal
from collections import namedtuple

import numpy as np
import ujson as json
from cassandra.cluster import Cluster, default_lbp_factory
from cassandra.cqlengine.connection import Connection, DEFAULT_CONNECTION, _connections
from cassandra.query import named_tuple_factory
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import OrderedMapSerializedKey
from flask_cqlalchemy import CQLAlchemy

from smfrcore.utils import RUNNING_IN_DOCKER, DEFAULT_HANDLER, FALSE_VALUES
from smfrcore.models.sql import TwitterCollection, create_app
from smfrcore.models.utils import get_cassandra_hosts


logger = logging.getLogger('models')
logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)

logging.getLogger('cassandra').setLevel(logging.WARNING)

cqldb = CQLAlchemy()

_keyspace = os.environ.get('CASSANDRA_KEYSPACE', 'smfr_persistent')
_port = os.environ.get('CASSANDRA_PORT', 9042)
_cassandra_user = os.environ.get('CASSANDRA_USER')
_cassandra_password = os.environ.get('CASSANDRA_PASSWORD')

cluster_kwargs = {'compression': True, 'load_balancing_policy': default_lbp_factory(),
                  'auth_provider': PlainTextAuthProvider(username=_cassandra_user, password=_cassandra_password)}


_hosts = get_cassandra_hosts()
cassandra_cluster = Cluster(_hosts, port=_port, **cluster_kwargs) if RUNNING_IN_DOCKER else Cluster(**cluster_kwargs)
cassandra_session = cassandra_cluster.connect()
cassandra_session.default_timeout = None
cassandra_session.default_fetch_size = os.environ.get('CASSANDRA_FETCH_SIZE', 1000)

cassandra_default_connection = Connection.from_session(DEFAULT_CONNECTION, session=cassandra_session)
_connections[DEFAULT_CONNECTION] = cassandra_default_connection


flask_app = create_app()


class Tweet(cqldb.Model):
    """
    Object representing the `tweet` column family in Cassandra
    """
    __keyspace__ = _keyspace

    session = cassandra_session
    session.row_factory = named_tuple_factory
    ANNOTATED_TYPE = 'annotated'
    COLLECTED_TYPE = 'collected'
    GEOTAGGED_TYPE = 'geotagged'
    TYPES = [
        (ANNOTATED_TYPE, 'Annotated'),
        (COLLECTED_TYPE, 'Collected'),
        (GEOTAGGED_TYPE, 'Geocoded'),
    ]
    NO_COLLECTION_ID = -1

    tweetid = cqldb.columns.Text(primary_key=True, required=True)
    tweet_id = cqldb.columns.BigInt(index=True)
    """
    Id of the tweet
    """
    created_at = cqldb.columns.DateTime(index=True, required=True)
    collectionid = cqldb.columns.Integer(required=True, default=0, partition_key=True, index=True, )
    """
    Relation to collection id in MySQL virtual_twitter_collection table
    """
    ttype = cqldb.columns.Text(required=True, partition_key=True)

    nuts2 = cqldb.columns.Text()
    nuts2source = cqldb.columns.Text()

    geo = cqldb.columns.Map(
        cqldb.columns.Text, cqldb.columns.Text,
    )
    """
    Map column for geo information
    """

    annotations = cqldb.columns.Map(
        cqldb.columns.Text,
        cqldb.columns.Tuple(
            cqldb.columns.Text,
            cqldb.columns.Decimal(9, 6)
        )
    )
    """
    Map column for annotations
    """

    tweet = cqldb.columns.Text(required=True)
    """
    Twitter data serialized as JSON text
    """

    latlong = cqldb.columns.Tuple(cqldb.columns.Decimal(9, 6), cqldb.columns.Decimal(9, 6))
    latlong.db_type = 'frozen<tuple<decimal, decimal>>'
    """
    Coordinates
    """

    lang = cqldb.columns.Text(index=True)
    """
    Language of the tweet
    """
    cache_collections = {}
    """
    simple dict to hold TwitterCollection objects (temporary caching solution!)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        return '\nCollection {o.collectionid}\n' \
               '{o.created_at} - {o.lang} \n{o.full_text:.120}' \
               '\nGeo: {o.geo}\nAnnotations: {o.annotations}'.format(o=self)

    @classmethod
    def fields(cls):
        return list(cls._defined_columns.keys())

    @property
    def use_pipeline(self):
        if self.collectionid in self.cache_collections:
            return self.cache_collections[self.collectionid].is_using_pipeline
        with flask_app.app_context():
            collection = TwitterCollection.query.get(self.collectionid)
        self.cache_collections[self.collectionid] = collection
        return collection.is_using_pipeline

    @classmethod
    def to_obj(cls, row):
        """
        :param row: a tuple representing a row in Cassandra tweet table
        :return: A Tweet object
        """
        return cls(**row._asdict())

    @classmethod
    def to_tuple(cls, row):
        return row

    @classmethod
    def to_dict(cls, row):
        """
        :param row: a tuple representing a row in Cassandra tweet table
        :return: A dictionary that can be serialized
        """
        return row._asdict()

    @classmethod
    def to_json(cls, row):
        """
        Needs to be encoded because of OrderedMapSerializedKey and other specific Cassandra objects
        :param row: a tuple representing a row in Cassandra tweet table
        :return: A dictionary that can be serialized
        """
        res = {}
        for k, v in row._asdict().items():
            if isinstance(v, (np.float32, np.float64, Decimal)):
                res[k] = float(v)
            elif isinstance(v, (np.int32, np.int64)):
                res[k] = int(v)
            elif isinstance(v, datetime.datetime):
                res[k] = v.isoformat()
            elif isinstance(v, tuple):
                res[k] = [float(i) if isinstance(i, (np.float32, np.float64, Decimal)) else i for i in v]
            elif isinstance(v, OrderedMapSerializedKey):
                # cassandra Map column
                innerres = {}
                for inner_k, inner_v in v.items():
                    if isinstance(inner_v, tuple):
                        encoded_v = [float(i) if isinstance(i, (np.float32, np.float64, Decimal)) else i for i in
                                     inner_v]
                        try:
                            innerres[inner_k] = dict((encoded_v,))
                        except ValueError:
                            innerres[inner_k] = (encoded_v[0], encoded_v[1])
                    else:
                        innerres[inner_k] = inner_v

                res[k] = innerres
            else:
                res[k] = v
        return res

    @classmethod
    def get_iterator(cls, collection_id, ttype, lang=None, out_format='tuple', last_tweetid=None):
        """

        :param collection_id:
        :param ttype: 'annotated', 'collected' OR 'geotagged'
        :param lang: two chars lang code (e.g. en)
        :param out_format: can be 'obj', 'json' or 'dict'
        :param last_tweetid:
        :return: smfrcore.models.cassandra.Tweet object, dictionary or JSON encoded, according out_format param
        """
        if out_format not in ('obj', 'json', 'dict', 'tuple'):
            raise ValueError('out_format is not valid')

        if not hasattr(cls, 'stmt'):
            cls.generate_prepared_statements()

        if last_tweetid:
            results = cls.session.execute(cls.stmt_with_last_tweetid,
                                          parameters=(collection_id, ttype, int(last_tweetid)),
                                          timeout=None)
        else:
            results = cls.session.execute(cls.stmt, parameters=(collection_id, ttype),
                                          timeout=None)

        lang = lang.lower() if lang else None
        return (getattr(cls, 'to_{}'.format(out_format))(row) for row in results if not lang or row.lang == lang)

    @classmethod
    def generate_prepared_statements(cls):
        """
        Generate prepared CQL statements for existing tables
        """
        cls.samples_stmt = cls.session.prepare(
            'SELECT * FROM {}.tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC LIMIT ?'.format(cls.__keyspace__)
        )
        cls.stmt = cls.session.prepare(
            'SELECT * FROM {}.tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC'.format(cls.__keyspace__)
        )
        cls.stmt_with_last_tweetid = cls.session.prepare(
            'SELECT * FROM {}.tweet WHERE collectionid=? AND ttype=? AND tweet_id>?'.format(cls.__keyspace__)
        )

    @classmethod
    def make_table_object(cls, numrow, tweet_tuple):
        """
        Return dictionary that can be used in HTML5 tables / Jinja templates
        :param numrow: int: numrow
        :param tweet_tuple: namedtuple representing Tweet row in smfr_persistent.tweet column family
        :return:
        """
        obj = cls.to_obj(tweet_tuple)
        original_tweet = obj.original_tweet_as_dict
        full_text = obj.full_text
        twid = obj.tweetid

        obj = {
            'rownum': numrow,
            'Full Text': full_text,
            'Tweet id': '<a href="https://twitter.com/statuses/{}">{}</a>'.format(twid, twid),
            'original_tweet': obj.original_tweet_as_string,
            'Type': obj.ttype,
            'Lang': obj.lang or '-',
            'Annotations': cls.pretty_annotations(tweet_tuple.annotations),
            'raw_coords': (obj.latlong[0], obj.latlong[1]) if obj.latlong else (None, None),
            'LatLon': '<a href="https://www.openstreetmap.org/#map=13/{}/{}" target="_blank">lat: {}, lon: {}</a>'.format(
                obj.latlong[0], obj.latlong[1], obj.latlong[0], obj.latlong[1]
            ) if obj.latlong else '',
            'Collected at': obj.created_at or '',
            'Tweeted at': original_tweet['created_at'].replace('+0000', '') or '',
            'Geo': cls.pretty_geo(obj.geo),
        }
        return obj

    @classmethod
    def get_samples(cls, collection_id, ttype, size=10):
        if not hasattr(cls, 'stmt'):
            cls.generate_prepared_statements()
        rows = cls.session.execute(cls.samples_stmt, parameters=[collection_id, ttype, size])
        return rows

    def validate(self):
        # TODO validate tweet content
        super().validate()

    @property
    def original_tweet_as_string(self):
        """
        The string of the original tweet to store in Cassandra column.
        :return: JSON string representing the original tweet dictionary as received from Twitter Streaming API
        """
        return json.dumps(self.original_tweet_as_dict, indent=2, sort_keys=True)

    @property
    def original_tweet_as_dict(self):
        """
        The string of the original tweet to store in Cassandra column.
        :return: JSON string representing the original tweet dictionary as received from Twitter Streaming API
        """
        return json.loads(self.tweet)

    @property
    def is_european(self):
        return self.geo.get('is_european', False) not in FALSE_VALUES

    @classmethod
    def pretty_annotations(cls, annotations):
        if not annotations:
            return '-'
        out = ['{}: {:.3f}'.format(k.replace('_', ' ').title(), v[1]) if isinstance(v, tuple) else '{}: {:.3f}'.format(k.replace('_', ' ').title(), v['yes']) for k, v in annotations.items()]
        return '<pre>{}</pre>'.format('\n'.join(out))

    @classmethod
    def pretty_geo(cls, geo):
        if not geo:
            return '-'
        out = ['{}: {}'.format(k, v) for k, v in geo.items() if v]
        return '<pre>{}</pre>'.format('\n'.join(out))

    def serialize(self):
        """
        Method to serialize Tweet object to Kafka
        :return: string version in JSON format
        """

        outdict = {}
        for k, v in self.__dict__['_values'].items():
            if isinstance(v.value, (datetime.datetime, datetime.date)):
                outdict[k] = v.value.isoformat()
            else:
                outdict[k] = v.value
        return json.dumps(outdict, ensure_ascii=False).encode('utf-8')

    @classmethod
    def serializetuple(cls, row):
        """
        Method to serialize a tuple representing a row in Cassandra tweet table
        :return: string version in JSON format
        """
        return cls.to_obj(row).serialize()

    @classmethod
    def build_from_tweet(cls, collectionid, tweet, ttype='collected'):
        """

        :param ttype:
        :param collectionid:
        :param tweet:
        :return:
        """
        created = tweet.get('created_at')
        return cls(
            tweetid=tweet['id_str'], tweet_id=int(tweet['id_str']),
            collectionid=collectionid,
            created_at=time.mktime(
                time.strptime(created, '%a %b %d %H:%M:%S +0000 %Y')) if created else datetime.datetime.now().replace(
                microsecond=0),
            ttype=ttype,
            lang=tweet['lang'], tweet=json.dumps(tweet, ensure_ascii=False),
        )

    @classmethod
    def build_from_kafka_message(cls, message):
        """

        :param message: json string from Kafka queue
        :return: Tweet object
        """
        values = json.loads(message)
        obj = cls()
        for k, v in values.items():
            if v is None:
                continue
            if k == 'created_at':
                v = datetime.datetime.strptime(
                    v.partition('.')[0],
                    '%Y-%m-%dT%H:%M:%S') if v is not None else datetime.datetime.now().replace(microsecond=0)
            setattr(obj, k, v)
        return obj

    @property
    def full_text(self):
        return self.full_text_from_raw_tweet(self.original_tweet_as_dict)

    @classmethod
    def full_text_from_raw_tweet(cls, data):
        text = ''
        for d in (data, data.get('quoted_status'), data.get('retweeted_status')):
            if not d:
                continue

            full_text = d.get('extended_tweet', {}).get('full_text', '') or d.get('full_text', '')
            text = d.get('text') if not text else text  # backup text if full_text field is not available
            if full_text:
                return full_text
        if not full_text:
            full_text = text
        return full_text

    @classmethod
    def get_full_text(cls, tweet_tuple):
        tweet = cls.to_obj(tweet_tuple)
        return tweet.full_text

    @classmethod
    def centroid_from_raw_tweet(cls, data):
        lat, lon = (None, None)
        min_tweet_lat, max_tweet_lat, min_tweet_lon, max_tweet_lon = cls.get_tweet_bbox(data)
        if all((min_tweet_lat, max_tweet_lat, min_tweet_lon, max_tweet_lon)):
            width, heigth = max_tweet_lon - min_tweet_lon, max_tweet_lat - min_tweet_lat
            lat = min_tweet_lat + heigth / 2
            lon = min_tweet_lon + width / 2
        return lat, lon

    @classmethod
    def coords_from_raw_tweet(cls, data):
        latlong = (None, None)
        coords = (None, None)
        coordtype = None
        for d in (data, data.get('quoted_status'), data.get('retweeted_status')):
            if not d:
                continue
            for k in ('geo', 'coordinates'):
                section = d.get(k) or {}
                if section.get('coordinates'):
                    coords = section['coordinates']
                    coordtype = k
                    break
            if coords != (None, None):
                break

        if coords:
            latlong = (coords[1], coords[0]) if coordtype == 'coordinates' else (coords[0], coords[1])

        return latlong

    @classmethod
    def get_tweet_bbox(cls, data):
        min_tweet_lat, max_tweet_lat, min_tweet_lon, max_tweet_lon = (None, None, None, None)
        for d in (data, data.get('quoted_status'), data.get('retweeted_status')):
            if not d or not d.get('place'):
                continue
            bbox = d['place'].get('bounding_box', {}).get('coordinates', [[[]]])[0]
            if bbox:
                for lon, lat in bbox:
                    min_tweet_lat = min(min_tweet_lat, lat) if min_tweet_lat else lat
                    min_tweet_lon = min(min_tweet_lon, lon) if min_tweet_lon else lon
                    max_tweet_lat = max(max_tweet_lat, lat) if max_tweet_lat else lat
                    max_tweet_lon = max(max_tweet_lon, lon) if max_tweet_lon else lon
                break
        return min_tweet_lat, max_tweet_lat, min_tweet_lon, max_tweet_lon

    @classmethod
    def user_location_from_raw_tweet(cls, data):
        for d in (data, data.get('quoted_status'), data.get('retweeted_status')):
            if not d or not d.get('user') or not d['user'].get('location'):
                continue
            return d['user']['location']

    @classmethod
    def get_contributing_match_keywords_fields(cls, data):
        """
        The text of the Tweet and some entity fields are considered for matches.
        Specifically, the text attribute of the Tweet, expanded_url and display_url for links and media,
        text for hashtags, and screen_name for user mentions are checked for matches.
        :param data:
        :return:
        """
        res = {cls.full_text_from_raw_tweet(data)}
        user_loc = cls.user_location_from_raw_tweet(data)
        if user_loc:
            res.add(user_loc)
        for d in (data, data.get('quoted_status'), data.get('retweeted_status')):
            if not d:
                continue
            # texts
            res.add(d.get('text'))
            for entities in (d.get('entities'), d.get('extended_entities')):
                if not entities:
                    continue
                # urls
                urls = entities.get('urls', []) + entities.get('media', [])
                for url in urls:
                    res |= {url.get('display_url'), url.get('expanded_url')}
                # hashtags
                res |= {h.get('text', '') for h in entities.get('hashtags', [])}
                # user mentions
                res |= {um.get('screen_name') or '' for um in entities.get('user_mentions', [])}

            # full texts and extended entities
            if d.get('extended_tweet'):
                extended_tweet = d['extended_tweet']
                res |= {extended_tweet.get('full_text') or ''}
                for ext_entities in (extended_tweet.get('entities'), extended_tweet.get('extended_entities')):
                    if not ext_entities:
                        continue
                    res |= {h.get('text') or '' for h in ext_entities.get('hashtags', [])}
                    urls = ext_entities.get('urls', []) + ext_entities.get('media', [])
                    for url in urls:
                        res |= {url.get('display_url'), url.get('expanded_url')}
                    # user mentions
                    res |= {um.get('screen_name') or '' for um in ext_entities.get('user_mentions', [])}
        return ' '.join(list(res)).strip()

    @property
    def coordinates(self):
        tweet = self.original_tweet_as_dict
        return self.coords_from_raw_tweet(tweet)

    @property
    def centroid(self):
        tweet = self.original_tweet_as_dict
        return self.centroid_from_raw_tweet(tweet)

    @property
    def user_location(self):
        return self.user_location_from_raw_tweet(self.original_tweet_as_dict)


TweetTuple = namedtuple('TweetTuple', Tweet.fields())
