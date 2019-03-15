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

from cachetools import TTLCache
from cassandra.cluster import Cluster, default_lbp_factory, NoHostAvailable
from cassandra.cqlengine.connection import Connection, DEFAULT_CONNECTION, _connections
from cassandra.query import named_tuple_factory, PreparedStatement
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import OrderedMapSerializedKey
from flask_cqlalchemy import CQLAlchemy

from smfrcore.utils import IN_DOCKER, DEFAULT_HANDLER, FALSE_VALUES
from smfrcore.models.sql import TwitterCollection, Nuts2Finder, create_app
from smfrcore.models.utils import get_cassandra_hosts


logger = logging.getLogger('models')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)

logging.getLogger('cassandra').setLevel(logging.WARNING)

cqldb = CQLAlchemy()

_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'smfr_persistent')
_port = os.getenv('CASSANDRA_PORT', 9042)
_cassandra_user = os.getenv('CASSANDRA_USER')
_cassandra_password = os.getenv('CASSANDRA_PASSWORD')
_hosts = get_cassandra_hosts()
flask_app = create_app()


def new_cassandra_session():
    retries = 3
    while retries >= 0:
        try:
            cluster_kwargs = {'compression': True, 'load_balancing_policy': default_lbp_factory(),
                              'auth_provider': PlainTextAuthProvider(username=_cassandra_user, password=_cassandra_password)}
            cassandra_cluster = Cluster(_hosts, port=_port, **cluster_kwargs) if IN_DOCKER else Cluster(**cluster_kwargs)
            cassandra_session = cassandra_cluster.connect()
            cassandra_session.default_timeout = None
            cassandra_session.default_fetch_size = os.getenv('CASSANDRA_FETCH_SIZE', 1000)
            cassandra_session.row_factory = named_tuple_factory
            cassandra_default_connection = Connection.from_session(DEFAULT_CONNECTION, session=cassandra_session)
            _connections[DEFAULT_CONNECTION] = cassandra_default_connection
        except NoHostAvailable:
            logger.warning('Cassandra host not available yet...sleeping 10 secs')
            retries -= 1
            time.sleep(10)
        except Exception:
            retries = -1
        else:
            return cassandra_session


class CassandraHelpers:

    cache_collections = TTLCache(500, 60 * 60 * 6)  # max size 500, 6 hours caching

    @classmethod
    def fields(cls):
        return list(cls._defined_columns.keys())

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
        res['full_text'] = cls.get_full_text(row)
        return res

    def serialize(self, **additional):
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
        outdict.update(additional)
        return json.dumps(outdict, ensure_ascii=False).encode('utf-8')

    @classmethod
    def serializetuple(cls, row):
        """
        Method to serialize a tuple representing a row in Cassandra tweet table
        :return: string version in JSON format
        """
        return cls.to_obj(row).serialize()


class Tweet(cqldb.Model, CassandraHelpers):
    """
    A class representing the `tweet` column family in Cassandra
    """
    session = new_cassandra_session()
    __keyspace__ = _keyspace

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
    created_at = cqldb.columns.DateTime(required=True)
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        return '\nCollection {o.collectionid}\n' \
               '{o.created_at} - {o.lang} \n{o.full_text:.120}' \
               '\nGeo: {o.geo}\nAnnotations: {o.annotations}'.format(o=self)

    @property
    def efas_cycle(self):
        hour = '12' if self.created_at.hour < 12 else '00'
        efas_day = self.created_at if hour == '12' else self.created_at + datetime.timedelta(days=1)
        res = efas_day.strftime('%Y%m%d')
        return '{}{}'.format(res, hour)

    @property
    def use_pipeline(self):
        return self.collection.is_using_pipeline

    @property
    def is_ondemand(self):
        return self.collection.is_ondemand

    @property
    def collection_bbox(self):
        return self.collection.locations

    @property
    def collection(self):
        if not self.cache_collections.get(self.collectionid):
            with flask_app.app_context():
                collection = TwitterCollection.query.get(self.collectionid)
                if not collection:
                    return None
                self.cache_collections[self.collectionid] = collection
        return self.cache_collections[self.collectionid]

    @classmethod
    def get_iterator(cls, collection_id, ttype, lang=None, out_format='tuple',
                     last_tweetid=None, forked_process=False):
        """

        :param collection_id: id number of Twitter collection as stored in MySQL table
        :param ttype: 'annotated', 'collected' OR 'geotagged'
        :param lang: two chars lang code (e.g. en)
        :param out_format: can be 'obj', 'json', 'tuple' or 'dict'
        :param last_tweetid:
        :param forked_process:
        :return: Iterator of smfrcore.models.cassandra.Tweet objects, tuples, dictionary or JSON encoded,
                    according out_format param
        """
        if out_format not in ('obj', 'json', 'dict', 'tuple'):
            raise ValueError('out_format is not valid')

        cls.generate_prepared_statements(forked_process)
        cls.session.row_factory = named_tuple_factory

        if last_tweetid:
            results = cls.session.execute(cls.stmt_with_last_tweetid,
                                          parameters=(collection_id, ttype, int(last_tweetid)),
                                          timeout=None)
        else:
            results = cls.session.execute(cls.stmt,
                                          parameters=(collection_id, ttype),
                                          timeout=None)

        lang = lang.lower() if lang else None
        return (getattr(cls, 'to_{}'.format(out_format))(row) for row in results if not lang or row.lang == lang)

    @classmethod
    def get_tweet(cls, collection_id, ttype, tweetid):
        cls.generate_prepared_statements()
        cls.session.row_factory = named_tuple_factory
        results = list(cls.session.execute(cls.stmt_single, parameters=(collection_id, ttype, tweetid), timeout=None))
        return cls.to_obj(results[0]) if results and len(results) == 1 else None

    @classmethod
    def generate_prepared_statements(cls, forked_process=False):
        """
        Generate prepared CQL statements for existing tables
        """
        if forked_process or not hasattr(cls, 'session') or not cls.session:
            cls.session = new_cassandra_session()
            cls.session.row_factory = named_tuple_factory

        if not hasattr(cls, 'samples_stmt') or not isinstance(cls.samples_stmt, PreparedStatement):
            cls.samples_stmt = cls.session.prepare(
                'SELECT * FROM {}.tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC LIMIT ?'.format(cls.__keyspace__)
            )
        if not hasattr(cls, 'stmt') or not isinstance(cls.stmt, PreparedStatement):
            cls.stmt = cls.session.prepare(
                'SELECT * FROM {}.tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC'.format(cls.__keyspace__)
            )
        if not hasattr(cls, 'stmt_with_last_tweetid') or not isinstance(cls.stmt_with_last_tweetid, PreparedStatement):
            cls.stmt_with_last_tweetid = cls.session.prepare(
                'SELECT * FROM {}.tweet WHERE collectionid=? AND ttype=? AND tweet_id>?'.format(cls.__keyspace__)
            )
        if not hasattr(cls, 'stmt_single') or not isinstance(cls.stmt_single, PreparedStatement):
            cls.stmt_single = cls.session.prepare(
                'SELECT * FROM {}.tweet WHERE collectionid=? AND ttype=? and tweetid=?'.format(cls.__keyspace__)
            )
        fetch_size = os.getenv('CASSANDRA_FETCH_SIZE', 1000)
        cls.samples_stmt.fetch_size = fetch_size
        cls.stmt.fetch_size = fetch_size
        cls.stmt_with_last_tweetid.fetch_size = fetch_size
        cls.stmt_single.fetch_size = fetch_size

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
        cls.generate_prepared_statements()
        cls.session.row_factory = named_tuple_factory
        rows = cls.session.execute(cls.samples_stmt, parameters=[collection_id, ttype, size])
        return rows

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
        out = [
            '{}: {:.3f}'.format(k.replace('_', ' ').title(), v[1])
            if isinstance(v, tuple)
            else '{}: {:.3f}'.format(k.replace('_', ' ').title(), v['yes'])
            for k, v in annotations.items()
        ]
        return '<pre>{}</pre>'.format('\n'.join(out))

    @classmethod
    def pretty_geo(cls, geo):
        if not geo:
            return '-'
        out = ['{}: {}'.format(k, v) for k, v in geo.items() if v]
        return '<pre>{}</pre>'.format('\n'.join(out))

    @classmethod
    def from_tweet(cls, collectionid, tweet, ttype=COLLECTED_TYPE):
        """
        Build a Tweet object from raw tweet and collectionid
        :param ttype:
        :param collectionid:
        :param tweet: tweet dictionary coming from Twitter API (stream API)
        :return:
        """
        created = tweet.get('created_at')
        if collectionid != cls.NO_COLLECTION_ID and not cls.cache_collections.get(collectionid):
            with flask_app.app_context():
                collection = TwitterCollection.query.get(collectionid)
                cls.cache_collections[collectionid] = collection
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
    def from_json(cls, message):
        """

        :param message: json string representing a Tweet object (tipically from Kafka queue)
        :return: Tweet object
        """
        values = message
        if not isinstance(message, dict):
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
        for d in (data, data.get('quoted_status'), data.get('retweeted_status')):
            if not d:
                continue
            coordinates = d.get('coordinates') or {}
            if coordinates and 'coordinates' in coordinates:
                coords = coordinates['coordinates']
                latlong = (coords[1], coords[0])
            else:
                place = d.get('place') or {}
                if 'coordinates' in place.get('bounding_box', {}):
                    latlong = cls.centroid_from_raw_tweet(data)

            if latlong != (None, None):
                break

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
        The text of the Tweet and some entity fields are considered for matches by Twitter Stream API.
        Specifically, the text attribute of the Tweet, expanded_url and display_url for links and media,
        text for hashtags, and screen_name for user mentions are checked for matches.
        :param data: tweet dictionary
        :return: text to match for keywords filtering in stream api
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
                    res |= {url.get('display_url') or '', url.get('expanded_url') or ''}
                # hashtags
                res |= {h.get('text', '') for h in entities.get('hashtags', [])}
                # user mentions
                res |= {um.get('screen_name') or '' for um in entities.get('user_mentions', [])}

            # full texts and extended entities
            if d.get('extended_tweet'):
                extended_tweet = d['extended_tweet']
                res |= {extended_tweet.get('full_text') or extended_tweet.get('text') or ''}
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
        return self.coords_from_raw_tweet(self.original_tweet_as_dict)

    @property
    def centroid(self):
        return self.centroid_from_raw_tweet(self.original_tweet_as_dict)

    @property
    def user_location(self):
        return self.user_location_from_raw_tweet(self.original_tweet_as_dict)

    def set_geo(self, latlong, nuts2, nuts_source, country_code, place, geonameid):
        self.ttype = self.GEOTAGGED_TYPE
        self.latlong = latlong
        self.nuts2 = str(nuts2.efas_id) if nuts2 else None
        self.nuts2source = nuts_source
        country, is_european = Nuts2Finder.find_country(country_code)
        self.geo = {
            'nuts_efas_id': str(nuts2.efas_id) if nuts2 else '',
            'nuts_id': nuts2.nuts_id if nuts2 and nuts2.nuts_id is not None else '',
            'nuts_source': nuts_source or '',
            'latitude': str(latlong[0]),
            'longitude': str(latlong[1]),
            'country': country if not nuts2 or not nuts2.country else nuts2.country,
            'is_european': str(is_european),  # Map<Text, Text> fields must have text values (bool are not allowed)
            'country_code': country_code,
            'efas_name': nuts2.efas_name if nuts2 and nuts2.efas_name else '',
            'place': place,
            'geonameid': geonameid,
        }


TweetTuple = namedtuple('TweetTuple', Tweet.fields())
