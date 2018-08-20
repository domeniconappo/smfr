"""
Module for CQLAlchemy models to map to Cassandra keyspaces
"""
import logging
import os
import time
import datetime
from decimal import Decimal

import numpy as np
import ujson as json
from cassandra.cluster import Cluster, default_lbp_factory, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.cqlengine.connection import register_connection, set_default_connection
from cassandra.query import named_tuple_factory
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import OrderedMapSerializedKey
from flask_cqlalchemy import CQLAlchemy

from smfrcore.utils import RUNNING_IN_DOCKER, LOGGER_FORMAT, LOGGER_DATE_FORMAT
from smfrcore.models.sqlmodels import TwitterCollection

logging.basicConfig(format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
logger = logging.getLogger('models')
logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))
cqldb = CQLAlchemy()

_keyspace = os.environ.get('CASSANDRA_KEYSPACE', 'smfr_persistent')
_hosts = [os.environ.get('CASSANDRA_HOST', 'cassandrasmfr')]
_port = os.environ.get('CASSANDRA_PORT', 9042)
_cassandra_user = os.environ.get('CASSANDRA_USER')
_cassandra_password = os.environ.get('CASSANDRA_PASSWORD')
_profile = ExecutionProfile(request_timeout=100, load_balancing_policy=default_lbp_factory(),
                            row_factory=named_tuple_factory)


def _cassandra_session_factory():
    cluster_kwargs = {'compression': True,
                      'auth_provider': PlainTextAuthProvider(username=_cassandra_user, password=_cassandra_password),
                      'execution_profiles': {EXEC_PROFILE_DEFAULT: _profile}}
    cluster = Cluster(_hosts, port=_port, **cluster_kwargs) if RUNNING_IN_DOCKER else Cluster(**cluster_kwargs)
    session = cluster.connect()
    session.execute('USE {}'.format(_keyspace))
    return session


_session = _cassandra_session_factory()
register_connection(str(_session), session=_session)
set_default_connection(str(_session))


class Tweet(cqldb.Model):
    """
    Object representing the `tweet` column family in Cassandra
    """
    __keyspace__ = _keyspace

    session = _session
    session.default_fetch_size = os.environ.get('CASSANDRA_FETCH_SIZE', 1000)

    TYPES = [
        ('annotated', 'Annotated'),
        ('collected', 'Collected'),
        ('geotagged', 'Geocoded'),
    ]

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        return '\nTweet\n{o.nuts2}\n' \
               '{o.created_at} - {o.lang}: {o.full_text:.120}' \
               '\nGeo: {o.geo}\nAnnotations: {o.annotations}'.format(o=self)

    @classmethod
    def to_obj(cls, row):
        return cls(**row._asdict())

    @classmethod
    def to_tuple(cls, row):
        return row

    @classmethod
    def to_dict(cls, row):
        return row._asdict()

    @classmethod
    def to_json(cls, row):
        """
        Needs to be encoded because of OrderedMapSerializedKey and other specific Cassandra objects
        :param row: dictionary representing a row in Cassandra tweet table
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
        return res

    @classmethod
    def get_iterator(cls, collection_id, ttype, lang=None, out_format='tuple', last_tweetid=None):
        """

        :param collection_id:
        :param ttype: 'annotated', 'collected' OR 'geotagged'
        :param lang: two chars lang code (e.g. en)
        :param out_format: can be 'obj', 'json' or 'dict'
        :param last_tweetid:
        :return: smfrcore.models.cassandramodels.Tweet object, dictionary or JSON encoded, according out_format param
        """
        if out_format not in ('obj', 'json', 'dict', 'tuple'):
            raise ValueError('out_format is not valid')

        if not hasattr(cls, 'stmt'):
            cls.generate_prepared_statements()

        if last_tweetid:
            results = cls.session.execute(cls.stmt_with_last_tweetid,
                                          parameters=(collection_id, ttype, int(last_tweetid)))
        else:
            results = cls.session.execute(cls.stmt, parameters=(collection_id, ttype))

        lang = lang.lower() if lang else None
        for row in results:
            if lang and row.get('lang') != lang:
                continue
            yield getattr(cls, 'to_{}'.format(out_format))(row)

    @classmethod
    def generate_prepared_statements(cls):
        """
        Generate prepared SQL statements for existing tables
        """
        cls.samples_stmt = cls.session.prepare(
            "SELECT * FROM tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC LIMIT ?"
        )
        cls.stmt = cls.session.prepare("SELECT * FROM tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC")
        cls.stmt_with_last_tweetid = cls.session.prepare(
            "SELECT * FROM tweet WHERE collectionid=? AND ttype=? AND tweet_id>?"
        )

    @classmethod
    def make_table_object(cls, numrow, tweet_tuple):
        """
        Return dictionary that can be used in HTML5 tables / Jinja templates
        :param numrow: int: numrow
        :param tweet_tuple: namedtuple representing Tweet row in smfr_persistent.tweet column family
        :return:
        """
        original_tweet = json.loads(tweet_tuple.tweet)
        full_text = tweet_tuple.full_text
        twid = tweet_tuple.tweetid

        obj = {
            'rownum': numrow,
            'Full Text': full_text,
            'Tweet id': '<a href="https://twitter.com/statuses/{}">{}</a>'.format(twid, twid),
            'original_tweet': json.dumps(original_tweet, indent=2, sort_keys=True),
            'Type': tweet_tuple.ttype,
            'Lang': tweet_tuple.lang or '-',
            'Annotations': Tweet.pretty_annotations(tweet_tuple.annotations),

            'LatLon': '<a href="https://www.openstreetmap.org/#map=13/{}/{}" target="_blank">lat: {}, lon: {}</a>'.format(
                tweet_tuple.latlong[0], tweet_tuple.latlong[1], tweet_tuple.latlong[0], tweet_tuple.latlong[1]
            ) if tweet_tuple.latlong else '',
            'Collected at': tweet_tuple.created_at or '',
            'Tweeted at': original_tweet['created_at'] or ''
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

    @classmethod
    def pretty_annotations(cls, annotations):
        if not annotations:
            return '-'
        out = ''
        for k, v in annotations.items():
            out += '{}: {} - {}\n'.format(k, v[0], v[1])

        return '<pre>{}</pre>'.format(out)

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
        Method to serialize Tweet object to Kafka
        :return: string version in JSON format
        """

        outdict = {}
        for k, v in row._asdict().items():
            if isinstance(v.value, (datetime.datetime, datetime.date)):
                outdict[k] = v.value.isoformat()
            else:
                outdict[k] = v.value
        return json.dumps(outdict, ensure_ascii=False).encode('utf-8')

    @classmethod
    def build_from_tweet(cls, collection, tweet, ttype='collected'):
        """

        :param ttype:
        :param collection:
        :param tweet:
        :return:
        """
        created = tweet.get('created_at')
        collectionid = collection.id if isinstance(collection, TwitterCollection) else int(collection)
        return cls(
            tweetid=tweet['id_str'], tweet_id=int(tweet['id_str']),
            collectionid=collectionid,
            created_at=time.mktime(
                time.strptime(created, '%a %b %d %H:%M:%S +0000 %Y')) if created else datetime.datetime.now().replace(
                microsecond=0),
            ttype=ttype,
            nuts2=collection.nuts2 if isinstance(collection, TwitterCollection) else '',
            lang=tweet['lang'], tweet=json.dumps(tweet, ensure_ascii=False),
        )

    @classmethod
    def build_from_kafka_message(cls, message):
        """

        :param message:
        :return:
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
        tweet = json.loads(self.tweet)
        full_text = None

        for status in ('retweeted_status', 'quoted_status'):

            if status in tweet:
                nature = status.split('_')[0].title()
                extended = tweet[status].get('extended_tweet', {})
                if not (extended.get('full_text') or extended.get('text')):
                    continue
                full_text = '{} - {}'.format(nature, extended.get('full_text') or extended.get('text', ''))
                break

        if not full_text:
            full_text = tweet.get('full_text') or tweet.get('extended_tweet', {}).get('full_text', '') or tweet.get('text', '')

        return full_text
