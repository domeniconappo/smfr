"""
Module for CQLAlchemy models to map to Cassandra keyspaces
"""

import os
import time
import datetime

import ujson as json
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask_cqlalchemy import CQLAlchemy

from smfrcore.utils import RUNNING_IN_DOCKER

cqldb = CQLAlchemy()


_keyspace = os.environ.get('CASSANDRA_KEYSPACE', 'smfr_persistent')
_hosts = [os.environ.get('CASSANDRA_HOST', 'cassandra')]


def cassandra_session_factory():
    # Remote access to Cassandra is via its thrift port for Cassandra >= 2.0.
    # In Cassandra >= 2.0.x, the default cqlsh listen port is 9160
    # which is defined in cassandra.yaml by the rpc_port parameter.
    # https://stackoverflow.com/questions/36133127/how-to-configure-cassandra-for-remote-connection
    cluster = Cluster(_hosts, port=9042) if RUNNING_IN_DOCKER else Cluster()
    session = cluster.connect()
    session.row_factory = dict_factory
    session.execute("USE {}".format(_keyspace))
    return session


class Tweet(cqldb.Model):
    """
    Object representing the `tweet` column family in Cassandra
    """
    __keyspace__ = _keyspace

    session = cassandra_session_factory()
    session.default_fetch_size = 1000

    TYPES = [
        ('annotated', 'Annotated'),
        ('collected', 'Collected'),
        ('geotagged', 'Geo Tagged'),
    ]

    tweetid = cqldb.columns.Text(primary_key=True, required=True)
    """
    Id of the tweet
    """
    created_at = cqldb.columns.DateTime(index=True, required=True)
    collectionid = cqldb.columns.Integer(required=True, default=0, partition_key=True, index=True, )
    """
    Relation to collection id in MySQL virtual_twitter_collection table
    """
    ttype = cqldb.columns.Text(required=True, partition_key=True)

    nuts3 = cqldb.columns.Text()
    nuts3source = cqldb.columns.Text()

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
        Tweet.generate_prepared_statements()

    @classmethod
    def get_iterator(cls, collection_id, ttype, lang=None):
        if not hasattr(cls, 'stmt'):
            cls.generate_prepared_statements()
        results = cls.session.execute(cls.stmt, parameters=[collection_id, ttype])
        for row in results:
            if lang and row.get('lang') != lang:
                continue
            yield cls(**row)

    @classmethod
    def generate_prepared_statements(cls):
        """
        Generate prepared SQL statements for existing tables
        """
        cls.samples_stmt = cls.session.prepare(
            "SELECT * FROM tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC LIMIT ?")
        cls.stmt = cls.session.prepare("SELECT * FROM tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC")
        cls.stmt_with_lang = cls.session.prepare("SELECT * FROM tweet WHERE collectionid=? AND ttype=? AND lang=?")

    @classmethod
    def make_table_object(cls, numrow, tweet_dict):
        """

        :param numrow: int: numrow
        :param tweet_dict: dict representing Tweet row in smfr_persistent.tweet column family
        :return:
        """
        tweet_obj = cls(**tweet_dict)
        tweet_dict['tweet'] = json.loads(tweet_dict['tweet'])

        full_text = tweet_obj.full_text

        tweet_dict['tweet']['full_text'] = full_text
        user_name = tweet_dict['tweet']['user']['screen_name']
        profile_img = tweet_dict['tweet']['user']['profile_image_url'] or ''

        obj = {'rownum': numrow, 'Full Text': full_text,
               'Tweet id': '<a href="https://twitter.com/statuses/{}"'.format(tweet_dict['tweetid']),
               'original_tweet': tweet_obj.original_tweet_as_string,
               'Profile': '<a href="https://twitter.com/{}"><img src="{}"/></a>'.format(user_name, profile_img),
               'Name': '<a href="https://twitter.com/{}">{}</a>'.format(user_name, user_name),
               'Type': tweet_dict['ttype'], 'Lang': tweet_dict['lang'] or '-',
               'Annotations': tweet_obj.pretty_annotations,
               'LatLon': tweet_obj.latlong or '-',
               'Collected at': tweet_dict['created_at'] or '',
               'Tweeted at': tweet_dict['tweet']['created_at'] or ''}
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
    def pretty_annotations(self):
        if not self.annotations:
            return '-'
        out = ''
        for k, v in self.annotations.items():
            out += '{}: {} - {}\n'.format(k, v[0], v[1])

        return '<pre>{}</pre>'.format(out)

    def serialize(self):
        """
        Method to serialize object to Kafka
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
    def build_from_tweet(cls, collection, tweet, ttype='collected'):
        """

        :param ttype:
        :param collection:
        :param tweet:
        :return:
        """
        return cls(
            tweetid=tweet['id_str'],
            collectionid=collection.id,
            created_at=time.mktime(time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')),
            ttype=ttype,
            nuts3=collection.nuts3,
            nuts3source=collection.nuts3source,
            annotations={}, lang=tweet['lang'],
            tweet=json.dumps(tweet, ensure_ascii=False),
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
            if k == 'created_at':
                v = datetime.datetime.strptime(v.partition('.')[0], '%Y-%m-%dT%H:%M:%S') if v is not None else datetime.datetime.now().replace(microsecond=0)
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
