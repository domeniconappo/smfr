import ujson as json
import time
import datetime

from sqlalchemy_utils import ChoiceType, ScalarListType, JSONType

from server.config import RestServerConfiguration
from server.models.utils import cassandra_session_factory

config = RestServerConfiguration()
mysql = config.db_mysql
cassandra = config.db_cassandra


class VirtualTwitterCollection(mysql.Model):
    ACTIVE_STATUS = 'active'
    INACTIVE_STATUS = 'inactive'

    TYPES = [
        ('keywords', 'Keywords'),
        ('geo', 'Geotagged'),
    ]

    TRIGGERS = [
        ('background', 'Background'),
        ('on-demand', 'On Demand'),
        ('manual', 'Manual'),
    ]

    STATUSES = [
        (ACTIVE_STATUS, 'Active'),
        (INACTIVE_STATUS, 'Inactive'),
    ]

    id = mysql.Column(mysql.Integer, primary_key=True, autoincrement=True, nullable=False)
    forecast_id = mysql.Column(mysql.Integer)
    trigger = mysql.Column(ChoiceType(TRIGGERS), nullable=False)
    ctype = mysql.Column(ChoiceType(TYPES), nullable=False)
    tracking_keywords = mysql.Column(ScalarListType(str), nullable=True)
    locations = mysql.Column(JSONType, nullable=True)
    languages = mysql.Column(ScalarListType(str), nullable=True)
    status = mysql.Column(ChoiceType(STATUSES), nullable=False, default='inactive')
    nuts3 = mysql.Column(mysql.String(50), nullable=True)
    nuts3source = mysql.Column(mysql.String(255), nullable=True)
    started_at = mysql.Column(mysql.TIMESTAMP, nullable=True)
    stopped_at = mysql.Column(mysql.TIMESTAMP, nullable=True)
    runtime = mysql.Column(mysql.TIMESTAMP, nullable=True)

    def __str__(self):
        return 'VirtualTwitterCollection<{o.id}: {o.forecast_id} - {o.trigger.value}/{o.ctype.value}>'.format(o=self)

    @classmethod
    def build_from_collector(cls, collector):
        """
        Construct a VirtualTwitterCollection object mapped to a row in MySQL
        :param collector: A :class:`daemons.collector.Collector` object
        :return: A :class:`VirtualTwitterCollection` object
        """
        query = collector.query
        collection = cls(
            trigger=collector.trigger,
            ctype=collector.ctype,
            forecast_id=collector.forecast_id,
            nuts3=collector.nuts3,
            nuts3source=collector.nuts3source,
            tracking_keywords=query['track'],
            languages=query['languages'],
            locations=query['locations'],
            runtime=collector.runtime,
        )
        kwargs = {}
        for k, v in vars(collection).items():
            if k in ('_sa_instance_state', 'id'):
                continue
            kwargs[k] = v if not isinstance(v, ChoiceType) else v.value

        existing = VirtualTwitterCollection.query.filter_by(**kwargs).first()
        if existing:
            return existing
        collection.save()
        return collection

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = mysql.session.merge(self)
        mysql.session.add(attached_obj)
        mysql.session.commit()
        self.id = attached_obj.id

    def delete(self):
        mysql.session.delete(self)
        mysql.session.commit()

    def deactivate(self):
        self.status = self.INACTIVE_STATUS
        self.stopped_at = datetime.datetime.utcnow()
        self.save()

    def activate(self):
        self.status = self.ACTIVE_STATUS
        self.started_at = datetime.datetime.utcnow()
        self.save()


class StoredCollector(mysql.Model):
    id = mysql.Column(mysql.Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_id = mysql.Column(mysql.Integer, mysql.ForeignKey('virtual_twitter_collection.id'))
    collection = mysql.relationship("VirtualTwitterCollection",
                                    backref=mysql.backref("virtual_twitter_collection", uselist=False))
    parameters = mysql.Column(JSONType, nullable=False)

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = mysql.session.merge(self)
        mysql.session.add(attached_obj)
        mysql.session.commit()
        self.id = attached_obj.id

    def delete(self):
        mysql.session.delete(self)
        mysql.session.commit()

    def __str__(self):
        return 'Collector stored ID: {} (collection: {})'.format(self.id, self.collection_id)


class NutsBoundingBox(mysql.Model):
    """

    """
    id = mysql.Column(mysql.Integer, primary_key=True, nullable=False, autoincrement=False)
    min_lon = mysql.Column(mysql.Float)
    max_lon = mysql.Column(mysql.Float)
    min_lat = mysql.Column(mysql.Float)
    max_lat = mysql.Column(mysql.Float)

    @classmethod
    def nuts3_bbox(cls, id_nuts):
        """

        :param id_nuts:
        :return:
        """
        row = cls.query.filter_by(id=id_nuts).first()
        bbox = {'min_lat': row.min_lat, 'max_lat': row.max_lat, 'min_lon': row.min_lon, 'max_lon': row.max_lon}
        return bbox


# CASSANDRA MODELS

class Tweet(cassandra.Model):
    """
    """
    __keyspace__ = config.server_config['cassandra_keyspace']

    session = cassandra_session_factory()
    session.default_fetch_size = 1000
    samples_stmt = session.prepare("SELECT * FROM tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC LIMIT ?")
    stmt = session.prepare("SELECT * FROM tweet WHERE collectionid=? AND ttype=? ORDER BY tweetid DESC")
    stmt_with_lang = session.prepare("SELECT * FROM tweet WHERE collectionid=? AND ttype=? AND lang=?")

    TYPES = [
        ('annotated', 'Annotated'),
        ('collected', 'Collected'),
        ('geotagged', 'Geo Tagged'),
    ]

    tweetid = cassandra.columns.Text(primary_key=True, required=True)
    created_at = cassandra.columns.DateTime(index=True, required=True)
    collectionid = cassandra.columns.Integer(required=True, default=0, partition_key=True, index=True,)
    ttype = cassandra.columns.Text(required=True, partition_key=True)

    nuts3 = cassandra.columns.Text()
    nuts3source = cassandra.columns.Text()

    annotations = cassandra.columns.Map(
        cassandra.columns.Text,
        cassandra.columns.Tuple(
            cassandra.columns.Text,
            cassandra.columns.Decimal(9, 6)
        )
    )

    tweet = cassandra.columns.Text(required=True)
    """
    Twitter data serialized as JSON text
    """

    latlong = cassandra.columns.Tuple(cassandra.columns.Decimal(9, 6), cassandra.columns.Decimal(9, 6))
    latlong.db_type = 'frozen<tuple<decimal, decimal>>'
    """
    Coordinates
    """

    lang = cassandra.columns.Text(index=True)
    """
    """

    @classmethod
    def get_iterator(cls, collection_id, ttype, lang=None):
        results = cls.session.execute(cls.stmt, parameters=[collection_id, ttype])
        for row in results:
            if lang and row.get('lang') != lang:
                continue
            yield cls(**row)

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

        obj = {'rownum': numrow, 'Full Text': full_text, 'Tweet id': tweet_dict['tweetid'],
               'original_tweet': tweet_obj.original_tweet_as_string,
               'Profile': '<img src="{}"/>'.format(tweet_dict['tweet']['user']['profile_image_url']) if tweet_dict['tweet']['user']['profile_image_url'] else '',
               'Name': tweet_dict['tweet']['user']['screen_name'] or '',
               'Type': tweet_dict['ttype'], 'Lang': tweet_dict['lang'] or '-',
               'Annotations': tweet_obj.pretty_annotations,
               'LatLon': tweet_obj.latlong or '-',
               'Collected at': tweet_dict['created_at'] or '',
               'Tweeted at': tweet_dict['tweet']['created_at'] or ''}
        return obj

    @classmethod
    def get_samples(cls, collection_id, ttype, size=10):
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
        return out

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
                full_text = '{} - {}'.format(nature, extended.get('full_text') or extended.get('text', ''))
                break

        if not full_text:
            full_text = tweet.get('full_text') or tweet.get('extended_tweet', {}).get('full_text', '') \
                        or tweet.get('text', '')

        return full_text
