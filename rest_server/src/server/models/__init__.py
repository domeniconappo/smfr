import logging
import json
import time

import datetime
from sqlalchemy_utils import ChoiceType, ScalarListType, JSONType

from server.config import server_configuration, LOGGER_FORMAT, DATE_FORMAT

config = server_configuration()
mysql = config.db_mysql
cassandra = config.db_cassandra


logging.basicConfig(level=logging.INFO if not config.debug else logging.DEBUG,
                    format=LOGGER_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger(__name__)


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


class Tweet(cassandra.Model):
    """
    """
    __keyspace__ = config.server_config['cassandra_keyspace']

    TYPES = [
        ('annotated', 'Annotated'),
        ('collected', 'Collected'),
        ('geotagged', 'Geo Tagged'),
    ]
    tweetid = cassandra.columns.Text(primary_key=True, required=True)
    created_at = cassandra.columns.DateTime(index=True, required=True)
    collectionid = cassandra.columns.Integer(required=True, default=0, partition_key=True)
    ttype = cassandra.columns.Text(required=True, partition_key=True)
    nuts3 = cassandra.columns.Text()
    nuts3source = cassandra.columns.Text()
    annotations = cassandra.columns.Map(cassandra.columns.DateTime, cassandra.columns.Text)
    tweet = cassandra.columns.Text(required=True)
    """
    Twitter data serialized as JSON text
    """

    def validate(self):
        # TODO validate tweet content

        super().validate()

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
    def build_from_tweet(cls, collection, tweet):
        return cls(
            tweetid=tweet['id_str'],
            collectionid=collection.id,
            created_at=time.mktime(time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')),
            ttype='collected',
            nuts3=collection.nuts3,
            nuts3source=collection.nuts3source,
            annotations={},
            tweet=json.dumps(tweet, ensure_ascii=False, indent=True),
        )

    @classmethod
    def build_from_kafka_message(cls, message):
        values = json.loads(message)
        obj = cls()
        for k, v in values.items():
            if k != 'tweet':
                logger.debug('---- Building from kafka message %s %s', k, str(v))
            if k == 'created_at':
                v = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S')
            setattr(obj, k, v)
        return obj
