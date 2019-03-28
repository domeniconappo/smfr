import datetime

from smfrcore.models.sql import SMFRModel, sqldb
from smfrcore.models.sql.base import LongJSONType
from sqlalchemy import Column, ForeignKey, Integer, TIMESTAMP, String, Float, BigInteger


class BackgroundTweet(SMFRModel):
    """

    """
    __tablename__ = 'background_tweet'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_id = Column(Integer, ForeignKey('virtual_twitter_collection.id'))
    collection = sqldb.relationship('TwitterCollection', backref=sqldb.backref('tweets', uselist=True))
    created_at = Column(TIMESTAMP, nullable=True, default=datetime.datetime.utcnow)
    lang = Column(String(3))
    ttype = Column(String(20))
    nuts2 = Column(String(10), nullable=True)
    nuts2_source = Column(String(100), nullable=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    annotations = Column(LongJSONType, nullable=True)
    geo = Column(LongJSONType, nullable=True)
    tweet = Column(LongJSONType, nullable=True)


class LastCassandraExport(SMFRModel):
    """

    """
    __tablename__ = 'last_cassandra_export'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    created_at = Column(TIMESTAMP, nullable=True, default=datetime.datetime.utcnow)
    last_tweetid_collected = Column(BigInteger, nullable=True)
    last_tweetid_annotated = Column(BigInteger, nullable=True)
    last_tweetid_geotagged = Column(BigInteger, nullable=True)
    collection_id = Column(Integer, ForeignKey('virtual_twitter_collection.id'))
    collection = sqldb.relationship('TwitterCollection', backref=sqldb.backref('last_export', uselist=True))
