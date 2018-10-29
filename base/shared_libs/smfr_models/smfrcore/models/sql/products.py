from sqlalchemy import Column, Integer, TIMESTAMP
from sqlalchemy_utils import ScalarListType

from .base import sqldb, SMFRModel, LongJSONType


class Product(SMFRModel):
    """

    """
    __tablename__ = 'product'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_ids = Column(ScalarListType(int))
    collection = sqldb.relationship('TwitterCollection', backref=sqldb.backref('aggregation', uselist=False))
    highlights = Column(LongJSONType, nullable=True)
    created_at = Column(TIMESTAMP, nullable=True)
    relevant_tweets = Column(LongJSONType, nullable=True)
    aggregated = Column(LongJSONType, nullable=False)

    def __str__(self):
        return 'Product ID: {}'.format(self.id)
