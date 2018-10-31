import datetime

from sqlalchemy import Column, Integer, TIMESTAMP
from sqlalchemy_utils import ScalarListType

from .base import SMFRModel, LongJSONType


class Product(SMFRModel):
    """

    """
    __tablename__ = 'product'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_ids = Column(ScalarListType(int))
    highlights = Column(LongJSONType, nullable=True)
    created_at = Column(TIMESTAMP, nullable=True)
    relevant_tweets = Column(LongJSONType, nullable=True)
    aggregated = Column(LongJSONType, nullable=False)

    def __init__(self, *args, **kwargs):
        self.created_at = datetime.datetime.utcnow()
        self.aggregated = kwargs.get('aggregated')
        self.relevant_tweets = kwargs.get('relevant_tweets')
        self.collection_ids = kwargs.get('collection_ids')
        self.highlights = kwargs.get('highlights')
        super().__init__(*args, **kwargs)

    def __str__(self):
        return 'Product ID: {}'.format(self.id)
