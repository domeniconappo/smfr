import logging
import os

from .base import sqldb, SMFRModel, create_app

from .users import User
from .collections import TwitterCollection, Aggregation
from .nuts import Nuts2, Nuts3, Nuts2Finder
from .products import Product
from .tweets import BackgroundTweet, LastCassandraExport

logger = logging.getLogger('SQL')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
