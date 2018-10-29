import logging
import os

from .base import sqldb, SMFRModel, create_app

from .users import User
from .collections import TwitterCollection, Aggregation
from .nuts import (Nuts2, Nuts3)
from .products import Product

logger = logging.getLogger('SQL')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))


