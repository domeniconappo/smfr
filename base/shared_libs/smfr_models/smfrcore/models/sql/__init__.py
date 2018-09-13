import logging
import os

from .base import sqldb, SMFRModel, create_app

from .users import User
from .collections import TwitterCollection, Aggregation
from .nuts import (Nuts2, Nuts3)

logger = logging.getLogger('SQL')
logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))


