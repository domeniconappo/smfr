import configparser
from itertools import chain
import os
import datetime
from decimal import Decimal

from cassandra.util import OrderedMapSerializedKey
import numpy as np

from smfrcore.models.cassandra import Tweet




def import_env(env_file):
    parser = configparser.ConfigParser()
    with open(env_file) as lines:
        lines = chain(("[root]",), lines)  # make it a proper properties file for ConfigParser
        parser.read_file(lines)
    for option in parser.options('root'):
        os.environ[option.upper()] = parser.get('root', option)
