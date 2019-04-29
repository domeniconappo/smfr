import configparser
from itertools import chain
import os
import datetime
from decimal import Decimal

from cassandra.util import OrderedMapSerializedKey
import numpy as np

from smfrcore.models.cassandra import Tweet


def serialize(t):
    res = dict()
    for k, v in t._asdict().items():
        # v = v.value
        if isinstance(v, (np.float32, np.float64, Decimal)):
            res[k] = float(v)
        elif isinstance(v, (np.int32, np.int64)):
            res[k] = int(v)
        elif isinstance(v, datetime.datetime):
            res[k] = v.isoformat()
        elif isinstance(v, tuple):
            res[k] = [float(i) if isinstance(i, (np.float32, np.float64, Decimal)) else i for i in v]
        elif isinstance(v, (dict, OrderedMapSerializedKey)):
            # cassandra Map column
            innerres = {}
            for inner_k, inner_v in v.items():
                if isinstance(inner_v, tuple):
                    encoded_v = [float(i) if isinstance(i, (np.float32, np.float64, Decimal)) else i for i in inner_v]
                    try:
                        innerres[inner_k] = dict((encoded_v,))
                    except ValueError:
                        innerres[inner_k] = (encoded_v[0], encoded_v[1])
                else:
                    innerres[inner_k] = inner_v
            res[k] = innerres
        else:
            res[k] = v
    res['full_text'] = Tweet.get_full_text(t)
    return res


def import_env(env_file):
    parser = configparser.ConfigParser()
    with open(env_file) as lines:
        lines = chain(("[root]",), lines)  # make it a proper properties file for ConfigParser
        parser.read_file(lines)
    for option in parser.options('root'):
        os.environ[option.upper()] = parser.get('root', option)
