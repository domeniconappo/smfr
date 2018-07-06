import argparse
import json
import os
import sys
import configparser
from decimal import Decimal
from datetime import datetime
from itertools import chain

import numpy as np
from cassandra.util import OrderedMapSerializedKey


class ParserHelpOnError(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


def import_env(env_file):
    parser = configparser.ConfigParser()
    with open(env_file) as lines:
        lines = chain(("[root]",), lines)  # This line does the trick.
        parser.read_file(lines)
    for option in parser.options('root'):
        os.environ[option.upper()] = parser.get('root', option)


class CustomJSONEncoder(json.JSONEncoder):
    """

    """
    def default(self, obj):
        print(obj)
        print(type(obj))
        if isinstance(obj, (np.float32, np.float64, Decimal)):
            return float(obj)
        elif isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, OrderedMapSerializedKey):
            res = {}
            for k, v in obj.items():
                if isinstance(v, tuple):
                    try:
                        res[k] = dict((v,))
                    except ValueError:
                        res[k] = (v[0], v[1])
                else:
                    res[k] = v

            return res
        return super().default(obj)
