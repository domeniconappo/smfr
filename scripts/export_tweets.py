"""
Export tweets for SMFR
When running this script, the cassandrasmfr service must be running.

Usage:

    python scripts/export_tweets.py -c 0 -t geotagged -n 1000

"""

import sys
from datetime import datetime
from decimal import Decimal
import json

import ujson

import numpy as np
from cassandra.util import OrderedMapSerializedKey

from smfrcore.models.cassandramodels import Tweet

from scripts.utils import ParserHelpOnError


class CustomJSONEncoder(json.JSONEncoder):
    """

    """
    def default(self, obj):
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


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='collection id', type=int,
                        metavar='collection_id', required=True)
    parser.add_argument('-t', '--ttype', help='Can be "annotated", "collected" or "geotagged"',
                        metavar='ttype', required=True)
    parser.add_argument('-l', '--lang', help='Optional, language of tweets to export',
                        metavar='language', default=None)
    parser.add_argument('-o', '--output_file', help='Path to output json file',
                        metavar='output_file', default='./exported_tweets.json')
    parser.add_argument('-n', '--maxnum', help='Number of tweets to export. Optional', type=int,
                        metavar='output_file', default=None)


def main(args):
    parser = ParserHelpOnError(description='Export tweets for SMFR')

    add_args(parser)
    conf = parser.parse_args(args)
    tweets = Tweet.get_iterator(conf.collection_id, conf.ttype, conf.lang, to_obj=False)
    out = []
    for i, t in enumerate(tweets, start=1):
        t['tweet'] = ujson.loads(t['tweet'])
        out.append(t)
        if conf.maxnum and i >= conf.maxnum:
            break
    with open(conf.output_file, 'w', encoding='utf-8') as fd:
        json.dump(out, fd, indent=2, ensure_ascii=False, sort_keys=True, default=CustomJSONEncoder().default)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
