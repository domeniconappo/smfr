"""
Export tweets for SMFR
When running this script, the cassandrasmfr service must be running.

Usage:

    python scripts/export_tweets.py -c 0 -t geotagged -n 1000

"""

import sys
import argparse
from datetime import datetime
from decimal import Decimal
import json

import numpy as np
from cassandra.util import OrderedMapSerializedKey

from smfrcore.models.cassandramodels import Tweet


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
                res[k] = (v[0], float(v[1]))
            return res
        return super().default(obj)


class ParserHelpOnError(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

    def add_args(self):
        self.add_argument('-c', '--collection_id', help='collection id', type=int,
                          metavar='collection_id', required=True)
        self.add_argument('-t', '--ttype', help='Can be "annotated", "collected" or "geotagged"',
                          metavar='ttype', required=True)
        self.add_argument('-l', '--lang', help='Optional, language of tweets to export',
                          metavar='language', default=None)
        self.add_argument('-o', '--output_file', help='Path to output json file',
                          metavar='output_file', default='./exported_tweets.json')
        self.add_argument('-n', '--maxnum', help='Number of tweets to export. Optional', type=int,
                          metavar='output_file', default=None)


def main(args):
    parser = ParserHelpOnError(description='Export tweets for SMFR')

    parser.add_args()
    conf = parser.parse_args(args)
    tweets = Tweet.get_iterator(conf.collection_id, conf.ttype, conf.lang, to_obj=False)
    out = []
    for i, t in enumerate(tweets, start=1):
        t['tweet'] = json.loads(t['tweet'])
        out.append(t)
        if conf.maxnum and i >= conf.maxnum:
            break
    with open(conf.output_file, 'w', encoding='utf-8') as fd:
        json.dump(out, fd, indent=2, ensure_ascii=False, sort_keys=True, default=CustomJSONEncoder().default)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
