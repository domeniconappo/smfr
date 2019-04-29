"""
Usage:

python scripts/export.py -c 123 -t collected
python scripts/export.py -c 450 -t geotagged -o manual_450.jsonl
python scripts/export.py -c 10 -t annotated -d 20180101-20181231 -o background_10_20180101_20180331.jsonl -z
python scripts/export.py -c 10 -t annotated -d 20180101-20181231 -o background_10_20180101_20180331.jsonl -z -p
python scripts/export.py -c 10 -t annotated -d 20180101-20181231 -o background_10_20180101_20180331.jsonl -z -p -s 5000
"""

import os
import sys
import shutil
import gzip
import datetime
from decimal import Decimal

import ujson
import jsonlines
from cassandra.connection import Event
from cassandra.query import named_tuple_factory, SimpleStatement
from cassandra.util import OrderedMapSerializedKey
import numpy as np

from smfrcore.models.cassandra import new_cassandra_session, Tweet
from smfrcore.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='A TwitterCollection id.', type=int, required=True)
    parser.add_argument('-t', '--ttype', help='Which type of stored tweets to export',
                        choices=["annotated", "collected", "geotagged"],
                        metavar='tweet_type', required=True)
    parser.add_argument('-d', '--dates', help='Time window defined as YYYYMMDD-YYYYMMDD')
    parser.add_argument('-o', '--output_file', help='Path to output json file', default='exported_tweets.json')
    parser.add_argument('-s', '--fetch_size', help='Num of rows per page. Can it be tuned for better performances', type=int, default=5000)
    parser.add_argument('-p', '--split', help='Flag to split export in multiple files of <fetch_size> rows each', action='store_true', default=False)
    parser.add_argument('-z', '--gzip', help='Compress file', action='store_true', default=False)
    parser.add_argument('-T', '--timeout', help='Timeout for query (in seconds)', type=int, default=240)


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


class PagedResultHandler:

    def __init__(self, fut, conf):
        self.count = 0
        self.error = None
        self.conf = conf
        self.finished_event = Event()
        self.future = fut
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)

    def handle_page(self, page):
        tweets = []
        for t in page:
            t = serialize(t)
            t['tweet'] = ujson.loads(t['tweet'])
            tweets.append(t)
        self.count += len(page)
        filenum = int(self.count / self.conf.fetch_size) if self.conf.split else None
        # write page...
        write_jsonl(self.conf, tweets, filenum)
        sys.stdout.write('\r')
        sys.stdout.write('                                                                                      ')
        sys.stdout.write('\r')
        sys.stdout.write('Exported: %d' % self.count)
        sys.stdout.flush()

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()


def main():
    print('=============> Execution started: ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
    parser = ParserHelpOnError(description='Export SMFR tweets to a jsonlines file')
    add_args(parser)
    conf = parser.parse_args()
    # force output file extension to be coherent with the output format
    conf.output_file = '{}.jsonl'.format(os.path.splitext(conf.output_file)[0])

    session = new_cassandra_session()
    session.row_factory = named_tuple_factory

    # query = Tweet.objects.filter(Tweet.collectionid == conf.collection_id, Tweet.ttype == conf.ttype)
    query = 'SELECT * FROM smfr_persistent.tweet WHERE collectionid={} AND ttype=\'{}\''.format(conf.collection_id, conf.ttype)

    if conf.dates:
        from_date, to_date = conf.dates.split('-')
        print('Exporting from', from_date, 'to', to_date)
        from_date = datetime.datetime.strptime(from_date, '%Y%m%d')
        to_date = datetime.datetime.strptime(to_date, '%Y%m%d')
        print('Dates: ', from_date, to_date)
        # query = query.filter(Tweet.created_at >= from_date, Tweet.created_at <= to_date)
        query = '{} AND created_at>=\'{}\' AND created_at<=\'{}\''.format(query, from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

    statement = SimpleStatement(query, fetch_size=conf.fetch_size)
    future = session.execute_async(statement)
    handler = PagedResultHandler(future, conf)
    handler.finished_event.wait()
    if handler.error:
        raise handler.error

    if not handler.count:
        print('<============= Execution ended - no results: ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
        print('Empty queryset. Please, check parameters')
        return 0
    if conf.gzip and not conf.split:
        zipped_filename = '{}.gz'.format(conf.output_file)
        print('Compressing file', conf.output_file, 'into', zipped_filename)
        with open(conf.output_file, 'rt') as f_in:
            with gzip.open(zipped_filename, 'wt') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print('Deleting file', conf.output_file)
        os.remove(conf.output_file)
    print('<============= Execution ended: ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))


def write_jsonl(conf, tweets, filenum=None):
    if not conf.split:
        output_file = '{}.jsonl'.format(os.path.splitext(conf.output_file)[0])
    else:
        output_file = '{}.{}.jsonl'.format(os.path.splitext(conf.output_file)[0], filenum)

    with jsonlines.open(output_file, mode='a') as writer:
        writer.write_all(tweets)

    if conf.split and conf.gzip:
        zipped_filename = '{}.gz'.format(output_file)
        with open(output_file, 'rt') as f_in:
            with gzip.open(zipped_filename, 'wt') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(output_file)


if __name__ == '__main__':
    sys.exit(main())
