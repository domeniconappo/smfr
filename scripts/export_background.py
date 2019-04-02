import os
import sys
import shutil
import gzip
import datetime

import ujson
import jsonlines
from cassandra.cqlengine.functions import Token

from smfrcore.models.cassandra import Tweet
from smfrcore.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='A TwitterCollection id.', type=int, required=True)
    parser.add_argument('-t', '--ttype', help='Which type of stored tweets to export',
                        choices=["annotated", "collected", "geotagged"], metavar='ttype', required=True)
    parser.add_argument('-d', '--dates', help='Time window defined as YYYYMMDD-YYYYMMDD', metavar='dates')
    parser.add_argument('-o', '--output_file', help='Path to output json file',
                        metavar='output_file', default='exported_tweets.json')
    parser.add_argument('-z', '--gzip', help='Compress file', action='store_true', default=True)


def main():
    parser = ParserHelpOnError(description='Export tweets for SMFR')

    add_args(parser)
    conf = parser.parse_args()
    # force output file extension to be coherent with the output format
    conf.output_file = '{}.jsonl'.format(os.path.splitext(conf.output_file)[0])
    query = Tweet.objects.filter(Tweet.collectionid == conf.collection_id, Tweet.ttype == conf.ttype)
    if conf.dates:
        from_date, to_date = conf.dates.split('-')
        from_date = datetime.datetime.strptime(from_date, '%Y%m%d')
        to_date = datetime.datetime.strptime(to_date, '%Y%m%d')
        query.filter(Tweet.created_at >= from_date, Tweet.created_at <= to_date)

    query = query.limit(1000)

    page = list(query)
    count = 0
    while page:
        tweets = []
        for i, t in enumerate(page):
            tweets.append(t.serialize())
        write_jsonl(conf, tweets)
        count += len(page)
        print('Exported: ', str(count))
        last = page[-1]
        page = list(query.filter(pk__token__gt=Token(last.key)))

    if conf.gzip and count:
        zipped_filename = '{}.gz'.format(conf.output_file)
        print('Compressing file', conf.output_file, 'into', zipped_filename)
        with open(conf.output_file, 'rt') as f_in:
            with gzip.open(zipped_filename, 'wt') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print('Deleting file', conf.output_file)
        os.remove(conf.output_file)
    else:
        print('Empty queryset. Please, check parameters')


def write_jsonl(conf, tweets):
    with jsonlines.open(conf.output_file, mode='a+') as writer:
        for t in tweets:
            t['tweet'] = ujson.loads(t['tweet'])
            writer.write(t)


if __name__ == '__main__':
    sys.exit(main())
