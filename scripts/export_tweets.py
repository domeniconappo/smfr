"""
Export tweets for SMFR
When running this script, the cassandrasmfr service must be running.

Usage:

    python scripts/export_tweets.py -c 0 -t geotagged -n 1000

"""

import os
import sys
import ujson as json
import gzip
import shutil

import jsonlines
import ujson

from smfrcore.models.cassandra import Tweet

from scripts.utils import ParserHelpOnError, CustomJSONEncoder


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='collection id', type=int,
                        metavar='collection_id', required=True)
    parser.add_argument('-t', '--ttype', help='Which type of stored tweets to export',
                        choices=["annotated", "collected", "geotagged"],
                        metavar='ttype', required=True)
    parser.add_argument('-l', '--lang', help='Optional, language of tweets to export',
                        metavar='language', default=None)
    parser.add_argument('-o', '--output_file', help='Path to output json file',
                        metavar='output_file', default='exported_tweets.json')
    parser.add_argument('-n', '--maxnum', help='Number of tweets to export. Optional', type=int,
                        metavar='maxnum', default=None)
    parser.add_argument('-f', '--format', help='Export format. It can be JSONL (jsonlines.org) or standard JSON',
                        type=str, choices=['json', 'jsonl'], metavar='Format', default='jsonl')
    parser.add_argument('-e', '--exportonlytweets', action='store_true', default=False,
                        help='If passed, the export will contain only tweets and not the entire object')
    parser.add_argument('-z', '--gzip', help='Compress file', action='store_true', default=True)


def main():
    parser = ParserHelpOnError(description='Export tweets for SMFR')

    add_args(parser)
    conf = parser.parse_args()
    # force output file extension to be coherent with the output format
    conf.output_file = '{}.{}'.format(os.path.splitext(conf.output_file)[0], conf.format)
    tweets = Tweet.get_iterator(conf.collection_id, conf.ttype, conf.lang, out_format='json')
    write_method = getattr(sys.modules[__name__], 'write_{}'.format(conf.format))
    write_method(conf, tweets)
    if conf.gzip:
        zipped_filename = '{}.gz'.format(conf.output_file)
        print('Compressing file', conf.output_file, 'into', zipped_filename)
        with open(conf.output_file, 'rt') as f_in:
            with gzip.open(zipped_filename, 'wt') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print('Deleting file', conf.output_file)
        os.remove(conf.output_file)


def write_jsonl(conf, tweets):
    with jsonlines.open(conf.output_file, mode='w') as writer:
        for i, t in enumerate(tweets, start=1):
            t['tweet'] = ujson.loads(t['tweet'])
            writer.write(t if not conf.exportonlytweets else t['tweet'])
            if conf.maxnum and i >= conf.maxnum:
                break
            if not (i % 500):
                print('Exported so far...', str(i))


def write_json(conf, tweets):
    out = []
    for i, t in enumerate(tweets, start=1):
        t['tweet'] = ujson.loads(t['tweet'])
        out.append(t if not conf.exportonlytweets else t['tweet'])
        if conf.maxnum and i >= conf.maxnum:
            break
        if not (i % 500):
            print('Exported so far...', str(i))
    with open(conf.output_file, 'w', encoding='utf-8') as fd:
        json.dump(out, fd, indent=2, ensure_ascii=False, sort_keys=True, default=CustomJSONEncoder().default)


if __name__ == '__main__':
    sys.exit(main())
