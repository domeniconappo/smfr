"""
Import tweets for SMFR from a JSON file
When running this script, the cassandrasmfr service must be running.

Usage:

    python3 scripts/import_tweets.py -c <collection_id>

"""
import argparse
import sys
import ujson

from smfrcore.models.cassandra import Tweet

from scripts.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='collection id', type=int,
                        metavar='collection_id', required=True)
    parser.add_argument('-i', '--input_file', help='Path to json file', type=argparse.FileType('r'),
                        metavar='input_file', required=True)


def main():

    parser = ParserHelpOnError(description='Export tweets for SMFR')

    add_args(parser)
    conf = parser.parse_args()
    collection_id = conf.collection_id
    tweets_to_import = ujson.load(conf.input_file, precise_float=True)
    print('<<<<<<<<<<<< STARTING INGESTION OF TWEETS')
    for t in tweets_to_import:
        tweet = Tweet.from_tweet(collection_id, t)
        tweet.save()
    print('>>>>>>>>>>>> ENDED INGESTION OF TWEETS')


if __name__ == '__main__':
    sys.exit(main())
