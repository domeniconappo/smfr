"""
Usage:
Remove tweets of non existing collections
python clean_db.py

Remove all on demand collections, their aggregations entries and tweets.
It removes tweets of non existing collections as well
python clean_db.py -D

Remove on demand collections (except 42 and 556 collections), their aggregations entries and tweets. It removes tweets of non existing collections as well
python clean_db.py -D -e 42 -e 556
"""

import sys

from cassandra.cqlengine.query import BatchQuery
from smfrcore.models.sql import TwitterCollection, Aggregation, create_app
from smfrcore.models.cassandra import Tweet

from scripts.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-r', '--remove', help='Explicitly remove one collection',
                        type=int, metavar='remove', required=False)
    parser.add_argument('-e', '--exclude', help='Collection ids to not remove',
                        type=int, action='append',
                        metavar='exclude', required=False)
    parser.add_argument('-D', '--drop', help='If true, drop on-demand collections and their aggregations and tweets, '
                                             'excluding on-demand collections from exclude list',
                        default=False, action='store_true')


def remove_tweets(collection_id):
    b = BatchQuery()
    for ttype in (Tweet.COLLECTED_TYPE, Tweet.ANNOTATED_TYPE, Tweet.GEOTAGGED_TYPE):
        Tweet.objects(collectionid=collection_id, ttype=ttype).batch(b).delete()
    b.execute()
    print('>>>> Deleted tweets from collection {}'.format(collection_id))


def do():
    app = create_app()
    app.app_context().push()
    parser = ParserHelpOnError(description='Clean SMFR dbs. Launched without options, '
                                           'will remove tweets without an existing collection')

    add_args(parser)
    conf = parser.parse_args()
    if conf.remove:
        cid = int(conf.remove)
        c = TwitterCollection.get_collection(cid)
        remove_tweets(cid)
        aggregation_to_drop = Aggregation.get_by_collection(cid)
        if aggregation_to_drop:
            aggregation_to_drop.delete()
        c.delete()
        print('>>>> Deleteted collection/aggregation {}'.format(cid))
    else:
        collections = TwitterCollection.get_ondemand()
        collections = {c.id: c for c in collections}
        ids = collections.keys()
        max_id = max(ids)
        for cid in range(max_id + 1):
            if cid not in ids:
                c = TwitterCollection.get_collection(cid)
                if c is not None and not c.is_ondemand:
                    print('Skipping {}. Not ON DEMAND...'.format(cid))
                    continue
                print('>>>> Removing tweets {}'.format(cid))
                remove_tweets(cid)
            elif conf.drop and cid not in conf.exclude:
                print('DROP and {} not in exclude list'.format(cid))
                print('>>>> Removing tweets/aggregation/collection of {}'.format(cid))
                remove_tweets(cid)
                collection_to_drop = collections[cid]
                aggregation_to_drop = Aggregation.get_by_collection(cid)
                if aggregation_to_drop:
                    aggregation_to_drop.delete()
                collection_to_drop.delete()
                print('>>>> Deleteted collection/aggregation {}'.format(cid))


if __name__ == '__main__':
    sys.exit(do())
