"""
Usage:
Reset Aggregation values and relevant tweets
python scripts/clean_aggregations.py -c 100 -c 99
python scripts/clean_aggregations.py --running
python scripts/clean_aggregations.py --ondemand
python scripts/clean_aggregations.py --all
"""

import sys

from smfrcore.models.sql import TwitterCollection, Aggregation, create_app

from scripts.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-r', '--running',
                        help='Reset aggregations for running collections',
                        default=False, action='store_true', required=False)
    parser.add_argument('-c', '--collection_id', help='Collection ids for which aggregations must be reset',
                        type=int, action='append',
                        metavar='collections', required=False)
    parser.add_argument('-d', '--ondemand',
                        help='Reset aggregations for running on demand',
                        default=False, action='store_true')
    parser.add_argument('-a', '--all',
                        help='Reset aggregations for all collections in the system',
                        default=False, action='store_true')


def do():
    app = create_app()
    app.app_context().push()
    parser = ParserHelpOnError(description='Reset aggregations')

    add_args(parser)
    conf = parser.parse_args()
    collections = []
    if conf.running:
        collections = TwitterCollection.get_running()
    elif conf.ondemand:
        collections = TwitterCollection.get_active_ondemand()
    elif conf.collection_id:
        collections = TwitterCollection.get_collections(conf.collection_id)
    elif conf.all:
        collections = TwitterCollection.query.all()

    for c in collections:
        agg = c.aggregation
        if not agg:
            continue
        print('>>>> Reset aggregations for collection {}'.format(agg.collection_id))
        agg.values = {}
        agg.relevant_tweets = {}
        agg.trends = {}
        agg.last_tweetid_collected = None
        agg.last_tweetid_annotated = None
        agg.last_tweetid_geotagged = None
        agg.timestamp_start = None
        agg.timestamp_end = None
        agg.save()

    print('Cleaning orphans')
    for agg in Aggregation.query.all():
        if agg.collection:
            continue
        print('Removing orphan ', str(agg))
        agg.delete()


if __name__ == '__main__':
    sys.exit(do())
