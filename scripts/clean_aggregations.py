"""
Usage:
Reset Aggregation values and relevant tweets
python scripts/clean_aggregations.py -c 100
python scripts/clean_aggregations.py --running
python scripts/clean_aggregations.py --ondemand
"""

import sys

from smfrcore.models.sql import TwitterCollection, Aggregation, create_app

from scripts.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-r', '--running',
                        help='Reset aggregations for running collections',
                        default=False, action='store_true', metavar='running', required=False)
    parser.add_argument('-c', '--collection_id', help='Collection ids for which aggregations must be reset',
                        type=int, action='append',
                        metavar='collections', required=False)
    parser.add_argument('-d', '--ondemand',
                        help='Reset aggregations for running on demand',
                        default=False, action='store_true')


def do():
    app = create_app()
    app.app_context().push()
    parser = ParserHelpOnError(description='Clean SMFR dbs. Launched without options, '
                                           'will remove tweets without an existing collection')

    add_args(parser)
    conf = parser.parse_args()
    collections = []
    if conf.running:
        collections = TwitterCollection.get_running()
    elif conf.ondemand:
        collections = TwitterCollection.get_active_ondemand()
    elif conf.collections:
        collections = TwitterCollection.get_collections(conf.collections)

    cids = (c.id for c in collections)

    aggregations = Aggregation.query.filter(Aggregation.collection_id.in_(cids)).all()
    for agg in aggregations:
        agg.values = {}
        agg.relevant_tweets = {}
        agg.last_tweetid_collected = None
        agg.last_tweetid_annotated = None
        agg.last_tweetid_geotagged = None
        agg.timestamp_start = None
        agg.timestamp_end = None
        agg.save()


if __name__ == '__main__':
    sys.exit(do())
