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
    collections = select_collections(conf)
    aggregations = Aggregation.query.filter(Aggregation.collection_id.in_((c.id for c in collections))).all()
    for agg in aggregations:
        print('Removing aggregation', str(agg))
        agg.delete()

    for agg in Aggregation.query.all():
        if agg.collection:
            continue
        print('Removing orphan', str(agg))
        agg.delete()


def select_collections(conf):
    collections = []
    if conf.running:
        collections = TwitterCollection.get_running()
    elif conf.ondemand:
        collections = TwitterCollection.get_active_ondemand()
    elif conf.collection_id:
        collections = TwitterCollection.get_collections(conf.collection_id)
    elif conf.all:
        collections = TwitterCollection.query.all()
    return collections


if __name__ == '__main__':
    sys.exit(do())
