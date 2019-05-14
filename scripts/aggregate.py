import sys

from smfrcore.models.sql import create_app, Aggregation, TwitterCollection

from smfrcore.utils import ParserHelpOnError
from aggregator.src.aggregator import run_single_aggregation


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='Collection id to aggregate',
                        type=int, metavar='collection_id', required=False)
    parser.add_argument('-r', '--running', help='Perform aggregation for all running collections',
                        action='store_true', required=False, default=False)
    parser.add_argument('-a', '--all', help='Perform aggregation for all collections',
                        action='store_true', required=False, default=False)


def perform_aggregation(collection_id):
    aggregation = Aggregation.query.filter_by(collection_id=collection_id).first()
    if not aggregation:
        aggregation = Aggregation(collection_id=collection_id, values={}, relevant_tweets={}, trends={})
    else:
        print('>>>> Reset aggregations for collection {}'.format(collection_id))
        aggregation.values = {}
        aggregation.relevant_tweets = {}
        aggregation.trends = {}
        aggregation.last_tweetid_collected = None
        aggregation.last_tweetid_annotated = None
        aggregation.last_tweetid_geotagged = None
        aggregation.timestamp_start = None
        aggregation.timestamp_end = None
    aggregation.save()

    print(aggregation)
    print('>>>> Performing aggregation for collection {}'.format(collection_id))
    run_single_aggregation(collection_id,
                           aggregation.last_tweetid_collected, aggregation.last_tweetid_annotated, aggregation.last_tweetid_geotagged,
                           aggregation.timestamp_start, aggregation.timestamp_end,
                           aggregation.values, aggregation.relevant_tweets, aggregation.trends,
                           aggregation)


def do():
    app = create_app()
    app.app_context().push()
    parser = ParserHelpOnError(description='Aggregate a collection')

    add_args(parser)
    conf = parser.parse_args()
    if conf.collection_id:
        collection_id = conf.collection_id
        perform_aggregation(collection_id)
    elif conf.running:
        collections = TwitterCollection.get_running()
        for c in collections:
            perform_aggregation(c.id)
    elif conf.all:
        collections = TwitterCollection.query.all()
        for c in collections:
            perform_aggregation(c.id)


if __name__ == '__main__':
    sys.exit(do())
