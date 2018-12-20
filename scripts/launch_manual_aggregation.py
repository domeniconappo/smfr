import sys

from smfrcore.models.sql import create_app, Aggregation, TwitterCollection

from scripts.utils import ParserHelpOnError
from aggregator.src.aggregator import run_single_aggregation


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='Collection id to aggregate',
                        type=int, metavar='collection_id', required=False)
    parser.add_argument('-r', '--running', help='Collection id to aggregate',
                        action='store_true', required=False)


def perform_aggregation(collection_id):
    aggregation = Aggregation.query.filter_by(collection_id=collection_id).first()
    if not aggregation:
        aggregation = Aggregation(collection_id=collection_id, values={}, relevant_tweets={})
        aggregation.save()
    print(aggregation)
    run_single_aggregation(collection_id, aggregation.last_tweetid_collected,
                           aggregation.last_tweetid_annotated,
                           aggregation.last_tweetid_geotagged,
                           aggregation.timestamp_start, aggregation.timestamp_end,
                           aggregation.values, aggregation.relevant_tweets)


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


if __name__ == '__main__':
    sys.exit(do())
