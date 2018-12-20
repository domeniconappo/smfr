import sys
import logging
import multiprocessing

from cassandra.cqlengine.query import BatchQuery
from smfrcore.models.sql import create_app
from smfrcore.models.cassandra import Tweet
from smfrcore.utils import DEFAULT_HANDLER
from smfrcore.ml.annotator import Annotator
from smfrcore.ml.helpers import available_languages
from smfrcore.geocoding.geocoder import Geocoder

from scripts.utils import ParserHelpOnError

logger = logging.getLogger('Reannotator')
logger.setLevel(logging.DEBUG)
logger.addHandler(DEFAULT_HANDLER)

buffer_to_annotate = []


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='collection id', type=int,
                        metavar='collection_id', required=True)


def process(conf, lang):
    app = create_app()
    with app.app_context():
        geocoder = Geocoder()
        tweets = Tweet.get_iterator(conf.collection_id, Tweet.COLLECTED_TYPE,
                                    lang=lang, out_format='obj', forked_process=True)
        model, tokenizer = Annotator.load_annotation_model(lang)
        for i, t in enumerate(tweets, start=1):
            if not (i % 500):
                logger.info('Processed so far %d', i)
            buffer_to_annotate.append(t)
            if len(buffer_to_annotate) >= 100:
                annotate_and_geocode_tweets(buffer_to_annotate, model, tokenizer, geocoder)
                buffer_to_annotate.clear()
        # flush the rest
        if buffer_to_annotate:
            annotate_and_geocode_tweets(buffer_to_annotate, model, tokenizer, geocoder)
            buffer_to_annotate.clear()
    logger.info('Finished processing for lang: %s', lang)


def coords_in_collection_bbox(coordinates, tweet):
    if not tweet.is_ondemand:
        return True
    bbox = tweet.collection_bbox
    if not bbox:
        return True
    lat, lon = coordinates
    return bbox['max_lat'] >= lat >= bbox['min_lat'] and bbox['max_lon'] >= lon >= bbox['min_lon']


def main():

    parser = ParserHelpOnError(description='Reannotate tweets for a collection')

    add_args(parser)
    conf = parser.parse_args()
    logger.info(conf)
    logger.info(available_languages)
    b = BatchQuery()
    Tweet.objects(collectionid=conf.collection_id, ttype=Tweet.GEOTAGGED_TYPE).batch(b).delete()
    Tweet.objects(collectionid=conf.collection_id, ttype=Tweet.ANNOTATED_TYPE).batch(b).delete()
    b.execute()
    print('>>>> Deleted annotated and geotagged tweets from collection {}'.format(conf.collection_id))
    processes = []
    for lang in available_languages:
        logger.info('Starting annotation for %s', lang)
        p = multiprocessing.Process(target=process, args=(conf, lang), name='Annotator-{}'.format(lang))
        processes.append(p)
        p.daemon = True
        p.start()
    for p in processes:
        logger.info('Joining %s', p.name)
        p.join()


def annotate_and_geocode_tweets(tweets_to_annotate, model, tokenizer, geocoder):
    annotated_tweets = Annotator.annotate(model, tweets_to_annotate, tokenizer)
    for tweet in annotated_tweets:
        # save annotated tweet
        tweet.save()
        tweet.ttype = Tweet.GEOTAGGED_TYPE
        tweet.geo = {}
        tweet.latlong = None
        coordinates, nuts2, nuts_source, country_code, place, geonameid = geocoder.find_nuts_heuristic(tweet)
        if not coordinates:
            logger.debug('No coordinates for %s...skipping', tweet.tweetid)
        elif not coords_in_collection_bbox(coordinates, tweet):
            logger.debug('Out of bbox for %s...skipping', tweet.tweetid)
        else:
            tweet.set_geo(coordinates, nuts2, nuts_source, country_code, place, geonameid)
        # save geotagged tweet
        tweet.save()


if __name__ == '__main__':
    sys.exit(main())
