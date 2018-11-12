import sys
import logging

from smfrcore.models.cassandra import Tweet
from smfrcore.client.api_client import AnnotatorClient
from smfrcore.utils import DEFAULT_HANDLER
from smfrcore.ml.annotator import Annotator

from scripts.utils import ParserHelpOnError

logger = logging.getLogger('Reannotator')
logger.setLevel(logging.DEBUG)
logger.addHandler(DEFAULT_HANDLER)

buffer_to_annotate = []
previous_annotation = {}


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='collection id', type=int,
                        metavar='collection_id', required=True)
    parser.add_argument('-t', '--ttype', help='Which type of stored tweets to export',
                        choices=["annotated", "collected", "geotagged"], default='annotated',
                        metavar='ttype', required=True)
    parser.add_argument('-l', '--lang', help='Optional, language of tweets to export',
                        metavar='language', default='en')


def main():

    parser = ParserHelpOnError(description='Reannotate tweets for a collection')

    add_args(parser)
    conf = parser.parse_args()
    available_languages = AnnotatorClient.available_languages()
    if conf.lang not in available_languages:
        sys.exit('Cannot annotate: model not available %s' % conf.lang)
    tweets = Tweet.get_iterator(conf.collection_id, conf.ttype, conf.lang, out_format='obj')
    model, tokenizer = Annotator.load_annotation_model(conf.lang)

    for i, t in enumerate(tweets, start=1):
        buffer_to_annotate.append(t)
        previous_annotation[t.tweetid] = t.annotations['flood_probability'][0]
        if len(buffer_to_annotate) >= 100:
            annotate_tweets(buffer_to_annotate, model, tokenizer)
            buffer_to_annotate.clear()

    if buffer_to_annotate:
        annotate_tweets(buffer_to_annotate, model, tokenizer)


def annotate_tweets(tweets_to_annotate, model, tokenizer):
    annotated_tweets = Annotator.annotate(model, tweets_to_annotate, tokenizer)
    for tweet in annotated_tweets:
        if previous_annotation[tweet.tweetid] != tweet.annotations['flood_probability'][0]:
            logger.warning('%s -> old: %s new: %s', tweet.tweetid, previous_annotation[tweet.tweetid], tweet.annotations['flood_probability'][0])
        tweet.save()
