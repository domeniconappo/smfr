import os
import sys
import logging

from kafka import KafkaProducer
from smfrcore.models import Tweet
from smfrcore.client.api_client import AnnotatorClient
from smfrcore.utils import DEFAULT_HANDLER
from smfrcore.ml.annotator import Annotator

from scripts.utils import ParserHelpOnError

logger = logging.getLogger('Reannotator')
logger.setLevel(logging.DEBUG)
logger.addHandler(DEFAULT_HANDLER)


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='collection id', type=int,
                        metavar='collection_id', required=True)
    parser.add_argument('-t', '--ttype', help='Which type of stored tweets to export',
                        choices=["annotated", "collected", "geotagged"], default='annotated',
                        metavar='ttype', required=True)
    parser.add_argument('-l', '--lang', help='Optional, language of tweets to export',
                        metavar='language', default='en')


def main():
    bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER', '127.0.0.1:9092')
    annotator_topic = os.getenv('ANNOTATOR_KAFKA_TOPIC', 'annotator')

    parser = ParserHelpOnError(description='Reannotate tweets for a collection')
    producer = KafkaProducer(bootstrap_servers=bootstrap_server, compression_type='gzip',
                             request_timeout_ms=50000, buffer_memory=134217728,
                             linger_ms=500, batch_size=1048576)

    add_args(parser)
    conf = parser.parse_args()
    available_languages = AnnotatorClient.available_languages()
    if conf.lang not in available_languages:
        sys.exit('Cannot annotate: model not available %s' % lang)
    tweets = Tweet.get_iterator(conf.collection_id, conf.ttype, conf.lang, out_format='obj')
    model, tokenizer = Annotator.load_annotation_model(conf.lang)
    buffer_to_annotate = []
    for i, t in enumerate(tweets, start=1):
        buffer_to_annotate.append(t)
        if len(buffer_to_annotate) >= 100:
            annotate_tweets(buffer_to_annotate, model, tokenizer)
    if buffer_to_annotate:
        annotate_tweets(buffer_to_annotate, model, tokenizer)


def annotate_tweets(buffer_to_annotate, model, tokenizer):
    annotated_tweets = Annotator.annotate(model, buffer_to_annotate, tokenizer)
    for tweet in annotated_tweets:
        tweet.save()
