import logging
import json
import threading

import sklearn.externals
import stop_words

from daemons.utils import compute_features_tweet
from server.config import server_configuration, LOGGER_FORMAT, DATE_FORMAT
from server.models import Tweet


class Annotator:

    models = {'en': '20180319.relevance-cnn-init.en'}

    def __init__(self, collection_id, ttype='collected', lang='en'):
        self.rest_server_conf = server_configuration()
        logging.basicConfig(level=logging.INFO if not self.rest_server_conf.debug else logging.DEBUG,
                            format=LOGGER_FORMAT, datefmt=DATE_FORMAT)

        self.logger = logging.getLogger(__name__)
        self.kafka_topic = self.rest_server_conf.server_config['kafka_topic']
        self.producer = self.rest_server_conf.kafka_producer
        self.collection_id = collection_id
        self.ttype = ttype
        self.lang = lang
        model_file = self.models[self.lang] + ".model"
        vectorizer_file = self.models[self.lang] + ".vectorizer"
        self.model = sklearn.externals.joblib.load(model_file)
        self.vectorizer = sklearn.externals.joblib.load(vectorizer_file)
        self.stopwords = dict([(k, True) for k in stop_words.get_stop_words(self.lang)])

    def start(self):
        tweets = Tweet.get_iterator(self.collection_id, self.ttype)
        for t in tweets:
            original_json = json.loads(t.tweet)
            features_as_dict = compute_features_tweet(original_json, [], self.stopwords)
            features_as_vector = self.vectorizer.transform([features_as_dict])
            predict_class = self.model.predict_proba(features_as_vector)[0][1]
            t.annotations = {'flood_probability': predict_class}
            message = t.serialize()
            self.logger.info('Sending to queue: {}'.format(message[:120]))
            self.producer.send(self.kafka_topic, message)

    def launch(self):
        """
        Launch an Annotator process in a separate thread
        """
        t = threading.Thread(target=self.start, name=str(self))
        t.start()
