import os
import logging
import ujson as json
import threading

import sklearn.externals
import keras.models
from keras.preprocessing.sequence import pad_sequences
import stop_words

from daemons.utils import create_text_for_cnn, CNN_MAX_SEQUENCE_LENGTH
from server.config import server_configuration, LOGGER_FORMAT, DATE_FORMAT
from server.models import Tweet


os.environ['KERAS_BACKEND'] = 'theano'


class Annotator:
    models_path = os.path.join(os.path.dirname(__file__), '../config/classifier/models/')

    models = {'en': '20180319.relevance-cnn-init.en'}

    @classmethod
    def _model_path(cls, f):
        return os.path.join(cls.models_path, f)

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
        self.tokenizer = sklearn.externals.joblib.load(self._model_path(self.models[self.lang] + ".tokenizer"))
        self.tokenizer.oov_token = None
        self.model = keras.models.load_model(self._model_path(self.models[self.lang] + ".model.h5"))
        self.model.summary()
        self.stopwords = dict([(k, True) for k in stop_words.get_stop_words(self.lang)])

    def start(self):
        self.logger.info('Starting Annotation collection: {} for "{}" tweets'.format(self.collection_id, self.ttype))

        tweets = Tweet.get_iterator(self.collection_id, self.ttype)

        for t in tweets:
            original_json = json.loads(t.tweet)
            text = create_text_for_cnn(original_json, [])
            sequences = self.tokenizer.texts_to_sequences([text])
            data = pad_sequences(sequences, maxlen=CNN_MAX_SEQUENCE_LENGTH)
            predictions_list = self.model.predict(data)
            prediction = 1. * predictions_list[:, 1][0]
            t.annotations = {'flood_probability': ('yes', prediction)}
            t.ttype = 'annotated'
            message = t.serialize()
            self.logger.info('Sending to queue: {}'.format(message[:120]))
            self.producer.send(self.kafka_topic, message)

    def launch(self):
        """
        Launch an Annotator process in a separate thread
        """
        t = threading.Thread(target=self.start, name='Annotator - collection id: {}'.format(self.collection_id))
        t.start()
