import os
import logging
import sys
import threading
import time

import ujson as json
from subprocess import Popen, PIPE

import tensorflow as tf
import keras
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from keras.preprocessing.sequence import pad_sequences
import sklearn

from smfrcore.models.cassandramodels import Tweet
from smfrcore.utils import RUNNING_IN_DOCKER

from utils import create_text_for_cnn, models_by_language


class Annotator:
    """
    Annotator component implementation
    """
    _running = []
    _stop_signals = []
    _lock = threading.RLock()
    logger = logging.getLogger(__name__)
    kafka_bootstrap_server = '{}:9092'.format('kafka' if RUNNING_IN_DOCKER else '127.0.0.1')
    models_path = os.path.join(os.environ.get('MODELS_PATH', '/'), 'models')
    current_models_mapping = os.path.join(models_path, 'current-model.json')
    models = models_by_language(current_models_mapping)

    kafkaup = False
    retries = 5
    while (not kafkaup) and retries >= 0:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server, compression_type='gzip')
        except NoBrokersAvailable:
            logger.warning('Waiting for Kafka to boot...')
            time.sleep(5)
            retries -= 1
            if retries < 0:
                sys.exit(1)
        else:
            kafkaup = True
            break

    kafka_topic = os.environ.get('KAFKA_TOPIC', 'persister')

    @classmethod
    def log_config(cls):
        cls.logger.info('CNN Models folder %s', cls.models_path)
        cls.logger.info('Loaded models')
        for lang, model in cls.models.items():
            cls.logger.info('%s --> %s', lang, model)

    @classmethod
    def download_cnn_models(cls):

        git_command = ['/usr/bin/git', 'pull', 'origin', 'master']
        repository = os.path.join(Annotator.models_path, '../')

        git_query = Popen(git_command, cwd=repository, stdout=PIPE, stderr=PIPE)
        git_status, error = git_query.communicate()
        cls.logger.info('Fetching new models if any')
        cls.logger.info(git_status)
        cls.logger.info(error)
        cls.models = models_by_language(cls.current_models_mapping)

    @classmethod
    def running(cls):
        """
        Return current Annotation processes as list of (collection, lang) tuples
        :return: List of couples (collection_id, lang) for current Annotating processes.
        :rtype: list
        """
        return cls._running

    @classmethod
    def is_running_for(cls, collection_id, lang):
        """
        Return True if annotation is _running for (collection_id, lang) couple. False otherwise
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        :param lang: str two characters string denoting a language (e.g. 'en')
        :return: bool True if annotation is _running for (collection_id, lang) couple
        """
        return (collection_id, lang) in cls._running

    @classmethod
    def start(cls, collection_id, lang):
        """
        Annotation process for a collection using a specified language model.
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        :param lang: str two characters string denoting a language (e.g. 'en')
                     or 'multilang' for multilanguage annotations
        """
        ttype = 'collected'
        graph = tf.Graph()
        with graph.as_default():
            session = tf.Session()
            with session.as_default():
                tokenizer_path = os.path.join(cls.models_path, cls.models[lang] + '.tokenizer')
                tokenizer = sklearn.externals.joblib.load(tokenizer_path)
                tokenizer.oov_token = None
                model_path = os.path.join(cls.models_path, cls.models[lang] + '.model.h5')
                model = keras.models.load_model(model_path)

                cls.logger.info('Starting Annotation collection: {} {}'.format(collection_id, lang))

                # add tuple (collection_id, language) to `_running` list
                with cls._lock:
                    cls._running.append((collection_id, lang))

                filter_by_lang = lang if lang not in ('no_language', 'multilanguage', 'multilang') else None
                tweets = Tweet.get_iterator(collection_id, ttype, lang=filter_by_lang)
                i = 0
                for t in tweets:
                    if (collection_id, lang) in cls._stop_signals:
                        cls.logger.info('Stopping annotation {} - {}'.format(collection_id, lang))
                        with cls._lock:
                            cls._stop_signals.remove((collection_id, lang))
                        break
                    original_json = json.loads(t.tweet)
                    text = create_text_for_cnn(original_json, [])
                    sequences = tokenizer.texts_to_sequences([text])
                    data = pad_sequences(sequences, maxlen=model.layers[0].input_shape[1])
                    predictions_list = model.predict(data)
                    prediction = 1. * predictions_list[:, 1][0]
                    t.annotations = {'flood_probability': ('yes', prediction)}
                    t.ttype = 'annotated'
                    message = t.serialize()
                    cls.logger.debug('Sending annotated tweet to queue: %s', str(t))
                    cls.producer.send(cls.kafka_topic, message)
                    i += 1
                    if not (i % 1000):
                        cls.logger.info('Annotated so far.... %d', i)

        # remove from `_running` list
        cls._running.remove((collection_id, lang))
        cls.logger.info('Annotation process terminated! Collection: {} for "{}" tweets'.format(collection_id, ttype))

    @classmethod
    def stop(cls, collection_id, lang):
        """
        Stop signal for a _running annotation process. If the Annotator is not _running, operation is ignored.
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        :param lang: str two characters string denoting a language (e.g. 'en')
        """
        with cls._lock:
            if not cls.is_running_for(collection_id, lang):
                return
            cls._stop_signals.append((collection_id, lang))

    @classmethod
    def launch_in_background(cls, collection_id, lang):
        """
        Start Annotator for a collection in background (i.e. in a different thread)
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        :param lang: str two characters string denoting a language (e.g. 'en')
        """
        t = threading.Thread(target=cls.start, args=(collection_id, lang), name='Annotator {} {}'.format(collection_id, lang))
        t.start()
