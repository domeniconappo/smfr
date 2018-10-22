import os
import logging
import sys
import threading
import multiprocessing
import time

from cassandra import InvalidRequest
from cassandra.cqlengine import CQLEngineException, ValidationError
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, CommitFailedError, KafkaTimeoutError
import sklearn
from keras.models import load_model
from keras.preprocessing.sequence import pad_sequences

from smfrcore.models import Tweet
from smfrcore.utils import RUNNING_IN_DOCKER
from smfrcore.text_utils import create_text_for_cnn

try:
    from helpers import models, models_path, logger
except (ModuleNotFoundError, ImportError):
    from .helpers import models, models_path, logger

DEVELOPMENT = bool(int(os.environ.get('DEVELOPMENT', 0)))


class Annotator:
    """
    Annotator component implementation
    """
    _running = []
    _stop_signals = []
    _lock = threading.RLock()

    kafka_bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP_SERVER', 'kafka:9094') if RUNNING_IN_DOCKER else '127.0.0.1:9094'
    available_languages = list(models.keys())
    producer = None

    persister_kafka_topic = os.environ.get('PERSISTER_KAFKA_TOPIC', 'persister')
    annotator_kafka_topic = os.environ.get('ANNOTATOR_KAFKA_TOPIC', 'annotator')
    # next topic in pipeline
    geocoder_kafka_topic = os.environ.get('GEOCODER_KAFKA_TOPIC', 'geocoder')

    @classmethod
    def connect_producer(cls, kafka_server=None):
        if not kafka_server:
            kafka_server = cls.kafka_bootstrap_server
        retries = 5
        while retries >= 0:
            try:
                cls.producer = KafkaProducer(bootstrap_servers=kafka_server, retries=5,
                                             compression_type='gzip', buffer_memory=134217728,
                                             batch_size=1048576)
                logger.info('[OK] KAFKA Producer')
            except NoBrokersAvailable:
                logger.warning('Waiting for Kafka to boot...')
                time.sleep(5)
                retries -= 1
                if retries < 0:
                    logger.error('Kafka server was not listening. Exiting...')
                    sys.exit(1)
            else:
                break

    @classmethod
    def running(cls):
        """
        Return current Annotation processes as list of collection ids
        :return: List of int [id1,...,idN] for current Annotating processes.
        :rtype: list
        """
        return cls._running

    @classmethod
    def is_running_for(cls, collection_id):
        """
        Return True if annotation is running for (collection_id, lang) couple. False otherwise
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        :param lang: str two characters string denoting a language (e.g. 'en')
        :return: bool True if annotation is running for (collection_id, lang) couple
        """
        return collection_id in cls._running

    @classmethod
    def start(cls, collection_id):
        """
        Annotation process for a collection. Useful to force annotation overwriting in some cases (e.g. to test a new model)
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        """
        logger.info('>>>>>>>>>>>> Forcing Annotation for collection: %s', collection_id)
        with cls._lock:
            cls._running.append(collection_id)
        ttype = 'geotagged'
        dataset = Tweet.get_iterator(collection_id, ttype)

        for i, tweet in enumerate(dataset, start=1):
            try:
                if collection_id in cls._stop_signals:
                    logger.info('Stopping annotation %s', collection_id)
                    with cls._lock:
                        cls._stop_signals.remove(collection_id)
                    break

                if not (i % 1000):
                    logger.info('%s: Scan so far %d', lang.capitalize(), i)

                lang = tweet.lang
                if lang not in cls.available_languages or (DEVELOPMENT and lang != 'en'):
                    logger.debug('Skipping tweet %s - language %s', tweet.tweetid, lang)
                    continue

                message = Tweet.serializetuple(tweet)
                topic = '{}_{}'.format(cls.annotator_kafka_topic, lang)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Sending tweet to ANNOTATOR %s %s', lang, tweet.tweetid, )
                cls.producer.send(topic, message)
            except KafkaTimeoutError as e:
                logger.error(e)
                logger.error('Kafka has problems to allocate memory for the message: throttling')
                time.sleep(10)

        # remove from `_running` list
        with cls._lock:
            cls._running.remove(collection_id)
        logger.info('<<<<<<<<<<<<< Annotation tweets selection terminated for collection: %s', collection_id)

    @classmethod
    def annotate(cls, model, t, tokenizer):
        """

        :param model: CNN model used for prediction
        :param t: smfrcore.models.Tweet object
        :param tokenizer:
        :return:
        """
        original_json = t.original_tweet_as_dict
        text = create_text_for_cnn(original_json, [])
        sequences = tokenizer.texts_to_sequences([text])
        data = pad_sequences(sequences, maxlen=model.layers[0].input_shape[1])
        predictions_list = model.predict(data)
        flood_probability = 1. * predictions_list[:, 1][0]
        t.annotations = {'flood_probability': ('yes', flood_probability)}
        t.ttype = 'annotated'
        return t

    @classmethod
    def stop(cls, collection_id):
        """
        Stop signal for a running annotation process. If the Annotator is not running, operation is ignored.
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        :param lang: str two characters string denoting a language (e.g. 'en')
        """
        with cls._lock:
            if not cls.is_running_for(collection_id):
                return
            cls._stop_signals.append(collection_id)

    @classmethod
    def launch_in_background(cls, collection_id):
        """
        Start Annotator for a collection in background (i.e. in a different thread)
        :param collection_id: int Collection Id as it's stored in MySQL virtual_twitter_collection table
        """
        if not cls.producer:
            cls.connect_producer()
        p = multiprocessing.Process(target=cls.start, args=(collection_id,), name='Annotator collection {}'.format(collection_id))
        p.daemon = True
        p.start()

    @classmethod
    def available_models(cls):
        return {'models': models} if not DEVELOPMENT else {'models': {'en': models['en']}}

    @classmethod
    def consumer_in_background(cls, lang='en'):
        """
        Start Annotator consumer in background (i.e. in a different thread)
        :param lang: str two characters string denoting a language (e.g. 'en')
        """
        if not cls.producer:
            cls.connect_producer()
        p = multiprocessing.Process(target=cls.start_consumer, args=(lang,), name='Annotator Consumer {}'.format(lang))
        p.daemon = True
        p.start()

    @classmethod
    def start_consumer(cls, lang='en'):
        """
        Main method that iterate over messages coming from Kafka queue,
        build a Tweet object and send it to next in pipeline.
        """
        import tensorflow as tf

        session_conf = tf.ConfigProto(intra_op_parallelism_threads=1, inter_op_parallelism_threads=1)
        session = tf.Session(graph=tf.get_default_graph(), config=session_conf)

        with session.as_default():
            model, tokenizer = cls.load_annotation_model(lang)

            try:
                topic = '{}_{}'.format(cls.annotator_kafka_topic, lang)

                consumer = KafkaConsumer(
                    topic, check_crcs=False,
                    group_id='ANNOTATOR-{}'.format(lang),
                    auto_offset_reset='earliest', max_poll_records=300, max_poll_interval_ms=1000000,
                    bootstrap_servers=cls.kafka_bootstrap_server,
                    session_timeout_ms=60000, heartbeat_interval_ms=60000
                )
                logger.info('+++++++++++++ Annotator consumer lang=%s connected', lang)

                for i, msg in enumerate(consumer, start=1):
                    tweet = None
                    try:
                        msg = msg.value.decode('utf-8')
                        tweet = Tweet.from_json(msg)
                        tweet = cls.annotate(model, tweet, tokenizer)
                        message = tweet.serialize()

                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug('Sending annotated tweet to PERSISTER: %s', tweet.annotations)

                        # persist the annotated tweet
                        cls.producer.send(cls.persister_kafka_topic, message)

                        # send annotated tweet to geocoding if pipeline is enabled
                        if tweet.use_pipeline and tweet.ttype != Tweet.GEOTAGGED_TYPE:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug('Sending annotated tweet to GEOCODER: %s', tweet.annotations)
                            cls.producer.send(cls.geocoder_kafka_topic, message)

                        if not (i % 1000):
                            logger.info('%s: Annotated so far %d', lang.capitalize(), i)
                    except (ValidationError, ValueError, TypeError, InvalidRequest) as e:
                        logger.error(e)
                        logger.error('Poison message for Cassandra: %s', tweet if tweet else msg)
                    except (CQLEngineException, KafkaTimeoutError) as e:
                        logger.error(e)
                    except Exception as e:
                        logger.error(type(e))
                        logger.error(e)
                        logger.error(msg)

            except CommitFailedError:
                logger.error('Annotator consumer was disconnected during I/O operations. Exited.')
            except ValueError:
                # tipically an I/O operation on closed epoll object
                # as the consumer can be disconnected in another thread (see signal handling in start.py)
                if consumer._closed:
                    logger.info('Annotator consumer was disconnected during I/O operations. Exited.')
                else:
                    consumer.close()
            except KeyboardInterrupt:
                consumer.close()

    @classmethod
    def load_annotation_model(cls, lang):
        tokenizer_path = os.path.join(models_path, models[lang] + '.tokenizer')
        tokenizer = sklearn.externals.joblib.load(tokenizer_path)
        tokenizer.oov_token = None
        model_path = os.path.join(models_path, models[lang] + '.model.h5')
        model = load_model(model_path)
        return model, tokenizer
