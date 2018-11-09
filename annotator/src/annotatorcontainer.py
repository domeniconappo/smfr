import os
import logging
import sys
import multiprocessing
import time

from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, CommitFailedError, KafkaTimeoutError
# import sklearn
# from keras.models import load_model
# from keras.preprocessing.sequence import pad_sequences

from smfrcore.models import Tweet
from smfrcore.utils import IN_DOCKER
# from smfrcore.text_utils import create_text_for_cnn
from smfrcore.ml.helpers import models, logger, available_languages
from smfrcore.ml.annotator import Annotator

# try:
#     from helpers import models, models_path, logger
# except (ModuleNotFoundError, ImportError):
#     from .helpers import models, models_path, logger

DEVELOPMENT = bool(int(os.getenv('DEVELOPMENT', 0)))


class AnnotatorContainer:
    """
    Annotator component implementation
    """
    _running = []
    _stop_signals = []
    _lock = multiprocessing.RLock()
    _manager = multiprocessing.Manager()
    shared_counter = _manager.dict()

    kafka_bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'kafka:9092') if IN_DOCKER else '127.0.0.1:9092'
    producer = None

    persister_kafka_topic = os.getenv('PERSISTER_KAFKA_TOPIC', 'persister')
    annotator_kafka_topic = os.getenv('ANNOTATOR_KAFKA_TOPIC', 'annotator')

    @classmethod
    def connect_producer(cls, kafka_server=None):
        if not kafka_server:
            kafka_server = cls.kafka_bootstrap_server
        retries = 5
        while retries >= 0:
            try:
                cls.producer = KafkaProducer(bootstrap_servers=kafka_server, retries=5, max_block_ms=120000,
                                             compression_type='gzip', buffer_memory=134217728,
                                             linger_ms=500, batch_size=1048576,)
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
                lang = tweet.lang
                if not (i % 1000):
                    logger.info('%s: Scan so far %d', lang.capitalize(), i)

                if lang not in available_languages or (DEVELOPMENT and lang != 'en'):
                    logger.debug('Skipping tweet %s - language %s', tweet.tweetid, lang)
                    continue

                message = Tweet.serializetuple(tweet)
                topic = '{}-{}'.format(cls.annotator_kafka_topic, lang)
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
        p.start()

    @classmethod
    def available_models(cls):
        return {'models': models} if not DEVELOPMENT else {'models': {'en': models['en']}}

    @classmethod
    def counters(cls):
        return dict(cls.shared_counter)

    @classmethod
    def consumer_in_background(cls, lang='en'):
        """
        Start Annotator consumer in background (i.e. in a different thread)
        :param lang: str two characters string denoting a language (e.g. 'en')
        """
        if not cls.producer:
            cls.connect_producer()
        p = multiprocessing.Process(target=cls.start_consumer, args=(lang,), name='Annotator Consumer {}'.format(lang))
        p.start()

    # @classmethod
    # def annotate(cls, model, tweets, tokenizer):
    #     """
    #     Annotate the tweet t using model and tokenizer
    #
    #     :param model: CNN model used for prediction
    #     :param tweets: list of smfrcore.models.Tweet objects
    #     :param tokenizer:
    #     :return:
    #     """
    #     texts = (create_text_for_cnn(t.original_tweet_as_dict, []) for t in tweets)
    #     sequences = tokenizer.texts_to_sequences(texts)
    #     data = pad_sequences(sequences, maxlen=model.layers[0].input_shape[1])
    #     predictions_list = model.predict(data)
    #     res = []
    #     predictions = predictions_list[:, 1]
    #     for i, t in enumerate(tweets):
    #         flood_probability = 1. * predictions[i]
    #         t.annotations = {'flood_probability': ('yes', flood_probability)}
    #         t.ttype = Tweet.ANNOTATED_TYPE
    #         res.append(t)
    #     return res

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
            model, tokenizer = Annotator.load_annotation_model(lang)

            try:
                topic = '{}-{}'.format(cls.annotator_kafka_topic, lang)

                consumer = KafkaConsumer(
                    topic, check_crcs=False,
                    group_id='ANNOTATOR-{}'.format(lang),
                    auto_offset_reset='earliest',
                    max_poll_records=300, max_poll_interval_ms=1000000,
                    bootstrap_servers=cls.kafka_bootstrap_server,
                    session_timeout_ms=10000, heartbeat_interval_ms=3000
                )
                logger.info('+++++++++++++ Annotator consumer lang=%s connected', lang)
                cls.shared_counter[lang] = 0
                cls.shared_counter['waiting-{}'.format(lang)] = 0
                buffer_to_annotate = []

                for i, msg in enumerate(consumer, start=1):
                    tweet = None
                    try:
                        msg = msg.value.decode('utf-8')
                        tweet = Tweet.from_json(msg)
                        buffer_to_annotate.append(tweet)
                        cls.shared_counter['waiting-{}'.format(lang)] += 1

                        if len(buffer_to_annotate) >= 100:
                            tweets = Annotator.annotate(model, buffer_to_annotate, tokenizer)
                            cls.shared_counter[lang] += len(buffer_to_annotate)

                            for tweet in tweets:
                                message = tweet.serialize()

                                if logger.isEnabledFor(logging.DEBUG):
                                    logger.debug('Sending annotated tweet to PERSISTER: %s', tweet.annotations)

                                # persist the annotated tweet
                                sent_to_persister = False
                                while not sent_to_persister:
                                    try:
                                        cls.producer.send(cls.persister_kafka_topic, message)
                                    except KafkaTimeoutError as e:
                                        # try to mitigate kafka timeout error
                                        # KafkaTimeoutError: Failed to allocate memory
                                        # within the configured max blocking time
                                        logger.error(e)
                                        time.sleep(2)
                                    else:
                                        sent_to_persister = True

                            buffer_to_annotate.clear()
                            cls.shared_counter['waiting-{}'.format(lang)] = 0

                    except (ValidationError, ValueError, TypeError, InvalidRequest) as e:
                        logger.error(e)
                        logger.error('Poison message for Cassandra: %s', tweet if tweet else msg)
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

    # @classmethod
    # def load_annotation_model(cls, lang):
    #     tokenizer_path = os.path.join(models_path, models[lang] + '.tokenizer')
    #     tokenizer = sklearn.externals.joblib.load(tokenizer_path)
    #     tokenizer.oov_token = None
    #     model_path = os.path.join(models_path, models[lang] + '.model.h5')
    #     model = load_model(model_path)
    #     return model, tokenizer
