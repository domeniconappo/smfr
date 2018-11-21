import copy
import os
import logging
import multiprocessing
import threading
import time

import schedule
from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError
from kafka.errors import CommitFailedError, KafkaTimeoutError

from smfrcore.models.cassandra import Tweet
from smfrcore.utils import run_continuously
from smfrcore.utils.kafka import make_kafka_consumer, make_kafka_producer
from smfrcore.ml.helpers import models, logger, available_languages
from smfrcore.ml.annotator import Annotator


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

    persister_kafka_topic = os.getenv('PERSISTER_KAFKA_TOPIC', 'persister')
    annotator_kafka_topic = os.getenv('ANNOTATOR_KAFKA_TOPIC', 'annotator')

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
        producer = make_kafka_producer()
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
                producer.send(topic, message)
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
        p = multiprocessing.Process(target=cls.start, args=(collection_id,),
                                    name='Annotator collection {}'.format(collection_id))
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
        p = multiprocessing.Process(target=cls.start_consumer, args=(lang,), name='Annotator-{}'.format(lang))
        p.daemon = True
        p.start()

    @classmethod
    def consume_buffer(cls, buffer, lang, lock):
        """
        Actual method, executed as a scheduled thread, that performs annotation on a buffer of collected tweets
        :param buffer: list of tweets objects
        :param lang: the lang defining the current annotator process
        :param lock: a lock to safely modify the buffer (it can be modified by the main kafka consumer thread)
        """
        with lock:
            tweets_to_annotate = copy.deepcopy(buffer)
            buffer.clear()

        if not tweets_to_annotate:
            return

        import tensorflow as tf
        session_conf = tf.ConfigProto(intra_op_parallelism_threads=1, inter_op_parallelism_threads=1)
        session = tf.Session(graph=tf.get_default_graph(), config=session_conf)

        with session.as_default():
            logger.info('Annotating buffer for: %s', lang)
            try:
                model, tokenizer = Annotator.load_annotation_model(lang)
                tweets = Annotator.annotate(model, tweets_to_annotate, tokenizer)
            except Exception as e:
                logger.error(lang)
                logger.error(type(e))
                logger.error(e)
                with lock:
                    # restore entire buffer in case of an error in annotation
                    buffer += tweets_to_annotate
                    return

        producer = make_kafka_producer()
        for tweet in tweets:
            message = tweet.serialize()

            # persist the annotated tweet
            sent_to_persister = False
            while not sent_to_persister:
                try:
                    producer.send(cls.persister_kafka_topic, message)
                except KafkaTimeoutError as e:
                    # try to mitigate kafka timeout error
                    # KafkaTimeoutError: Failed to allocate memory
                    # within the configured max blocking time
                    logger.error(e)
                    time.sleep(3)
                except Exception as e:
                    logger.error(type(e))
                    logger.error(e)
                    logger.error(message)
                else:
                    sent_to_persister = True
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Sent annotated tweet to PERSISTER: %s', tweet.annotations)

        cls.shared_counter[lang] += len(tweets_to_annotate)
        cls.shared_counter['buffered-{}'.format(lang)] = 0

    @classmethod
    def start_consumer(cls, lang='en'):
        """
        Main method that iterate over messages coming from Kafka queue,
        build a Tweet object to annotate (and save it in a buffer) and send it to next in pipeline (geocoder)
        """
        consumer = make_kafka_consumer(topic='{}-{}'.format(cls.annotator_kafka_topic, lang))
        buffer_to_annotate = []
        lock = threading.RLock()

        # put some interleaving to avoid starting threads at same time and start randomly between 1 and 5 minutes
        interval = sum(ord(c) for c in lang)
        schedule.every().to(5).minutes.do(
            cls.consume_buffer,
            *(buffer_to_annotate, lang, lock)
        ).tag('annotate-buffer')

        run_continuously(interval=interval)

        try:
            cls.shared_counter[lang] = 0
            cls.shared_counter['buffered-{}'.format(lang)] = 0

            for i, msg in enumerate(consumer, start=1):
                tweet = None
                try:
                    msg = msg.value.decode('utf-8')
                    tweet = Tweet.from_json(msg)
                    with lock:
                        buffer_to_annotate.append(tweet)
                    cls.shared_counter['buffered-{}'.format(lang)] += 1

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
        finally:
            cls.consume_buffer(buffer_to_annotate, lang, lock, producer)
