import logging
import os
import multiprocessing
import time

from kafka.errors import CommitFailedError, KafkaTimeoutError

from smfrcore.models.cassandra import Tweet
from smfrcore.utils import DefaultDictSyncManager, DEFAULT_HANDLER
from smfrcore.utils.kafka import make_kafka_consumer, make_kafka_producer, send_to_persister
from sqlalchemy.exc import StatementError, InvalidRequestError

logger = logging.getLogger('GEOCODER')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)


class GeocoderContainer:
    """
    Class implementing the Geocoder component
    """

    _running = []
    stop_signals = []
    _lock = multiprocessing.RLock()
    _manager = DefaultDictSyncManager()
    _manager.start()
    _counters = _manager.defaultdict(int)

    geocoder_kafka_topic = os.getenv('GEOCODER_KAFKA_TOPIC', 'geocoder')

    @classmethod
    def counters(cls):
        return dict(cls._counters)

    @classmethod
    def is_running_for(cls, collection_id):
        """
        Return True if Geocoding is running for collection_id
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        :return: True if Geocoding is active for the given collection, False otherwise
        :rtype: bool
        """
        return collection_id in cls._running

    @classmethod
    def running(cls):
        """
        Return the list of current collection ids under geocoding
        :return: Running Geocoding processes
        :rtype: list
        """
        return cls._running

    @classmethod
    def launch_in_background(cls, collection_id):
        """
        Start a new process with main geocoding method `start` as target method
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        """
        t = multiprocessing.Process(target=cls.start, args=(collection_id,),
                                    name='Geocoder {}'.format(collection_id))
        t.start()

    @classmethod
    def stop(cls, collection_id):
        """
        Send a stop signal for a running Geocoding process
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        """
        with cls._lock:
            if not cls.is_running_for(collection_id):
                return
            cls.stop_signals.append(collection_id)

    @classmethod
    def consumer_in_background(cls):
        """
        Start Geocoder consumer in background (i.e. in a different thread)
        """
        p = multiprocessing.Process(target=cls.start_consumer, name='Geocoder Consumer')
        p.daemon = True
        p.start()
        return p

    @classmethod
    def start(cls, collection_id):
        """
        Main Geocoder method. It's usually executed in a background thread.
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        """
        logger.info('>>>>>>>>>>>> Starting Geocoding tweets selection for collection: %d', collection_id)
        with cls._lock:
            cls._running.append(collection_id)

        dataset = Tweet.get_iterator(collection_id, Tweet.ANNOTATED_TYPE)
        producer = make_kafka_producer()
        for i, tweet in enumerate(dataset, start=1):
            try:
                if collection_id in cls.stop_signals:
                    logger.info('Stopping Geocoding process %d', collection_id)
                    with cls._lock:
                        cls.stop_signals.remove(collection_id)
                    break

                message = Tweet.serializetuple(tweet)
                producer.send(cls.geocoder_kafka_topic, message)
            except KafkaTimeoutError as e:
                logger.error(e)
                logger.error('Kafka has problems to allocate memory for the message: throttling')
                time.sleep(10)

        # remove from `_running` list
        with cls._lock:
            cls._running.remove(collection_id)
        logger.info('<<<<<<<<<<<<< Geocoding tweets selection terminated for collection: %s', collection_id)

    @classmethod
    def start_consumer(cls):
        """
        """
        from smfrcore.models.sql import create_app
        from smfrcore.geocoding.geocoder import Geocoder
        flask_app = create_app()
        producer = make_kafka_producer()
        consumer = make_kafka_consumer(topic=cls.geocoder_kafka_topic)
        try:
            flask_app.app_context().push()
            errors = 0
            geocoder = Geocoder()
            logger.info('[OK] +++++++++++++ Geocoder consumer starting')
            for i, msg in enumerate(consumer, start=1):
                msg = msg.value.decode('utf-8')
                tweet = Tweet.from_json(msg)
                try:
                    # COMMENT OUT CODE BELOW: we will geolocate everything for the moment
                    # flood_prob = t.annotations.get('flood_probability', ('', 0.0))[1]
                    # if flood_prob <= cls.min_flood_prob:
                    #     continue
                    coordinates, nuts2, nuts_source, country_code, place, geonameid = geocoder.find_nuts_heuristic(tweet)
                    if not coordinates:
                        logger.debug('No coordinates for %s...skipping', tweet.tweetid)
                        continue
                    tweet.set_geo(coordinates, nuts2, nuts_source, country_code, place, geonameid)

                    # persist the geotagged tweet
                    send_to_persister(producer, tweet)
                    cls._counters[tweet.lang] += 1

                    if not (i % 1000) and logger.isEnabledFor(logging.INFO):
                        logger.info('Geotagged so far %d', i)
                except (StatementError, InvalidRequestError) as e:
                    logger.error(e)
                    continue
                except Exception as e:
                    logger.error(type(e))
                    logger.error('An error occured during geotagging: %s', e)
                    errors += 1
                    if errors >= 500:
                        logger.error('Too many errors...going to terminate geocoding')
                        consumer.close(30)
                        producer.close(30)
                        break
                    continue
        except ConnectionError:
            logger.error('ES Gazetter is not responding. Exited.')
        except CommitFailedError:
            logger.error('Geocoder consumer was disconnected during I/O operations. Exited.')
        except ValueError:
            # tipically an I/O operation on closed epoll object
            # as the consumer can be disconnected in another thread (see signal handling in start.py)
            if consumer._closed:
                logger.info('Geocoder consumer was disconnected during I/O operations. Exited.')
            else:
                consumer.close()
        except KeyboardInterrupt:
            consumer.close()

        logger.warning('Geocoder Consumer disconnected.')
        if not producer._closed:
            producer.close(30)
        if not consumer._closed:
            consumer.close(30)
