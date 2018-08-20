import logging
import os
import socket
import sys

# TODO replace by launching a subprocess
# (note: it probably does not work out of the box in docker containers inside a VM...need to reconsider it)
import threading

import time

from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError, CQLEngineException
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, CommitFailedError
from mordecai import Geoparser
from shapely.geometry import Point, Polygon

from smfrcore.models.cassandramodels import Tweet
from smfrcore.models.sqlmodels import Nuts2, create_app
from smfrcore.utils import RUNNING_IN_DOCKER


logger = logging.getLogger(__name__)


class Nuts2Finder:

    """
    Simple class with a single method that returns NUTS2 id for a Point(Long, Lat).
    Warning: the method does not return NUTS2 code but the NUTS2 id as it's stored in EFAS NUTS2 table.
    TODO: Evaluate if it's better to refactor to a function instead of keeping this onemethod static class
    """

    @classmethod
    def _is_in_poly(cls, point, geo):
        poly = Polygon(geo)
        return point.within(poly)

    @classmethod
    def find_nuts2(cls, lat, lon):
        """
        Check if a point (lat, lon) is in a NUTS2 region and returns its id. None otherwise.
        :param lat: Latitude of a point
        :rtype lat: float
        :param lon: Longitute of a point
        :rtype lon: float
        :return: Nuts2 object

        """
        point = Point(float(lon), float(lat))
        nuts2_candidates = Nuts2.get_nuts2(lat, lon)
        logger.debug('Returned %d nuts2 geometries to check', len(nuts2_candidates))

        for nuts2 in nuts2_candidates:
            geometry = nuts2.geometry[0]
            try:
                if cls._is_in_poly(point, geometry):
                    return nuts2
            except ValueError:
                for subgeometry in geometry:
                    if cls._is_in_poly(point, subgeometry):
                        return nuts2

        logger.debug('No NUTS2 polygons is containing the point %s', str(point))
        return None


class Geocoder:
    """
    Class implementing the Geocoder component
    """

    _running = []
    stop_signals = []
    _lock = threading.RLock()
    logger = logging.getLogger(__name__)
    geonames_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'geonames'
    kafka_bootstrap_server = '{}:9092'.format('kafka' if RUNNING_IN_DOCKER else '127.0.0.1')

    flask_app = create_app()

    # FIXME duplicated code (same as Annotator)
    # TODO Need a class in shared_lib where to put common code for microservices like this
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

    min_flood_prob = float(os.environ.get('MIN_FLOOD_PROBABILITY', 0.59))
    persister_kafka_topic = os.environ.get('PERSISTER_KAFKA_TOPIC', 'persister')
    geocoder_kafka_topic = os.environ.get('GEOCODER_KAFKA_TOPIC', 'geocoder')

    consumer = KafkaConsumer(geocoder_kafka_topic, group_id='GEOCODER',
                             auto_offset_reset='earliest',
                             bootstrap_servers=kafka_bootstrap_server,
                             session_timeout_ms=30000, heartbeat_interval_ms=10000)
    # try to use new mordecai with 'threads'
    tagger = Geoparser(geonames_host)

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
        Start a new thread with main geocoding method `start` as target method
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        """
        t = threading.Thread(target=cls.start, args=(collection_id,),
                             name='Geocoder collection id: {}'.format(collection_id))
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
    def geoparse_tweet(cls, tweet):
        """
        Mordecai geoparsing
        :param tweet: smfrcore.models.cassandramodels.Tweet object
        :return: list of tuples of lat/lon coordinates
        """
        # try to geoparse
        latlong_list = []
        res = []
        retries = 0
        es_up = False
        while not es_up and retries <= 10:
            try:
                res = cls.tagger.geoparse(tweet.full_text)
            except socket.timeout:
                logger.warning('ES not responding...throttling a bit')
                time.sleep(3)
                retries += 1
            else:
                es_up = True
        # FIXME: hard coded minimum country confidence from mordecai
        for result in res:
            if result.get('country_conf', 0) < 0.4 or 'lat' not in result.get('geo', {}):
                continue
            latlong_list.append((float(result['geo']['lat']), float(result['geo']['lon'])))
        return latlong_list

    @classmethod
    def get_coordinates_from_tweet(cls, tweet):
        """

        :param tweet:
        :return:
        """
        t = tweet.original_tweet_as_dict
        latlong = None
        if t.get('coordinates') or t.get('geo'):
            coords = t.get('coordinates', {}).get('coordinates') or t.get('geo', {}).get('coordinates')
            if coords:
                latlong = coords[1], coords[0]
        return latlong

    @classmethod
    def find_nuts_heuristic(cls, tweet):
        """
        The following heuristic is applied:

        #1 First, a gazetteer is run on the tweet to find location mentions

        #2 If no location mention is found:
            If the tweet contains (longitude, latitude):
                the NUTS2 area containing that (longitude, latitude) is returned (nuts2source="coordinates")
            Otherwise, NULL is returned
        #3 If location mentions mapping to a list of NUTS2 areas are found:
            If the tweet contains (longitude, latitude), then if any of the NUTS2 areas contain that point, that NUTS2
            area is returned (nuts2source="coordinates-and-mentions")
            Otherwise
                If there is a single NUTS2 area in the list, that NUTS2 area is returned (nuts2source="mentions")
                Otherwise, check if user location is in one of the NUTS list and return it. If not, NULL is returned

        :param tweet: Tweet object
        :return: tuple (nuts2, nuts_source, coordinates)
        """
        # TODO refactor to use shorter private methods

        with cls.flask_app.app_context():
            no_results = (None, None, None)
            mentions = cls.geoparse_tweet(tweet)
            tweet_coords = cls.get_coordinates_from_tweet(tweet)

            if not mentions:
                if tweet_coords:
                    nuts2 = Nuts2Finder.find_nuts2(*tweet_coords)
                    if nuts2:
                        coordinates = tweet_coords
                        nuts_source = 'coordinates'
                        logger.debug('Found Nuts from tweet geo... - coordinates')
                        return nuts2, nuts_source, coordinates
                return no_results
            else:
                if tweet_coords and len(mentions) > 1:
                    nuts2_from_tweet = Nuts2Finder.find_nuts2(*tweet_coords)
                    for latlong in mentions:
                        # checking the list of mentioned places coordinates
                        nuts2 = Nuts2Finder.find_nuts2(*latlong)
                        if nuts2 == nuts2_from_tweet:
                            coordinates = latlong
                            nuts_source = 'coordinates-and-mentions'
                            logger.debug('Found Nuts from tweet geo and mentions... - coordinates-and-mentions')
                            return nuts2, nuts_source, coordinates
                    return no_results
                else:
                    if len(mentions) == 1:
                        coordinates = mentions[0]
                        nuts2 = Nuts2Finder.find_nuts2(*coordinates)
                        if nuts2:
                            nuts_source = 'mentions'
                            logger.debug('Found Nuts... - Exactly one mention')
                            return nuts2, nuts_source, coordinates

                        return no_results
                    else:
                        # no geolocated tweet and more than one mention
                        user_location = tweet.original_tweet_as_dict['user'].get('location')
                        res = cls.tagger.geoparse(user_location) if user_location else None
                        if res and res[0] and 'lat' in res[0].get('geo', {}):
                            res = res[0]
                            user_coordinates = (float(res['geo']['lat']), float(res['geo']['lon']))
                            user_nuts2 = Nuts2Finder.find_nuts2(*user_coordinates)
                            for latlong in mentions:
                                # checking the list of mentioned places coordinates
                                nuts2 = Nuts2Finder.find_nuts2(*latlong)
                                if nuts2 == user_nuts2:
                                    coordinates = latlong
                                    nuts_source = 'mentions-and-user'
                                    logger.debug('Found Nuts... - User location')
                                    return nuts2, nuts_source, coordinates
                        return no_results

    @classmethod
    def start(cls, collection_id):
        """
        Main Geocoder method. It's usually executed in a background thread.
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        """
        logger.info('Starting Geocoding for collection: {}'.format(collection_id))
        with cls._lock:
            cls._running.append(collection_id)

        ttype = 'annotated'
        dataset = Tweet.get_iterator(collection_id, ttype)

        for i, tweet in enumerate(dataset, start=1):
            if collection_id in cls.stop_signals:
                logger.info('Stopping Geocoding process {}'.format(collection_id))
                with cls._lock:
                    cls.stop_signals.remove(collection_id)
                break

            message = Tweet.serializetuple(tweet)
            cls.producer.send(cls.geocoder_kafka_topic, message)

            # remove from `_running` list
        cls._running.remove(collection_id)
        logger.info('Geocoding process terminated! Collection: %s', collection_id)

    @classmethod
    def set_geo_fields(cls, latlong, nuts2_source, nutsitem, t):
        t.ttype = 'geotagged'
        t.latlong = latlong
        t.nuts2 = str(nutsitem.id) if nutsitem and nutsitem.id is not None else None
        t.nuts2source = nuts2_source
        t.geo = {
            'nuts_efas_id': str(nutsitem.id) if nutsitem and nutsitem.id is not None else '',
            'nuts_id': str(nutsitem.nuts_id) if nutsitem and nutsitem.nuts_id is not None else '',
            'nuts_source': nuts2_source or '',
            'latitude': str(latlong[0]),
            'longitude': str(latlong[1]),
            'country': nutsitem.country if nutsitem and nutsitem.country else '',
            'country_code': nutsitem.country_code if nutsitem and nutsitem.country_code else '',
            'efas_name': nutsitem.efas_name if nutsitem and nutsitem.efas_name else '',
        }

    @classmethod
    def consumer_in_background(cls):
        """
        Start Geocoder consumer in background (i.e. in a different thread)
        """
        t = threading.Thread(target=cls.start_consumer, name='Geocoder Consumer')
        t.start()

    @classmethod
    def start_consumer(cls):
        logger.info('+++++++++++++ Geocoder consumer starting')
        try:
            for i, msg in enumerate(cls.consumer):
                tweet = None
                errors = 0
                try:
                    msg = msg.value.decode('utf-8')
                    tweet = Tweet.build_from_kafka_message(msg)
                    logger.debug('Read from queue: %s', str(tweet))
                    try:
                        # COMMENT OUT CODE BELOW: we will geolocate everything for the moment
                        # flood_prob = t.annotations.get('flood_probability', ('', 0.0))[1]
                        # if flood_prob <= cls.min_flood_prob:
                        #     continue

                        nutsitem, nuts2_source, latlong = cls.find_nuts_heuristic(tweet)
                        if not latlong:
                            continue

                        cls.set_geo_fields(latlong, nuts2_source, nutsitem, tweet)
                        message = tweet.serialize()
                        logger.debug('Send geocoded tweet to persister: %s', str(tweet))
                        cls.producer.send(cls.persister_kafka_topic, message)

                    except Exception as e:
                        logger.error(type(e))
                        logger.error('An error occured during geotagging: %s', str(e))
                        errors += 1
                        if errors >= 500:
                            logger.error('Too many errors...going to terminate geolocalization')
                            cls.consumer.close()
                            break
                        continue
                except (ValidationError, ValueError, TypeError, InvalidRequest) as e:
                    logger.error(e)
                    logger.error('Poison message for Cassandra: %s', str(tweet) if tweet else msg)
                except CQLEngineException as e:
                    logger.error(e)
                except Exception as e:
                    logger.error(type(e))
                    logger.error(e)
                    logger.error(msg)

        except CommitFailedError:
            logger.error('Geocoder consumer was disconnected during I/O operations. Exited.')
        except ValueError:
            # tipically an I/O operation on closed epoll object
            # as the consumer can be disconnected in another thread (see signal handling in start.py)
            if cls.consumer._closed:
                logger.info('Geocoder consumer was disconnected during I/O operations. Exited.')
            else:
                cls.consumer.close()
        except KeyboardInterrupt:
            cls.consumer.close()
