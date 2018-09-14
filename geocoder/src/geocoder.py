import logging
import os
import socket
import sys
import tarfile

import ujson

# TODO replace by launching a subprocess
# (note: it probably does not work out of the box in docker containers inside a VM...need to reconsider it)
import threading

import time

from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError, CQLEngineException
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, CommitFailedError, KafkaTimeoutError
from mordecai import Geoparser
from shapely.geometry import Point, Polygon, MultiPolygon

from smfrcore.models import Tweet, Nuts2, create_app
from smfrcore.utils import RUNNING_IN_DOCKER


logger = logging.getLogger('GEOCODER')
logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))


class Nuts2Finder:

    """
    Helper class with Nuts2 methods for finding Nuts2 and countries
    Warning: the method does not return NUTS2 code but the NUTS2 id as it stored in EFAS NUTS2 table.
    """
    with tarfile.open('/config/countries.json.tar.gz', 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        countries = ujson.load(init_f)

    @classmethod
    def _is_in_poly(cls, point, geo):
        poly = Polygon(geo)
        return point.within(poly)

    @classmethod
    def find_nuts2_by_point(cls, lat, lon):
        """
        Check if a point (lat, lon) is in a NUTS2 region and returns its id. None otherwise.
        :param lat: Latitude of a point
        :rtype lat: float
        :param lon: Longitute of a point
        :rtype lon: float
        :return: Nuts2 object

        """
        if lat is None or lon is None:
            return None

        lat, lon = float(lat), float(lon)
        point = Point(lon, lat)
        nuts2_candidates = Nuts2.get_nuts2(lat, lon)

        for nuts2 in nuts2_candidates:
            geometry = nuts2.geometry[0]
            try:
                if cls._is_in_poly(point, geometry):
                    return nuts2
            except ValueError:
                for subgeometry in geometry:
                    if cls._is_in_poly(point, subgeometry):
                        return nuts2

        return None

    @classmethod
    def find_country(cls, code):
        """
        Return country name based on country code
        :param code: Country code ISO3
        :return: tuple<str, bool> (country name, is european)
        """
        res = cls.countries.get(code)
        if res:
            return res['name'], res.get('continent') == 'EU'
        nuts2 = Nuts2.by_country_code(code)
        # get the first item with country field populated
        for nut in nuts2:
            if nut.country:
                return nut.country, True
        return '', False

    @classmethod
    def find_nuts2_by_name(cls, user_location):
        return Nuts2.query.filter_by(efas_name=user_location).first()


class Geocoder:
    """
    Class implementing the Geocoder component
    """

    _running = []
    stop_signals = []
    _lock = threading.RLock()
    logger = logging.getLogger(__name__)
    geonames_host = 'geonames' if RUNNING_IN_DOCKER else '127.0.0.1'
    kafka_bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP_SERVER', 'kafka:9094') if RUNNING_IN_DOCKER else '127.0.0.1:9094'

    flask_app = create_app()

    # FIXME duplicated code (same as Annotator)
    # TODO Need a class in shared_lib where to put common code for microservices like this
    retries = 5
    persister_kafka_topic = os.environ.get('PERSISTER_KAFKA_TOPIC', 'persister')
    geocoder_kafka_topic = os.environ.get('GEOCODER_KAFKA_TOPIC', 'geocoder')

    while retries >= 0:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server, retries=5,
                                     compression_type='gzip', buffer_memory=134217728,
                                     batch_size=1048576)
            logger.info('[OK] KAFKA Producer')
            consumer = KafkaConsumer(geocoder_kafka_topic,
                                     group_id='GEOCODER',
                                     check_crcs=False,
                                     max_poll_records=100, max_poll_interval_ms=600000,
                                     auto_offset_reset='earliest',
                                     bootstrap_servers=kafka_bootstrap_server,
                                     session_timeout_ms=40000, heartbeat_interval_ms=15000)
        except NoBrokersAvailable:
            logger.warning('Waiting for Kafka to boot...retry')
            time.sleep(5)
            retries -= 1
            if retries < 0:
                sys.exit(1)
        else:
            break

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
    def start(cls, collection_id):
        """
        Main Geocoder method. It's usually executed in a background thread.
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        """
        logger.info('>>>>>>>>>>>> Starting Geocoding tweets selection for collection: %d', collection_id)
        with cls._lock:
            cls._running.append(collection_id)

        ttype = 'annotated'
        dataset = Tweet.get_iterator(collection_id, ttype)

        for i, tweet in enumerate(dataset, start=1):
            try:
                if collection_id in cls.stop_signals:
                    logger.info('Stopping Geocoding process %d', collection_id)
                    with cls._lock:
                        cls.stop_signals.remove(collection_id)
                    break

                message = Tweet.serializetuple(tweet)
                cls.producer.send(cls.geocoder_kafka_topic, message)
            except KafkaTimeoutError as e:
                logger.error(e)
                logger.error('Kafka has problems to allocate memory for the message: throttling')
                time.sleep(10)

            # remove from `_running` list
        with cls._lock:
            cls._running.remove(collection_id)
        logger.info('<<<<<<<<<<<<< Geocoding tweets selection terminated for collection: %s', collection_id)

    @classmethod
    def consumer_in_background(cls):
        """
        Start Geocoder consumer in background (i.e. in a different thread)
        """
        t = threading.Thread(target=cls.start_consumer, name='Geocoder Consumer')
        t.start()

    @classmethod
    def geoparse_tweet(cls, tweet, tagger):
        """
        Mordecai geoparsing
        :param tagger: mordecai.Geoparser instance
        :param tweet: smfrcore.models.cassandra.Tweet object
        :return: list of tuples of lat/lon/country_code/place_name
        """
        # try to geoparse
        mentions_list = []
        res = []
        retries = 0
        es_up = False
        while not es_up and retries <= 10:
            try:
                res = tagger.geoparse(tweet.full_text)
            except socket.timeout:
                logger.warning('ES not responding...throttling a bit')
                time.sleep(5)
                retries += 1
            else:
                es_up = True
        # FIXME: hard coded minimum country confidence from mordecai

        for result in res:
            if result.get('country_conf', 0) < 0.5 or 'lat' not in result.get('geo', {}):
                continue
            mentions_list.append((float(result['geo']['lat']),
                                  float(result['geo']['lon']),
                                  result['geo'].get('country_code3', ''),
                                  result['geo'].get('place_name', ''),
                                  result['geo'].get('geonameid', '')))
        return mentions_list


    @classmethod
    def find_nuts_heuristic(cls, tweet, tagger):
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

        :param tagger: mordecai.Geoparser instance
        :param tweet: Tweet object
        :return: tuple (coordinates, nuts2, nuts_source, country_code, place, geonameid)
        """

        mentions = cls.geoparse_tweet(tweet, tagger)
        tweet_coords = tweet.coordinates if tweet.coordinates != (None, None) else tweet.centroid
        user_location = tweet.user_location
        coordinates, nuts2, nuts_source, country_code, place, geonameid = None, None, '', '', '', ''

        if tweet_coords != (None, None):
            coordinates, nuts2, nuts_source, country_code, place, geonameid = cls._nuts_from_tweet_coords(mentions, tweet_coords)
        elif len(mentions) == 1:
            coordinates, nuts2, nuts_source, country_code, place, geonameid = cls._nuts_from_one_mention(mentions)
        elif user_location:
            # no geolocated tweet and one or more mentions...try to get location from user
            coordinates, nuts2, nuts_source, country_code, place, geonameid = cls._nuts_from_user_location(mentions, tagger, user_location)

        return coordinates, nuts2, nuts_source, country_code, place, geonameid

    @classmethod
    def _nuts_from_user_location(cls, mentions, tagger, user_location):
        coordinates, nuts2, nuts_source, country_code, place, geonameid = None, None, '', '', '', ''
        res = tagger.geoparse(user_location)
        if res and res[0] and 'lat' in res[0].get('geo', {}):
            res = res[0]
            user_coordinates = (float(res['geo']['lat']), float(res['geo']['lon']))
            nuts2user = Nuts2Finder.find_nuts2_by_point(*user_coordinates) or Nuts2Finder.find_nuts2_by_name(user_location)

            if not nuts2user:
                coordinates = user_coordinates
                nuts_source = 'user'
                country_code = res['geo'].get('country_code3', '')
                place = user_location
                geonameid = res['geo'].get('geonameid', '')
                return coordinates, nuts2, nuts_source, country_code, place, geonameid

            for mention in mentions:
                # checking the list of mentioned places coordinates
                latlong = mention[0], mention[1]
                nuts2mentions = Nuts2Finder.find_nuts2_by_point(*latlong)
                if nuts2mentions and nuts2mentions.id == nuts2user.id:
                    coordinates = latlong
                    nuts_source = 'mentions-and-user'
                    nuts2 = nuts2mentions
                    country_code = mention[2]
                    place = mention[3]
                    geonameid = mention[4]
                    break
        return coordinates, nuts2, nuts_source, country_code, place, geonameid

    @classmethod
    def _nuts_from_one_mention(cls, mentions):
        nuts2 = None
        mention = mentions[0]
        coordinates = mention[0], mention[1]
        country_code = mention[2]
        place = mention[3]
        geonameid = mention[4]
        nuts_source = 'mentions'
        nuts2mention = Nuts2Finder.find_nuts2_by_point(*coordinates)
        if nuts2mention:
            nuts2 = nuts2mention
        return coordinates, nuts2, nuts_source, country_code, place, geonameid

    @classmethod
    def _nuts_from_tweet_coords(cls, mentions, tweet_coords):
        nuts2, country_code, place, geonameid = None, '', '', ''

        coordinates = tweet_coords
        nuts_source = 'coordinates'

        nuts2tweet = Nuts2Finder.find_nuts2_by_point(*tweet_coords)

        if not nuts2tweet:
            return coordinates, nuts2, nuts_source, country_code, place, geonameid

        if not mentions:
            nuts2 = nuts2tweet
        elif len(mentions) > 1:
            for mention in mentions:
                # checking the list of mentioned places coordinates
                latlong = mention[0], mention[1]
                nuts2mention = Nuts2Finder.find_nuts2_by_point(*latlong)
                if nuts2mention and nuts2tweet.id == nuts2mention.id:
                    coordinates = latlong
                    nuts2 = nuts2tweet
                    nuts_source = 'coordinates-and-mentions'
                    country_code = mention[2]
                    place = mention[3]
                    geonameid = mention[4]
        elif len(mentions) == 1:
            coordinates, nuts2, nuts_source, country_code, place, geonameid = cls._nuts_from_one_mention(mentions)
        return coordinates, nuts2, nuts_source, country_code, place, geonameid

    @classmethod
    def set_geo_fields(cls, latlong, nuts2, nuts2_source, country_code, place, geonameid, tweet):
        tweet.ttype = 'geotagged'
        tweet.latlong = latlong
        tweet.nuts2 = str(nuts2.efas_id) if nuts2 else None
        tweet.nuts2source = nuts2_source
        country, is_european = Nuts2Finder.find_country(country_code)
        tweet.geo = {
            'nuts_efas_id': str(nuts2.efas_id) if nuts2 else '',
            'nuts_id': nuts2.nuts_id if nuts2 and nuts2.nuts_id is not None else '',
            'nuts_source': nuts2_source or '',
            'latitude': str(latlong[0]),
            'longitude': str(latlong[1]),
            'country': country if not nuts2 or not nuts2.country else nuts2.country,
            'is_european': str(is_european),  # Map<Text, Text> fields must have text values (bool are not allowed)
            'country_code': country_code,
            'efas_name': nuts2.efas_name if nuts2 and nuts2.efas_name else '',
            'place': place,
            'geonameid': geonameid,
        }

    @classmethod
    def start_consumer(cls):
        logger.info('+++++++++++++ Geocoder consumer starting')
        try:
            tagger = Geoparser(cls.geonames_host)
            with cls.flask_app.app_context():
                for i, msg in enumerate(cls.consumer, start=1):
                    tweet = None
                    errors = 0
                    try:
                        msg = msg.value.decode('utf-8')
                        tweet = Tweet.build_from_kafka_message(msg)
                        try:
                            # COMMENT OUT CODE BELOW: we will geolocate everything for the moment
                            # flood_prob = t.annotations.get('flood_probability', ('', 0.0))[1]
                            # if flood_prob <= cls.min_flood_prob:
                            #     continue

                            coordinates, nuts2, nuts_source, country_code, place, geonameid = cls.find_nuts_heuristic(tweet, tagger)
                            if not coordinates:
                                continue

                            cls.set_geo_fields(coordinates, nuts2, nuts_source, country_code, place, geonameid, tweet)
                            message = tweet.serialize()
                            logger.info('Send geocoded tweet to PERSISTER: %s', tweet.geo)
                            cls.producer.send(cls.persister_kafka_topic, message)
                            if not (i % 1000):
                                logger.info('Geotagged so far %d', i)

                        except Exception as e:
                            logger.error(type(e))
                            logger.error('An error occured during geotagging: %s', e)
                            errors += 1
                            if errors >= 500:
                                logger.error('Too many errors...going to terminate geocoding')
                                cls.consumer.close()
                                break
                            continue
                    except (ValidationError, ValueError, TypeError, InvalidRequest) as e:
                        logger.error(e)
                        logger.error('Poison message for Cassandra: %s', tweet or msg)
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
