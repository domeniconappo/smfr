import logging
import os
import sys
import threading
import time
from collections import Counter

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from mordecai import Geoparser
from shapely.geometry import Point, Polygon

from smfrcore.models.cassandramodels import Tweet
from smfrcore.utils import RUNNING_IN_DOCKER

from utils import read_geojson


class Nuts3Finder:
    """
    Simple class with a single method that returns NUTS3 id for a Point(Long, Lat).
    Warning: the method does not return NUTS3 code but the NUTS3 id as it's stored in EFAS NUTS3 table.
    TODO: Evaluate if it's better to refactor to a function instead of keeping this onemethod static class
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(os.environ.get('LOGGING_LEVEL', 'DEBUG')))
    config_dir = '/config/' if RUNNING_IN_DOCKER else os.path.join(os.path.dirname(__file__), '../config')
    shapefile_name = os.environ.get('NUTS3_SHAPEFILE', '2018_GlobalRegionsWGS84_CUT_WGS84Coord.shp')
    geojson_name = os.environ.get('NUTS3_GEOJSON', 'GlobalRegions_052018.geojson')
    path = os.path.join(config_dir, geojson_name)
    nutsitems = read_geojson(path)

    @classmethod
    def find_nuts3(cls, lat, lon):
        """
        Check if a point (lat, lon) is in a NUTS3 region and returns its id. None otherwise.
        :param lat: float Latitude of a point
        :param lon: float Longituted of a point
        :return: NutsItem object with object_id, nuts_id, polygon geometry and other properties
                like ('COUNTRY': 'Benin', 'ISO_CODE': 'BJBO', 'ISO_CC': 'BJ', 'ISO_SUB': 'BO', 'NUTS_ID': None,
                      'CNTR_CODE': None, 'EFAS_name': 'Borgou', 'AREA_GEO': 26714565260.2)

        """
        point = Point(float(lon), float(lat))
        nuts = None
        geo = None
        try:
            for nuts in cls.nutsitems:
                geo = None
                for geo in nuts.geometry:
                    if isinstance(geo[0], tuple):
                        poly = Polygon(geo)
                        if point.within(poly):
                            return nuts
                    else:
                        for ggeo in geo:
                            poly = Polygon(ggeo)
                            if point.within(poly):
                                return nuts
        except (KeyError, IndexError) as e:
            cls.logger.error('An error occured %s %s', type(e), str(e))
            cls.logger.error(nuts)
            cls.logger.error(geo)
            return None
        except Exception as e:
            cls.logger.error('An error occured %s %s', type(e), str(e))
            return None


class Geocoder:
    """
    Class implementing the Geocoder component
    """

    _running = []
    stop_signals = []
    _lock = threading.RLock()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(os.environ.get('LOGGING_LEVEL', 'DEBUG')))
    geonames_host = '127.0.0.1' if not RUNNING_IN_DOCKER else 'geonames'
    kafka_bootstrap_server = '{}:9092'.format('kafka' if RUNNING_IN_DOCKER else '127.0.0.1')

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
    kafka_topic = os.environ.get('KAFKA_TOPIC', 'persister')

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
    def geoparse_tweet(cls, tweet, tagger):
        """
        Mordecai geoparsing
        :param tweet: smfrcore.models.cassandramodels.Tweet object
        :return: list of tuples of lat/lon coordinates
        """
        # try to geoparse
        latlong_list = []
        res = tagger.geoparse(tweet.full_text)
        for result in res:
            if result.get('country_conf', 0) < 0.5 or 'lat' not in result.get('geo', {}):
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
    def find_nuts_heuristic(cls, tweet, tagger):
        """
        The following heuristic is applied:
        #1 First, a gazetteer is run on the tweet to find location mentions

        #2 If no location mention is found:
            If the tweet contains (longitude, latitude): the NUTS3 area containing that (longitude, latitude) is returned (nuts3source="coordinates")
            Otherwise, NULL is returned
        #3 If location mentions mapping to a list of NUTS3 areas are found:
            If the tweet contains (longitude, latitude), then if any of the NUTS3 areas contain that point, that NUTS3 area is returned (nuts3source="coordinates-and-mentions")
            Otherwise
                If there is a single NUTS3 area in the list, that NUTS3 area is returned (nuts3source="mentions")
                Otherwise, check if user location is in one of the NUTS list and return it. If not, NULL is returned

        :param tagger:
        :param tweet: Tweet object
        :return: tuple (nuts3, nuts_source, coordinates)
        """
        no_results = (None, None, None)
        mentions = cls.geoparse_tweet(tweet, tagger)
        tweet_coords = cls.get_coordinates_from_tweet(tweet)

        if not mentions:
            if tweet_coords:
                nuts3 = Nuts3Finder.find_nuts3(*tweet_coords)
                if nuts3:
                    coordinates = tweet_coords
                    nuts_source = 'coordinates'
                    cls.logger.debug('Found Nuts from tweet geo... - coordinates')
                    return nuts3, nuts_source, coordinates
            return no_results
        else:
            if tweet_coords and len(mentions) > 1:
                nuts3_from_tweet = Nuts3Finder.find_nuts3(*tweet_coords)
                for latlong in mentions:
                    # checking the list of mentioned places coordinates
                    nuts3 = Nuts3Finder.find_nuts3(*latlong)
                    if nuts3 == nuts3_from_tweet:
                        coordinates = latlong
                        nuts_source = 'coordinates-and-mentions'
                        cls.logger.debug('Found Nuts from tweet geo and mentions... - coordinates-and-mentions')
                        return nuts3, nuts_source, coordinates
                return no_results
            else:
                if len(mentions) == 1:
                    coordinates = mentions[0]
                    nuts3 = Nuts3Finder.find_nuts3(*coordinates)
                    if nuts3:
                        nuts_source = 'mentions'
                        cls.logger.debug('Found Nuts... - Exactly one mention')
                        return nuts3, nuts_source, coordinates

                    return no_results
                else:
                    # no geolocated tweet and more than one mention
                    user_location = tweet.original_tweet_as_dict['user'].get('location')
                    res = tagger.geoparse(user_location) if user_location else None
                    if res and res[0] and 'lat' in res[0].get('geo', {}):
                        res = res[0]
                        user_coordinates = (float(res['geo']['lat']), float(res['geo']['lon']))
                        user_nuts3 = Nuts3Finder.find_nuts3(*user_coordinates)
                        for latlong in mentions:
                            # checking the list of mentioned places coordinates
                            nuts3 = Nuts3Finder.find_nuts3(*latlong)
                            if nuts3 == user_nuts3:
                                coordinates = latlong
                                nuts_source = 'mentions-and-user'
                                cls.logger.debug('Found Nuts... - User location')
                                return nuts3, nuts_source, coordinates
                    return no_results

    @classmethod
    def start(cls, collection_id):
        """
        Main Geocoder method. It's usually executed in a background thread.
        :param collection_id: MySQL id of collection as it's stored in virtual_twitter_collection table
        :type collection_id: int
        """
        ttype = 'annotated'
        cls.logger.info('Starting Geotagging collection: {}'.format(collection_id))
        cls._running.append(collection_id)

        tweets = Tweet.get_iterator(collection_id, ttype)

        errors = 0
        c = Counter()
        tagger = Geoparser(cls.geonames_host)
        for x, t in enumerate(tweets, start=1):
            if collection_id in cls.stop_signals:
                cls.logger.info('Stopping geotagging process {}'.format(collection_id))
                with cls._lock:
                    cls.stop_signals.remove(collection_id)
                break

            try:
                # COMMENT OUT CODE BELOW: we will geolocate everything for the moment
                # flood_prob = t.annotations.get('flood_probability', ('', 0.0))[1]
                # if flood_prob <= cls.min_flood_prob:
                #     continue

                t.ttype = 'geotagged'
                nutsitem, nuts3_source, latlong = cls.find_nuts_heuristic(t, tagger)

                if not latlong:
                    continue

                t.latlong = latlong
                t.nuts3 = str(nutsitem.id) if nutsitem and nutsitem.id is not None else None
                t.nuts3source = nuts3_source
                nuts_properties = nutsitem.properties if nutsitem else {}

                t.geo = {
                    'nuts_efas_id': str(nutsitem.id) if nutsitem and nutsitem.id is not None else '',
                    'nuts_id': str(nutsitem.nuts_id) if nutsitem and nutsitem.nuts_id is not None else '',
                    'nuts_source': nuts3_source or '',
                    'latitude': str(latlong[0]),
                    'longitude': str(latlong[1]),
                    'country': nuts_properties.get('COUNTRY') or '',
                    'iso_code': nuts_properties.get('ISO_CODE') or '',
                    'iso_cc': nuts_properties.get('ISO_CC') or '',
                    'efas_name': nuts_properties.get('EFAS_name') or '',
                }

                message = t.serialize()
                cls.logger.debug('Send geocoded tweet to persister: %s', str(t))

                cls.producer.send(cls.kafka_topic, message)
                counter_key = '{}#{}'.format(t.lang, nuts3_source)
                c[counter_key] += 1

            except Exception as e:
                cls.logger.error(type(e))
                cls.logger.error('An error occured during geotagging: %s', str(e))
                errors += 1
                if errors >= 100:
                    cls.logger.error('Too many errors...going to terminate geolocalization')
                    break
                continue
            finally:
                if not (x % 500):
                    cls.logger.info('\nExaminated: %d \n===========\nGeotagged so far: %d\n %s', x, sum(c.values()), str(c))
                    # workaround for lru_cache "memory leak" problems
                    # https://benbernardblog.com/tracking-down-a-freaky-python-memory-leak/
                    tagger.query_geonames.cache_clear()
                    tagger.query_geonames_country.cache_clear()

        # remove from `_running` list
        cls._running.remove(collection_id)
        cls.logger.info('Geotagging process terminated! Collection: {}'.format(collection_id))
