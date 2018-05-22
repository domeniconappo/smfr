import logging
import os
import sys
import threading
import time
from collections import Counter, namedtuple

import ujson as json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from mordecai import Geoparser
from shapely.geometry import Point, Polygon

from smfrcore.models.cassandramodels import Tweet
from smfrcore.utils import RUNNING_IN_DOCKER


NutsItem = namedtuple('NutsItem', 'id, nuts_id, properties, geometry')


def read_geojson(path):
    items = []
    try:
        with open(path) as f:
            data = json.load(f, precise_float=True)
    except FileNotFoundError:
        data = {'features': []}
        Nuts3Finder.logger.error('File not found: %s', path)

    for feat in data['features']:
        items.append(
            NutsItem(
                feat['properties']['ObjectID'],
                feat['properties']['NUTS_ID'],
                feat['properties'],
                feat['geometry']['coordinates']
            )
        )
    return items


class Nuts3Finder:
    """
    Simple class with a single method that returns NUTS3 id for a Point(Long, Lat).
    Warning: the method does not return NUTS3 code but the NUTS3 id as it's stored in EFAS NUTS3 table.
    TODO: Evaluate if it's better to refactor to a function instead of keeping this onemethod static class
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(os.environ.get('LOGGING_LEVEL', 'DEBUG')))
    config_dir = '/config/' if RUNNING_IN_DOCKER else './config'
    shapefile_name = os.environ.get('NUTS3_SHAPEFILE', '2018_GlobalRegionsWGS84_CUT_WGS84Coord.shp')
    geojson_name = os.environ.get('NUTS3_GEOJSON', 'GlobalRegions_052018.geojson')

    # path = os.path.join(config_dir, shapefile_name)
    # polygons = [pol for pol in fiona.open(path)]
    path = os.path.join(config_dir, geojson_name)
    nutsitems = read_geojson(path)

    @classmethod
    def find_nuts3(cls, lat, lon):
        """
        Check if a point (lat, lon) is in a NUTS3 region and returns its id. None otherwise.
        :param lat: float Latitude of a point
        :param lon: float Longituted of a point
        :return: int NUTS3 id as it's stored in EFAS table
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

        :param tweet:
        :return:
        """
        # try to geoparse
        latlong_list = []
        res = cls.tagger.geoparse(tweet.full_text)
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
        if 'coordinates' in t or t.get('geo'):
            coords = t['coordinates'].get('coordinates') or t['geo'].get('coordinates')
            if coords:
                latlong = coords[1], coords[0]
        return latlong

    @classmethod
    def find_nuts_heuristic(cls, tweet):
        """
        The following heuristic is applied:
        First, a gazetteer is run on the tweet to find location mentions
        If no location mention is found:
            If the tweet contains (longitude, latitude): the NUTS3 area containing that (longitude, latitude) is returned (nuts3source="coordinates")
            Otherwise, NULL is returned
        If location mentions mapping to a list of NUTS3 areas are found:
            If the tweet contains (longitude, latitude), then if any of the NUTS3 areas contain that point, that NUTS3 area is returned (nuts3source="coordinates-and-mentions")
            Otherwise
                If there is a single NUTS3 area in the list, that NUTS3 area is returned (nuts3source="mentions")
                Otherwise, NULL is returned

        :param tweet:
        :return: tuple (nuts3, nuts_source, coordinates)
        """
        nuts3, nuts_source, coordinates = None, None, None
        latlong_list = cls.geoparse_tweet(tweet)
        latlong_from_tweet = cls.get_coordinates_from_tweet(tweet)

        if not latlong_list and latlong_from_tweet:
            # No location mention is found and the tweet has coordinates
            nuts3 = Nuts3Finder.find_nuts3(*latlong_from_tweet)
            if nuts3:
                coordinates = latlong_from_tweet
                nuts_source = 'coordinates'
            return nuts3, nuts_source, coordinates
        elif len(latlong_list) > 1 and latlong_from_tweet:
            nuts3_from_tweet = Nuts3Finder.find_nuts3(*latlong_from_tweet)
            for latlong in latlong_list:
                # checking the list of mentioned places coordinates
                nuts3 = Nuts3Finder.find_nuts3(*latlong)
                if nuts3 == nuts3_from_tweet:
                    coordinates = latlong
                    nuts_source = 'coordinates-and-mentions'
                    break
            return nuts3, nuts_source, coordinates
        elif len(latlong_list) == 1 and not latlong_from_tweet:
            # returning the only mention that is found
            coordinates = latlong_list[0]
            nuts3 = Nuts3Finder.find_nuts3(*coordinates)
            nuts_source = 'mentions'
            return nuts3, nuts_source, coordinates
        elif len(latlong_list) > 1 and not latlong_from_tweet:
            return None, None, None
        return None, None, None

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
                nuts3_id, nuts3_source, latlong = cls.find_nuts_heuristic(t)

                if not latlong:
                    continue

                t.latlong = latlong
                t.nuts3 = str(nuts3_id) if nuts3_id else None
                t.nuts3source = nuts3_source
                message = t.serialize()
                cls.logger.debug('Send geocoded tweet to persister: %s', str(t))

                cls.producer.send(cls.kafka_topic, message)
                c[t.lang] += 1

            except Exception as e:
                cls.logger.error('An error occured during geotagging: %s', str(e))
                errors += 1
                if errors >= 100:
                    cls.logger.error('Too many errors...going to terminate geolocalization')
                    break
                continue
            finally:
                if not (x % 250):
                    cls.logger.info('\nExaminated: %d \n===========\nGeotagged so far: %d\n %s', sum(x, c.values()), str(c))
                    # workaround for lru_cache "memory leak" problems
                    # https://benbernardblog.com/tracking-down-a-freaky-python-memory-leak/
                    cls.tagger.query_geonames.cache_clear()
                    cls.tagger.query_geonames_country.cache_clear()

        # remove from `_running` list
        cls._running.remove(collection_id)
        cls.logger.info('Geotagging process terminated! Collection: {}'.format(collection_id))
