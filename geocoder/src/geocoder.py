import logging
import os
import sys
import threading
import time
from collections import Counter

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from mordecai import Geoparser
import fiona
from shapely.geometry import Point, Polygon

from smfrcore.models.cassandramodels import Tweet
from smfrcore.utils import RUNNING_IN_DOCKER


class Nuts3Finder:
    """
    Simple class with a single method that returns NUTS3 id for a Point(Long, Lat).
    Warning: the method does not return NUTS3 code but the NUTS3 id as it's stored in EFAS NUTS3 table.
    TODO: Evaluate if it's better to refactor to a function instead of keeping this onemethod static class
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(os.environ.get('LOGGING_LEVEL', 'DEBUG')))
    shapefile_dir = '/config/'
    shapefile_name = os.environ.get('NUTS3_SHAPEFILE', '2018_GlobalRegionsWGS84_CUT_WGS84Coord.shp')
    path = os.path.join(shapefile_dir, shapefile_name)
    polygons = [pol for pol in fiona.open(path)]

    @classmethod
    def find_nuts3_id(cls, lat, lon):
        """
        Check if a point (lat, lon) is in a NUTS3 region and returns its id. None otherwise.
        :param lat: float Latitude of a point
        :param lon: float Longituted of a point
        :return: int NUTS3 id as it's stored in EFAS table
        """
        lat, lon = float(lat), float(lon)
        point = Point(lon, lat)
        p = None
        geo = None
        try:
            for p in cls.polygons:
                geo = None
                for geo in p['geometry']['coordinates']:
                    if isinstance(geo[0], tuple):
                        poly = Polygon(geo)
                        if point.within(poly):
                            return p['properties'].get('ID') or p.get('id', '')
                    else:
                        for ggeo in geo:
                            poly = Polygon(ggeo)
                            if point.within(poly):
                                return p['properties'].get('ID') or p.get('id', '')
        except (KeyError, IndexError) as e:
            cls.logger.error('An error occured %s %s', type(e), str(e))
            cls.logger.error(p)
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

        tagger = Geoparser(cls.geonames_host)
        errors = 0
        i = 0
        c = Counter()
        for x, t in enumerate(tweets, start=1):
            if collection_id in cls.stop_signals:
                cls.logger.info('Stopping geotagging process {}'.format(collection_id))
                with cls._lock:
                    cls.stop_signals.remove(collection_id)
                break
            c[t.lang] += 1
            try:
                # COMMENT OUT CODE BELOW: we will geolocate everything for the moment
                # flood_prob = t.annotations.get('flood_probability', ('', 0.0))[1]
                # if flood_prob <= cls.min_flood_prob:
                #     continue

                t.ttype = 'geotagged'
                res = tagger.geoparse(t.full_text)
                for result in res:
                    if result.get('country_conf', 0) < 0.5 or 'lat' not in result.get('geo', {}):
                        continue
                    latlong = (float(result['geo']['lat']), float(result['geo']['lon']))
                    t.latlong = latlong
                    nuts3_id = Nuts3Finder.find_nuts3_id(*latlong)
                    t.nuts3 = str(nuts3_id) if nuts3_id else None
                    message = t.serialize()
                    cls.logger.debug('Sending to queue: %s', str(message[:120]))

                    cls.producer.send(cls.kafka_topic, message)
                    i += 1
                    # we just take the first available result
                    break
            except Exception as e:
                cls.logger.error('An error occured during geotagging: %s', str(e))
                errors += 1
                if errors >= 100:
                    cls.logger.error('Too many errors...going to terminate geolocalization')
                    break
                continue
            finally:
                if not (x % 250):
                    cls.logger.info('Geotagged so far.... %d. Examinated %d: %s', i, sum(c.values()), str(c))
                    # workaround for lru_cache "memory leak" problems
                    # https://benbernardblog.com/tracking-down-a-freaky-python-memory-leak/
                    tagger.query_geonames.cache_clear()
                    tagger.query_geonames_country.cache_clear()

        # remove from `_running` list
        cls._running.remove(collection_id)
        cls.logger.info('Geotagging process terminated! Collection: {}'.format(collection_id))
