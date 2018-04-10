import os
import logging
import threading

from mordecai import Geoparser
import fiona
from shapely.geometry import Point, Polygon

from errors import SMFRError
from server.config import RestServerConfiguration
from server.models import Tweet


class Nuts3Finder:
    """

    """
    rest_server_conf = RestServerConfiguration()
    logger = logging.getLogger(__name__)
    logger.setLevel(rest_server_conf.logger_level)
    shapefile_dir = os.path.join(rest_server_conf.config_dir, 'nuts3/')
    shapefile_name = rest_server_conf.server_config.get('nut3_shapefile', '2018_GlobalRegionsWGS84_CUT_WGS84Coord.shp')
    path = os.path.join(shapefile_dir, shapefile_name)
    polygons = [pol for pol in fiona.open(path)]

    @classmethod
    def find_nuts3_id(cls, lat, lon):
        """

        :param lat:
        :param lon:
        :return:
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


class Geotagger:
    """

    """

    running = []

    rest_server_conf = RestServerConfiguration()
    logger = logging.getLogger(__name__)
    logger.setLevel(rest_server_conf.logger_level)

    def __init__(self, collection_id, ttype='annotated', lang='en'):
        if self.is_running_for(collection_id):
            raise SMFRError('Geotagging is already ongoing on collection id {}'.format(collection_id))

        self.kafka_topic = self.rest_server_conf.server_config['kafka_topic']
        self.producer = self.rest_server_conf.kafka_producer
        self.collection_id = collection_id
        self.ttype = ttype
        self.lang = lang

    def start(self):
        self.logger.info('Starting Geotagging collection: {}'.format(self.collection_id))
        self.running.append(self.collection_id)
        tweets = Tweet.get_iterator(self.collection_id, self.ttype, lang=self.lang)
        min_flood_prob = self.rest_server_conf.min_flood_probability
        tagger = Geoparser('geonames')
        errors = 0
        for i, t in enumerate(tweets):
            try:
                flood_prob = t.annotations.get('flood_probability', ('', 0.0))[1]
                if flood_prob <= min_flood_prob:
                    continue

                t.ttype = 'geotagged'
                res = tagger.geoparse(t.full_text)

                for result in res:
                    if result.get('country_conf', 0) < 0.5 or 'lat' not in result.get('geo', {}):
                        continue
                    latlong = (float(result['geo']['lat']), float(result['geo']['lon']))
                    t.latlong = latlong
                    nuts3_id = Nuts3Finder.find_nuts3_id(*latlong)
                    t.nuts3 = str(nuts3_id) if nuts3_id else None
                    if nuts3_id:
                        self.logger.debug('Nuts3 was found!: %s', t.nuts3)
                    message = t.serialize()
                    self.logger.debug('Sending to queue: %s', str(message[:120]))
                    self.producer.send(self.kafka_topic, message)
                    # we just take the first available result
                    break
            except Exception as e:
                self.logger.error('An error occured during geotagging: %s', str(e))
                errors += 1
                if errors >= 100:
                    self.logger.error('Too many errors...going to terminate geolocalization')
                    break
                continue
            finally:
                if not (i % 250):
                    # workaround for lru_cache "memory leak" problems
                    # https://benbernardblog.com/tracking-down-a-freaky-python-memory-leak/
                    tagger.query_geonames.cache_clear()
                    tagger.query_geonames_country.cache_clear()

        # remove from `running` list
        self.running.remove(self.collection_id)
        self.logger.info('Geotagging process terminated! Collection: {}'.format(self.collection_id))

    def launch(self):
        """
        Launch a Geotagger process in a separate thread
        """
        t = threading.Thread(target=self.start, name='Geotagger collection id: {}'.format(self.collection_id))
        t.start()

    @classmethod
    def is_running_for(cls, collection_id):
        """

        :param collection_id:
        :return:
        """
        return collection_id in cls.running
