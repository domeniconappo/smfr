import logging
import threading

from mordecai import Geoparser

from errors import SMFRError
from server.config import server_configuration
from server.models import Tweet


class Geotagger:

    running = []

    rest_server_conf = server_configuration()
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
        # from pympler import tracker
        # tr = tracker.SummaryTracker()
        for i, t in enumerate(tweets):
            try:
                flood_prob = t.annotations.get('flood_probability', ('', 0.0))[1]
                if flood_prob <= min_flood_prob:
                    self.logger.debug('Skipping low flood probability')
                    continue
                t.ttype = 'geotagged'
                res = tagger.geoparse(t.full_text)

                for result in res:
                    if result.get('country_conf', 0) < 0.5 or 'lat' not in result.get('geo', {}):
                        self.logger.debug('Skipping low geotagging confidence: %s', str(result))
                        continue

                    t.latlong = (float(result['geo']['lat']), float(result['geo']['lon']))
                    message = t.serialize()
                    self.logger.info('Sending to queue: {}'.format(message[:120]))
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
                    # objgraph.show_most_common_types()
                    # tr.print_diff()
                    # time.sleep(3)
                    # workaround for lru_cache memory leak problems
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
        return collection_id in cls.running
