import logging
import os
import sys
import threading

import yaml

from daemons.streamers import CollectorStreamer
from server.config import RestServerConfiguration, server_configuration
from errors import SMFRDBError
from server.models import StoredCollector, VirtualTwitterCollection


logging.basicConfig(level=logging.INFO if not RestServerConfiguration.debug else logging.DEBUG,
                    format='%(asctime)s:[%(levelname)s] (%(threadName)-10s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class Collector:
    trigger = None
    running_time = None
    account_keys = {'background': 'twitterbg',
                    'on-demand': 'twitterod'}

    _running_instances = {}

    @classmethod
    def running_instances(cls):
        return cls._running_instances

    def hashedid(self):
        return hash('{o.ctype}{o.trigger}{o.forecast_id}{o.keywords_file}{o.locations_file}'.format(o=self))

    def __init__(self, config_file,
                 keywords_file=None, locations_file=None,
                 running_time=0, forecast_id=None,
                 nuts3=None, nuts3source=None):

        self.forecast_id = forecast_id
        self.rest_server_conf = server_configuration()
        self.config_file = config_file
        self.keywords_file = keywords_file
        self.locations_file = locations_file
        self.ctype = 'keywords' if keywords_file else 'geo'
        self.runtime = running_time if self.running_time is None else self.running_time
        self.nuts3 = nuts3
        self.nuts3source = nuts3source
        self.stored_instance = None

        collector_config = yaml.load(open(self.config_file).read())
        client_args = {}
        if os.environ.get('http_proxy'):
            client_args = {
                'proxies': {
                    'http': os.environ['http_proxy'],
                    'https': os.environ.get('https_proxy') or os.environ['http_proxy']
                }
            }
        # Build a query from a keywords or locations file containing pairs lang, keyword or a list of bounding boxes
        self.query = self.build_query()

        from server.models import VirtualTwitterCollection
        self.collection = VirtualTwitterCollection.build_from_collector(self)
        tw_api_account = collector_config[self.account_keys[self.trigger]]
        self.streamer = CollectorStreamer(
            tw_api_account['consumer_key'],
            tw_api_account['consumer_secret'],
            tw_api_account['access_token'],
            tw_api_account['access_token_secret'],
            client_args=client_args,
            collection=self.collection,
            producer=self.rest_server_conf.kafka_producer
        )

    @classmethod
    def get_collector_class(cls, trigger_type):
        trigger = trigger_type.replace('-', '').title()
        clazz = '{}Collector'.format(trigger)
        return getattr(sys.modules[__name__], clazz)

    @classmethod
    def from_payload(cls, payload):
        return cls(config_file=payload['config'], keywords_file=payload.get('kwfile'),
                   locations_file=payload.get('locfile'),
                   running_time=payload.get('runtime'), forecast_id=payload.get('forecast'),
                   nuts3=payload.get('nuts3'), nuts3source=payload.get('nuts3source'))

    def build_query(self):
        """
        Build a dictionary containing information about tracking keywords or bounding box
        :return: dict {'languages': ['it', 'en'], 'track': ['',..., ''], 'locations': []}
        """

        query = {'languages': [], 'track': [], 'locations': []}

        if self.keywords_file and os.path.exists(self.keywords_file):
            with open(self.keywords_file) as f:
                # will build a dict like {'en': ['flooded', 'flood'], 'it': ['inondato']}
                keywords = yaml.load(f)

            query['languages'] = sorted(list(keywords.keys()))
            query['track'] = sorted(list(set(w for s in keywords.values() for w in s)))

        if self.locations_file and os.path.exists(self.locations_file):
            with open(self.locations_file) as f:
                # will build a dict like {'bbox1': '2.3, 4.5, 6.7 8,9', 'bbox2': '10, 11, 12, 13'}
                bboxes = yaml.load(f)
            # to query by locations, we need to concatenate all bboxes
            # https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters
            query['locations'] = sorted(list(bboxes.values()))

        return query

    def start(self):
        """
        Start a Collector process in same thread
        """
        filter_args = {k: ','.join(v) for k, v in self.query.items() if k != 'languages' and self.query[k]}
        logger.info('Starting twython filtering with {}'.format(filter_args))
        self.collection.activate()

        try:
            self._running_instances[self.hashedid()] = self
            self.streamer.statuses.filter(**filter_args)
        except KeyboardInterrupt:
            self.stop()

    def stop(self, reanimate=False):
        """
        Stop twython streamers and set collection stopped into db (virtual_twitter_collections)
        :param reanimate: bool
        """
        logger.info('Stopping collector %s', self)
        self._running_instances[self.hashedid()] = None
        self.streamer.disconnect()
        if not reanimate:
            self.collection.deactivate()

    def __repr__(self):
        return 'Collector {} - {}'.format(self.trigger, self.ctype)

    @classmethod
    def is_running(cls, collector_id):
        for c in cls._running_instances.values():
            if c.stored_instance.id == collector_id:
                return True
        return False

    @classmethod
    def resume(cls, collector_or_collector_id):
        """
        Restart a collector object identified by its MySQL id
        :param collector_or_collector_id: Collector or int
        :return: the collector instance
        """
        stored_collector = collector_or_collector_id

        if isinstance(collector_or_collector_id, int):
            stored_collector = StoredCollector.query.get(collector_or_collector_id)

        if not stored_collector:
            raise SMFRDBError('Invalid Collector id. Not existing in DB')

        clazz = Collector.get_collector_class(stored_collector.parameters['trigger'])
        collector = clazz.from_payload(stored_collector.parameters)
        server_conf = server_configuration()
        server_conf.db_mysql.session.expunge(stored_collector)
        collector.stored_instance = stored_collector
        return collector

    def launch(self):
        """
        Launch a Collector process in a separate thread
        """
        t = threading.Thread(target=self.start, name=str(self), daemon=True)
        t.start()

    @classmethod
    def resume_active(cls):
        stored_collectors = StoredCollector.query.join(VirtualTwitterCollection, StoredCollector.collection_id == VirtualTwitterCollection.id).filter(VirtualTwitterCollection.status == VirtualTwitterCollection.ACTIVE_STATUS)
        for c in stored_collectors:
            logger.info('Resuming %s', str(c))
            collector = cls.resume(c)
            collector.launch()

    @classmethod
    def start_all(cls):
        stored_collectors = StoredCollector.query.join(VirtualTwitterCollection,
                                                       StoredCollector.collection_id == VirtualTwitterCollection.id).filter(
            VirtualTwitterCollection.status == VirtualTwitterCollection.ACTIVE_STATUS)
        for c in stored_collectors:
            logger.info('Resuming %s', str(c))
            collector = cls.resume(c)
            collector.launch()


class BackgroundCollector(Collector):
    trigger = 'background'
    running_time = 0


class OndemandCollector(Collector):
    trigger = 'on-demand'
