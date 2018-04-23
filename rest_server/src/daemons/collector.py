import logging
import os
import sched
import sys
import threading

import time
import yaml

from dateutil import parser

from smfrcore.models.sqlmodels import TwitterCollection, StoredCollector

from daemons.streamers import CollectorStreamer
from server.config import RestServerConfiguration
from smfrcore.errors import SMFRDBError


class Collector:
    """

    """
    trigger = None
    account_keys = {'background': 'twitterbg',
                    'on-demand': 'twitterod',
                    'manual': 'twitterod'}

    _running_instances = {}
    logger = logging.getLogger(__name__)
    logger.setLevel(RestServerConfiguration.logger_level)
    server_conf = RestServerConfiguration()

    @classmethod
    def running_instances(cls):
        return ((hashed_id, running_coll) for hashed_id, running_coll in cls._running_instances.items() if running_coll)

    def hashedid(self):
        return hash('{o.config}{o.nuts3source}{o.nuts3}{o.runtime}{o.ctype}{o.trigger}{o.forecast_id}{o.kwfile}{o.locfile}'.format(o=self))

    def __init__(self, config_file,
                 keywords_file=None, locations_file=None,
                 running_time=None, forecast_id=None,
                 nuts3=None, nuts3source=None, tz=None):

        self.forecast_id = forecast_id
        self.config = config_file
        self.kwfile = keywords_file
        self.locfile = locations_file
        self.ctype = 'keywords' if keywords_file else 'geo'
        self.runtime = running_time
        self.user_tzone = tz
        self.nuts3 = nuts3
        self.nuts3source = nuts3source
        self.stored_instance = None

        collector_config = yaml.load(open(self.config).read())
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

        self.collection = TwitterCollection.build_from_collector(self)
        tw_api_account = collector_config[self.account_keys[self.trigger]]
        self.streamer = CollectorStreamer(
            tw_api_account['consumer_key'],
            tw_api_account['consumer_secret'],
            tw_api_account['access_token'],
            tw_api_account['access_token_secret'],
            client_args=client_args,
            collection=self.collection,
            producer=self.server_conf.kafka_producer
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
                   running_time=payload.get('runtime'), tz=payload.get('tzclient'),
                   forecast_id=payload.get('forecast'),
                   nuts3=payload.get('nuts3'), nuts3source=payload.get('nuts3source'))

    def build_query(self):
        """
        Build a dictionary containing information about tracking keywords or bounding box
        :return: dict {'languages': ['it', 'en'], 'track': ['',..., ''], 'locations': []}
        """

        query = {'languages': [], 'track': [], 'locations': []}

        if self.kwfile and os.path.exists(self.kwfile):
            with open(self.kwfile) as f:
                # will build a dict like {'en': ['flooded', 'flood'], 'it': ['inondato']}
                keywords = yaml.load(f)

            query['languages'] = sorted(list(keywords.keys()))
            query['track'] = sorted(list(set(w for s in keywords.values() for w in s)))

        if self.locfile and os.path.exists(self.locfile):
            with open(self.locfile) as f:
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
        self.logger.info('Starting twython filtering with %s', str(filter_args))
        self.server_conf.flask_app.app_context().push()
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
        self.logger.info('Stopping collector %s', self)
        self._running_instances[self.hashedid()] = None
        self.streamer.disconnect()
        if not reanimate:
            self.collection.deactivate()

    def __repr__(self):
        return 'Collector {} - {}'.format(self.trigger, self.ctype)

    @classmethod
    def is_running(cls, collector_id):
        """

        :param collector_id:
        :return:
        """
        for c in cls._running_instances.values():
            if c and c.stored_instance.id == collector_id:
                return True
        return False

    @classmethod
    def get_running_collector(cls, collector_id):
        """

        :param collector_id:
        :return:
        """
        for c in cls._running_instances.values():
            if c.stored_instance.id == collector_id:
                return c
        return None

    @classmethod
    def resume(cls, collector_or_collector_id):
        """
        Return a collector object identified by its MySQL id
        :param collector_or_collector_id: Collector object or int
        :return: the collector instance
        """
        stored_collector = collector_or_collector_id

        if isinstance(collector_or_collector_id, int):
            stored_collector = StoredCollector.query.get(collector_or_collector_id)

        if not stored_collector:
            raise SMFRDBError('Invalid Collector id. Not existing in DB')

        clazz = Collector.get_collector_class(stored_collector.parameters['trigger'])
        cls.server_conf.db_mysql.session.expunge(stored_collector)

        collector = clazz.from_payload(stored_collector.parameters)
        collector.stored_instance = stored_collector

        return collector

    def launch(self):
        """
        Launch a Collector process in a separate thread
        """
        t = threading.Thread(target=self.start, name=str(self), daemon=True)
        t.start()
        if self.runtime:
            # schedule the stop
            s = sched.scheduler(time.time, time.sleep)
            self.logger.info('---+ Collector scheduled to stop at %s %s...', self.runtime, self.user_tzone)
            stop_at = parser.parse('{} {}'.format(self.runtime, self.user_tzone))  # - tz_diff(self.user_tzone)
            s.enterabs(stop_at.timestamp(), 1, self.stop)
            t = threading.Thread(target=s.run, name='stop_at_%s' % str(stop_at))
            t.start()

    @classmethod
    def resume_active(cls):
        """
        Resume and launch all collectors for collections that are in ACTIVE status (i.e. they were _running before a shutdown)
        """
        collectors_active_collections = StoredCollector.query\
            .join(TwitterCollection,
                  StoredCollector.collection_id == TwitterCollection.id)\
            .filter(TwitterCollection.status == TwitterCollection.ACTIVE_STATUS)

        return (cls.resume(c) for c in collectors_active_collections)

    @classmethod
    def resume_inactive(cls):
        """
        Resume and launch all collectors for collections that are in INACTIVE status
        """
        collectors_inactive_collections = StoredCollector.query \
            .join(TwitterCollection, StoredCollector.collection_id == TwitterCollection.id) \
            .filter(TwitterCollection.status == TwitterCollection.INACTIVE_STATUS)

        return (cls.resume(c) for c in collectors_inactive_collections)

    @classmethod
    def resume_all(cls):
        """

        :return: Generator of Collector instances resumed based on StoredCollector db items stored in MySQL
        """
        stored_collectors = StoredCollector.query\
            .join(TwitterCollection, StoredCollector.collection_id == TwitterCollection.id)
        return (cls.resume(c) for c in stored_collectors)


class BackgroundCollector(Collector):
    trigger = 'background'


class OndemandCollector(Collector):
    trigger = 'on-demand'


class ManualCollector(Collector):
    trigger = 'manual'
