import datetime
import logging
import os
import sched
import threading
import time

import yaml
from dateutil import parser

from smfrcore.models import TwitterCollection

from daemons.streamers import CollectorStreamer
from server.config import RestServerConfiguration, CONFIG_FOLDER
from smfrcore.errors import SMFRDBError


class Collector:
    """

    """
    trigger = None
    account_keys = {
        'background': 'twitterbg',
        'on-demand': 'twitterod',
        'manual': 'twitterod'
    }

    _running_instances = {}

    logger = logging.getLogger('RestServer Collector')
    logger.setLevel(RestServerConfiguration.logger_level)
    server_conf = RestServerConfiguration()
    defaults = {'kwfile': os.path.join(CONFIG_FOLDER, 'flood_keywords.yaml'),

                'config': os.path.join(CONFIG_FOLDER, 'admin_collector.yaml'),
                }

    def __init__(self, collection, timezone=None):
        """

        :param collection: TwitterCollection object
        """
        self.collection = collection
        self.query = self.build_query()
        self.user_tzone = timezone or '+00:00'

        tw_api_account = collection.configuration
        client_args = {}
        if os.environ.get('http_proxy'):
            client_args = {
                'proxies': {
                    'http': os.environ['http_proxy'],
                    'https': os.environ.get('https_proxy') or os.environ['http_proxy']
                }
            }

        self.streamer = CollectorStreamer(
            tw_api_account.consumer_key,
            tw_api_account.consumer_secret,
            tw_api_account.access_token,
            tw_api_account.access_token_secret,
            client_args=client_args,
            collection=self.collection,
            producer=self.server_conf.kafka_producer
        )

    @classmethod
    def user_collector_config_file(cls, user=None):
        # TODO return collector config based on user object
        return os.path.join(CONFIG_FOLDER, 'admin_collector.yaml')

    @classmethod
    def content_config(cls, config_dict):
        config = yaml.dump(config_dict, default_flow_style=False)
        return yaml.dump({'twitterbg': config, 'twitterod': config}, default_flow_style=False)

    @classmethod
    def running_instances(cls):
        return ((hashed_id, running_coll) for hashed_id, running_coll in cls._running_instances.items() if running_coll)

    def hashedid(self):
        return hash(
            '{o.nuts2}{o.runtime}{o.trigger}{o.forecast_id}{o.locations}'
            .format(o=self.collection)
        )

    def build_query(self):
        """
        Build a dictionary containing information about tracking keywords or bounding box
        :return: dict {'languages': ['it', 'en'], 'track': ['',..., ''], 'locations': []}
        """
        query = {}
        if self.collection.locations:
            locations = self.collection.locations
            locations = ['{},{},{},{}'.format(locations['min_lon'], locations['min_lat'],
                                              locations['max_lon'], locations['max_lat'])]
            query.update({'locations': locations})
        query.update({'languages': self.collection.languages,
                      'track': self.collection.tracking_keywords})
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

    def __str__(self):
        return 'Collector({}, {})'.format(self.collection.trigger, self.query)

    @classmethod
    def is_running(cls, collection_id):
        """

        :param collection_id:
        :return:
        """
        for c in cls._running_instances.values():
            if c and c.collection.id == collection_id:
                return True
        return False

    @classmethod
    def get_running_collector(cls, collection_id):
        """

        :param collection_id:
        :return:
        """
        for c in cls._running_instances.values():
            if c.collection.id == collection_id:
                return c
        return None

    @classmethod
    def resume(cls, collection_or_collection_id):
        """
        Return a collector object identified by its MySQL id
        :param collection_or_collection_id: Collector object or int
        :return: the collector instance
        """
        collection = collection_or_collection_id

        if isinstance(collection_or_collection_id, int):
            collection = TwitterCollection.query.get(collection_or_collection_id)

        if not collection:
            raise SMFRDBError('Invalid collection id. Not existing in DB')

        cls.server_conf.db_mysql.session.expunge(collection)

        collector = Collector(collection)
        return collector

    def launch(self):
        """
        Launch a Collector process in a separate thread
        """
        t = threading.Thread(target=self.start, name='Streamer {}'.format(self.collection), daemon=True)
        t.start()
        if self.collection.runtime:
            # schedule the stop
            s = sched.scheduler(time.time, time.sleep)
            self.logger.info('---+ Collector scheduled to stop at %s %s...', self.collection.runtime, self.user_tzone)
            stop_at = parser.parse('{} {}'.format(self.collection.runtime, self.user_tzone))
            s.enterabs(stop_at.timestamp(), 1, self.stop)
            t = threading.Thread(target=s.run, name='stop_at_%s' % str(stop_at))
            t.start()

    @classmethod
    def resume_active(cls):
        """
        Resume and launch all collectors for collections that are in ACTIVE status (i.e. they were running before a shutdown)
        """
        active_collections = TwitterCollection.query.filter_by(status=TwitterCollection.ACTIVE_STATUS)
        return (cls.resume(c) for c in active_collections)

    @classmethod
    def resume_inactive(cls):
        """
        Resume and launch all collectors for collections that are in INACTIVE status
        """
        inactive_collections = TwitterCollection.query.filter_by(status=TwitterCollection.INACTIVE_STATUS)
        return (cls.resume(c) for c in inactive_collections)

    @classmethod
    def resume_all(cls):
        """

        :return: Generator of Collector instances to resume
        """
        collections = TwitterCollection.query.all()
        return (cls.resume(c) for c in collections)

    @classmethod
    def create_from_event(cls, event, user=None):
        """

        :param user:
        :param event: {'bbox': {'max_lat': 40.6587, 'max_lon': -1.14236, 'min_lat': 39.2267, 'min_lon': -3.16142},
                       'lead_time': 4, 'trigger': 'on-demand',
                       'forecast': '2018061500', 'keywords': 'Cuenca', 'efas_id': 1436}
        :type: dict
        :return: Collector object
        """
        runtime = cls.runtime_from_leadtime(event['lead_time'])
        kwargs = {'trigger': event['trigger'], 'runtime': runtime, 'locations': event['bbox'],
                  'keywords': event['keywords'], 'nuts2': event['efas_id'], 'forecast_id': int(event['forecast'])}
        collection = TwitterCollection.create(**kwargs)
        return cls(collection)

    @classmethod
    def runtime_from_leadtime(cls, lead_time):
        """

        :param lead_time: number of days before the peak occurs
        :return: runtime in format %Y-%m-%d %H:%M
        """
        runtime = datetime.datetime.now() + datetime.timedelta(days=int(lead_time))
        return runtime.strftime('%Y-%m-%d %H:%M')
