import os
import multiprocessing
from abc import ABC, abstractmethod
import logging
from time import sleep

from smfrcore.models.sql import TwitterCollection
from smfrcore.utils import DEFAULT_HANDLER

from streamers import BackgroundStreamer, OnDemandStreamer, ManualStreamer


logger = logging.getLogger('Collectors')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)
logger.propagate = False


class BaseCollector(ABC):
    twitter_keys_iden = ''
    type = None
    StreamerClass = None

    def __init__(self):
        api_keys = self.twitter_keys()
        self.streamer = self.StreamerClass(**api_keys)
        super().__init__()

    def __str__(self):
        return 'Collector {o.type} - collections {o.collections}'.format(o=self)

    @abstractmethod
    def start(self):
        pass

    def stop(self, deactivate=True):
        self.streamer.disconnect(deactivate)

    def restart(self):
        self.stop(deactivate=False)
        self.start()

    @classmethod
    def admin_twitter_keys(cls, iden):
        keys = {
            k: os.getenv('{}_{}'.format(iden, k).upper())
            for k in ('consumer_key', 'consumer_secret', 'access_token', 'access_token_secret')
        }
        return keys

    @classmethod
    def twitter_keys(cls):
        return cls.admin_twitter_keys(cls.twitter_keys_iden)

    @property
    def collections(self):
        return self.streamer.collections


class BackgroundCollector(BaseCollector):
    twitter_keys_iden = 'twitterbg'
    type = TwitterCollection.TRIGGER_BACKGROUND
    StreamerClass = BackgroundStreamer

    def start(self):
        if self.streamer.process and isinstance(self.streamer.process, multiprocessing.Process):
            logger.info('Sending SIGTERM signal to streamer')
            self.streamer.process.terminate()
            sleep(2)
        if self.streamer.is_connected.value == 1:
            logger.info('Trying to start an already connected streamer %s', self.streamer)
            return
        collection = TwitterCollection.get_active_background()
        if not collection:
            return
        # Launch streamer based on track/location params from collection
        self.streamer.run_collections([collection])


class OnDemandCollector(BaseCollector):
    twitter_keys_iden = 'twitterod'
    type = TwitterCollection.TRIGGER_ONDEMAND
    StreamerClass = OnDemandStreamer

    def start(self):
        if self.streamer.process and isinstance(self.streamer.process, multiprocessing.Process):
            logger.info('Sending SIGTERM signal to streamer')
            self.streamer.process.terminate()
            sleep(2)
            self.streamer.process = None
        if self.streamer.is_connected.value == 1:
            logger.info('Trying to start an already connected streamer %s', self.streamer)
            return
        collections = TwitterCollection.get_active_ondemand()
        if not collections:
            return
        # Launch streamer based on track/location params from collection
        self.streamer.run_collections(collections)


class ManualCollector(OnDemandCollector):
    twitter_keys_iden = 'twitterma'
    type = TwitterCollection.TRIGGER_MANUAL
    StreamerClass = ManualStreamer

    def start(self):
        if self.streamer.process and isinstance(self.streamer.process, multiprocessing.Process):
            logger.info('Sending SIGTERM signal to streamer')
            self.streamer.process.terminate()
            sleep(2)
        if self.streamer.is_connected.value == 1:
            logger.info('Trying to start an already connected streamer %s', self.streamer)
            return
        collections = TwitterCollection.get_active_manual()
        if not collections:
            return
        self.streamer.run_collections(collections)
