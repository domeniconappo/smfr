from abc import ABC, abstractmethod

from smfrcore.models import TwitterCollection

from daemons.streamers import BackgroundStreamer, OnDemandStreamer, ManualStreamer
from server.config import RestServerConfiguration


class BaseCollector(ABC):
    twitter_keys_iden = None
    type = None
    StreamerClass = None

    def __init__(self):
        self.server_conf = RestServerConfiguration()
        api_keys = self.twitter_keys()
        self.streamer = self.StreamerClass(
            producer=self.server_conf.kafka_producer,
            **api_keys
        )
        super().__init__()

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    def restart(self):
        self.stop()
        self.start()

    @classmethod
    def twitter_keys(cls):
        return RestServerConfiguration.admin_twitter_keys(cls.twitter_keys_iden)

    @property
    def collections(self):
        return self.streamer.collections


class BackgroundCollector(BaseCollector):
    twitter_keys_iden = 'twitterbg'
    type = TwitterCollection.TRIGGER_BACKGROUND
    StreamerClass = BackgroundStreamer

    def start(self):
        collection = TwitterCollection.get_active_background()
        if not collection:
            return
        self.streamer.run_collections([collection])

    def stop(self):
        self.streamer.disconnect()
        collection = TwitterCollection.get_active_background()
        if not collection:
            return


class OnDemandCollector(BaseCollector):
    twitter_keys_iden = 'twitterod'
    type = TwitterCollection.TRIGGER_ONDEMAND
    StreamerClass = OnDemandStreamer

    def start(self):
        collections = TwitterCollection.get_active_ondemand()
        if not collections:
            return
        self.streamer.run_collections(collections)

    def stop(self):
        self.streamer.disconnect()


class ManualCollector(OnDemandCollector):
    type = TwitterCollection.TRIGGER_MANUAL
    StreamerClass = ManualStreamer

    def start(self):
        collections = TwitterCollection.get_active_manual()
        if not collections:
            return
        self.streamer.run_collections(collections)
