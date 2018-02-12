import logging
import threading

from cassandra.cqlengine import ValidationError
from kafka import KafkaConsumer

from server.config import server_configuration, RestServerConfiguration
from server.models import Tweet

logging.basicConfig(level=logging.INFO if not RestServerConfiguration.debug else logging.DEBUG,
                    format='%(asctime)s:[%(levelname)s] (%(threadName)-10s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class Consumer:
    config = server_configuration()
    _running_instance = None

    @classmethod
    def running_instance(cls):
        return cls._running_instance

    @classmethod
    def build_and_start(cls):
        consumer = cls()
        t_cons = threading.Thread(target=consumer.start, name='Consumer', daemon=True)
        t_cons.start()

    def __init__(self, group_id='SMFR', auto_offset_reset='earliest'):
        self.topic = self.config.kafka_topic
        self.bootstrap_server = self.config.kafka_bootstrap_server
        self.auto_offset_reset = auto_offset_reset
        self.group_id = group_id
        self.consumer = KafkaConsumer(self.topic, group_id=self.group_id,
                                      auto_offset_reset=self.auto_offset_reset,
                                      bootstrap_servers=self.bootstrap_server)

    def start(self):
        if self._running_instance:
            self._running_instance.stop()
        self._running_instance = self

        try:
            for i, msg in enumerate(self.consumer):
                try:
                    msg = msg.value.decode('utf-8')
                    logger.info('Reading from queue: %s', msg[:100])
                    tweet = Tweet.build_from_kafka_message(msg)
                    tweet.save()
                except ValidationError as e:
                    logger.error(msg[:100])
                    logger.error('Poison message for Cassandra: %s', str(e))
                except TypeError as e:
                    logger.error(msg[:100])
                    logger.error(e)
                except Exception as e:
                    logger.error(type(e))
                    logger.error(msg[:100])
                    logger.error(e)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        logger.info('Closing consumer connection...')
        self.consumer.close()
        self._running_instance = None
        logger.info('Consumer connection closed!')
