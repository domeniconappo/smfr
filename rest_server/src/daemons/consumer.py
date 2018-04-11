import logging
import threading

from cassandra.cqlengine import ValidationError
from kafka import KafkaConsumer

from server.config import RestServerConfiguration


class Consumer:
    config = RestServerConfiguration()
    _running_instance = None
    _lock = threading.RLock()

    logger = logging.getLogger(__name__)
    logger.setLevel(config.logger_level)

    @classmethod
    def running_instance(cls):
        """

        :return:
        """
        with cls._lock:
            return cls._running_instance

    @classmethod
    def set_running(cls, inst=None):
        with cls._lock:
            cls._running_instance = inst

    @classmethod
    def build_and_start(cls):
        """

        :return:
        """
        consumer = cls()
        t_cons = threading.Thread(target=consumer.start, name='Consumer', daemon=True)
        t_cons.start()
        assert cls.running_instance() == consumer

    def __init__(self, group_id='SMFR', auto_offset_reset='earliest'):
        self.topic = self.config.kafka_topic
        self.bootstrap_server = self.config.kafka_boot_server
        self.auto_offset_reset = auto_offset_reset
        self.group_id = group_id
        self.consumer = KafkaConsumer(self.topic, group_id=self.group_id,
                                      auto_offset_reset=self.auto_offset_reset,
                                      bootstrap_servers=self.bootstrap_server)

    def start(self):
        """

        :return:
        """
        with self._lock:
            if self._running_instance:
                self._running_instance.stop()
            self.logger.debug('Setting running instance to %s', str(self))
            self.set_running(inst=self)

        self.logger.info('Consumer started %s', str(self))
        from server.models import Tweet

        try:
            for i, msg in enumerate(self.consumer):
                try:
                    msg = msg.value.decode('utf-8')
                    self.logger.debug('Reading from queue: %s', msg[:120])
                    tweet = Tweet.build_from_kafka_message(msg)
                    tweet.save()
                except ValidationError as e:
                    self.logger.error(msg[:100])
                    self.logger.error('Poison message for Cassandra: %s', str(e))
                except (ValueError, TypeError) as e:
                    self.logger.error(msg[:100])
                    self.logger.error(e)
                except Exception as e:
                    self.logger.error(type(e))
                    self.logger.error(msg[:100])
                    self.logger.error(e)
        except ValueError:
            # tipically an I/O operation on closed epoll object
            # as the consumer can be disconnected in another thread (see signal handling in start.py)
            if self.consumer._closed:
                self.logger.info("Consumer was disconnected during I/O operations. Exited.")
            elif self.running_instance() and not self.consumer._closed:
                self.running_instance().stop()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.consumer.close()
        self.set_running(inst=None)
        self.logger.info('Consumer connection closed!')
