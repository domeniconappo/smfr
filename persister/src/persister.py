import os
import logging
import threading
from collections import namedtuple

from cassandra import InvalidRequest
from cassandra.cqlengine import ValidationError, CQLEngineException
from kafka import KafkaConsumer
from kafka.errors import CommitFailedError

from smfrcore.models.cassandramodels import Tweet


PersisterConfiguration = namedtuple('PersisterConfiguration', ['logger_level', 'kafka_topic', 'kafka_bootstrap_server'])


class Persister:
    """
        Persister component to save Tweet messages in Cassandra.
        It listens to the Kafka queue, build a Tweet object from messages and save it in Cassandra.
        """
    config = PersisterConfiguration(logger_level=os.environ.get('LOGGING_LEVEL', 'DEBUG'),
                                    kafka_topic=os.environ.get('KAFKA_TOPIC', 'persister'),
                                    kafka_bootstrap_server=os.environ.get('KAFKA_BOOTSTRAP_SERVER', 'kafka:9092'))
    _running_instance = None
    _lock = threading.RLock()

    logger = logging.getLogger(__name__)
    logger.setLevel(config.logger_level)

    @classmethod
    def running_instance(cls):
        """
        The _running Persister object
        :return: Persister instance
        """
        with cls._lock:
            return cls._running_instance

    @classmethod
    def set_running(cls, inst=None):
        """
        Set _running instance
        :param inst: Persister object
        """
        with cls._lock:
            cls._running_instance = inst

    @classmethod
    def build_and_start(cls):
        """
        Instantiate a Persister object and call Persister.start() method in another thread
        """
        persister = cls()
        t_cons = threading.Thread(target=persister.start, name='Persister {}'.format(id(persister)), daemon=True)
        t_cons.start()
        assert cls.running_instance() == persister

    def __init__(self, group_id='SMFR', auto_offset_reset='earliest'):
        self.topic = self.config.kafka_topic
        self.bootstrap_server = self.config.kafka_bootstrap_server
        self.auto_offset_reset = auto_offset_reset
        self.group_id = group_id
        self.consumer = KafkaConsumer(self.topic, group_id=self.group_id,
                                      auto_offset_reset=self.auto_offset_reset,
                                      bootstrap_servers=self.bootstrap_server)

    def start(self):
        """
        Main method that iterate over messages coming from Kafka queue, build a Tweet object and save it in Cassandra
        """

        self.logger.info('Persister started %s', str(self))
        self.set_running(inst=self)

        try:
            for i, msg in enumerate(self.consumer):
                tweet = None
                try:
                    msg = msg.value.decode('utf-8')
                    tweet = Tweet.build_from_kafka_message(msg)
                    self.logger.debug('Read from queue: %s', str(tweet))
                    tweet.save()
                except (ValidationError, ValueError, TypeError, InvalidRequest) as e:
                    self.logger.error(e)
                    self.logger.error('Poison message for Cassandra: %s', str(tweet) if tweet else msg)
                except CQLEngineException as e:
                    self.logger.error(e)
                except Exception as e:
                    self.logger.error(type(e))
                    self.logger.error(e)
                    self.logger.error(msg)

        except CommitFailedError:
            self.logger.error('Persister was disconnected during I/O operations. Exited.')
        except ValueError:
            # tipically an I/O operation on closed epoll object
            # as the consumer can be disconnected in another thread (see signal handling in start.py)
            if self.consumer._closed:
                self.logger.info('Persister was disconnected during I/O operations. Exited.')
            elif self.running_instance() and not self.consumer._closed:
                self.running_instance().stop()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Stop processing messages from queue, close KafkaConsumer and unset _running instance.
        """
        self.consumer.close()
        self.set_running(inst=None)
        self.logger.info('Persister connection closed!')

    def __str__(self):
        return 'Consumer ({}): {}@{}:{}'.format(id(self), self.topic, self.bootstrap_server, self.group_id)
