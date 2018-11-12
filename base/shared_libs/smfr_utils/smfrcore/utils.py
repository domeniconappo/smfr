"""
Core Utils module
"""
import logging
from logging import StreamHandler
import functools
import time
import threading
import os
from datetime import timedelta

import schedule
from flask.json import JSONEncoder

logger = logging.getLogger('SMFR utils')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))

FALSE_VALUES = (False, 0, None, 'False', 'false', 'NO', 'no', 'No', '0', 'FALSE', 'null', 'None', 'NULL', 'NONE')

LOGGER_FORMAT = '[%(asctime)s -> (%(threadName)s@%(processName)s) <%(name)s@%(filename)s:%(lineno)d>-%(levelname)s] %(message)s'
LOGGER_DATE_FORMAT = '%Y%m%d%H%M%S'
SMFR_DATE_FORMAT = '%Y-%m-%d %H:%M'

DEFAULT_HANDLER = StreamHandler()
formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
DEFAULT_HANDLER.setFormatter(formatter)
NULL_HANDLER = logging.NullHandler()

RGB = {'red': '255 0 0', 'orange': '255 128 0', 'gray': '225 225 225'}

smfr_json_encoder = JSONEncoder().default


def _running_in_docker():
    """
    Check if the calling code is running in a Docker
    :return: True if caller code is running inside a Docker container
    :rtype: bool
    """
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


IN_DOCKER = _running_in_docker()
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9090,kafka:9092') if IN_DOCKER else '127.0.0.1:9090,127.0.0.1:9092'


def logged_job(job_func):
    @functools.wraps(job_func)
    def wrapper(*args, **kwargs):
        time_start = time.time()
        result = job_func(*args, **kwargs)
        elapsed = time.time() - time_start
        logger.info('Logged Job "%s" completed. Elapsed time: %s' % (job_func.__name__, str(timedelta(seconds=elapsed))))
        return result
    return wrapper


def job_exceptions_catcher(job_func, cancel_on_failure=False):
    @functools.wraps(job_func)
    def wrapper(*args, **kwargs):
        try:
            return job_func(*args, **kwargs)
        except:
            import traceback
            logger.error(traceback.format_exc())
            if cancel_on_failure:
                logger.warning('!!!! ------> Cancelling job: %s', job_func.__name__)
                return schedule.CancelJob
    return wrapper


def run_continuously(interval=1):
    """Continuously run, while executing pending jobs at each elapsed
    time interval.
    @return cease_continuous_run: threading.Event which can be set to
    cease continuous run.
    Please note that it is *intended behavior that run_continuously()
    does not run missed jobs*. For example, if you've registered a job
    that should run every minute and you set a continuous run interval
    of one hour then your job won't be run 60 times at each interval but
    only once.
    """
    cease_continuous_run = threading.Event()

    class ScheduleThread(threading.Thread):
        @classmethod
        def run(cls):
            while not cease_continuous_run.is_set():
                schedule.run_pending()
                time.sleep(interval)

    continuous_thread = ScheduleThread()
    continuous_thread.start()
    return cease_continuous_run


def make_kafka_consumer(topic, kafka_servers=None):
    if not kafka_servers:
        kafka_servers = kafka_bootstrap_servers.split(',')
    retries = 5
    while retries >= 0:
        try:
            consumer = KafkaConsumer(
                topic, check_crcs=False,
                group_id=topic,
                auto_offset_reset='earliest',
                max_poll_records=300, max_poll_interval_ms=1000000,
                bootstrap_servers=kafka_servers,
                session_timeout_ms=10000, heartbeat_interval_ms=3000
            )
            logger.info('+++++++++++++ Consumer to %s was connected', topic)
        except NoBrokersAvailable:
            logger.warning('Waiting for Kafka to boot...')
            time.sleep(5)
            retries -= 1
            if retries < 0:
                logger.error('Kafka server was not listening. Exiting...')
                sys.exit(1)
        else:
            return consumer


def make_kafka_producer(kafka_servers=None):
    if not kafka_servers:
        kafka_servers = kafka_bootstrap_servers.split(',')
    retries = 5
    while retries >= 0:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_servers, retries=5, max_block_ms=120000,
                                     compression_type='gzip', buffer_memory=134217728,
                                     linger_ms=500, batch_size=1048576, )
            logger.info('[OK] KAFKA Producer')

        except NoBrokersAvailable:
            logger.warning('Waiting for Kafka to boot...')
            time.sleep(5)
            retries -= 1
            if retries < 0:
                logger.error('Kafka server was not listening. Exiting...')
                sys.exit(1)
        else:
            return producer
