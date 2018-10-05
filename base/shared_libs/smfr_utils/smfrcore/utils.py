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
logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))

LOGGER_FORMAT = '%(asctime)s: SMFR - <%(name)s[%(filename)s:%(lineno)d]>[%(levelname)s] (%(threadName)s) %(message)s'
LOGGER_DATE_FORMAT = '%Y%m%d %H:%M:%S'
SMFR_DATE_FORMAT = '%Y-%m-%d %H:%M'

DEFAULT_HANDLER = StreamHandler()
formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
DEFAULT_HANDLER.setFormatter(formatter)


def _running_in_docker():
    """
    Check if the calling code is running in a Docker
    :return: True if caller code is running inside a Docker container
    :rtype: bool
    """
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


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


smfr_json_encoder = JSONEncoder().default
RUNNING_IN_DOCKER = _running_in_docker()
