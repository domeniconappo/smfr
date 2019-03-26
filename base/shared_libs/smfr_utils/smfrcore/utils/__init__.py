"""
Core Utils module
"""
import sys
import argparse
import logging
from logging import StreamHandler
from collections import defaultdict
import functools
import time
import threading
from multiprocessing.managers import DictProxy, BaseManager
import os
from datetime import timedelta

import schedule
from flask.json import JSONEncoder

logger = logging.getLogger('SMFR utils')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
IS_DEVELOPMENT = os.getenv('DEVELOPMENT', '0') in ('1', 'yes', 'YES', 'Yes')
UNDER_TESTS = any('pytest' in x for x in sys.argv)
FALSE_VALUES = (False, 0, None, 'False', 'false', 'NO', 'no', 'No', '0', 'FALSE', 'null', 'None', 'NULL', 'NONE')

LOGGER_FORMAT = '[%(asctime)s: %(name)s (%(threadName)s@%(processName)s) <%(filename)s:%(lineno)d>-%(levelname)s] %(message)s'
LOGGER_DATE_FORMAT = '%Y%m%d%H%M%S'
SMFR_DATE_FORMAT = '%Y-%m-%d %H:%M'

DEFAULT_HANDLER = StreamHandler()
formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
DEFAULT_HANDLER.setFormatter(formatter)
NULL_HANDLER = logging.NullHandler()

RGB = {'red': '255 0 0', 'orange': '255 128 0', 'gray': '225 225 225'}


def _running_in_docker():
    """
    Check if the calling code is running in a Docker
    :return: True if caller code is running inside a Docker container
    :rtype: bool
    """
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


IN_DOCKER = _running_in_docker()


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
        except Exception:
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


class DefaultDictSyncManager(BaseManager):
    pass


class Singleton(type):
    """

    """
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]


class CustomJSONEncoder(JSONEncoder):
    """

    """

    def default(self, obj):
        if isinstance(obj, (np.float32, np.float64, Decimal)):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Choice):
            return float(obj.code)
        elif isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, OrderedMapSerializedKey):
            res = {}
            for k, v in obj.items():
                if isinstance(v, tuple):
                    try:
                        res[k] = dict((v,))
                    except ValueError:
                        res[k] = (v[0], v[1])
                else:
                    res[k] = v

            return res
        return super().default(obj)


class ParserHelpOnError(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


# multiprocessing.Manager does not include defaultdict: we need to use a customized Manager
DefaultDictSyncManager.register('defaultdict', defaultdict, DictProxy)

smfr_json_encoder = CustomJSONEncoder().default
