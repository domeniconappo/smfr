"""
Core Utils module
"""
from flask.json import JSONEncoder

LOGGER_FORMAT = '%(asctime)s: SMFR - <%(name)s[%(filename)s:%(lineno)d]>[%(levelname)s] (%(threadName)s) %(message)s'
LOGGER_DATE_FORMAT = '%Y%m%d %H:%M:%S'
SMFR_DATE_FORMAT = '%Y-%m-%d %H:%M'


def _running_in_docker():
    """
    Check if the calling code is running in a Docker
    :return: True if caller code is running inside a Docker container
    :rtype: bool
    """
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


smfr_json_encoder = JSONEncoder().default


RUNNING_IN_DOCKER = _running_in_docker()
