import os

from smfrcore.utils import running_in_docker


LOGGER_FORMAT = '%(asctime)s: Client - <%(name)s>[%(levelname)s] (%(threadName)-10s) %(message)s'
DATE_FORMAT = '%Y%m%d %H:%M:%S'


class ServerConfiguration:
    debug = os.environ.get('CLIENT_DEBUG', True)
    restserver_port = os.environ['RESTSERVER_PORT']
    restserver_host = 'localhost' if not running_in_docker() else 'restserver'
    restserver_basepath = os.environ['RESTSERVER_BASEPATH']
    restserver_baseurl = 'http://{}:{}{}'.format(restserver_host, restserver_port, restserver_basepath)
