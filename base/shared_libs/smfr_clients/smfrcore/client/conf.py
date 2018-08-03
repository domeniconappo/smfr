import os

from smfrcore.utils import RUNNING_IN_DOCKER


class ServerConfiguration:
    debug = os.environ.get('CLIENT_DEBUG', False)
    restserver_port = os.environ.get('RESTSERVER_PORT', 5555)
    restserver_host = 'localhost' if not RUNNING_IN_DOCKER else 'restserver'
    restserver_basepath = os.environ.get('RESTSERVER_BASEPATH', '/1.0')
    restserver_baseurl = 'http://{}:{}{}'.format(restserver_host, restserver_port, restserver_basepath)
