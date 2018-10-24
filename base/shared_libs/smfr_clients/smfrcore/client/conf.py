import os

from smfrcore.utils import RUNNING_IN_DOCKER


class ServerConfiguration:
    debug = os.getenv('CLIENT_DEBUG', False)
    restserver_port = os.getenv('RESTSERVER_PORT', 5555)
    restserver_host = 'localhost' if not RUNNING_IN_DOCKER else 'restserver'
    restserver_basepath = os.getenv('RESTSERVER_BASEPATH', '/1.0')
    restserver_baseurl = 'http://{}:{}{}'.format(restserver_host, restserver_port, restserver_basepath)
    development = os.getenv('DEVELOPMENT', '0') in ('1', 'yes', 'Yes', 'True', 'true')
