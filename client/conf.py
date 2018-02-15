import os

import yaml


def running_in_docker():
    with open('/proc/1/cgroup', 'rt') as f:
        return 'docker' in f.read()


restserver_host = 'localhost' if not running_in_docker() else 'restserver'
os.environ['NO_PROXY'] = restserver_host


def _read_server_configuration():
    config_path = os.path.join(os.path.dirname(__file__), 'config/')
    config_file = 'config.yaml.tpl' if not os.path.exists(os.path.join(config_path, 'config.yaml')) else 'config.yaml'
    with open(os.path.join(config_path, config_file)) as f:
        server_config = yaml.load(f)
    return server_config


class ServerConfiguration:
    server_config = _read_server_configuration()
    debug = server_config['debug']
    rest_server_port = server_config['rest_server_port']
    rest_server_host = restserver_host
    base_path = server_config['base_path']
