import logging

from smfrcore.utils import DEFAULT_HANDLER

from server.config import RestServerConfiguration

logger = logging.getLogger('RestServer Collectors')
logger.setLevel(RestServerConfiguration.logger_level)
logger.addHandler(DEFAULT_HANDLER)


def get():
    """
    GET /collectors
    Get all collectors
    :return:
    """
    config = RestServerConfiguration()
    collectors = config.collectors
    res = []
    for ttype, c in collectors.items():
        item = {
            'trigger_type': ttype,
            'errors': list(c.streamer.errors),
            'status': 'connected' if c.streamer.connected else 'disconnected',
            'collections': [co.id for co in c.streamer.collections],
        }
        res.append(item)
    return res, 200


def restart(trigger_type):
    config = RestServerConfiguration()
    collector_to_restart = config.collectors[trigger_type]
    collector_to_restart.restart()
    return {'succes': True}, 201
