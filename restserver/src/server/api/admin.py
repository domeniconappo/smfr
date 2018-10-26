import logging

from smfrcore.utils import DEFAULT_HANDLER

from server.api.clients import AnnotatorClient, PersisterClient
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
    res = {'collectors': [], 'counters': None}
    for ttype, c in config.collectors.items():
        item = {
            'trigger_type': ttype,
            'errors': c.streamer.errors,
            'status': 'connected' if c.streamer.connected else 'disconnected',
            'collections': [co.id for co in c.streamer.collections],
        }
        res['collectors'].append(item)
    res['counters'] = AnnotatorClient.counters()[0]
    res['persisted'] = PersisterClient.counters()[0]
    return res, 200


def restart_collectors(trigger_type):
    config = RestServerConfiguration()
    collector_to_restart = config.collectors[trigger_type]
    collector_to_restart.restart()
    return {'succes': True}, 201
