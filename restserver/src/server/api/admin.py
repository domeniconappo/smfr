import logging

from smfrcore.client.api_client import AnnotatorClient, GeocoderClient, PersisterClient, CollectorsClient, SMFRRestException
from smfrcore.utils import DEFAULT_HANDLER
from smfrcore.models.sql import TwitterCollection
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
    res = CollectorsClient.get('all')[0]
    collectors = res['collectors']

    res = {
        'collectors': collectors,
        'counters': AnnotatorClient.counters()[0],
        'persisted': PersisterClient.counters()[0],
        'geo_counters': GeocoderClient.counters()[0]
    }
    res['background_collected'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_BACKGROUND, 'collected'), 0)
    res['ondemand_collected'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_ONDEMAND, 'collected'), 0)
    res['manual_collected'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_MANUAL, 'collected'), 0)
    res['background_annotated'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_BACKGROUND, 'annotated'), 0)
    res['ondemand_annotated'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_ONDEMAND, 'annotated'), 0)
    res['manual_annotated'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_MANUAL, 'annotated'), 0)
    res['background_geotagged'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_BACKGROUND, 'geotagged'), 0)
    res['ondemand_geotagged'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_ONDEMAND, 'geotagged'), 0)
    res['manual_geotagged'] = res['persisted'].pop('{}-{}'.format(TwitterCollection.TRIGGER_MANUAL, 'geotagged'), 0)
    return res, 200


def restart_collectors(trigger_type):
    try:
        CollectorsClient.restart(trigger_type)
    except SMFRRestException as e:
        return {'error': str(e)}, 500
    else:
        return {'succes': True}, 201
