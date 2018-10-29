import os
import logging
from json import JSONDecodeError

import requests

from smfrcore.utils import RGB, DEFAULT_HANDLER

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)


class HereClient:
    api_urls = {
        'incidents': 'https://traffic.api.here.com/traffic/6.3/incidents.json?app_id={app_id}&app_code={app_code}'
    }
    app_id = os.getenv('HERE_APP_ID', '')
    app_code = os.getenv('HERE_APP_CODE', '')

    def __init__(self, app='incidents'):
        self.base_uri = self.api_urls[app].format(app_id=self.app_id, app_code=self.app_code)

    @classmethod
    def _is_flooding_incident(cls, item):
        return 'TRAFFIC_ITEM_DETAIL' in item and \
               'INCIDENT' in item['TRAFFIC_ITEM_DETAIL'] and \
               item['TRAFFIC_ITEM_DETAIL']['INCIDENT'].get('ROAD_HAZARD_INCIDENT', {}).get('ROAD_HAZARD_TYPE_DESC') == 'flooding'

    @classmethod
    def _risk_color(cls, criticality):
        desc = criticality.get('DESCRIPTION', 'minor')
        crit_id = criticality.get('ID', 2)
        if crit_id == '1' or desc == 'major':
            return RGB['orange']
        elif crit_id == '0' or desc == 'critical':
            return RGB['red']
        # crit_id in ('2', '3') or desc in ('lowImpact', 'minor')
        return RGB['gray']

    def get_by_bbox(self, bbox):
        """
        Get Traffic Item incidents by defining a Bounding Box
        :param bbox: A bounding box represents a rectangular area specified by two latitude/longitude pairs;
            the first pair defines the top-left corner of the bounding box, the second the bottom-right corner.
            Format: max_lat,min_lon;min_lat,max_lon
            Example: '42.022,14.851;39.681,18.535'
            Note: The maximum allowed size of the bounding box (width and height) is limited to 10 degrees
        :return: JSON response
        """
        incidents = []
        url = '{}&bbox={}'.format(self.base_uri, bbox)
        res = requests.get(url)
        try:
            res = res.json()
        except JSONDecodeError as e:
            logger.error('Cannot parse response: %s - %s', res.text, str(e))
        else:
            if 'TRAFFIC_ITEMS' not in res or 'TRAFFIC_ITEM' not in res['TRAFFIC_ITEMS'] or not res['TRAFFIC_ITEMS']['TRAFFIC_ITEM']:
                return incidents
            items = [i for i in res['TRAFFIC_ITEMS']['TRAFFIC_ITEM'] if self._is_flooding_incident(i)]
            for item in items:
                try:
                    incident = {
                        'traffic_item_id': item['TRAFFIC_ITEM_ID'],
                        'start_date': item['START_TIME'],
                        'end_date': item['END_TIME'],
                        'lat': item['GEOLOC']['ORIGIN']['LATITUDE'],
                        'lon': item['GEOLOC']['ORIGIN']['LONGITUDE'],
                        'text': 'Flooding incident: from {} to {}. Severity: {}'.format(
                            item['START_TIME'], item['END_TIME'], item['CRITICALITY'].get('DESCRIPTION', 'minor'))
                        ,
                        'risk_color': self._risk_color(item['CRITICALITY'])
                    }
                except KeyError:
                    continue
                else:
                    incidents.append(incident)
        return incidents

