import os

import requests


api_urls = {'incidents': 'https://traffic.api.here.com/traffic/6.3/incidents.json?app_id={app_id}&app_code={app_code}'}


class HereClient:
    app_id = os.getenv('HERE_APP_ID', '')
    app_code = os.getenv('HERE_APP_CODE', '')

    def __init__(self, app='incidents'):
        self.base_uri = api_urls[app].format(app_id=self.app_id, app_code=self.app_code)

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
        url = '{}&bbox={}'.format(self.base_uri, bbox)
        res = requests.get(url)
        return res.json()
