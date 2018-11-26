import os
import time
import socket
from urllib3.exceptions import ProtocolError
import logging

from elasticsearch.exceptions import TransportError
from mordecai import Geoparser

from smfrcore.models.sql import Nuts2Finder
from smfrcore.utils import IN_DOCKER, DEFAULT_HANDLER


logger = logging.getLogger('GEOCODER')
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))
logger.addHandler(DEFAULT_HANDLER)

geonames_host = 'geonames' if IN_DOCKER else '127.0.0.1'


class Geocoder:
    def __init__(self):
        ok = False
        retries = 10
        while not ok:
            try:
                self.tagger = Geoparser(geonames_host)
            except (TransportError, ConnectionError) as e:
                logger.error(e)
                logger.warning('ES Geonames is not responding...waiting for bootstrap')
                time.sleep(5)
                retries -= 1
                if retries < 0:
                    raise ConnectionError
            else:
                ok = True

    def geoparse_tweet(self, tweet):
        """
        Mordecai geoparsing
        :param tweet: smfrcore.models.cassandra.Tweet object
        :return: list of tuples of lat/lon/country_code/place_name
        """
        # try to geoparse
        mentions_list = []
        res = []
        retries = 10
        while retries >= 0:
            try:
                res = self.tagger.geoparse(tweet.full_text)
            except (ConnectionResetError, ProtocolError, socket.timeout) as e:
                logger.error(e)
                logger.warning('ES gazetter is not responding...throttling 15 secs')
                time.sleep(15)
                retries -= 1
            except Exception as e:
                logger.error(e)
                time.sleep(3)
                retries -= 1
            else:
                break

        for result in res:
            if 'lat' not in result.get('geo', {}):
                continue
            mentions_list.append((float(result['geo']['lat']),
                                  float(result['geo']['lon']),
                                  result['geo'].get('country_code3', ''),
                                  result['geo'].get('place_name', ''),
                                  result['geo'].get('geonameid', '')))
        return mentions_list

    def find_nuts_heuristic(self, tweet):
        """
        The following heuristic is applied:

        #1 First, a gazetteer is run on the tweet to find location mentions

        #2 If no location mention is found:
            If the tweet contains (longitude, latitude):
                the NUTS2 area containing that (longitude, latitude) is returned (nuts2source="coordinates")
            Otherwise, NULL is returned
        #3 If location mentions mapping to a list of NUTS2 areas are found:
            If the tweet contains (longitude, latitude), then if any of the NUTS2 areas contain that point, that NUTS2
            area is returned (nuts2source="coordinates-and-mentions")
            Otherwise
                If there is a single NUTS2 area in the list, that NUTS2 area is returned (nuts2source="mentions")
                Otherwise, check if user location is in one of the NUTS list and return it. If not, NULL is returned

        :param tweet: Tweet object
        :return: tuple (coordinates, nuts2, nuts_source, country_code, place, geonameid)
        """
        mentions = self.geoparse_tweet(tweet)
        tweet_coords = tweet.coordinates if tweet.coordinates != (None, None) else tweet.centroid
        user_location = tweet.user_location
        coordinates, nuts2, nuts_source, country_code, place, geonameid = None, None, '', '', '', ''

        if tweet_coords != (None, None):

            coordinates, nuts2, nuts_source, country_code, place, geonameid = self._nuts_from_tweet_coords(mentions, tweet_coords)
        elif len(mentions) == 1:
            coordinates, nuts2, nuts_source, country_code, place, geonameid = self._nuts_from_one_mention(mentions)
        elif user_location:
            # no geolocated tweet and one or more mentions...try to get location from user
            coordinates, nuts2, nuts_source, country_code, place, geonameid = self._nuts_from_user_location(mentions, user_location)

        return coordinates, nuts2, nuts_source, country_code, place, geonameid

    def _nuts_from_user_location(self, mentions, user_location):
        coordinates, nuts2, nuts_source, country_code, place, geonameid = None, None, '', '', '', ''
        res = self.tagger.geoparse(user_location)
        if res and res[0] and 'lat' in res[0].get('geo', {}):
            res = res[0]
            user_coordinates = (float(res['geo']['lat']), float(res['geo']['lon']))
            nuts2user = Nuts2Finder.find_nuts2_by_point(*user_coordinates) or Nuts2Finder.find_nuts2_by_name(user_location)

            if not nuts2user:
                coordinates = user_coordinates
                nuts_source = 'user'
                country_code = res['geo'].get('country_code3', '')
                place = user_location
                geonameid = res['geo'].get('geonameid', '')
                return coordinates, nuts2, nuts_source, country_code, place, geonameid

            for mention in mentions:
                # checking the list of mentioned places coordinates
                latlong = mention[0], mention[1]
                nuts2mentions = Nuts2Finder.find_nuts2_by_point(*latlong)
                if nuts2mentions and nuts2mentions.id == nuts2user.id:
                    coordinates = latlong
                    nuts_source = 'mentions-and-user'
                    nuts2 = nuts2mentions
                    country_code = mention[2]
                    place = mention[3]
                    geonameid = mention[4]
                    break
        return coordinates, nuts2, nuts_source, country_code, place, geonameid

    def _nuts_from_one_mention(self, mentions):
        nuts2 = None
        mention = mentions[0]
        coordinates = mention[0], mention[1]
        country_code = mention[2]
        place = mention[3]
        geonameid = mention[4]
        nuts_source = 'mentions'
        nuts2mention = Nuts2Finder.find_nuts2_by_point(*coordinates)
        if nuts2mention:
            nuts2 = nuts2mention
        return coordinates, nuts2, nuts_source, country_code, place, geonameid

    def _nuts_from_tweet_coords(self, mentions, tweet_coords):
        nuts2, country_code, place, geonameid = None, '', '', ''

        coordinates = tweet_coords
        nuts_source = 'coordinates'

        nuts2tweet = Nuts2Finder.find_nuts2_by_point(*tweet_coords)

        if not nuts2tweet:
            return coordinates, nuts2, nuts_source, country_code, place, geonameid

        if not mentions:
            nuts2 = nuts2tweet
        elif len(mentions) > 1:
            for mention in mentions:
                # checking the list of mentioned places coordinates
                latlong = mention[0], mention[1]
                nuts2mention = Nuts2Finder.find_nuts2_by_point(*latlong)
                if nuts2mention and nuts2tweet.id == nuts2mention.id:
                    coordinates = latlong
                    nuts2 = nuts2tweet
                    nuts_source = 'coordinates-and-mentions'
                    country_code = mention[2]
                    place = mention[3]
                    geonameid = mention[4]
        elif len(mentions) == 1:
            coordinates, nuts2, nuts_source, country_code, place, geonameid = self._nuts_from_one_mention(mentions)
        return coordinates, nuts2, nuts_source, country_code, place, geonameid
