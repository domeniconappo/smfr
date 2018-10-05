import os
import datetime
import copy
from math import cos, sin, atan2, sqrt

import arrow
from arrow.parser import ParserError
from cachetools import TTLCache
from fuzzywuzzy import process, fuzz
from sqlalchemy import Column, Integer, ForeignKey, TIMESTAMP, Boolean, BigInteger, orm
from sqlalchemy_utils import ChoiceType, ScalarListType, JSONType

from .base import sqldb, SMFRModel, LongJSONType
from .users import User
from .nuts import Nuts2

ENABLE_GEOPARSER_TWEET = bool(os.environ.get('ENABLE_GEOPARSER_TWEET', False))


class TwitterCollection(SMFRModel):
    """

    """
    __tablename__ = 'virtual_twitter_collection'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    ACTIVE_STATUS = 'active'
    INACTIVE_STATUS = 'inactive'

    TRIGGER_ONDEMAND = 'on-demand'
    TRIGGER_BACKGROUND = 'background'
    TRIGGER_MANUAL = 'manual'

    TRIGGERS = [
        (TRIGGER_BACKGROUND, 'Background'),
        (TRIGGER_ONDEMAND, 'On Demand'),
        (TRIGGER_MANUAL, 'Manual'),
    ]

    STATUSES = [
        (ACTIVE_STATUS, 'Running'),
        (INACTIVE_STATUS, 'Stopped'),
    ]

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    forecast_id = Column(Integer)
    trigger = Column(ChoiceType(TRIGGERS), nullable=False)
    tracking_keywords = Column(ScalarListType(str), nullable=True)
    locations = Column(JSONType, nullable=True)
    languages = Column(ScalarListType(str), nullable=True)
    status = Column(ChoiceType(STATUSES), nullable=False, default='inactive')
    efas_id = Column(Integer, nullable=True)
    started_at = Column(TIMESTAMP, nullable=True)
    stopped_at = Column(TIMESTAMP, nullable=True)
    runtime = Column(TIMESTAMP, nullable=True)
    use_pipeline = Column(Boolean, nullable=False, default=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    user = sqldb.relationship('User', backref=sqldb.backref('collections', uselist=True))

    cache = TTLCache(50, 60 * 60 * 6)  # 6 hours caching

    cache_keys = {
        'on-demand': 'active-on-demand',
        'background': 'background-collection',
        'collection': 'id-{}',
        'manual': 'active-manual',
    }

    def __str__(self):
        return 'Collection<{o.id}: {o.forecast_id} - {o.trigger}>' \
               '\n Tracking: {o.tracking_keywords} ' \
               '\n Bbox: {o.locations}'.format(o=self)

    def __eq__(self, other):
        return self.efas_id == other.efas_id and self.trigger == other.trigger \
               and self.tracking_keywords == other.tracking_keywords \
               and self.languages == other.languages and self.locations == other.locations

    def __init__(self, *args, **kwargs):
        self.nuts2 = None
        if kwargs.get('efas_id') is not None:
            self.nuts2 = Nuts2.get_by_efas_id(kwargs['efas_id'])
        super().__init__(*args, **kwargs)

    def _set_keywords_and_languages(self, keywords, languages):

        if not keywords:
            keywords = []
        if not languages:
            languages = []

        self.languages = []
        self.tracking_keywords = []

        if isinstance(keywords, list):
            self.tracking_keywords = keywords
        elif isinstance(keywords, str):
            if ':' in keywords:
                # keywords from Web UI as text in the form of groups "lang1:kw1,kw2 lang2:kw3,kw4"
                kwdict = {}
                groups = keywords.split(' ')
                for g in groups:
                    lang, kws = list(map(str.strip, g.split(':')))
                    kwdict[lang] = list(map(str.strip, kws.split(',')))
                self.languages = sorted(list(kwdict.keys()))
                self.tracking_keywords = sorted(list(set(w for s in kwdict.values() for w in s)))
            else:
                # keywords from Web UI as text in the form of comma separated words "kw1,kw2,kw3,kw4"
                self.tracking_keywords = list(map(str.strip, sorted(list(set(w for w in keywords.split(','))))))
                self.languages = []

        if languages and not self.languages and isinstance(languages, list):
            # default keywords with languages
            self.languages = languages

        # refine keywords: some NUTS3 names contains more than one city
        refined_keywords = []
        for kw in self.tracking_keywords:
            l_split = kw.split(' and ')
            for subkw in l_split:
                sub_list = subkw.split(' & ')
                refined_keywords += sub_list
        self.tracking_keywords = list(set(refined_keywords))

    def _set_locations(self, locations):
        locations = locations or {}
        if isinstance(locations, dict) and not all(
                locations.get(k) for k in ('min_lon', 'min_lat', 'max_lon', 'max_lat')):
            locations = {}
        if locations:
            if isinstance(locations, str):
                coords = list(map(str.strip, locations.split(',')))
                locations = {'min_lon': coords[0], 'min_lat': coords[1], 'max_lon': coords[2], 'max_lat': coords[3]}
            elif isinstance(locations, dict):
                tmp_locations = locations.copy()
                for k, v in tmp_locations.items():
                    if k not in ('min_lon', 'min_lat', 'max_lon', 'max_lat'):
                        del locations[k]
                        continue
                    locations[k] = round(float(v), 3)
        self.locations = locations

    @orm.reconstructor
    def init_on_load(self):
        self.nuts2 = None
        if self.efas_id is not None:
            self.nuts2 = Nuts2.get_by_efas_id(efas_id=self.efas_id)

    @classmethod
    def create(cls, **data):
        user = data.get('user') or User.query.filter_by(role='admin').first()
        if not user:
            raise SystemError('You have to configure at least one admin user for SMFR system')

        obj = cls()
        obj.status = data.get('status', cls.INACTIVE_STATUS)
        trigger = data['trigger']
        runtime = cls.convert_runtime(data.get('runtime'))
        obj.runtime = runtime
        obj.forecast_id = data.get('forecast_id')

        if trigger == cls.TRIGGER_BACKGROUND:
            existing = cls.get_active_background()
            if existing:
                raise ValueError("You can't have more than one running background collection. "
                                 "First stop the active background collection.")
        elif trigger == cls.TRIGGER_ONDEMAND:
            existing = cls.query.filter_by(efas_id=data['efas_id']).first()
            if existing:
                obj = existing
                obj.runtime = existing.runtime if existing.forecast_id == obj.forecast_id else obj.runtime
            if not existing or not obj.started_at or (obj.stopped_at and obj.started_at and obj.stopped_at > obj.started_at):
                obj.started_at = datetime.datetime.utcnow()
            obj.status = cls.ACTIVE_STATUS  # force active status when creating/updating on demand collections

        obj.efas_id = data.get('efas_id')
        obj.trigger = trigger
        obj.user_id = user.id if user else 1
        obj.use_pipeline = data.get('use_pipeline', False)
        obj._set_keywords_and_languages(data.get('keywords') or [], data.get('languages') or [])
        obj._set_locations(data.get('bounding_box') or data.get('locations'))
        obj.save()

        # updating caches
        cls._update_caches(obj)
        return obj

    @classmethod
    def _update_caches(cls, obj):
        if obj.trigger == cls.TRIGGER_BACKGROUND:
            cls.cache[cls.cache_keys['background']] = obj
        elif obj.trigger == cls.TRIGGER_ONDEMAND:
            current_ondemand_collections = copy.deepcopy(cls.cache[cls.cache_keys['on-demand']])
            for collection in current_ondemand_collections:
                if obj.efas_id == collection.efas_id:
                    current_ondemand_collections.remove(collection)
            if obj not in current_ondemand_collections:
                # This test would fail if keywords/locations are different.
                # That's why we first remove the collection with same efas_id of the one we are adding
                current_ondemand_collections.append(obj)
            cls.cache[cls.cache_keys['on-demand']] = current_ondemand_collections
        elif obj.trigger == cls.TRIGGER_MANUAL:
            current_manual_collections = copy.deepcopy(cls.cache[cls.cache_keys['manual']])
            current_manual_collections.append(obj)
            cls.cache[cls.cache_keys['manual']] = current_manual_collections

        cls.cache[cls.cache_keys['collection'].format(obj.id)] = obj

    @classmethod
    def add_rra_events(cls, events):
        """

        :param events: Dictionary with the structure
            events[event['ID']] = {
            'efas_id': efas_id,  # event['ID'] is the same of efas_id
            'trigger': 'on-demand', 'efas_name': nuts2.efas_name,
            'nuts': nuts_id, 'country': country_name, 'lead_time': lead_time,
            'keywords': cities, 'bbox': bbox, 'forecast': date  # forecast will have format YYYYMMDDHH
            }
        :type events: dict
        :return:
        """
        collections = []
        for efas_id, event in events.items():
            runtime = cls.runtime_from_leadtime(event['lead_time'])
            data = {'trigger': event['trigger'], 'runtime': runtime, 'status': cls.ACTIVE_STATUS,
                    'locations': event['bbox'], 'use_pipeline': True,
                    'timezone': event.get('tzclient', '+00:00'), 'keywords': event['keywords'],
                    'efas_id': event['efas_id'], 'forecast_id': int(event['forecast'])}
            collection = cls.create(**data)
            collections.append(collection)
        return collections

    @classmethod
    def get_active_background(cls):
        key = cls.cache_keys['background']
        res = cls.cache.get(key)
        if not res:
            res = cls.query.filter_by(trigger=cls.TRIGGER_BACKGROUND, status=cls.ACTIVE_STATUS).first()
            cls.cache[key] = res
        return res

    @classmethod
    def get_active_ondemand(cls):
        key = cls.cache_keys['on-demand']
        res = cls.cache.get(key)
        if not res:
            res = cls.query.filter_by(trigger=cls.TRIGGER_ONDEMAND, status=cls.ACTIVE_STATUS).all()
            cls.cache[key] = res
        return res

    @classmethod
    def get_active_manual(cls):
        key = cls.cache_keys['manual']
        res = cls.cache.get(key)
        if not res:
            res = cls.query.filter_by(trigger=cls.TRIGGER_MANUAL, status=cls.ACTIVE_STATUS).all()
            cls.cache[key] = res
        return copy.deepcopy(res)

    @classmethod
    def get_collection(cls, collection_id):
        key = cls.cache_keys['collection'].format(collection_id)
        res = cls.cache.get(key)
        if not res:
            res = cls.query.get(collection_id)
            cls.cache[key] = res
        return res

    def deactivate(self):
        self.status = self.INACTIVE_STATUS
        self.stopped_at = datetime.datetime.utcnow()
        self.save()

    def activate(self):
        self.status = self.ACTIVE_STATUS
        self.started_at = datetime.datetime.utcnow()
        self.save()

    def delete(self):
        key = self.cache_keys['collection'].format(self.id)
        self.cache[key] = None
        super().delete()

    @classmethod
    def update_status_by_runtime(cls):
        """
        Deactivate collections if runtime is 'expired'
        This method should be scheduled every 12 hours
        """
        updated = False
        collections = cls.query.filter(cls.runtime.isnot(None), cls.status == cls.ACTIVE_STATUS)
        for c in collections:
            if c.runtime < datetime.datetime.now():
                c.status = cls.INACTIVE_STATUS
                c.save()
                updated = True
        return updated

    @classmethod
    def runtime_from_leadtime(cls, lead_time):
        """

        :param lead_time: number of days before the peak occurs
        :return: runtime in format %Y-%m-%d %H:%M
        """
        runtime = (datetime.datetime.utcnow() + datetime.timedelta(days=int(lead_time) + 2)).replace(hour=0, minute=0, second=0, microsecond=0)
        return runtime.strftime('%Y-%m-%d %H:%M')

    @classmethod
    def convert_runtime(cls, runtime):
        """
        datetime objects are serialized by Flask json decoder in the format 'Thu, 02 Aug 2018 02:45:00 GMT'
        arrow format is 'ddd, DD MMM YYYY HH:mm:ss ZZZ', equivalent to '%a, %d %b %Y %I:%M:%S %Z'
        :param runtime:
        :return: datetime object
        """
        if not runtime:
            return None
        try:
            res = arrow.get(runtime).datetime.replace(tzinfo=None)
        except ParserError:
            res = arrow.get(runtime, 'ddd, DD MMM YYYY HH:mm:ss ZZZ').datetime.replace(tzinfo=None)
        return res

    @property
    def efas_name(self):
        return None if self.efas_id is None else self.nuts2.efas_name

    @property
    def efas_country(self):
        return None if self.efas_id is None else self.nuts2.country

    @property
    def bboxfinder(self):
        bbox = ''
        if self.locations and all(v for v in self.locations.values()):
            bbox = '{},{},{},{}'.format(self.locations['min_lat'], self.locations['min_lon'],
                                        self.locations['max_lat'], self.locations['max_lon'])
        return '' if not bbox else 'http://bboxfinder.com/#{}'.format(bbox)

    @property
    def bounding_box(self):
        bbox = ''
        if self.locations and all(v for v in self.locations.values()):
            bbox = 'Lower left: {min_lon} - {min_lat}, Upper Right: {max_lon} - {max_lat}'.format(**self.locations)
        return bbox

    def is_tweet_in_bounding_box(self, original_tweet_dict):
        from smfrcore.models import Tweet
        if not self.locations or not self.locations.get('min_lat'):
            return False
        res = False

        min_lat, max_lat, min_lon, max_lon = self.locations['min_lat'], self.locations['max_lat'], self.locations['min_lon'], self.locations['max_lon']
        lat, lon = Tweet.coords_from_raw_tweet(original_tweet_dict)
        min_tweet_lat, max_tweet_lat, min_tweet_lon, max_tweet_lon = Tweet.get_tweet_bbox(original_tweet_dict)

        if lat and lon:
            res = min_lat <= lat <= max_lat and min_lon <= lon <= max_lon

        if not res and all((min_tweet_lat, min_tweet_lon, max_tweet_lat, max_tweet_lon)):
            # determine the coordinates of the intersection rectangle
            lon_left = max(min_lon, min_tweet_lon)
            lat_bottom = max(min_lat, min_tweet_lat)
            lon_right = min(max_lon, max_tweet_lon)
            lat_top = min(max_lat, max_tweet_lat)

            res = lon_left <= lon_right and lat_bottom <= lat_top
        return res

    @property
    def centroid(self):
        """
        Provide a relatively accurate center lat, lon returned as a list pair, given
        a list of list pairs.
        ex: in: geolocations = ((lat1,lon1), (lat2,lon2),)
            out: (center_lat, center_lon)
        """
        if not self.locations or not self.locations.get('min_lat'):
            # return europe center (ukraine)
            return 48.499998, 23.3833318
        x = 0
        y = 0
        z = 0
        coords = ((self.locations['min_lat'], self.locations['min_lon']), (self.locations['max_lat'], self.locations['max_lon']))
        for lat, lon in coords:
            lat = float(lat)
            lon = float(lon)
            x += cos(lat) * cos(lon)
            y += cos(lat) * sin(lon)
            z += sin(lat)

        x = float(x / len(coords))
        y = float(y / len(coords))
        z = float(z / len(coords))
        return atan2(y, x), atan2(z, sqrt(x * x + y * y))

    def tweet_matched_keyword(self, original_tweet_dict):
        from smfrcore.models import Tweet

        matching_properties_text = Tweet.get_contributing_match_keywords_fields(original_tweet_dict)
        text_to_match = matching_properties_text.strip()
        res = None
        for kw in self.tracking_keywords:
            if kw in text_to_match:
                res = kw
        if not res:
            res = process.extractOne(text_to_match, self.tracking_keywords,
                                     scorer=fuzz.partial_ratio,
                                     score_cutoff=80)
        return res[0] if res else None

    @property
    def is_active(self):
        return self.status == self.ACTIVE_STATUS

    @property
    def is_ondemand(self):
        return self.trigger == self.TRIGGER_ONDEMAND

    @property
    def is_using_pipeline(self):
        return self.is_ondemand or self.use_pipeline


class Aggregation(SMFRModel):
    """

    """
    __tablename__ = 'aggregation'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_id = Column(Integer, ForeignKey('virtual_twitter_collection.id'))
    collection = sqldb.relationship('TwitterCollection', backref=sqldb.backref('aggregation', uselist=False))
    values = Column(LongJSONType, nullable=False)
    last_tweetid_collected = Column(BigInteger, nullable=True)
    last_tweetid_annotated = Column(BigInteger, nullable=True)
    last_tweetid_geotagged = Column(BigInteger, nullable=True)
    timestamp_start = Column(TIMESTAMP, nullable=True)
    timestamp_end = Column(TIMESTAMP, nullable=True)
    relevant_tweets = Column(LongJSONType, nullable=True)

    @property
    def data(self):
        # TODO rearrange values dictionary for cleaner output...
        return self.values

    def __str__(self):
        return 'Aggregation ID: {} (collection: {})'.format(self.id, self.collection_id)