import os
import tarfile

import ujson
from sqlalchemy import Index, Column, Integer, String, Float
from sqlalchemy_utils import JSONType
from shapely.geometry import Point, Polygon

from .base import SMFRModel, LongJSONType


class Nuts2(SMFRModel):
    """

    """

    __tablename__ = 'nuts2'
    __table_args__ = (
        Index('bbox_index', 'min_lon', 'min_lat', 'max_lon', 'max_lat'),
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4',
         'mysql_collate': 'utf8mb4_general_ci'}
    )
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    efas_id = Column(Integer, nullable=False, index=True)
    efas_name = Column(String(1000))
    nuts_id = Column(String(10))
    country = Column(String(500))
    geometry = Column(LongJSONType, nullable=False)
    country_code = Column(String(5))
    country_code3 = Column(String(5))
    min_lon = Column(Float)
    max_lon = Column(Float)
    min_lat = Column(Float)
    max_lat = Column(Float)
    _preloaded = {}

    @classmethod
    def load_nuts(cls):
        rows = cls.query.all()
        cls._preloaded = {r.efas_id: r for r in rows}
        return cls._preloaded

    @classmethod
    def get_by_efas_id(cls, efas_id):
        return cls._preloaded.get(efas_id) or cls.query.filter_by(efas_id=efas_id).first()

    @classmethod
    def efas_id_bbox(cls, efas_id):
        nuts2 = cls._preloaded.get(efas_id) or cls.query.filter_by(efas_id=efas_id).first()
        return nuts2.bbox

    @property
    def bbox(self):
        """

        :return:
        """
        if not self.min_lat:
            return None

        plain_bbox = '({}, {}, {}, {})'.format(self.min_lon, self.min_lat, self.max_lon, self.max_lat)
        bbox = {'min_lat': self.min_lat, 'max_lat': self.max_lat, 'min_lon': self.min_lon, 'max_lon': self.max_lon,
                'plain': plain_bbox,
                'bboxfinder': 'http://bboxfinder.com/#{},{},{},{}'.format(self.min_lat, self.min_lon, self.max_lat, self.max_lon)}
        return bbox

    def coordinates_in_bbox(self, lat, lon):
        return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon

    @classmethod
    def get_nuts2(cls, lat, lon):
        """

        :param lat:
        :param lon:
        :return:
        """
        rows = cls.query.filter(Nuts2.min_lon <= lon, Nuts2.max_lon >= lon, Nuts2.min_lat <= lat, Nuts2.max_lat >= lat)
        return list(rows)

    @classmethod
    def by_country_code(cls, code):
        return list(cls.query.filter_by(country_code3=code.upper()))

    @classmethod
    def from_feature(cls, feature):
        """

        :param feature:
        :return:
        """
        properties = feature['properties']
        efas_id = feature['id']
        geometry = feature['geometry']['coordinates']
        return cls(
            id=properties['ID'],
            efas_id=efas_id,
            efas_name=properties['EFAS_name'],
            nuts_id=properties['NUTS_ID'],
            country=properties['COUNTRY'],
            geometry=geometry,
            country_code=properties['CNTR_CODE'],
        )


class Nuts3(SMFRModel):
    """

    """
    __tablename__ = 'nuts3'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    efas_id = Column(Integer, nullable=False, index=True)
    name = Column(String(500), nullable=False)
    name_ascii = Column(String(500), nullable=False, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    names = Column(JSONType, nullable=False)
    properties = Column(JSONType, nullable=False)
    country_name = Column(String(500), nullable=False)
    nuts_id = Column(String(10), nullable=True)
    country_code = Column(String(5), nullable=False)
    country_code3 = Column(String(5))

    @classmethod
    def from_feature(cls, feature):
        """

        :param feature:
        :return:
        """
        properties = feature['properties']
        names_by_lang = {lang.split('_')[1]: cityname
                         for lang, cityname in properties.items() if lang.startswith('name_')
                         }
        additional_props = {
            'is_megacity': bool(properties['MEGACITY']),
            'is_worldcity': bool(properties['WORLDCITY']),
            'is_admcap': bool(properties['ADM0CAP']),
        }

        return cls(join_id=properties['ID'],
                   name=properties['NAME'] or properties['NUTS_NAME'],
                   name_ascii=properties['NAMEASCII'] or properties['NAME_ASCI'],
                   nuts_id=properties['NUTS_ID'],
                   country_name=properties['SOV0NAME'],
                   country_code=properties['ISO_A2'] or properties['CNTR_CODE'],
                   latitude=properties['LAT'],
                   longitude=properties['LON'],
                   names=names_by_lang,
                   properties=additional_props)


class Nuts2Finder:

    """
    Helper class with Nuts2 methods for finding Nuts2 and countries
    Warning: the method does not return NUTS2 code (e.g. IT6) but the NUTS2 id as it stored in EFAS NUTS2 table
    (that is: the efas_id)
    """
    current_package_dir, _ = os.path.split(__file__)
    data_path = os.path.join(current_package_dir, '../data', 'countries.json.tar.gz')
    with tarfile.open(data_path, 'r:gz') as tar:
        archive = tar.getmembers()[0]
        init_f = tar.extractfile(archive)
        countries = ujson.load(init_f)

    @classmethod
    def _is_in_poly(cls, point, geo):
        try:
            poly = Polygon(geo)
        except AssertionError:
            return False
        else:
            return point.within(poly)

    @classmethod
    def find_nuts2_by_point(cls, lat, lon, check_first=None):
        """
        Check if a point (lat, lon) is in a NUTS2 region and returns its id. None otherwise.
        :param lat: Latitude of a point
        :type lat: float
        :param lon: Longitute of a point
        :type lon: float
        :param check_first: efas_id of the efas region to check first.
               It's tipically the efas id associated to the collection which the tweet belongs to
        :type: int
        :return: Nuts2 object
        """
        if lat is None or lon is None:
            return None
        nuts2 = None
        lat, lon = float(lat), float(lon)
        point = Point(lon, lat)

        if check_first:
            # if check_first is present, check if coords are inside the bbox
            nuts2_to_check_first = Nuts2.get_by_efas_id(check_first)
            if nuts2_to_check_first:
                geometry = nuts2_to_check_first.geometry[0]
                try:
                    if nuts2_to_check_first.coordinates_in_bbox(lat, lon) or cls._is_in_poly(point, geometry):
                        nuts2 = nuts2_to_check_first
                except ValueError:
                    for subgeometry in geometry:
                        if cls._is_in_poly(point, subgeometry):
                            nuts2 = nuts2_to_check_first
        if nuts2:
            return nuts2

        # check polygons, not bboxes
        nuts2_candidates = Nuts2.get_nuts2(lat, lon)
        for n in nuts2_candidates:
            geometry = n.geometry[0]
            try:
                if cls._is_in_poly(point, geometry):
                    nuts2 = n
                    break
            except ValueError:
                for subgeometry in geometry:
                    if cls._is_in_poly(point, subgeometry):
                        nuts2 = n
                        break
                if nuts2:
                    break
        return nuts2

    @classmethod
    def find_country(cls, code):
        """
        Return country name based on country code
        :param code: Country code ISO3
        :return: tuple<str, bool> (country name, is european)
        """
        res = cls.countries.get(code)
        if res:
            return res['name'], res.get('continent') == 'EU'
        nuts2 = Nuts2.by_country_code(code)
        # get the first item with country field populated
        for nut in nuts2:
            if nut.country:
                return nut.country, True
        return '', False

    @classmethod
    def find_nuts2_by_name(cls, user_location):
        return Nuts2.query.filter_by(efas_name=user_location).first()
