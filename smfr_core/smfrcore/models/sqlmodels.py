import datetime

from sqlalchemy import Column, Integer, String, TIMESTAMP, Float, ForeignKey
from sqlalchemy_utils import ChoiceType, ScalarListType, JSONType

from .base import SMFRModel, metadata
from ..ext.database import SQLAlchemy


sqldb = SQLAlchemy(metadata=metadata, session_options={'expire_on_commit': False})


class TwitterCollection(SMFRModel):
    """

    """
    __tablename__ = 'virtual_twitter_collection'

    ACTIVE_STATUS = 'active'
    INACTIVE_STATUS = 'inactive'

    TYPES = [
        ('keywords', 'Keywords'),
        ('geo', 'Geotagged'),
    ]

    TRIGGERS = [
        ('background', 'Background'),
        ('on-demand', 'On Demand'),
        ('manual', 'Manual'),
    ]

    STATUSES = [
        (ACTIVE_STATUS, 'Active'),
        (INACTIVE_STATUS, 'Inactive'),
    ]

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    forecast_id = Column(Integer)
    trigger = Column(ChoiceType(TRIGGERS), nullable=False)
    ctype = Column(ChoiceType(TYPES), nullable=False)
    tracking_keywords = Column(ScalarListType(str), nullable=True)
    locations = Column(JSONType, nullable=True)
    languages = Column(ScalarListType(str), nullable=True)
    status = Column(ChoiceType(STATUSES), nullable=False, default='inactive')
    nuts3 = Column(String(50), nullable=True)
    nuts3source = Column(String(255), nullable=True)
    started_at = Column(TIMESTAMP, nullable=True)
    stopped_at = Column(TIMESTAMP, nullable=True)
    runtime = Column(TIMESTAMP, nullable=True)

    def __str__(self):
        return 'VirtualTwitterCollection<{o.id}: {o.forecast_id} - {o.trigger.value}/{o.ctype.value}>'.format(o=self)

    @classmethod
    def build_from_collector(cls, collector):
        """
        Construct a VirtualTwitterCollection object mapped to a row in MySQL
        :param collector: A :class:`daemons.collector.Collector` object
        :return: A :class:`VirtualTwitterCollection` object
        """
        query = collector.query
        collection = cls(
            trigger=collector.trigger,
            ctype=collector.ctype,
            forecast_id=collector.forecast_id,
            nuts3=collector.nuts3,
            nuts3source=collector.nuts3source,
            tracking_keywords=query['track'],
            languages=query['languages'],
            locations=query['locations'],
            runtime=collector.runtime,
        )
        kwargs = {}
        for k, v in vars(collection).items():
            if k in ('_sa_instance_state', 'id'):
                continue
            kwargs[k] = v if not isinstance(v, ChoiceType) else v.value

        existing = TwitterCollection.query.filter_by(**kwargs).first()
        if existing:
            return existing
        collection.save()
        return collection

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def delete(self):
        sqldb.session.delete(self)
        sqldb.session.commit()

    def deactivate(self):
        self.status = self.INACTIVE_STATUS
        self.stopped_at = datetime.datetime.utcnow()
        self.save()

    def activate(self):
        self.status = self.ACTIVE_STATUS
        self.started_at = datetime.datetime.utcnow()
        self.save()


class StoredCollector(SMFRModel):
    """

    """
    __tablename__ = 'stored_collector'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    collection_id = Column(Integer, ForeignKey('virtual_twitter_collection.id'))
    collection = sqldb.relationship("VirtualTwitterCollection",
                                    backref=sqldb.backref("virtual_twitter_collection", uselist=False))
    parameters = Column(JSONType, nullable=False)

    def save(self):
        # we need 'merge' method because objects can be attached to db sessions in different threads
        attached_obj = sqldb.session.merge(self)
        sqldb.session.add(attached_obj)
        sqldb.session.commit()
        self.id = attached_obj.id

    def delete(self):
        sqldb.session.delete(self)
        sqldb.session.commit()

    def __str__(self):
        return 'Collector stored ID: {} (collection: {})'.format(self.id, self.collection_id)


class NutsBoundingBox(SMFRModel):
    """

    """

    __tablename__ = 'nuts_bounding_box'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    min_lon = Column(Float)
    max_lon = Column(Float)
    min_lat = Column(Float)
    max_lat = Column(Float)

    @classmethod
    def nuts3_bbox(cls, id_nuts):
        """

        :param id_nuts:
        :return:
        """
        row = cls.query.filter_by(id=id_nuts).first()
        bbox = {'min_lat': row.min_lat, 'max_lat': row.max_lat, 'min_lon': row.min_lon, 'max_lon': row.max_lon}
        return bbox
