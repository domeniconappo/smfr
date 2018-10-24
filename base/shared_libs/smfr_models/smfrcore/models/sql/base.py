import os
from math import ceil

import sqlalchemy_utils
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import orm, MetaData, inspect
from sqlalchemy.dialects.mysql import LONGTEXT
from flask import Flask

from flask_sqlalchemy import SQLAlchemy as SQLAlchemyBase

from smfrcore.utils import RUNNING_IN_DOCKER


class Pagination:
    """Class returned by `Query.paginate`. You can also construct
    it from any other SQLAlchemy query object if you are working
    with other libraries. Additionally it is possible to pass
    ``None`` as query object in which case the `prev` and `next`
    will no longer work.
    """

    def __init__(self, query, page, per_page, total, items):
        #: The query object that was used to create this pagination object.
        self.query = query

        #: The current page number (1 indexed).
        self.page = page

        #: The number of items to be displayed on a page.
        self.per_page = per_page

        #: The total number of items matching the query.
        self.total = total

        #: The items for the current page.
        self.items = items

        if self.per_page == 0:
            self.pages = 0
        else:
            #: The total number of pages.
            self.pages = int(ceil(self.total / float(self.per_page)))

        #: Number of the previous page.
        self.prev_num = self.page - 1

        #: True if a previous page exists.
        self.has_prev = self.page > 1

        #: Number of the next page.
        self.next_num = self.page + 1

        #: True if a next page exists.
        self.has_next = self.page < self.pages

    def prev(self, error_out=False):
        """Returns a `Pagination` object for the previous page."""
        assert self.query is not None, \
            'a query object is required for this method to work'
        return self.query.paginate(self.page - 1, self.per_page, error_out)

    def next(self, error_out=False):
        """Returns a `Pagination` object for the next page."""
        assert self.query is not None, \
            'a query object is required for this method to work'
        return self.query.paginate(self.page + 1, self.per_page, error_out)


class BaseQuery(orm.Query):
    """The default query object used for models. This can be
    subclassed and replaced for individual models by setting
    the Model.query_class attribute. This is a subclass of a
    standard SQLAlchemy sqlalchemy.orm.query.Query class and
    has all the methods of a standard query as well.
    """

    def paginate(self, page, per_page=20, error_out=True):
        """Return `Pagination` instance using already defined query
        parameters.
        """
        if error_out and page < 1:
            raise IndexError

        if per_page is None:
            per_page = self.DEFAULT_PER_PAGE

        items = self.page(page, per_page).all()

        if not items and page != 1 and error_out:
            raise IndexError

        # No need to count if we're on the first page and there are fewer items
        # than we expected.
        if page == 1 and len(items) < per_page:
            total = len(items)
        else:
            total = self.order_by(None).count()

        return Pagination(self, page, per_page, total, items)


class ModelBase:
    """Baseclass for custom user models."""

    #: the query class used. The `query` attribute is an instance
    #: of this class. By default a `BaseQuery` is used.
    query_class = BaseQuery

    #: an instance of `query_class`. Can be used to query the
    #: database for instances of this model.
    query = None

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class QueryProperty:
    """Query property accessor which gives a model access to query capabilities
    via `ModelBase.query` which is equivalent to ``session.query(Model)``.
    """
    def __init__(self, session):
        self.session = session

    def __get__(self, model, model_class):
        mapper = orm.class_mapper(model_class)

        if mapper:
            if not getattr(model_class, 'query_class', None):
                model_class.query_class = BaseQuery

            query_property = model_class.query_class(mapper, session=self.session())

            return query_property


def set_query_property(model_class, session):
    model_class.query = QueryProperty(session)


def save(self):
    # we need 'merge' method because objects can be attached to db sessions in different threads
    attached_obj = sqldb.session.merge(self)
    sqldb.session.add(attached_obj)
    sqldb.session.commit()
    self.id = attached_obj.id


def delete(self):
    sqldb.session.delete(self)
    sqldb.session.commit()


metadata = MetaData()

SMFRModel = declarative_base(cls=ModelBase, metadata=metadata)
SMFRModel.save = save
SMFRModel.delete = delete


class SQLAlchemy(SQLAlchemyBase):
    """Flask extension that integrates alchy with Flask-SQLAlchemy."""
    def __init__(self, app=None, use_native_unicode=True, session_options=None,
                 metadata=None, query_class=BaseQuery, model_class=SMFRModel):
        self.Model = SMFRModel

        super().__init__(app, use_native_unicode, session_options, metadata, query_class, model_class)

    def make_declarative_base(self, model, metadata=None):
        """Creates or extends the declarative base."""
        if self.Model is None:
            self.Model = super().make_declarative_base(model, metadata)
        else:
            set_query_property(self.Model, self.session)
        return self.Model

    @property
    def metadata(self):
        """Returns the metadata"""
        return self.Model.metadata


sqldb = SQLAlchemy(metadata=metadata, session_options={'expire_on_commit': False})


def create_app(app_name='SMFR'):
    _mysql_host = '127.0.0.1' if not RUNNING_IN_DOCKER else os.getenv('MYSQL_HOST', 'mysql')
    _mysql_db_name = os.getenv('MYSQL_DBNAME', 'smfr')
    _mysql_user = os.getenv('MYSQL_USER')
    _mysql_pass = os.getenv('MYSQL_PASSWORD')
    app = Flask(app_name)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://{}:{}@{}/{}?charset=utf8mb4'.format(
        _mysql_user, _mysql_pass, _mysql_host, _mysql_db_name
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_POOL_TIMEOUT'] = 3600
    app.config['SQLALCHEMY_POOL_RECYCLE'] = 1200
    app.config['SQLALCHEMY_POOL_SIZE'] = 10
    sqldb.init_app(app)
    return app


class LongJSONType(sqlalchemy_utils.types.json.JSONType):
    impl = LONGTEXT
