from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from smfrcore.models.sql import create_app, sqldb

from .utils import run_migrations

app = create_app()
db_uri = app.config['SQLALCHEMY_DATABASE_URI']
engine = create_engine(db_uri)

if not database_exists(engine.url):
    create_database(engine.url)

run_migrations('/smfr_libs/models/smfrcore/models/sql/migrations/', db_uri)

app.app_context().push()


def test_undertests():
    from smfrcore.utils import UNDER_TESTS
    assert UNDER_TESTS


def test_mysql():
    from smfrcore.models.sql import TwitterCollection
    res = TwitterCollection.query.all()
    assert len(res) == 0
