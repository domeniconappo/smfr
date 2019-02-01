import sys

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database, drop_database

from smfrcore.models.sql import create_app
from smfrcore.models.sql.migrations.tools import run_migrations

app = create_app()


def setup_module():
    db_uri = app.config['SQLALCHEMY_DATABASE_URI']
    if not app.config.get('TESTING') or 'test' not in db_uri:
        sys.exit(1)

    engine = create_engine(db_uri)

    if not database_exists(engine.url):
        print('CREATING DB TEST', engine.url)
        print('DB URI', db_uri)
        create_database(engine.url)
        run_migrations('/smfr_libs/models/smfrcore/models/sql/migrations/', db_uri)

    app.app_context().push()


def teardown_module():
    db_uri = app.config['SQLALCHEMY_DATABASE_URI']
    drop_database(db_uri)
