import os

from alembic.config import Config
from alembic import command

migration_dir = '/smfr_libs/models/smfrcore/models/sql/migrations/'


def run_migrations(app, for_tests=False):
    """
    """
    # retrieves the directory that *this* file is in
    # this assumes the alembic.ini is also contained in this same directory
    if (app.config.get('TESTING') and not for_tests) or (not app.config.get('TESTING') and for_tests):
        return
    config_file = os.path.join(migration_dir, "alembic.ini")
    db_uri = app.config['SQLALCHEMY_DATABASE_URI']
    config = Config(file_=config_file)
    config.set_main_option("script_location", migration_dir)
    config.set_main_option('script_location', migration_dir)
    config.set_main_option('sqlalchemy.url', db_uri)
    command.upgrade(config, 'head')
