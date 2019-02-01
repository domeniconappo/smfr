import os

from alembic.config import Config
from alembic import command


def run_migrations(migrations_dir, dsn):
    """
    run_migrations('/path/to/migrations', 'postgresql:///my_database')
    :param migrations_dir: path to migrations
    :param dsn: db uri
    """
    # retrieves the directory that *this* file is in
    # this assumes the alembic.ini is also contained in this same directory
    config_file = os.path.join(migrations_dir, "alembic.ini")

    config = Config(file_=config_file)
    config.set_main_option("script_location", migrations_dir)
    config.set_main_option('script_location', migrations_dir)
    config.set_main_option('sqlalchemy.url', dsn)
    command.upgrade(config, 'head')
