import os
import pathlib

import connexion

from server.config import RestServerConfiguration, SERVER_BOOTSTRAP, MYSQL_MIGRATION

os.environ['NO_PROXY'] = ','.join((RestServerConfiguration.restserver_host,
                                   RestServerConfiguration.annotator_host,
                                   RestServerConfiguration.persister_host,
                                   RestServerConfiguration.collectors_host,
                                   RestServerConfiguration.geocoder_host))


logger = RestServerConfiguration.logger


def create_app():
    if MYSQL_MIGRATION:
        logger.info('======= Configuring APP for database migrations')
        return RestServerConfiguration.configure_migrations()

    connexion_app = connexion.App('SMFR Rest Server', specification_dir='swagger/')
    config = RestServerConfiguration(connexion_app)
    connexion_app.add_api(pathlib.Path('smfr.yaml'), base_path=config.base_path)

    if SERVER_BOOTSTRAP:
        # this code is only executed at http server bootstrap
        # it's not executed for Flask CLI executions

        config.init_mysql()
        config.init_cassandra()

    return config.flask_app


app = create_app()
