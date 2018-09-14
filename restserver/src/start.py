import os
import logging
import pathlib
import signal

import connexion

from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT

from daemons.collector import BackgroundCollector, OnDemandCollector, ManualCollector
from server.config import RestServerConfiguration, SERVER_BOOTSTRAP, MYSQL_MIGRATION
from server.jobs import schedule_rra_jobs, add_rra_events, update_ondemand_collections_status

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
logging.getLogger('cassandra').setLevel(logging.WARNING)


os.environ['NO_PROXY'] = ','.join((RestServerConfiguration.restserver_host,
                                   RestServerConfiguration.annotator_host,
                                   RestServerConfiguration.geocoder_host))


logger = RestServerConfiguration.logger


def create_app():
    if MYSQL_MIGRATION:
        logger.info('======= Configuring APP for database migrations')
        return RestServerConfiguration.configure_migrations()

    connexion_app = connexion.App('SMFR Rest Server', specification_dir='swagger/')
    config = RestServerConfiguration(connexion_app)

    if SERVER_BOOTSTRAP:

        # this code is only executed at http server bootstrap
        # it's not executed for Flask CLI executions

        config.init_mysql()
        config.init_cassandra()

        update_ondemand_collections_status()
        add_rra_events()
        schedule_rra_jobs()

        background_collector = BackgroundCollector()
        background_collector.start()

        ondemand_collector = OnDemandCollector()
        ondemand_collector.start()

        manual_collector = ManualCollector()
        manual_collector.start()

        logger.debug('Registering collectors...\n%s', [background_collector, ondemand_collector, manual_collector])
        config.set_collectors({background_collector.type: background_collector,
                               ondemand_collector.type: ondemand_collector,
                               manual_collector.type: manual_collector})

        def stop_active_collectors(signum, _):
            logger.debug("Received %d", signum)
            logger.debug("Stopping any running collector...")
            background_collector.stop()
            ondemand_collector.stop()
            manual_collector.stop()

        signal.signal(signal.SIGINT, stop_active_collectors)
        signal.signal(signal.SIGTERM, stop_active_collectors)
        signal.signal(signal.SIGQUIT, stop_active_collectors)
        logger.debug('Registered %d %d and %d', signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)

    connexion_app.add_api(pathlib.Path('smfr.yaml'), base_path=config.base_path)
    return config.flask_app


app = create_app()
