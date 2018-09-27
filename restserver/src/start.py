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
    connexion_app.add_api(pathlib.Path('smfr.yaml'), base_path=config.base_path)

    if SERVER_BOOTSTRAP:

        # this code is only executed at http server bootstrap
        # it's not executed for Flask CLI executions

        config.init_mysql()
        config.init_cassandra()

        background_collector = BackgroundCollector()
        background_collector.start()

        ondemand_collector = OnDemandCollector()
        ondemand_collector.start()

        manual_collector = ManualCollector()
        manual_collector.start()

        logger.debug('---------- Registering collectors in main configuration:\n%s',
                     [background_collector, ondemand_collector, manual_collector])
        config.set_collectors({background_collector.type: background_collector,
                               ondemand_collector.type: ondemand_collector,
                               manual_collector.type: manual_collector})

        def stop_active_collectors(signum, _):
            deactivate_collections = False
            logger.info("Received %d", signum)
            logger.info("Stopping any running collector...")
            background_collector.stop(deactivate=deactivate_collections)
            ondemand_collector.stop(deactivate=deactivate_collections)
            manual_collector.stop(deactivate=deactivate_collections)

        signals = (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)
        for sig in signals:
            signal.signal(sig, stop_active_collectors)
        logger.debug('Registered signals for graceful shutdown: %s', signals)

        # RRA Scheduled jobs. First execution is performed ad bootstrap
        update_ondemand_collections_status()
        add_rra_events()
        schedule_rra_jobs()

    return config.flask_app


app = create_app()
