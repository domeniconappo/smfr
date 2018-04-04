import logging
import sys
import pathlib

import connexion

from server.config import RestServerConfiguration, LOGGER_FORMAT, DATE_FORMAT

logging.basicConfig(level=logging.INFO if not RestServerConfiguration.debug else logging.DEBUG,
                    format=LOGGER_FORMAT,
                    datefmt=DATE_FORMAT)


def create_app():
    cli_args = sys.argv
    is_server_bootstrapping = 'gunicorn' in cli_args[0]
    connexion_app = connexion.App('SMFR Rest Server', specification_dir='swagger/')
    config = RestServerConfiguration(connexion_app, bootstrap_server=is_server_bootstrapping)
    logger = RestServerConfiguration.logger

    if is_server_bootstrapping:

        # this code is only executed at http server bootstrap
        # it's not executed for Flask CLI executions

        import signal
        from daemons import Consumer, Collector
        with config.flask_app.app_context():
            config.init_mysql()
        config.init_cassandra()
        Consumer.build_and_start()
        collectors_to_resume = Collector.resume_active()
        for c in collectors_to_resume:
            logger.warning('Resuming collector %s', str(c))
            c.launch()

        # Signals handler
        logger.debug('Registering signals for graceful shutdown...')

        def stop_active_collectors(signum, _):
            logger.debug("Received %d", signum)
            logger.debug("Stopping any running collector...")
            for _id, running_collector in Collector.running_instances():
                logger.info("Stopping collector %s", str(_id))
                running_collector.stop(reanimate=True)

            c = Consumer.running_instance()
            if c:
                logger.info("Stopping consumer %s", str(c))
                Consumer.running_instance().stop()

        signal.signal(signal.SIGINT, stop_active_collectors)
        signal.signal(signal.SIGTERM, stop_active_collectors)
        signal.signal(signal.SIGQUIT, stop_active_collectors)
        logger.debug('Registered %d %d and %d', signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)

    connexion_app.add_api(pathlib.Path('smfr.yaml'), base_path=config.base_path)
    return config.flask_app


app = create_app()
