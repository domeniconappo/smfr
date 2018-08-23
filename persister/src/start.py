import logging
import os
import signal

from smfrcore.utils import LOGGER_FORMAT, LOGGER_DATE_FORMAT

from persister import Persister


logging.basicConfig(format=LOGGER_FORMAT, datefmt=LOGGER_DATE_FORMAT)
logger = logging.getLogger('PERSISTER')
logger.setLevel(os.environ.get('LOGGING_LEVEL', 'DEBUG'))


if __name__ == '__main__':
    def stop_active_collectors(signum, _):
        logger.debug("Received %d", signum)
        logger.debug("Stopping any running collector...")

        running_consumer = Persister.running_instance()
        if running_consumer:
            logger.info("Stopping consumer %s", str(running_consumer))
            Persister.running_instance().stop()

    signal.signal(signal.SIGINT, stop_active_collectors)
    signal.signal(signal.SIGTERM, stop_active_collectors)
    signal.signal(signal.SIGQUIT, stop_active_collectors)
    logger.debug('Registered %d %d and %d', signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)
    Persister().start()
