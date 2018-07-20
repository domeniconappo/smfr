import logging
import os
import signal

from smfrcore.utils import LOGGER_FORMAT, DATE_FORMAT

from persister import Persister


logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', 'DEBUG'), format=LOGGER_FORMAT, datefmt=DATE_FORMAT)


if __name__ == '__main__':

    def stop_active_collectors(signum, _):
        Persister.logger.debug("Received %d", signum)
        Persister.logger.debug("Stopping any _running collector...")

        running_consumer = Persister.running_instance()
        if running_consumer:
            Persister.logger.info("Stopping consumer %s", str(running_consumer))
            Persister.running_instance().stop()

    signal.signal(signal.SIGINT, stop_active_collectors)
    signal.signal(signal.SIGTERM, stop_active_collectors)
    signal.signal(signal.SIGQUIT, stop_active_collectors)
    Persister.logger.debug('Registered %d %d and %d', signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)
    Persister().start()
