import signal

import schedule
from smfrcore.models import TwitterCollection
from smfrcore.utils import logged_job

from persister import Persister, logger


@logged_job
def get_active_collections_for_persister(p):
    with Persister.app.app_context():
        p.collections = TwitterCollection.get_running()


if __name__ == '__main__':
    def stop_persister(signum, _):
        logger.debug("Received %d", signum)
        logger.debug("Stopping any running collector...")

        running_consumer = Persister.running_instance()
        if running_consumer:
            logger.info("Stopping consumer %s", str(running_consumer))
            Persister.running_instance().stop()

    signal.signal(signal.SIGINT, stop_persister)
    signal.signal(signal.SIGTERM, stop_persister)
    signal.signal(signal.SIGQUIT, stop_persister)
    logger.debug('Registered %d %d and %d', signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)
    persister = Persister()
    schedule.every(30).minutes.do(get_active_collections_for_persister, (persister,)).tag('update-running-collections')
    persister.start()
