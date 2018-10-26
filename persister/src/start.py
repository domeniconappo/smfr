import signal

import schedule
from flask_restful import Resource, fields, marshal_with_field, Api
from smfrcore.models import TwitterCollection
from smfrcore.utils import logged_job

from persister import Persister, logger


app = Persister.app
api = Api(app)


@logged_job
def get_active_collections_for_persister(p):
    with Persister.app.app_context():
        p.set_collections(TwitterCollection.get_running())


if __name__ in ('__main__', 'start'):
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

    class PersisterCounters(Resource):
        """
        API for `/counters` endpoint
        """

        @marshal_with_field(fields.Raw)
        def get(self):
            return persister.counters(), 200

    api.add_resource(PersisterCounters, '/counters')
    schedule.every(30).minutes.do(get_active_collections_for_persister, (persister,)).tag('update-running-collections')
    persister.start_in_background()
