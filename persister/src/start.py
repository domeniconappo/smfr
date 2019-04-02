import signal
import os

import schedule
from flask_restful import Resource, fields, marshal_with_field, Api

from smfrcore.models.sql import TwitterCollection, LastCassandraExport
from smfrcore.models.cassandra import Tweet
from smfrcore.models.reverse_tweet_data import import_tweets
from smfrcore.utils import logged_job

from persister import Persister, logger


app = Persister.app
api = Api(app)

background_collection_id = os.getenv('BACKGROUND_COLLECTION_ID')


@logged_job
def get_active_collections_for_persister(p):
    with app.app_context():
        p.set_collections(TwitterCollection.get_running())


@logged_job
def reverse_background_tweets(ttype):
    lce = LastCassandraExport.query.filter_by(collection_id=background_collection_id).first()
    if not lce:
        logger.warning('No data was exported. Run a first export first')
        return
    last_tweet_id = getattr(lce, 'last_tweetid_{}'.format(ttype))
    import_tweets(background_collection_id, ttype, last_tweet_id=last_tweet_id, remove_from_cassandra=True)


if __name__ in ('__main__', 'start'):
    persister = Persister()
    background_process = persister.start_in_background()

    def stop_persister(signum, _):
        logger.debug("Received %d", signum)
        logger.debug("Stopping any running persister/consumer...")

        if persister and background_process:
            logger.info("Stopping consumer %s", str(persister))
            background_process.terminate()
            persister.stop()

    signal.signal(signal.SIGINT, stop_persister)
    signal.signal(signal.SIGTERM, stop_persister)
    signal.signal(signal.SIGQUIT, stop_persister)
    logger.debug('Registered %d %d and %d', signal.SIGINT, signal.SIGTERM, signal.SIGQUIT)

    class PersisterCounters(Resource):
        """
        API for `/counters` endpoint
        """

        @marshal_with_field(fields.Raw)
        def get(self):
            return persister.counters(), 200

    api.add_resource(PersisterCounters, '/counters')

    schedule.every(30).minutes.do(get_active_collections_for_persister, (persister,)).tag('update-running-collections')
    # schedule.every().day.at('02:17').do(reverse_background_tweets, (Tweet.ANNOTATED_TYPE,)).tag('reverse-tweets-ann')
