"""
How to run a first export first

    from smfrcore.models.reverse_tweet_data import import_tweets
    from smfrcore.models.sql import LastCassandraExport

    collection_id = 10
    import_tweets(collection_id, 'collected', remove_from_cassandra=True)
    import_tweets(collection_id, 'annotated', remove_from_cassandra=True)
    import_tweets(collection_id, 'geotagged', remove_from_cassandra=True)

"""

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from cassandra.query import named_tuple_factory

from smfrcore.models.cassandra import new_cassandra_session
from smfrcore.models.sql import create_app, BackgroundTweet, LastCassandraExport


def import_tweets(collection_id, tweet_type,
                  last_tweet_id=None,
                  remove_from_cassandra=False):

    session = new_cassandra_session()
    session.row_factory = named_tuple_factory

    app = create_app()
    app.app_context().push()
    db_uri = app.config['SQLALCHEMY_DATABASE_URI']
    engine = create_engine(db_uri)
    s = Session(bind=engine)
    new_tweet_id = 0
    if not last_tweet_id:
        stmt = session.prepare(
            'SELECT * FROM smfr_persistent.tweet WHERE collectionid=? AND ttype=?'
        )
        stmt_del = session.prepare(
            'DELETE FROM smfr_persistent.tweet WHERE collectionid=? AND ttype=?'
        )
        parameters = (collection_id, tweet_type)
    else:
        stmt = session.prepare(
            'SELECT * FROM smfr_persistent.tweet WHERE collectionid=? AND ttype=? AND tweet_id>?'
        )
        stmt_del = session.prepare(
            'DELETE FROM smfr_persistent.tweet WHERE collectionid=? AND ttype=? AND tweet_id>?'
        )
        parameters = (collection_id, tweet_type, last_tweet_id)
        new_tweet_id = last_tweet_id

    res = session.execute(stmt, parameters=parameters, timeout=None)
    data = []
    for i, t in enumerate(res, start=1):
        tweet_lat = t.latlong[0] if t.latlong else None
        tweet_lon = t.latlong[1] if t.latlong else None
        bt = BackgroundTweet(id=t.tweet_id, collection_id=t.collectionid, created_at=t.created_at,
                             lang=t.lang, ttype=tweet_type, nuts2=t.nuts2, nuts2_source=t.nuts2source,
                             lat=tweet_lat, lon=tweet_lon, annotations=t.annotations, geo=t.geo, tweet=t.tweet)
        new_tweet_id = max(t.tweet_id, new_tweet_id)
        data.append(bt)
        if not (i % 2000):
            # bulk insert but must be controlled...
            s.bulk_save_objects(data)
            data = []
            s.commit()

    if data:
        # reverse remaining data
        s.bulk_save_objects(data)
        s.commit()

    if remove_from_cassandra:
        session.execute(stmt_del, parameters=parameters)

    # save export data for next run (to use last_tweet_id)
    update_field = 'last_tweetid_{}'.format(tweet_type)
    lce = LastCassandraExport(collection_id=collection_id)
    setattr(lce, update_field, new_tweet_id)
    lce.save()
