"""
Usage:

python scripts/geocode.py -c 123 -t collected
python scripts/geocode.py -c 450 -t geotagged
python scripts/geocode.py -c 10 -t annotated -d 20180101-20181231
"""

import sys
import datetime

from cassandra.connection import Event
# from cassandra.cqlengine.query import BatchQuery
from cassandra.query import named_tuple_factory, SimpleStatement
# from cassandra.cqlengine.connection import DEFAULT_CONNECTION, _connections

from smfrcore.geocoding.geocoder import Geocoder
from smfrcore.models.cassandra import new_cassandra_session, Tweet
from smfrcore.models.sql import create_app
from smfrcore.utils import ParserHelpOnError


def add_args(parser):
    parser.add_argument('-c', '--collection_id', help='A TwitterCollection id.', type=int, required=True)
    parser.add_argument('-t', '--ttype', help='Which type of stored tweets to export',
                        choices=["annotated", "collected", "geotagged"],
                        metavar='tweet_type', required=True)
    parser.add_argument('-d', '--dates', help='Time window defined as YYYYMMDD-YYYYMMDD')
    parser.add_argument('-s', '--fetch_size', help='Num of rows per page. Can it be tuned for better performances', type=int, default=5000)
    parser.add_argument('-T', '--timeout', help='Timeout for query (in seconds)', type=int, default=240)


class PagedResultHandler:

    def __init__(self, fut, conf):
        self.count = 0
        self.error = None
        self.conf = conf
        self.finished_event = Event()
        self.future = fut
        self.geocoder = Geocoder()
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)

    def handle_page(self, page):
        app = create_app()
        Tweet.session = new_cassandra_session()
        print('------------------------  NEW PAGE ------------------------------------')
        with app.app_context():
            # b = BatchQuery()
            for t in page:
                tweet = Tweet.to_obj(t)
                coordinates, nuts2, nuts_source, country_code, place, geonameid = self.geocoder.find_nuts_heuristic(tweet)
                if not coordinates:
                    continue
                tweet.set_geo(coordinates, nuts2, nuts_source, country_code, place, geonameid)
                # tweet.batch(b).save()
                tweet.save()
            # b.execute()

        self.count += len(page)
        sys.stdout.write('\r')
        sys.stdout.write('                                                                                      ')
        sys.stdout.write('\r')
        sys.stdout.write('Geocoded so far: %d \n\n' % self.count)
        sys.stdout.flush()

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()


def main():
    print('=============> Execution started: ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
    parser = ParserHelpOnError(description='Geocode SMFR tweets')
    add_args(parser)
    conf = parser.parse_args()
    # force output file extension to be coherent with the output format

    session = new_cassandra_session()
    session.row_factory = named_tuple_factory

    query = 'SELECT * FROM smfr_persistent.tweet WHERE collectionid={} AND ttype=\'{}\''.format(conf.collection_id, conf.ttype)

    if conf.dates:
        from_date, to_date = conf.dates.split('-')
        print('Selecting from', from_date, 'to', to_date)
        from_date = datetime.datetime.strptime(from_date, '%Y%m%d')
        to_date = datetime.datetime.strptime(to_date, '%Y%m%d')
        print('Dates: ', from_date, to_date)
        query = '{} AND created_at>=\'{}\' AND created_at<=\'{}\''.format(query, from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

    statement = SimpleStatement(query, fetch_size=conf.fetch_size)
    future = session.execute_async(statement)
    handler = PagedResultHandler(future, conf)
    handler.finished_event.wait()
    if handler.error:
        raise handler.error

    if not handler.count:
        print('<============= Execution ended - no results: ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
        print('Empty queryset. Please, check parameters')
        return 0

    print('<============= Execution ended: ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))
    return 0


if __name__ == '__main__':
    res = main()
    sys.exit(res)
