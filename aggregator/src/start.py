import schedule
import time
import argparse

from aggregator import aggregate


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', action='store_true', default=False,
                        help='If passed, aggregate all collection now')
    parser.add_argument('-b', '--background', action='store_true', default=False,
                        help='If passed, aggregate only background collections now')
    conf = parser.parse_args()
    if conf.all or conf.background:
        # aggregation starting now
        aggregate(everything=conf.all, background=conf.background)
    else:
        schedule.every(6).hour.do(aggregate).tag('aggregator-main')

        while True:
            schedule.run_pending()
            time.sleep(10)
