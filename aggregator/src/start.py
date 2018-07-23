import time
import argparse

import schedule

from aggregator import aggregate


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', action='store_true', default=False,
                        help='If passed, aggregate all collection now')
    parser.add_argument('-b', '--background', action='store_true', default=False,
                        help='If passed, aggregate only background collections now')
    parser.add_argument('-r', '--running', action='store_true', default=True,
                        help='If passed, aggregate running or recently (<6 hours) stopped collections')

    conf = parser.parse_args()
    kwargs = {'running_conf': conf}
    schedule.every(30).minutes.do(aggregate, **kwargs).tag('aggregator-main')

    while True:
        schedule.run_pending()
        time.sleep(30)
