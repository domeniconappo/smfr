import argparse
import os
import time

import schedule

from aggregator import aggregate, pretty_running_conf, logger


if __name__ == '__main__':
    scheduling_interval = int(os.getenv('AGGREGATOR_SCHEDULING_MINUTES', 30))

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', action='store_true', default=False,
                        help='If passed, aggregate all collection now')
    parser.add_argument('-b', '--background', action='store_true', default=False,
                        help='If passed, aggregate only background collections now')
    parser.add_argument('-r', '--running', action='store_true', default=False,
                        help='If passed, aggregate running or recently (<6 hours) stopped collections')
    parser.add_argument('-c', '--collections', nargs=argparse.ZERO_OR_MORE, type=int,
                        help='If passed as a list of space separated numbers, '
                             'aggregate on collection represented by these ids.')

    conf = parser.parse_args()
    logger.info('Configuration: %s every %s minutes', pretty_running_conf(conf), scheduling_interval)
    kwargs = {'running_conf': conf}

    # run first job
    aggregate(**kwargs)

    # schedule every X minutes, based on AGGREGATOR_SCHEDULING_MINUTES env variable
    schedule.every(scheduling_interval).minutes.do(aggregate, **kwargs).tag('aggregator-main')

    while True:
        schedule.run_pending()
        time.sleep(45 * scheduling_interval)
