import os
import time

import schedule

from efasproducts import Products, logger


if __name__ == '__main__':
    scheduling_interval = int(os.environ.get('PRODUCTS_SCHEDULING_MINUTES', 360))

    logger.info('Configuration: running Products every %s minutes', scheduling_interval)

    # run first job
    Products.produce()

    # schedule every X minutes, based on AGGREGATOR_SCHEDULING_MINUTES env variable
    schedule.every(scheduling_interval).minutes.do(Products.produce).tag('products-main')

    while True:
        schedule.run_pending()
        time.sleep(60)
