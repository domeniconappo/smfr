import os
import time

import schedule

from efasproducts import Products, logger


if __name__ == '__main__':
    scheduling_interval = int(os.getenv('PRODUCTS_SCHEDULING_MINUTES', 360))
    logger.info('Configuration: running Products every %d minutes', scheduling_interval)
    Products.log_config()

    # run first job
    Products.produce()

    schedule.every(scheduling_interval).minutes.do(Products.produce).tag('products-main')

    while True:
        schedule.run_pending()
        time.sleep(60)
