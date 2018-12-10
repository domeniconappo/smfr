import os
import time

import schedule

from efasproducts import Products, logger


if __name__ == '__main__':
    Products.makedirs()
    scheduling_interval = int(os.getenv('PRODUCTS_SCHEDULING_HOURS', '00,12'.split(',')))
    logger.info('Configuration: running Products: hours %s', scheduling_interval)
    Products.log_config()
    Products.produce()
    for hour in scheduling_interval:
        schedule.every().day.at('{}:30'.format(hour)).do(Products.produce, *(hour,)).tag('products-{}'.format(hour))

    while True:
        schedule.run_pending()
        time.sleep(60)
