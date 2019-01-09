import os
import time

import schedule

from smfrcore.utils import IS_DEVELOPMENT

from efasproducts import Products, logger
from helpers import rra_events_and_products, restserver_client


os.environ['NO_PROXY'] = restserver_client.config.restserver_host

if __name__ == '__main__':
    Products.makedirs()
    scheduling_interval = os.getenv('RRA_FETCH_SCHEDULING', '00,03,06,09,12,15,18,21').split(',')
    logger.info('Configuration: running RRA fetch/products push: hours %s', scheduling_interval)
    Products.log_config()

    if IS_DEVELOPMENT:
        # produce at service startup: useful for debugging
        rra_events_and_products()

    for hour in scheduling_interval:
        schedule.every().day.at('{}:07'.format(hour)).do(rra_events_and_products).tag('products-{}'.format(hour))

    while True:
        schedule.run_pending()
        time.sleep(60)
