import datetime
import logging
import os

import schedule

from smfrcore.models import TwitterCollection, create_app
from smfrcore.utils import logged_job, job_exceptions_catcher, run_continuously

from server.helpers import fetch_rra_helper, events_to_collections_payload
from server.config import RestServerConfiguration, DEVELOPMENT

logger = logging.getLogger(__name__)
logger.setLevel(RestServerConfiguration.logger_level)


@logged_job
@job_exceptions_catcher
def add_rra_events(since='latest'):
    current_app = create_app()
    with current_app.app_context():
        running_collections = TwitterCollection.get_active_ondemand()
        events, date = fetch_rra_helper(since)
        logger.debug('FETCHED RRA %s', events)
        results = events_to_collections_payload(events, date)
        logger.debug('Adding RRA collections %s', results)
        collections = TwitterCollection.add_rra_events(results)
        if any(c not in running_collections for c in collections):
            #  there is at least one new collection (even the same one with updated keywords)
            # Collector must be restarted
            on_demand_collector = RestServerConfiguration().collectors[TwitterCollection.TRIGGER_ONDEMAND]
            on_demand_collector.restart()
            logger.info(' ============= Added/Updated on-demand collections from RRA EFAS events:\n\n%s', '\n'.join(str(c) for c in collections))


@logged_job
@job_exceptions_catcher
def update_ondemand_collections_status():
    current_app = create_app()
    with current_app.app_context():
        updated = TwitterCollection.update_status_by_runtime()
        if updated:
            logger.info(' ============== Some on demand collections were stopped...restarting collector')
            on_demand_collector = RestServerConfiguration().collectors[TwitterCollection.TRIGGER_ONDEMAND]
            on_demand_collector.restart()


def schedule_rra_jobs():

    if not DEVELOPMENT:
        rra_fetch_scheduling = os.environ.get('RRA_FETCH_SCHEDULING', '00:00,12:00').split(',')
        check_ondemand_runtime_scheduling = os.environ.get('CHECK_ONDEMAND_RUNTIME_SCHEDULING', '00:00,12:00').split(',')
    else:
        hours = [datetime.time(i).strftime('%H') for i in range(24)]
        minutes = [str(i) if i > 9 else '0%s' % i for i in range(0, 60, 7)]
        alt_minutes = [str(i) if i > 9 else '0%s' % i for i in range(3, 60, 9)]

        rra_fetch_scheduling = ['%s:%s' % (hour, minute) for hour in hours for minute in minutes]
        check_ondemand_runtime_scheduling = ['%s:%s' % (hour, minute) for hour in hours for minute in alt_minutes]

    kwargs = {'since': 'latest'}
    logger.info('====== Scheduling "Fetch RRA" jobs at {} of every day ======'.format(rra_fetch_scheduling))
    for hour in rra_fetch_scheduling:
        schedule.every().day.at(hour).do(add_rra_events, **kwargs).tag('add-rra-events')

    logger.info('====== Scheduling "Updating On Demand collections" jobs at {} of every day ======'.format(check_ondemand_runtime_scheduling))
    for hour in check_ondemand_runtime_scheduling:
        schedule.every().day.at(hour).do(update_ondemand_collections_status).tag('update-collection-status')

    run_continuously(interval=60 * 3)
