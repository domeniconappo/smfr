import json
import logging
import os

from smfrcore.client.api_client import ApiLocalClient
from smfrcore.client.ftp import FTPEfas
from smfrcore.models.sql import Nuts3, Nuts2, create_app, TwitterCollection
from smfrcore.utils import logged_job, job_exceptions_catcher

from efasproducts import logger, Products


def fetch_rra_helper(since='latest'):
    ftp_client = FTPEfas()
    new_downloaded = ftp_client.download_rra(dated=since)
    ftp_client.close()
    if not new_downloaded:
        return [], ''  # empty, not downloaded as it was already existing locally
    events = []
    # RRA file content
    # [{"ID":1414,"SM_meanLT":2.0},{"ID":1436,"SM_meanLT":3.0},{"ID":1673,"SM_meanLT":7.0}]
    if os.path.getsize(ftp_client.localfilepath) > 0:
        with open(ftp_client.localfilepath) as f:
            events = json.load(f)

    return events, ftp_client.rra_date(since)


def events_to_collections_payload(events, date):
    results = {}

    for event in events:
        event_id = event['ID']
        efas_id = int(event_id)
        lead_time = int(event['SM_meanLT'])
        nuts3_data = list(Nuts3.query.with_entities(
            Nuts3.country_name, Nuts3.nuts_id, Nuts3.name_ascii
        ).filter_by(efas_id=efas_id))

        if not nuts3_data:
            logger.debug('No NUTS3 data found for RRA event id %d', efas_id)

        nuts2 = Nuts2.get_by_efas_id(efas_id)
        country_name = nuts3_data[0][0] if nuts3_data else nuts2.country
        nuts_id = nuts2.nuts_id or ''
        if nuts3_data:
            cities = ','.join(c for c in set([c[2] for c in nuts3_data if c and c[2]]) | {nuts2.efas_name})
        else:
            cities = nuts2.efas_name

        results[str(event_id)] = {
            'efas_id': efas_id, 'trigger': 'on-demand', 'efas_name': nuts2.efas_name,
            'nuts': nuts_id, 'country': country_name, 'lead_time': lead_time,
            'keywords': cities, 'bbox': nuts2.bbox, 'forecast': date
        }

    return results


@logged_job
@job_exceptions_catcher
def rra_events_and_products(since='latest', restart_ondemand=True):
    current_app = create_app()
    with current_app.app_context():
        running_collections = TwitterCollection.get_active_ondemand()
        events, date = fetch_rra_helper(since)
        if events:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('FETCHED RRA %s', events)
            results = events_to_collections_payload(events, date)
            collections = TwitterCollection.add_rra_events(results)
            if any(c not in running_collections for c in collections) and restart_ondemand:
                # There is at least one new collection (even the same one with updated keywords)
                # Collector must be restarted
                logger.info(' ============= Adding/Updating on-demand collections from RRA EFAS events:\n\n%s',
                            '\n'.join(str(c) for c in collections))
                restserver_client = ApiLocalClient()
                restserver_client.restart_collector(TwitterCollection.TRIGGER_ONDEMAND)
            # produce and push products only after a new RRA is downloaded
            Products.produce(date)
