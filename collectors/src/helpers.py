# import json
# import datetime
# import logging
# import os
#
# from smfrcore.client.ftp import FTPEfas
# from smfrcore.models.sql import TwitterCollection, Nuts3, Nuts2
#
#
# logger = logging.getLogger('RestServer API')
#
#
# def add_collection_helper(**data):
#     collection = TwitterCollection.create(**data)
#     # when adding an on-demand collection we start collecting tweets right away
#     logger.debug('Created collection %s', collection)
#     from server.config import RestServerConfiguration
#     collector = RestServerConfiguration().collectors[collection.trigger]
#     collector.restart()
#     return collection
#
#
# def runtime_from_leadtime(lead_time):
#     """
#
#     :param lead_time: number of days before the peak occurs
#     :return: runtime in format %Y-%m-%d %H:%M
#     """
#     runtime = datetime.datetime.now() + datetime.timedelta(days=int(lead_time) + 2)
#     return runtime.strftime('%Y-%m-%d %H:%M')
#
#
# def fetch_rra_helper(since='latest'):
#     ftp_client = FTPEfas()
#     ftp_client.download_rra(dated=since)
#     ftp_client.close()
#     events = []
#     # RRA file content
#     # [{"ID":1414,"SM_meanLT":2.0},{"ID":1436,"SM_meanLT":3.0},{"ID":1673,"SM_meanLT":7.0}]
#     if os.path.getsize(ftp_client.localfilepath) > 0:
#         with open(ftp_client.localfilepath) as f:
#             events = json.load(f)
#
#     return events, ftp_client.rra_date(since)
#
#
# def events_to_collections_payload(events, date):
#     results = {}
#
#     for event in events:
#         event_id = event['ID']
#         efas_id = int(event_id)
#         lead_time = int(event['SM_meanLT'])
#         nuts3_data = list(Nuts3.query.with_entities(
#             Nuts3.country_name, Nuts3.nuts_id, Nuts3.name_ascii
#         ).filter_by(efas_id=efas_id))
#
#         if not nuts3_data:
#             logger.debug('No NUTS3 data found for RRA event id %d', efas_id)
#
#         nuts2 = Nuts2.get_by_efas_id(efas_id)
#         country_name = nuts3_data[0][0] if nuts3_data else nuts2.country
#         nuts_id = nuts2.nuts_id or ''
#         if nuts3_data:
#             cities = ','.join(c for c in set([c[2] for c in nuts3_data if c and c[2]]) | {nuts2.efas_name})
#         else:
#             cities = nuts2.efas_name
#
#         results[str(event_id)] = {
#             'efas_id': efas_id, 'trigger': 'on-demand', 'efas_name': nuts2.efas_name,
#             'nuts': nuts_id, 'country': country_name, 'lead_time': lead_time,
#             'keywords': cities, 'bbox': nuts2.bbox, 'forecast': date
#         }
#
#     return results
#
#
# def add_collection_from_rra_event(**event):
#     runtime = runtime_from_leadtime(event['lead_time'])
#     data = {'trigger': event['trigger'], 'runtime': runtime,
#             'locations': event['bbox'], 'use_pipeline': True,
#             'timezone': event.get('tzclient', '+00:00'), 'keywords': event['keywords'],
#             'efas_id': event['efas_id'], 'forecast_id': int(event['forecast'])}
#     collection = add_collection_helper(**data)
#     return collection
